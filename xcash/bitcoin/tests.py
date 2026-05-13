from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.contrib.admin.sites import AdminSite
from django.test import SimpleTestCase
from django.test import TestCase
from django.utils import timezone

from bitcoin.adapter import BitcoinAdapter
from bitcoin.admin import BitcoinScanCursorAdmin
from bitcoin.models import BitcoinScanCursor
from bitcoin.rpc import BitcoinRpcError
from bitcoin.scanner.receipt import BitcoinReceiptScanner
from bitcoin.scanner.service import BitcoinChainScannerService
from bitcoin.scanner.service import BitcoinScanSummary
from chains.adapters import TxCheckStatus
from chains.models import Chain
from chains.models import ChainType
from currencies.models import Crypto
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage
from users.models import Customer


class BitcoinRpcClientTests(SimpleTestCase):
    def test_rpc_calls_ignore_proxy_environment(self):
        response = Mock()
        response.json.return_value = {"result": [], "error": None}
        response.is_error = False

        with patch("bitcoin.rpc.httpx.post", return_value=response) as post_mock:
            from bitcoin.rpc import BitcoinRpcClient

            BitcoinRpcClient("http://bitcoin.local").list_wallets()

        self.assertEqual(post_mock.call_args.kwargs["trust_env"], False)

    def test_rpc_retries_network_error_then_succeeds(self):
        # Bitcoin Core 重启/网络瞬断是常态，网络层异常应该被指数退避重试兜住。
        import httpx

        success_response = Mock()
        success_response.json.return_value = {"result": 42, "error": None}
        success_response.is_error = False

        with patch("bitcoin.rpc.httpx.post") as post_mock, patch(
            "bitcoin.rpc.time.sleep"
        ) as sleep_mock:
            post_mock.side_effect = [
                httpx.ConnectError("connection refused"),
                httpx.TimeoutException("read timeout"),
                success_response,
            ]
            from bitcoin.rpc import BitcoinRpcClient

            result = BitcoinRpcClient("http://bitcoin.local")._call("getblockcount")

        self.assertEqual(result, 42)
        self.assertEqual(post_mock.call_count, 3)
        # 指数退避：第 1 次失败后 sleep 1s，第 2 次失败后 sleep 2s；第 3 次直接成功不 sleep。
        self.assertEqual(sleep_mock.call_count, 2)
        self.assertEqual(sleep_mock.call_args_list[0].args, (1.0,))
        self.assertEqual(sleep_mock.call_args_list[1].args, (2.0,))

    def test_rpc_raises_after_exhausting_retries(self):
        # 重试耗尽后必须把失败明确抛出，扫块上层才能标记游标错误并等下一轮。
        import httpx

        from bitcoin.rpc import BitcoinRpcClient
        from bitcoin.rpc import BitcoinRpcError

        with patch("bitcoin.rpc.httpx.post") as post_mock, patch(
            "bitcoin.rpc.time.sleep"
        ):
            post_mock.side_effect = httpx.ConnectError("connection refused")
            with self.assertRaises(BitcoinRpcError):
                BitcoinRpcClient("http://bitcoin.local")._call("getblockcount")

        self.assertEqual(post_mock.call_count, 3)

    def test_rpc_does_not_retry_business_rpc_error(self):
        # JSON error payload 是 RPC 调用真失败（如方法不存在），重试没意义。
        error_response = Mock()
        error_response.json.return_value = {
            "result": None,
            "error": {"code": -1, "message": "Method not found"},
        }
        error_response.is_error = False

        from bitcoin.rpc import BitcoinRpcClient
        from bitcoin.rpc import BitcoinRpcError

        with patch(
            "bitcoin.rpc.httpx.post", return_value=error_response
        ) as post_mock, self.assertRaises(BitcoinRpcError):
            BitcoinRpcClient("http://bitcoin.local")._call("getblockcount")

        self.assertEqual(post_mock.call_count, 1)

    def test_rpc_does_not_retry_invalid_json(self):
        # 响应已到达但 JSON 解析失败属于服务端数据问题，不是网络层瞬断。
        bad_response = Mock()
        bad_response.json.side_effect = ValueError("not json")
        bad_response.is_error = False

        from bitcoin.rpc import BitcoinRpcClient
        from bitcoin.rpc import BitcoinRpcError

        with patch(
            "bitcoin.rpc.httpx.post", return_value=bad_response
        ) as post_mock, self.assertRaises(BitcoinRpcError):
            BitcoinRpcClient("http://bitcoin.local")._call("getblockcount")

        self.assertEqual(post_mock.call_count, 1)


class BitcoinScanCursorAdminTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="Bitcoin Admin Test",
            symbol="BTCA",
            coingecko_id="bitcoin-admin-test",
            decimals=8,
        )
        self.chain = Chain.objects.create(
            code="btc-admin-test",
            name="Bitcoin Admin Test",
            type=ChainType.BITCOIN,
            rpc="http://bitcoin.local",
            native_coin=self.native,
            active=True,
            confirm_block_count=3,
            latest_block_number=40,
        )
        self.cursor = BitcoinScanCursor.objects.create(
            chain=self.chain,
            last_scanned_block=9,
            last_safe_block=6,
            last_error="old error",
            last_error_at=timezone.now(),
        )
        self.admin = BitcoinScanCursorAdmin(BitcoinScanCursor, AdminSite())
        self.admin.message_user = Mock()

    @patch.object(Chain, "get_latest_block_number", new_callable=PropertyMock)
    def test_sync_selected_to_latest_advances_cursor(
        self, get_latest_block_number_mock
    ):
        get_latest_block_number_mock.side_effect = AssertionError(
            "should not fetch realtime block height"
        )
        Chain.objects.filter(pk=self.chain.pk).update(latest_block_number=55)

        self.admin.sync_selected_to_latest(
            request=Mock(),
            queryset=BitcoinScanCursor.objects.filter(pk=self.cursor.pk),
        )

        self.cursor.refresh_from_db()
        self.chain.refresh_from_db()

        self.assertEqual(self.cursor.last_scanned_block, 55)
        self.assertEqual(self.cursor.last_safe_block, 52)
        self.assertEqual(self.cursor.last_error, "")
        self.assertIsNone(self.cursor.last_error_at)
        self.assertEqual(self.chain.latest_block_number, 55)
        self.admin.message_user.assert_called_once()
        self.assertEqual(get_latest_block_number_mock.call_count, 0)


class BitcoinScannerTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="Bitcoin Test",
            symbol="BTCT",
            coingecko_id="bitcoin-test",
            decimals=8,
        )
        self.chain = Chain.objects.create(
            code="btc-test",
            name="Bitcoin Test",
            type=ChainType.BITCOIN,
            rpc="http://bitcoin.local",
            native_coin=self.native,
            active=True,
            confirm_block_count=1,
        )

    @patch("projects.models.RecipientAddress.objects")
    def test_watch_set_only_loads_recipient_addresses(self, recipient_qs_mock):
        # Bitcoin 扫描器只关注项目收款地址（Invoice 支付地址），不包含充币地址。
        from bitcoin.scanner.watchers import load_watch_set

        recipient_qs_mock.filter.return_value.values_list.return_value = [
            "bc1qexample",
        ]

        watched = load_watch_set()

        self.assertIn("bc1qexample", watched)
        recipient_qs_mock.filter.assert_called_once_with(
            chain_type=ChainType.BITCOIN,
            usage=RecipientAddressUsage.INVOICE,
        )

    def test_chain_scanner_service_wraps_receipt_scan_result(self):
        # 链级入口只负责编排 Bitcoin 收款扫描，并把结果折叠成统一摘要对象。
        with patch(
            "bitcoin.scanner.service.BitcoinReceiptScanner.scan_recent_receipts",
            return_value=3,
        ) as scan_mock:
            summary = BitcoinChainScannerService.scan_chain(chain=self.chain)

        self.assertEqual(summary, BitcoinScanSummary(created_receipts=3))
        scan_mock.assert_called_once_with(self.chain)

    def test_chain_scanner_service_rejects_non_bitcoin_chain(self):
        # 编排入口必须拒绝错误链类型，避免任务层把非 BTC 链误送进 UTXO 扫描逻辑。
        evm_chain = Chain(
            code="eth-test",
            name="Ethereum Test",
            type=ChainType.EVM,
            native_coin=self.native,
        )

        with self.assertRaisesMessage(ValueError, "仅支持扫描 Bitcoin 链"):
            BitcoinChainScannerService.scan_chain(chain=evm_chain)

    @patch("bitcoin.adapter.BitcoinRpcClient")
    def test_tx_result_falls_back_to_raw_transaction_when_wallet_not_loaded(
        self,
        bitcoin_client_cls,
    ):
        client = bitcoin_client_cls.return_value
        client.get_transaction.side_effect = BitcoinRpcError(
            "Bitcoin RPC error (gettransaction): Requested wallet does not exist or is not loaded"
        )
        client.get_raw_transaction.return_value = {
            "txid": "abc",
            "confirmations": 2,
        }

        result = BitcoinAdapter.tx_result(self.chain, "abc")

        self.assertEqual(result, TxCheckStatus.CONFIRMED)
        client.get_transaction.assert_called_once_with("abc")
        client.get_raw_transaction.assert_called_once_with("abc")

    def test_compute_scan_window_bootstraps_from_recent_blocks_for_new_cursor(self):
        # 首次创建游标时必须优先覆盖最近区块，否则现网已运行链的首轮扫描会追不到最新入账。
        cursor = BitcoinScanCursor(chain=self.chain)

        from_block, to_block = BitcoinReceiptScanner._compute_scan_window(
            cursor=cursor,
            latest_height=500,
            confirm_block_count=1,
            batch_size=BitcoinReceiptScanner.SCAN_BATCH_SIZE,
        )

        self.assertEqual(from_block, 357)
        self.assertEqual(to_block, 500)

    @patch("bitcoin.scanner.receipt.TransferService.create_observed_transfer")
    @patch("bitcoin.scanner.receipt.BitcoinReceiptScanner._resolve_sender_address")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_hash")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_count")
    @patch("bitcoin.scanner.receipt.load_watch_set")
    def test_scan_recent_receipts_advances_persistent_cursor(
        self,
        load_watch_set_mock,
        get_block_count_mock,
        get_block_hash_mock,
        get_block_mock,
        resolve_sender_address_mock,
        create_observed_transfer_mock,
    ):
        # BTC 扫描必须把推进位置落库，避免长时间停机后只能靠最近窗口猜测补扫。
        watched_address = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
        load_watch_set_mock.return_value = frozenset({watched_address})
        get_block_count_mock.return_value = 5
        get_block_hash_mock.side_effect = lambda height: f"block-{height}"
        get_block_mock.side_effect = lambda block_hash: {
            "height": int(block_hash.split("-")[1]),
            "time": 1_700_000_000,
            "tx": [
                {
                    "txid": "ab" * 32,
                    "blocktime": 1_700_000_000,
                    "vout": [
                        {
                            "n": 0,
                            "value": "0.01",
                            "scriptPubKey": {"address": watched_address},
                        }
                    ],
                }
            ],
        }
        resolve_sender_address_mock.return_value = (
            "1ExternalSenderAddress1111111111114T1an2"
        )
        create_observed_transfer_mock.return_value = SimpleNamespace(created=True)

        created_count = BitcoinReceiptScanner.scan_recent_receipts(self.chain)

        cursor = BitcoinScanCursor.objects.get(chain=self.chain)
        self.assertEqual(created_count, 6)
        self.assertEqual(cursor.last_scanned_block, 5)
        self.assertEqual(cursor.last_safe_block, 4)
        self.chain.refresh_from_db()
        self.assertEqual(self.chain.latest_block_number, 5)

    @patch("bitcoin.scanner.receipt.TransferService.create_observed_transfer")
    @patch("bitcoin.scanner.receipt.BitcoinReceiptScanner._resolve_sender_address")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_hash")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_count")
    @patch("bitcoin.scanner.receipt.load_watch_set")
    def test_scan_recent_receipts_rewinds_tail_window_idempotently(
        self,
        load_watch_set_mock,
        get_block_count_mock,
        get_block_hash_mock,
        get_block_mock,
        resolve_sender_address_mock,
        create_observed_transfer_mock,
    ):
        # 主游标推进后仍要回退一小段尾部重扫，以覆盖轻微重组，同时不能重复建单。
        watched_address = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
        load_watch_set_mock.return_value = frozenset({watched_address})
        get_block_count_mock.return_value = 30
        get_block_hash_mock.side_effect = lambda height: f"block-{height}"
        get_block_mock.side_effect = lambda block_hash: {
            "height": int(block_hash.split("-")[1]),
            "time": 1_700_000_000,
            "tx": [
                {
                    "txid": "cd" * 32,
                    "blocktime": 1_700_000_000,
                    "vout": [
                        {
                            "n": 0,
                            "value": "0.01",
                            "scriptPubKey": {"address": watched_address},
                        }
                    ],
                }
            ],
        }
        resolve_sender_address_mock.return_value = (
            "1ExternalSenderAddress1111111111114T1an2"
        )
        create_observed_transfer_mock.side_effect = [
            SimpleNamespace(created=True),
            *[SimpleNamespace(created=False) for _ in range(60)],
        ]

        first = BitcoinReceiptScanner.scan_recent_receipts(self.chain)
        second = BitcoinReceiptScanner.scan_recent_receipts(self.chain)

        cursor = BitcoinScanCursor.objects.get(chain=self.chain)
        self.assertGreater(first, 0)
        self.assertEqual(second, 0)
        self.assertEqual(cursor.last_scanned_block, 30)

    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_count")
    def test_scan_recent_receipts_records_cursor_error_when_rpc_fails(
        self,
        get_block_count_mock,
    ):
        # RPC 失败后必须把错误写回游标，方便后台与运维定位扫描停滞原因。
        get_block_count_mock.side_effect = BitcoinRpcError("rpc timeout")

        with self.assertRaises(BitcoinRpcError):
            BitcoinReceiptScanner.scan_recent_receipts(self.chain)

        cursor = BitcoinScanCursor.objects.get(chain=self.chain)
        self.assertEqual(cursor.last_scanned_block, 0)
        self.assertEqual(cursor.last_error, "rpc timeout")
        self.assertIsNotNone(cursor.last_error_at)

    @patch("bitcoin.scanner.receipt.load_watch_set")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_count")
    def test_scan_recent_receipts_skips_disabled_cursor(
        self,
        get_block_count_mock,
        load_watch_set_mock,
    ):
        # 后台禁用扫描游标后，任务应立即停扫且不再触发任何节点请求。
        BitcoinScanCursor.objects.create(
            chain=self.chain,
            last_scanned_block=12,
            last_safe_block=10,
            enabled=False,
        )
        get_block_count_mock.return_value = 99
        load_watch_set_mock.return_value = frozenset()

        created_count = BitcoinReceiptScanner.scan_recent_receipts(self.chain)

        self.assertEqual(created_count, 0)
        get_block_count_mock.assert_not_called()
        load_watch_set_mock.assert_not_called()
        cursor = BitcoinScanCursor.objects.get(chain=self.chain)
        self.assertEqual(cursor.last_scanned_block, 12)
        self.assertEqual(cursor.last_safe_block, 10)

    def test_advance_cursor_never_rewinds_database_value(self):
        # 慢扫描结束时持有的是入口处的内存 cursor 快照；若期间另一进程已把游标推得更远，
        # 用 Python 端 max(snapshot, scanned_to_block) 会把 DB 已推进的值写回旧位，
        # 触发整段重扫与 webhook 重发。Greatest(F(...)) 由 DB 评估保证单调向前。
        cursor = BitcoinScanCursor.objects.create(
            chain=self.chain,
            last_scanned_block=100,
            last_safe_block=99,
            enabled=True,
        )
        stale_cursor = BitcoinScanCursor.objects.get(pk=cursor.pk)
        BitcoinScanCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=150,
            last_safe_block=149,
        )

        BitcoinReceiptScanner._advance_cursor(
            cursor=stale_cursor,
            latest_height=120,
            scanned_to_block=120,
        )

        cursor.refresh_from_db()
        self.assertEqual(cursor.last_scanned_block, 150)
        self.assertEqual(cursor.last_safe_block, 149)

    def test_mark_cursor_idle_never_rewinds_last_safe_block(self):
        # Idle 路径同样不能用更小的 latest_height - confirm_block_count 覆盖已推进的 last_safe_block。
        cursor = BitcoinScanCursor.objects.create(
            chain=self.chain,
            last_scanned_block=200,
            last_safe_block=199,
            enabled=True,
        )
        BitcoinScanCursor.objects.filter(pk=cursor.pk).update(last_safe_block=199)

        BitcoinReceiptScanner._mark_cursor_idle(
            cursor=cursor,
            latest_height=150,
        )

        cursor.refresh_from_db()
        self.assertEqual(cursor.last_safe_block, 199)

    @patch("bitcoin.scanner.receipt.BitcoinReceiptScanner._resolve_sender_address")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_hash")
    @patch("bitcoin.scanner.receipt.BitcoinRpcClient.get_block_count")
    @patch("bitcoin.scanner.receipt.load_watch_set")
    def test_scan_never_rewinds_chain_latest_block_number(
        self,
        load_watch_set_mock,
        get_block_count_mock,
        get_block_hash_mock,
        get_block_mock,
        resolve_sender_address_mock,
    ):
        # 并发 scanner 拿到不同的 RPC 快照时，Chain.latest_block_number 必须单调向前。
        Chain.objects.filter(pk=self.chain.pk).update(latest_block_number=200)
        load_watch_set_mock.return_value = frozenset()
        get_block_count_mock.return_value = 150

        BitcoinReceiptScanner.scan_recent_receipts(self.chain)

        self.chain.refresh_from_db()
        self.assertEqual(self.chain.latest_block_number, 200)
        get_block_hash_mock.assert_not_called()
        get_block_mock.assert_not_called()
        resolve_sender_address_mock.assert_not_called()

    def test_should_track_output_filters_self_sends(self):
        # 砍掉充提后，只需过滤自发自收；sender 为空串时自动放行。
        self.assertFalse(
            BitcoinReceiptScanner._should_track_output(
                sender_address="same-address",
                recipient_address="same-address",
            )
        )
        self.assertTrue(
            BitcoinReceiptScanner._should_track_output(
                sender_address="external-address",
                recipient_address="project-recipient",
            )
        )
        self.assertTrue(
            BitcoinReceiptScanner._should_track_output(
                sender_address="",
                recipient_address="project-recipient",
            )
        )


class BitcoinTaskTests(TestCase):
    @patch("bitcoin.tasks.BitcoinChainScannerService.scan_chain")
    def test_scan_bitcoin_receipts_only_scans_active_bitcoin_chains(
        self, scan_chain_mock
    ):
        # 定时任务层只能挑启用中的 BTC 链，具体扫描细节统一下沉到链级 service。
        from bitcoin.tasks import scan_bitcoin_receipts

        native = Crypto.objects.create(
            name="Bitcoin Task",
            symbol="BTCQ",
            coingecko_id="bitcoin-task",
            decimals=8,
        )
        bitcoin_chain = Chain.objects.create(
            code="btc-active",
            name="Bitcoin Active",
            type=ChainType.BITCOIN,
            rpc="http://bitcoin.active",
            native_coin=native,
            active=True,
        )
        Chain.objects.create(
            code="btc-inactive",
            name="Bitcoin Inactive",
            type=ChainType.BITCOIN,
            rpc="http://bitcoin.inactive",
            native_coin=native,
            active=False,
        )
        Chain.objects.create(
            code="eth-active",
            name="Ethereum Active",
            type=ChainType.EVM,
            chain_id=1,
            rpc="http://eth.active",
            native_coin=native,
            active=True,
        )
        scan_chain_mock.return_value = BitcoinScanSummary(created_receipts=0)

        scan_bitcoin_receipts.run()

        scan_chain_mock.assert_called_once_with(chain=bitcoin_chain)


class BitcoinWatchSyncTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="Bitcoin Watch Sync",
            symbol="BTCSYNC",
            coingecko_id="bitcoin-watch-sync",
            decimals=8,
        )
        self.chain = Chain.objects.create(
            code="btc-sync",
            name="Bitcoin Sync",
            type=ChainType.BITCOIN,
            rpc="http://bitcoin.sync/wallet/xcash",
            native_coin=self.native,
            active=True,
            confirm_block_count=1,
        )
        with patch("chains.signer.RemoteSignerBackend.create_wallet", return_value=1):
            self.project = Project.objects.create(
                name="btc-sync-project",
            )
        self.customer = Customer.objects.create(project=self.project, uid="btc-sync-user")

    @patch("common.fields.AddressField.pre_save", return_value="1BoatSLRHtKNngkdXEeobR76b53LETtpyT")
    @patch("bitcoin.tasks.sync_bitcoin_watch_addresses.apply_async")
    def test_saving_bitcoin_recipient_address_schedules_watch_sync(
        self,
        apply_async_mock,
        _addr_field_mock,
    ):
        with self.captureOnCommitCallbacks(execute=True):
            RecipientAddress.objects.create(
                name="btc-sync-recipient",
                project=self.project,
                chain_type=ChainType.BITCOIN,
                address="1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
                usage=RecipientAddressUsage.INVOICE,
            )

        apply_async_mock.assert_called_once()

    @patch("common.fields.AddressField.pre_save", return_value="1BoatSLRHtKNngkdXEeobR76b53LETtpyT")
    @patch("bitcoin.watch_sync.BitcoinRpcClient.import_descriptor")
    @patch("bitcoin.watch_sync.BitcoinRpcClient.import_address")
    def test_sync_chain_imports_recipient_addresses_with_descriptor_fallback(
        self,
        import_address_mock,
        import_descriptor_mock,
        _addr_field_mock,
    ):
        test_address = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
        RecipientAddress.objects.create(
            name="btc-sync-import-recipient",
            project=self.project,
            chain_type=ChainType.BITCOIN,
            address=test_address,
            usage=RecipientAddressUsage.INVOICE,
        )

        import_address_mock.side_effect = BitcoinRpcError(
            "Only legacy wallets are supported by this command"
        )

        from bitcoin.watch_sync import BitcoinWatchSyncService

        imported_count = BitcoinWatchSyncService.sync_chain(self.chain)

        self.assertEqual(imported_count, 1)
        import_address_mock.assert_called_once()
        import_descriptor_mock.assert_called_once()
        descriptor = import_descriptor_mock.call_args.kwargs["descriptor"]
        self.assertEqual(descriptor, f"addr({test_address})")
