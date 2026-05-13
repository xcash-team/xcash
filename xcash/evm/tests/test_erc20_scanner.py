from decimal import Decimal
from unittest.mock import patch

from django.core.cache import cache
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import Wallet
from core.models import PLATFORM_SETTINGS_CACHE_KEY
from core.models import PlatformSettings
from currencies.models import ChainToken
from currencies.models import Crypto
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.erc20 import EvmErc20TransferScanner
from evm.scanner.native import EvmNativeDirectScanner
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcError
from evm.scanner.watchers import load_watch_set
from evm.tasks import scan_active_evm_erc20_chains
from evm.tasks import scan_active_evm_native_chains
from evm.tasks import scan_evm_chain
from evm.tasks import scan_evm_erc20_chain
from evm.tasks import scan_evm_native_chain
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage


class EvmErc20ScanWindowTests(SimpleTestCase):
    def test_erc20_compute_scan_window_initial_cursor_starts_from_first_batch(self):
        cursor = EvmScanCursor(last_scanned_block=0)
        from_block, to_block = EvmErc20TransferScanner._compute_scan_window(
            cursor=cursor,
            latest_block=2000,
            batch_size=100,
        )

        self.assertEqual(from_block, 1)
        self.assertEqual(to_block, 100)

    def test_erc20_compute_scan_window_batch_size_is_net_forward_progress(self):
        cursor = EvmScanCursor(last_scanned_block=1000)
        from_block, to_block = EvmErc20TransferScanner._compute_scan_window(
            cursor=cursor,
            latest_block=2000,
            batch_size=100,
        )

        self.assertEqual(from_block, 999)
        self.assertEqual(to_block, 1100)

    def test_erc20_compute_scan_window_caps_to_latest_when_near_chain_head(self):
        cursor = EvmScanCursor(last_scanned_block=1990)
        from_block, to_block = EvmErc20TransferScanner._compute_scan_window(
            cursor=cursor,
            latest_block=2000,
            batch_size=100,
        )

        self.assertEqual(from_block, 1989)
        self.assertEqual(to_block, 2000)

    def test_erc20_compute_scan_window_returns_empty_when_latest_block_is_zero(self):
        cursor = EvmScanCursor(last_scanned_block=0)
        from_block, to_block = EvmErc20TransferScanner._compute_scan_window(
            cursor=cursor,
            latest_block=0,
            batch_size=100,
        )

        self.assertGreater(from_block, to_block)


class EvmNativeScanWindowTests(SimpleTestCase):
    def test_native_compute_scan_window_initial_cursor_starts_from_first_batch(self):
        cursor = EvmScanCursor(last_scanned_block=0)
        from_block, to_block = EvmNativeDirectScanner._compute_scan_window(
            cursor=cursor,
            latest_block=2000,
            batch_size=16,
        )

        self.assertEqual(from_block, 1)
        self.assertEqual(to_block, 16)

    def test_native_compute_scan_window_batch_size_is_net_forward_progress(self):
        # batch_size 表示本轮应向前追多少新块；replay_blocks 只增加旧块复扫范围，
        # 不应吞掉净推进量，否则高确认数链会出现 last_scanned_block 缓慢推进但永远追不上。
        cursor = EvmScanCursor(last_scanned_block=1000)
        from_block, to_block = EvmNativeDirectScanner._compute_scan_window(
            cursor=cursor,
            latest_block=2000,
            batch_size=16,
        )

        self.assertEqual(from_block, 999)
        self.assertEqual(to_block, 1016)

    def test_native_compute_scan_window_caps_to_latest_when_near_chain_head(self):
        cursor = EvmScanCursor(last_scanned_block=1990)
        from_block, to_block = EvmNativeDirectScanner._compute_scan_window(
            cursor=cursor,
            latest_block=2000,
            batch_size=16,
        )

        self.assertEqual(from_block, 1989)
        self.assertEqual(to_block, 2000)

    def test_native_compute_scan_window_must_still_progress_when_far_behind(self):
        cursor = EvmScanCursor(last_scanned_block=10_516_050)
        from_block, to_block = EvmNativeDirectScanner._compute_scan_window(
            cursor=cursor,
            latest_block=10_516_343,
            batch_size=12,
        )

        self.assertEqual(from_block, 10_516_049)
        self.assertEqual(to_block, 10_516_062)


@override_settings(DEBUG=False)
class EvmErc20ScannerTests(TestCase):
    def setUp(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        self.native = Crypto.objects.create(
            name="Scanner BNB",
            symbol="BNB-SCANNER",
            coingecko_id="binancecoin-scanner",
        )
        self.chain = Chain.objects.create(
            code="bsc",
            name="BSC",
            type=ChainType.EVM,
            chain_id=56,
            rpc="http://bsc.local",
            native_coin=self.native,
            confirm_block_count=6,
            active=True,
        )
        self.token = Crypto.objects.create(
            name="Scanner Tether USD",
            symbol="USDT-SCANNER",
            coingecko_id="tether-scanner",
            decimals=18,
        )
        self.token_deployment = ChainToken.objects.create(
            crypto=self.token,
            chain=self.chain,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000aa"
            ),
            decimals=18,
        )
        self.wallet = Wallet.objects.create()
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000bb"
            ),
        )

    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        super().tearDown()

    @staticmethod
    def _address_topic(address: str) -> str:
        normalized = Web3.to_checksum_address(address)
        return "0x" + "0" * 24 + normalized[2:].lower()

    def _build_transfer_log(
        self,
        *,
        from_address: str,
        to_address: str,
        log_index: int = 5,
        value: int = 10**18,
        block_number: int = 100,
    ) -> dict:
        return {
            "address": self.token_deployment.address,
            "topics": [
                Web3.keccak(text="Transfer(address,address,uint256)"),
                self._address_topic(from_address),
                self._address_topic(to_address),
            ],
            "data": hex(value),
            "blockNumber": block_number,
            "logIndex": log_index,
            "transactionHash": bytes.fromhex("ab" * 32),
        }

    def _build_native_block(
        self,
        *,
        txs: list[dict],
        timestamp: int = 1_700_000_123,
    ) -> dict:
        return {
            "number": 20,
            "timestamp": timestamp,
            "transactions": txs,
        }

    @staticmethod
    def _build_native_tx(
        *,
        from_address: str,
        to_address: str,
        value: int,
        tx_hash_hex: str,
        input_data: str = "0x",
    ) -> dict:
        return {
            "hash": bytes.fromhex(tx_hash_hex * 32),
            "from": from_address,
            "to": to_address,
            "value": value,
            "input": input_data,
        }

    def _create_scan_dispatch_ignored_chains(self) -> None:
        Chain.objects.create(
            code="bsc-inactive",
            name="BSC Inactive",
            type=ChainType.EVM,
            chain_id=57,
            rpc="http://inactive-bsc.local",
            native_coin=self.native,
            active=False,
        )
        Chain.objects.create(
            code="tron-active",
            name="Tron Active",
            type=ChainType.TRON,
            native_coin=self.native,
            active=True,
        )

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_erc20_first_scan_without_cursor_starts_from_latest_tail_window(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        _get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 首次创建游标时不应从创世块补扫；应直接对齐到链头附近，仅覆盖近端重扫窗口。
        get_latest_block_number_mock.return_value = 100
        get_transfer_logs_mock.return_value = []

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        # erc20 replay_blocks = 2, from_block = 100 + 1 - 2 = 99
        self.assertEqual(result.from_block, 99)
        self.assertEqual(result.to_block, 100)
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_creates_transfer_and_advances_cursor(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 命中的 ERC20 OnchainTransfer 应落到统一 OnchainTransfer 表；首扫会直接对齐链头附近窗口。
        get_latest_block_number_mock.return_value = 100
        get_block_timestamp_mock.return_value = 1_700_000_000
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=self.addr.address,
            )
        ]

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        transfer = OnchainTransfer.objects.get()
        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )

        self.assertEqual(result.created_transfers, 1)
        self.assertEqual(result.observed_logs, 1)
        self.assertEqual(transfer.event_id, "erc20:5")
        self.assertEqual(transfer.hash, "0x" + "ab" * 32)
        self.assertEqual(
            transfer.to_address, Web3.to_checksum_address(self.addr.address)
        )
        self.assertEqual(transfer.amount, Decimal("1"))
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_rewind_window_keeps_transfer_idempotent(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 近端重扫会重复看到同一日志，但统一唯一键必须保证不会重复落库。
        get_latest_block_number_mock.return_value = 100
        get_block_timestamp_mock.return_value = 1_700_000_000
        repeated_log = self._build_transfer_log(
            from_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000cc"
            ),
            to_address=self.addr.address,
            block_number=100,
        )
        get_transfer_logs_mock.return_value = [repeated_log]

        first = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)
        second = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(first.created_transfers, 1)
        self.assertEqual(second.created_transfers, 0)
        self.assertEqual(OnchainTransfer.objects.count(), 1)
        self.assertEqual(cursor.last_scanned_block, 100)

    @override_settings(DEBUG=True)
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_debug_mode_bootstraps_cursor_once_from_latest_block_per_process(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
    ):
        # 本地 DEBUG 开发模式下，首次扫描应直接把历史游标提升到当前链头；
        # 但同一进程后续轮询不能重复执行这次"启动对齐"，否则会不断抹平正常增量进度。
        EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            last_scanned_block=12,
            last_safe_block=6,
            enabled=True,
        )
        get_latest_block_number_mock.side_effect = [100, 110]
        get_transfer_logs_mock.return_value = []

        first = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)
        second = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        # erc20 replay_blocks = 2, 第一轮 bootstrap 到 100: from = 100+1-2 = 99
        self.assertEqual(first.from_block, 99)
        self.assertEqual(first.to_block, 100)
        # 第二轮: last_scanned=100, from = 100+1-2 = 99
        self.assertEqual(second.from_block, 99)
        self.assertEqual(second.to_block, 110)
        self.assertEqual(cursor.last_scanned_block, 110)

    @patch(
        "currencies.models.Crypto.get_decimals",
        side_effect=AssertionError("scanner should use prefetched token decimals"),
    )
    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_uses_chain_token_decimals_without_extra_lookup(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
        _crypto_get_decimals_mock,
    ):
        # ERC20 扫描已持有 ChainToken 行数据，应直接复用链特定精度，避免逐条日志额外查库。
        self.token_deployment.decimals = 6
        self.token_deployment.save(update_fields=["decimals"])
        get_latest_block_number_mock.return_value = 100
        get_block_timestamp_mock.return_value = 1_700_000_000
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=self.addr.address,
                value=10**6,
            )
        ]

        EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        transfer = OnchainTransfer.objects.get()
        self.assertEqual(transfer.amount, Decimal("1"))

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_ignores_logs_outside_watch_set(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        create_observed_transfer_mock,
    ):
        # 非系统地址相关的日志必须在扫描层被过滤，避免把全链事件都送进业务入口。
        get_latest_block_number_mock.return_value = 40
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000dd"
                ),
                block_number=40,
            )
        ]

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=40)

        self.assertEqual(result.created_transfers, 0)
        self.assertEqual(result.observed_logs, 1)
        create_observed_transfer_mock.assert_not_called()
        self.assertEqual(OnchainTransfer.objects.count(), 0)

    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_uses_prefixed_transfer_topic_for_rpc_logs(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
    ):
        # 部分 RPC（如 NodeReal）要求日志 topic 必须是 0x 前缀 hex；少前缀会直接报 -32602。
        get_latest_block_number_mock.return_value = 100
        get_transfer_logs_mock.return_value = []

        EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        _, kwargs = get_transfer_logs_mock.call_args
        self.assertEqual(
            kwargs["topic0"],
            Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)")),
        )

    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_advances_cursor_when_no_tokens_configured(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
    ):
        # 当链上尚未配置任何 ERC20 合约时，不应长期显示积压；游标可直接追到当前链头。
        self.token_deployment.delete()
        get_latest_block_number_mock.return_value = 100

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(result.created_transfers, 0)
        self.assertEqual(result.observed_logs, 0)
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)
        get_transfer_logs_mock.assert_not_called()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch("evm.tasks.EvmChainScannerService.scan_chain")
    def test_scan_evm_chain_task_dispatches_chain_scanner(
        self,
        scan_chain_mock,
        reconcile_chain_mock,
    ):
        # Celery 入口应只负责链级调度，不再混入具体日志解析逻辑。
        scan_evm_chain(self.chain.pk)

        scan_chain_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch(
        "evm.tasks.EvmChainScannerService.scan_chain",
        side_effect=EvmScannerRpcError("rpc timeout"),
    )
    def test_scan_evm_chain_runs_coordinator_when_scanner_rpc_fails(
        self,
        scan_chain_mock,
        reconcile_chain_mock,
    ):
        # 主扫描 RPC 异常不能阻断内部 PENDING_CHAIN 任务的超时收口。
        scan_evm_chain(self.chain.pk)

        scan_chain_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch("evm.tasks.EvmChainScannerService.scan_erc20")
    def test_scan_evm_erc20_chain_task_dispatches_erc20_scanner(
        self,
        scan_erc20_mock,
        reconcile_chain_mock,
    ):
        # ERC20 扫描应有独立 Celery 入口，避免被 native full-block 扫描周期绑死。
        scan_erc20_mock.return_value = EvmErc20ScanResult(
            from_block=1,
            to_block=2,
            latest_block=100,
            observed_logs=3,
            created_transfers=1,
        )

        scan_evm_erc20_chain(self.chain.pk)

        scan_erc20_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch(
        "evm.tasks.EvmChainScannerService.scan_erc20",
        side_effect=EvmScannerRpcError("erc20 rpc timeout"),
    )
    def test_scan_evm_erc20_chain_runs_coordinator_when_rpc_fails(
        self,
        scan_erc20_mock,
        reconcile_chain_mock,
    ):
        scan_evm_erc20_chain(self.chain.pk)

        scan_erc20_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch("evm.tasks.EvmChainScannerService.scan_native")
    def test_scan_evm_native_chain_task_dispatches_native_scanner(
        self,
        scan_native_mock,
        reconcile_chain_mock,
    ):
        # native 扫描应有独立 Celery 入口，便于使用低于 ERC20 的扫描频率。
        scan_native_mock.return_value = EvmNativeScanResult(
            from_block=1,
            to_block=2,
            latest_block=100,
            observed_transfers=1,
            created_transfers=1,
        )

        scan_evm_native_chain(self.chain.pk)

        scan_native_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch(
        "evm.tasks.EvmChainScannerService.scan_native",
        side_effect=EvmScannerRpcError("native rpc timeout"),
    )
    def test_scan_evm_native_chain_runs_coordinator_when_rpc_fails(
        self,
        scan_native_mock,
        reconcile_chain_mock,
    ):
        scan_evm_native_chain(self.chain.pk)

        scan_native_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    @patch("evm.tasks.scan_evm_erc20_chain.delay")
    def test_scan_active_evm_erc20_chains_dispatches_erc20_task(
        self,
        delay_mock,
    ):
        self._create_scan_dispatch_ignored_chains()

        scan_active_evm_erc20_chains()

        delay_mock.assert_called_once_with(self.chain.pk)

    @patch("evm.tasks.scan_evm_native_chain.delay")
    def test_scan_active_evm_native_chains_skips_when_global_native_scanner_closed(
        self,
        delay_mock,
    ):
        self._create_scan_dispatch_ignored_chains()

        scan_active_evm_native_chains()

        delay_mock.assert_not_called()

    @patch("evm.tasks.scan_evm_native_chain.delay")
    def test_scan_active_evm_native_chains_dispatches_native_task(
        self,
        delay_mock,
    ):
        PlatformSettings.objects.create(open_native_scanner=True)
        self._create_scan_dispatch_ignored_chains()

        scan_active_evm_native_chains()

        delay_mock.assert_called_once_with(self.chain.pk)

    @patch("evm.tasks.scan_evm_native_chain.delay")
    def test_scan_active_evm_native_chains_skips_closed_native_scanner(
        self,
        delay_mock,
    ):
        self._create_scan_dispatch_ignored_chains()

        scan_active_evm_native_chains()

        delay_mock.assert_not_called()

    def test_watch_set_includes_recipient_addresses(self):
        # 收币地址同样属于系统观察集，后续 ERC20 扫描需要能命中这些地址。
        RecipientAddress.objects.create(
            name="project-recipient",
            project_id=self._create_project_id(),
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000dd"
            ),
            usage=RecipientAddressUsage.INVOICE,
        )

        watch_set = load_watch_set(chain=self.chain)

        self.assertIn(
            Web3.to_checksum_address("0x00000000000000000000000000000000000000dD"),
            watch_set.watched_addresses,
        )

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch(
        "evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status",
        return_value=None,
    )
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_first_scan_without_cursor_starts_from_latest_tail_window(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        _get_receipt_status_mock,
        _get_block_receipts_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 原生币首扫若系统中没有游标，也应只覆盖链头附近窗口，不能从 1 开始全量爬。
        get_latest_block_number_mock.return_value = 20
        get_full_block_mock.side_effect = (
            lambda *, block_number: self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        # native replay_blocks = 2, from_block = 20 + 1 - 2 = 19
        self.assertEqual(result.from_block, 19)
        self.assertEqual(result.to_block, 20)
        self.assertEqual(cursor.last_scanned_block, 20)
        self.assertEqual(cursor.last_safe_block, 14)

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch(
        "evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status",
        return_value=None,
    )
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_creates_transfer_for_direct_value_transfer(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        _get_block_receipts_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 顶层 input=0x 的 value transfer 若命中系统地址，应按 native:tx 统一落库。
        # 首扫窗口直接对齐链头附近，因此命中交易也应位于最新尾部区间内。
        get_latest_block_number_mock.return_value = 20
        get_receipt_status_mock.return_value = 1
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=self.addr.address,
                        value=10**18,
                        tx_hash_hex="cd",
                    )
                ]
            )
            if block_number == 20
            else self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        transfer = OnchainTransfer.objects.get(event_id="native:tx")
        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )

        self.assertEqual(result.observed_transfers, 1)
        self.assertEqual(result.created_transfers, 1)
        self.assertEqual(transfer.hash, "0x" + "cd" * 32)
        self.assertEqual(transfer.amount, Decimal("1"))
        self.assertEqual(cursor.last_scanned_block, 20)
        self.assertEqual(cursor.last_safe_block, 14)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch(
        "evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status",
        return_value=None,
    )
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_ignores_failed_transaction_without_creating_transfer(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        _get_block_receipts_status_mock,
        create_observed_transfer_mock,
    ):
        # status=0 的原生交易不应落成 OnchainTransfer；失败语义只属于内部任务协调器。
        get_latest_block_number_mock.return_value = 12
        get_receipt_status_mock.return_value = 0
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=self.addr.address,
                        to_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        value=10**18,
                        tx_hash_hex="de",
                    )
                ]
            )
            if block_number == 1
            else self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=3)

        self.assertEqual(result.observed_transfers, 0)
        self.assertEqual(result.created_transfers, 0)
        create_observed_transfer_mock.assert_not_called()
        self.assertEqual(OnchainTransfer.objects.count(), 0)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_ignores_contract_calls_with_calldata(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        create_observed_transfer_mock,
    ):
        # 原生币扫描首版只认直转；带 calldata 的合约调用即使 value>0 也必须跳过。
        get_latest_block_number_mock.return_value = 12
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=self.addr.address,
                        value=10**18,
                        tx_hash_hex="ef",
                        input_data="0xa9059cbb",
                    )
                ]
            )
            if block_number == 1
            else self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=3)

        self.assertEqual(result.observed_transfers, 0)
        self.assertEqual(result.created_transfers, 0)
        create_observed_transfer_mock.assert_not_called()

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch(
        "evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status",
        return_value=None,
    )
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_rewind_window_is_idempotent(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        _get_block_receipts_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 原生币尾部重扫会重复看到同一笔交易，但 OnchainTransfer 唯一键必须保证不重复落库。
        get_latest_block_number_mock.return_value = 12
        get_receipt_status_mock.return_value = 1
        repeated_block = self._build_native_block(
            txs=[
                self._build_native_tx(
                    from_address=Web3.to_checksum_address(
                        "0x00000000000000000000000000000000000000cc"
                    ),
                    to_address=self.addr.address,
                    value=10**18,
                    tx_hash_hex="fa",
                )
            ]
        )
        get_full_block_mock.side_effect = lambda *, block_number: (
            repeated_block if block_number == 11 else self._build_native_block(txs=[])
        )

        first = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=12)
        second = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=12)

        self.assertEqual(first.created_transfers, 1)
        self.assertEqual(second.created_transfers, 0)
        self.assertEqual(
            OnchainTransfer.objects.filter(event_id="native:tx").count(), 1
        )

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_uses_block_receipts_when_supported(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        get_block_receipts_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 节点支持 eth_getBlockReceipts 时，命中块只调一次整块 receipt 拉取，
        # 不应再走逐笔 eth_getTransactionReceipt 的老路径。
        tx_hash_hex = "ab"
        tx_hash = "0x" + tx_hash_hex * 32
        get_latest_block_number_mock.return_value = 20
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=self.addr.address,
                        value=10**18,
                        tx_hash_hex=tx_hash_hex,
                    )
                ]
            )
            if block_number == 20
            else self._build_native_block(txs=[])
        )
        get_block_receipts_status_mock.return_value = {tx_hash: 1}

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        self.assertEqual(result.created_transfers, 1)
        get_receipt_status_mock.assert_not_called()
        self.assertTrue(
            OnchainTransfer.objects.filter(hash=tx_hash, event_id="native:tx").exists()
        )

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_falls_back_when_block_receipts_misses_matched_tx(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        get_block_receipts_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 部分 RPC provider 的 eth_getBlockReceipts 可能返回缺项/缺 status；
        # 对已命中的 tx 必须单笔 fallback，否则会漏扫成功转账并推进游标。
        tx_hash_hex = "ac"
        tx_hash = "0x" + tx_hash_hex * 32
        get_latest_block_number_mock.return_value = 20
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=self.addr.address,
                        value=10**18,
                        tx_hash_hex=tx_hash_hex,
                    )
                ]
            )
            if block_number == 20
            else self._build_native_block(txs=[])
        )
        get_block_receipts_status_mock.return_value = {}
        get_receipt_status_mock.return_value = 1

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        self.assertEqual(result.created_transfers, 1)
        get_receipt_status_mock.assert_called_once_with(tx_hash=tx_hash)
        self.assertTrue(
            OnchainTransfer.objects.filter(hash=tx_hash, event_id="native:tx").exists()
        )

    def test_erc20_cursor_advance_never_rewinds_database_value(self):
        cursor = EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            last_scanned_block=100,
            last_safe_block=100,
        )
        stale_cursor = EvmScanCursor.objects.get(pk=cursor.pk)
        EvmScanCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=150,
            last_safe_block=150,
        )

        EvmErc20TransferScanner._advance_cursor(
            cursor=stale_cursor,
            latest_block=120,
            scanned_to_block=120,
        )

        cursor.refresh_from_db()
        self.assertEqual(cursor.last_scanned_block, 150)
        self.assertEqual(cursor.last_safe_block, 150)

    def test_native_cursor_advance_never_rewinds_database_value(self):
        cursor = EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            last_scanned_block=100,
            last_safe_block=100,
        )
        stale_cursor = EvmScanCursor.objects.get(pk=cursor.pk)
        EvmScanCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=150,
            last_safe_block=150,
        )

        EvmNativeDirectScanner._advance_cursor(
            cursor=stale_cursor,
            latest_block=120,
            scanned_to_block=120,
        )

        cursor.refresh_from_db()
        self.assertEqual(cursor.last_scanned_block, 150)
        self.assertEqual(cursor.last_safe_block, 150)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_block_receipts_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_skips_block_receipts_for_blocks_without_matches(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        get_block_receipts_status_mock,
        _create_observed_transfer_mock,
    ):
        # 空块 / 无命中的块不应再多付一次 eth_getBlockReceipts；
        # 老路径每块只 1 次 eth_getBlockByNumber，新路径必须保持相同上限。
        get_latest_block_number_mock.return_value = 20
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000dd"
                        ),
                        value=10**18,
                        tx_hash_hex="ee",
                    )
                ]
            )
        )

        EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        get_block_receipts_status_mock.assert_not_called()
        get_receipt_status_mock.assert_not_called()

    @patch(
        "evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number",
        side_effect=EvmScannerRpcError("rpc timeout"),
    )
    def test_erc20_scan_records_cursor_error_when_rpc_fails(
        self, _get_latest_block_number_mock
    ):
        # RPC 失败后必须把错误留在游标上，方便后台与运维定位扫描停滞原因。
        with self.assertRaises(EvmScannerRpcError):
            EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(cursor.last_scanned_block, 0)
        self.assertEqual(cursor.last_error, "rpc timeout")
        self.assertIsNotNone(cursor.last_error_at)

    def test_erc20_scan_records_full_cursor_error_when_rpc_error_is_long(self):
        # RPC 供应商返回的长错误通常包含限制规则和建议查询范围，游标必须完整保留。
        long_error = "rpc limit exceeded: " + "x" * 360

        with patch(
            "evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number",
            side_effect=EvmScannerRpcError(long_error),
        ):
            with self.assertRaises(EvmScannerRpcError):
                EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(cursor.last_error, long_error)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_erc20_scan_ignores_zero_value_transfer(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        create_observed_transfer_mock,
    ):
        # ERC20 OnchainTransfer 事件 value=0 无业务意义（如某些代币的 approve 触发），应在扫描层过滤。
        get_latest_block_number_mock.return_value = 40
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=self.addr.address,
                value=0,
                block_number=40,
            )
        ]

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=40)

        self.assertEqual(result.created_transfers, 0)
        create_observed_transfer_mock.assert_not_called()

    @patch(
        "evm.scanner.native.EvmScannerRpcClient.get_latest_block_number",
        side_effect=EvmScannerRpcError("node unreachable"),
    )
    def test_native_scan_records_cursor_error_when_rpc_fails(
        self, _get_latest_block_number_mock
    ):
        # 原生币扫描 RPC 失败后必须把错误留在游标上，与 ERC20 扫描行为一致。
        with self.assertRaises(EvmScannerRpcError):
            EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        self.assertEqual(cursor.last_scanned_block, 0)
        self.assertEqual(cursor.last_error, "node unreachable")
        self.assertIsNotNone(cursor.last_error_at)

    def _create_project_id(self) -> int:
        project = Project.objects.create(
            name="scanner-project",
            wallet=Wallet.objects.create(),
            webhook="https://example.com/webhook",
        )
        return project.pk
