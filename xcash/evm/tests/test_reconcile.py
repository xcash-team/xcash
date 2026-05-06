from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone
from web3 import Web3
from web3.exceptions import TransactionNotFound

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from chains.models import TxHash
from chains.models import Wallet
from core.models import PLATFORM_SETTINGS_CACHE_KEY
from core.models import PlatformSettings
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType


@override_settings(DEBUG=False)
class EvmReconcilePendingChainTests(TestCase):
    """兜底任务 reconcile_stale_pending_chain_evm 的关键行为测试。

    覆盖点：
    - 阈值内不兜底（避免抖动误触发）；
    - 所有历史 tx_hash 都要查 receipt；
    - receipt 命中时，按块号交给 scan_blocks_for_reconcile 复扫；
    - 未命中时不调用扫描器，且不改动任务状态。
    """

    def setUp(self):
        self.native = Crypto_create("Ether Recon", "ETHR", "ethereum-recon")
        self.chain = Chain.objects.create(
            code="eth-recon",
            name="Ether Recon",
            type=ChainType.EVM,
            chain_id=90_001,
            rpc="http://eth-recon.local",
            native_coin=self.native,
            confirm_block_count=6,
            active=True,
        )
        self.wallet = Wallet.objects.create()
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000a1"
            ),
        )
        # 缓存会污染 avg_block_interval 估算；每个用例先清掉，确保调用路径可复现。
        cache.delete(f"evm:avg_block_interval:{self.chain.pk}")

        # 任务入口会重新 Chain.objects.get 拿到一个新的实例，其 cached_property w3
        # 独立，导致测试中注入到 self.chain.__dict__["w3"] 的 mock 不会被命中。
        # 这里统一 patch 住，让任务拿到的 chain 就是 fixture 的同一个实例。
        chain_get_patch = patch(
            "chains.models.Chain.objects.get", return_value=self.chain
        )
        chain_get_patch.start()
        self.addCleanup(chain_get_patch.stop)

    def _make_task(
        self,
        *,
        tx_hash: str,
        stage: str = BroadcastTaskStage.PENDING_CHAIN,
        result: str = BroadcastTaskResult.UNKNOWN,
        aged_seconds: int = 0,
    ) -> BroadcastTask:
        task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.native,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000a2"
            ),
            amount=Decimal("1"),
            tx_hash=tx_hash,
            stage=stage,
            result=result,
        )
        if aged_seconds:
            # BroadcastTask.updated_at 是 auto_now，手动回写确保落到阈值之外/内。
            aged = timezone.now() - timedelta(seconds=aged_seconds)
            BroadcastTask.objects.filter(pk=task.pk).update(updated_at=aged)
            task.refresh_from_db()
        return task

    def _install_w3_receipt(self, receipt_map: dict[str, object]) -> Mock:
        """以 tx_hash → receipt 字典装 w3.eth.get_transaction_receipt mock。

        兜底任务内部通过 Chain.objects.get(pk=...) 重新加载 chain 实例，每次返回的
        Chain 都是新对象，其 cached_property "w3" 独立。为让 mock 对任务内部新 chain
        实例也生效，patch Chain.objects.get 让它返回测试 fixture 的 self.chain，
        并把 mock 安装到 self.chain.__dict__["w3"] 上。
        """

        def fake_receipt(tx_hash):
            value = receipt_map.get(tx_hash)
            if isinstance(value, Exception):
                raise value
            return value

        get_receipt_mock = Mock(side_effect=fake_receipt)
        self.chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=get_receipt_mock,
                block_number=123_456,
                get_block=Mock(return_value={"timestamp": 1_700_000_000}),
            )
        )
        return get_receipt_mock

    @patch(
        "evm.tasks.EvmChainScannerService.scan_blocks_for_reconcile",
    )
    @patch(
        "evm.tasks._compute_reconcile_threshold_seconds",
        return_value=(120, 12.0),
    )
    def test_reconcile_skips_task_still_within_threshold(
        self,
        _compute_threshold_mock,
        scan_blocks_mock,
    ):
        # 阈值内的任务属于正常确认窗口，兜底不应介入，以免不断扫未必要的块。
        from evm.tasks import reconcile_stale_pending_chain_evm

        self._make_task(tx_hash="0x" + "b1" * 32, aged_seconds=30)

        # 不会真正调 w3，但仍预置一个空 mock 避免万一被调导致解释性差的失败。
        self._install_w3_receipt({})

        reconcile_stale_pending_chain_evm.run(self.chain.pk)

        scan_blocks_mock.assert_not_called()

    def test_estimate_avg_block_interval_uses_chain_poa_retry(self):
        # 块时间采样也会读取区块；这里必须走链级 POA 自愈入口，
        # 否则 BSC 的 extraData 校验异常只会被吞掉并持续刷 warning。
        from evm.tasks import _estimate_avg_block_interval

        self.chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(block_number=100)
        )
        self.chain.get_block_with_poa_retry = Mock(
            side_effect=[
                {"timestamp": 1_700_000_200},
                {"timestamp": 1_700_000_000},
            ]
        )

        interval = _estimate_avg_block_interval(self.chain)

        self.assertEqual(interval, 10)
        self.chain.get_block_with_poa_retry.assert_any_call(100)
        self.chain.get_block_with_poa_retry.assert_any_call(80)

    @patch(
        "evm.tasks.EvmChainScannerService.scan_blocks_for_reconcile",
    )
    @patch(
        "evm.tasks._compute_reconcile_threshold_seconds",
        return_value=(60, 12.0),
    )
    def test_reconcile_dispatches_block_scan_when_receipt_found(
        self,
        _compute_threshold_mock,
        scan_blocks_mock,
    ):
        # 超阈值任务命中 receipt 时，应把对应块号交给扫描器复扫，兜底才能触发业务推进。
        from evm.tasks import reconcile_stale_pending_chain_evm

        tx_hash = "0x" + "b2" * 32
        self._make_task(tx_hash=tx_hash, aged_seconds=300)
        self._install_w3_receipt(
            {tx_hash: {"status": 1, "blockNumber": 12_345}}
        )

        # 防御：把 cursor 写入一个固定值，断言扫描器没有偷偷改动它。
        cursor = EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            last_scanned_block=99,
            last_safe_block=93,
        )

        reconcile_stale_pending_chain_evm.run(self.chain.pk)

        scan_blocks_mock.assert_called_once()
        kwargs = scan_blocks_mock.call_args.kwargs
        self.assertEqual(kwargs["chain"].pk, self.chain.pk)
        self.assertEqual(kwargs["block_numbers"], {12_345})

        cursor.refresh_from_db()
        self.assertEqual(cursor.last_scanned_block, 99)
        self.assertEqual(cursor.last_safe_block, 93)

    @patch(
        "evm.tasks.EvmChainScannerService.scan_blocks_for_reconcile",
    )
    @patch(
        "evm.tasks._compute_reconcile_threshold_seconds",
        return_value=(60, 12.0),
    )
    def test_reconcile_tries_all_historical_tx_hashes(
        self,
        _compute_threshold_mock,
        scan_blocks_mock,
    ):
        # gas 重签会产生多条 TxHash；兜底必须把所有历史 hash 都问一遍，命中任一即可复扫。
        from evm.tasks import reconcile_stale_pending_chain_evm

        first_hash = "0x" + "c1" * 32
        second_hash = "0x" + "c2" * 32
        third_hash = "0x" + "c3" * 32
        task = self._make_task(tx_hash=third_hash, aged_seconds=300)
        TxHash.objects.create(
            broadcast_task=task,
            chain=self.chain,
            hash=first_hash,
            version=1,
        )
        TxHash.objects.create(
            broadcast_task=task,
            chain=self.chain,
            hash=second_hash,
            version=2,
        )
        TxHash.objects.create(
            broadcast_task=task,
            chain=self.chain,
            hash=third_hash,
            version=3,
        )

        get_receipt_mock = self._install_w3_receipt(
            {
                first_hash: None,
                second_hash: None,
                third_hash: {"status": 1, "blockNumber": 99},
            }
        )

        reconcile_stale_pending_chain_evm.run(self.chain.pk)

        scan_blocks_mock.assert_called_once()
        self.assertEqual(
            scan_blocks_mock.call_args.kwargs["block_numbers"], {99}
        )
        # 所有历史 hash 都被查了一次，版本前的 hash 不应被提前短路。
        self.assertEqual(get_receipt_mock.call_count, 3)
        queried = {call.args[0] for call in get_receipt_mock.call_args_list}
        self.assertEqual(queried, {first_hash, second_hash, third_hash})

    @patch(
        "evm.tasks.EvmChainScannerService.scan_blocks_for_reconcile",
    )
    @patch(
        "evm.tasks._compute_reconcile_threshold_seconds",
        return_value=(60, 12.0),
    )
    def test_reconcile_skips_when_no_receipt_for_any_hash(
        self,
        _compute_threshold_mock,
        scan_blocks_mock,
    ):
        # 所有历史 hash 均未上链时，兜底不应调用扫描器，也不得改动任务状态；
        # 任务可以在下一轮 beat 再被检查，避免把 pending 状态错误地强制推进。
        from evm.tasks import reconcile_stale_pending_chain_evm

        stable_hash = "0x" + "d1" * 32
        task = self._make_task(tx_hash=stable_hash, aged_seconds=600)
        TxHash.objects.create(
            broadcast_task=task,
            chain=self.chain,
            hash=stable_hash,
            version=1,
        )

        self._install_w3_receipt(
            {
                stable_hash: TransactionNotFound(stable_hash),
            }
        )

        # 任务兜底不得抛异常，否则会拖垮 beat 调度。
        reconcile_stale_pending_chain_evm.run(self.chain.pk)

        scan_blocks_mock.assert_not_called()
        task.refresh_from_db()
        self.assertEqual(task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(task.result, BroadcastTaskResult.UNKNOWN)


@override_settings(DEBUG=False)
class EvmScanBlocksForReconcileTests(TestCase):
    """scan_blocks_for_reconcile 必须能复用主扫描的产出通路且不污染 cursor。"""

    def setUp(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        self.platform_settings = PlatformSettings.objects.create(
            open_native_scanner=True,
        )
        self.native = Crypto_create(
            "Ether Recon Scan", "ETHRS", "ethereum-recon-scan"
        )
        self.chain = Chain.objects.create(
            code="eth-recon-scan",
            name="Ether Recon Scan",
            type=ChainType.EVM,
            chain_id=90_002,
            rpc="http://eth-recon-scan.local",
            native_coin=self.native,
            confirm_block_count=6,
            active=True,
            latest_block_number=500,
        )
        self.wallet = Wallet.objects.create()
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000b1"
            ),
        )
        # 固定 cursor，断言复扫前后不发生推进。
        self.cursor = EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            last_scanned_block=100,
            last_safe_block=94,
        )

    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        super().tearDown()

    @staticmethod
    def _native_tx(*, from_address: str, to_address: str, value: int, tx_hash_hex: str) -> dict:
        return {
            "hash": bytes.fromhex(tx_hash_hex * 32),
            "from": from_address,
            "to": to_address,
            "value": value,
            "input": "0x",
        }

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    def test_scan_blocks_for_reconcile_creates_onchain_transfer_without_touching_cursor(
        self,
        get_full_block_mock,
        get_receipt_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 兜底复扫必须复用主扫描的落库路径——OnchainTransfer 能被创建，
        # 但 EvmScanCursor 不应被 reconcile 路径改动，否则会污染主扫描。
        from evm.scanner.service import EvmChainScannerService

        target_block = 250
        get_receipt_status_mock.return_value = 1
        get_full_block_mock.side_effect = lambda *, block_number: {
            "number": block_number,
            "timestamp": 1_700_000_123,
            "transactions": [
                self._native_tx(
                    from_address=Web3.to_checksum_address(
                        "0x00000000000000000000000000000000000000cc"
                    ),
                    to_address=self.addr.address,
                    value=10**18,
                    tx_hash_hex="a1",
                )
            ]
            if block_number == target_block
            else [],
        }

        cursor_last_scanned_before = self.cursor.last_scanned_block
        cursor_last_safe_before = self.cursor.last_safe_block

        result = EvmChainScannerService.scan_blocks_for_reconcile(
            chain=self.chain,
            block_numbers={target_block},
        )

        self.cursor.refresh_from_db()
        self.assertEqual(self.cursor.last_scanned_block, cursor_last_scanned_before)
        self.assertEqual(self.cursor.last_safe_block, cursor_last_safe_before)

        self.assertEqual(result.created_native, 1)
        self.assertTrue(
            OnchainTransfer.objects.filter(
                chain=self.chain, hash="0x" + "a1" * 32
            ).exists()
        )

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    def test_reconcile_is_idempotent_when_onchain_transfer_already_exists(
        self,
        get_full_block_mock,
        get_receipt_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 多次兜底调用必须幂等；(chain, hash, event_id) 唯一键保证不会重复落库，
        # 也不应抛 IntegrityError 中断任务。
        from evm.scanner.service import EvmChainScannerService

        target_block = 260
        get_receipt_status_mock.return_value = 1
        get_full_block_mock.side_effect = lambda *, block_number: {
            "number": block_number,
            "timestamp": 1_700_000_123,
            "transactions": [
                self._native_tx(
                    from_address=Web3.to_checksum_address(
                        "0x00000000000000000000000000000000000000cc"
                    ),
                    to_address=self.addr.address,
                    value=10**18,
                    tx_hash_hex="a2",
                )
            ]
            if block_number == target_block
            else [],
        }

        first = EvmChainScannerService.scan_blocks_for_reconcile(
            chain=self.chain,
            block_numbers={target_block},
        )
        second = EvmChainScannerService.scan_blocks_for_reconcile(
            chain=self.chain,
            block_numbers={target_block},
        )

        self.assertEqual(first.created_native, 1)
        self.assertEqual(second.created_native, 0)
        self.assertEqual(
            OnchainTransfer.objects.filter(
                chain=self.chain, hash="0x" + "a2" * 32
            ).count(),
            1,
        )

    @patch(
        "evm.scanner.service.EvmErc20TransferScanner.scan_range_without_cursor",
    )
    @patch(
        "evm.scanner.service.EvmNativeDirectScanner.scan_range_without_cursor",
    )
    def test_reconcile_sparse_blocks_scans_contiguous_segments(
        self,
        native_scan_mock,
        erc20_scan_mock,
    ):
        # 多个 stale 任务命中相距很远的块时，兜底只能扫命中的连续块段，
        # 不能扩成 [min..max] 巨大区间拖垮 RPC。
        from evm.scanner.service import EvmChainScannerService

        native_scan_mock.side_effect = [(2, 1), (2, 1), (1, 1)]
        erc20_scan_mock.side_effect = [
            ([object(), object()], 1),
            ([object(), object()], 1),
            ([object()], 1),
        ]

        result = EvmChainScannerService.scan_blocks_for_reconcile(
            chain=self.chain,
            block_numbers={10, 11, 500, 501, 900},
        )

        native_ranges = [
            (call.kwargs["from_block"], call.kwargs["to_block"])
            for call in native_scan_mock.call_args_list
        ]
        erc20_ranges = [
            (call.kwargs["from_block"], call.kwargs["to_block"])
            for call in erc20_scan_mock.call_args_list
        ]
        self.assertEqual(native_ranges, [(10, 11), (500, 501), (900, 900)])
        self.assertEqual(erc20_ranges, [(10, 11), (500, 501), (900, 900)])
        self.assertEqual(result.from_block, 10)
        self.assertEqual(result.to_block, 900)
        self.assertEqual(result.observed_native, 5)
        self.assertEqual(result.observed_erc20, 5)
        self.assertEqual(result.created_native, 3)
        self.assertEqual(result.created_erc20, 3)

    @patch(
        "evm.scanner.service.EvmErc20TransferScanner.scan_range_without_cursor",
    )
    @patch(
        "evm.scanner.service.EvmNativeDirectScanner.scan_range_without_cursor",
    )
    def test_reconcile_skips_native_scan_when_global_native_scanner_is_closed(
        self,
        native_scan_mock,
        erc20_scan_mock,
    ):
        from evm.scanner.service import EvmChainScannerService

        self.platform_settings.open_native_scanner = False
        self.platform_settings.save(update_fields=["open_native_scanner"])
        erc20_scan_mock.return_value = ([object()], 1)

        result = EvmChainScannerService.scan_blocks_for_reconcile(
            chain=self.chain,
            block_numbers={10},
        )

        native_scan_mock.assert_not_called()
        erc20_scan_mock.assert_called_once()
        self.assertEqual(result.observed_native, 0)
        self.assertEqual(result.created_native, 0)
        self.assertEqual(result.observed_erc20, 1)
        self.assertEqual(result.created_erc20, 1)


def Crypto_create(name: str, symbol: str, coingecko_id: str):
    """惰性引用 Crypto，避免模块导入阶段强依赖 currencies。

    当前项目里 currencies.Crypto 位于 currencies.models；测试用例小工具这里集中处理，
    保持各用例 setUp 代码更聚焦。
    """
    from currencies.models import Crypto

    return Crypto.objects.create(name=name, symbol=symbol, coingecko_id=coingecko_id)
