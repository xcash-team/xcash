from __future__ import annotations

from dataclasses import dataclass

from chains.models import Chain
from chains.models import ChainType
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.erc20 import EvmErc20TransferScanner
from evm.scanner.native import EvmNativeDirectScanner
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcClient
from evm.scanner.rpc import EvmScannerRpcError
from evm.scanner.watchers import load_watch_set

RECONCILE_SCAN_MAX_BLOCK_SPAN = 64


@dataclass(frozen=True)
class EvmScanSummary:
    """汇总单条链一次自扫描任务的结果。"""

    native: EvmNativeScanResult
    erc20: EvmErc20ScanResult


@dataclass(frozen=True)
class EvmReconcileScanResult:
    """汇总一次兜底复扫的产出，供调用方观测命中情况。

    from_block / to_block 仅记录合并出的扫描区间，便于日志与断言；不会映射到任何游标。
    """

    from_block: int
    to_block: int
    observed_native: int
    observed_erc20: int
    created_native: int
    created_erc20: int


class EvmChainScannerService:
    """统一编排一条 EVM 链上的自扫描流程。"""

    @staticmethod
    def _iter_reconcile_block_ranges(
        block_numbers: set[int],
        *,
        max_span: int = RECONCILE_SCAN_MAX_BLOCK_SPAN,
    ):
        """把命中块拆成连续且限宽的扫描窗口，避免稀疏块拉成长区间。"""
        if max_span <= 0:
            raise ValueError("max_span 必须大于 0")

        sorted_blocks = sorted(set(block_numbers))
        if not sorted_blocks:
            return

        start = end = sorted_blocks[0]
        for block_number in sorted_blocks[1:]:
            is_contiguous = block_number == end + 1
            exceeds_span = block_number - start + 1 > max_span
            if is_contiguous and not exceeds_span:
                end = block_number
                continue

            yield start, end
            start = end = block_number

        yield start, end

    @staticmethod
    def _is_enabled(*, chain: Chain, scanner_type: EvmScanCursorType) -> bool:
        enabled = (
            EvmScanCursor.objects.filter(
                chain=chain,
                scanner_type=scanner_type,
            )
            .values_list("enabled", flat=True)
            .first()
        )
        return True if enabled is None else bool(enabled)

    @staticmethod
    def _empty_native_result(*, chain: Chain) -> EvmNativeScanResult:
        return EvmNativeScanResult(
            from_block=0,
            to_block=0,
            latest_block=chain.latest_block_number,
            observed_transfers=0,
            created_transfers=0,
        )

    @staticmethod
    def _empty_erc20_result(*, chain: Chain) -> EvmErc20ScanResult:
        return EvmErc20ScanResult(
            from_block=0,
            to_block=0,
            latest_block=chain.latest_block_number,
            observed_logs=0,
            created_transfers=0,
        )

    @staticmethod
    def scan_chain(*, chain: Chain) -> EvmScanSummary:
        if chain.type != ChainType.EVM:
            raise ValueError(f"仅支持扫描 EVM 链，当前链为 {chain.code}")

        if EvmChainScannerService._is_enabled(
            chain=chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        ):
            try:
                native_result = EvmNativeDirectScanner.scan_chain(chain=chain)
            except EvmScannerRpcError:
                # 原生币逐块拉完整区块，RPC 压力和套餐限制都独立于 ERC20 日志扫描；
                # native 失败不能阻断 ERC20，否则会把一个扫描面的故障扩散到另一个扫描面。
                native_result = EvmChainScannerService._empty_native_result(chain=chain)
        else:
            native_result = EvmChainScannerService._empty_native_result(chain=chain)

        erc20_result = (
            EvmErc20TransferScanner.scan_chain(chain=chain)
            if EvmChainScannerService._is_enabled(
                chain=chain,
                scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            )
            else EvmChainScannerService._empty_erc20_result(chain=chain)
        )

        return EvmScanSummary(
            native=native_result,
            erc20=erc20_result,
        )

    @classmethod
    def scan_blocks_for_reconcile(
        cls,
        *,
        chain: Chain,
        block_numbers: set[int],
    ) -> EvmReconcileScanResult:
        """对指定块集合执行一次兜底复扫，不推进任何游标。

        - 按连续块段和最大跨度拆分窗口，避免稀疏块被扩成巨大 [min..max] 区间。
        - 复用 watch_set + OnchainTransfer 创建 + on_commit 派发 process 的既有管线，
          (chain, hash, event_id) 唯一约束天然保证复扫幂等。
        - 禁止读写 EvmScanCursor；主扫描负责游标管理，兜底只产生观测副作用。
        """
        if chain.type != ChainType.EVM:
            raise ValueError(f"仅支持扫描 EVM 链，当前链为 {chain.code}")
        if not block_numbers:
            return EvmReconcileScanResult(
                from_block=0,
                to_block=-1,
                observed_native=0,
                observed_erc20=0,
                created_native=0,
                created_erc20=0,
            )

        from_block = min(block_numbers)
        to_block = max(block_numbers)
        rpc_client = EvmScannerRpcClient(chain=chain)
        watch_set = load_watch_set(chain=chain)

        observed_native, created_native = 0, 0
        observed_erc20, created_erc20 = 0, 0

        native_enabled = cls._is_enabled(
            chain=chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        erc20_enabled = cls._is_enabled(
            chain=chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )

        for range_from_block, range_to_block in cls._iter_reconcile_block_ranges(
            block_numbers
        ):
            if native_enabled:
                range_observed_native, range_created_native = (
                    EvmNativeDirectScanner.scan_range_without_cursor(
                        chain=chain,
                        rpc_client=rpc_client,
                        watch_set=watch_set,
                        from_block=range_from_block,
                        to_block=range_to_block,
                    )
                )
                observed_native += range_observed_native
                created_native += range_created_native

            if not erc20_enabled:
                continue
            logs, range_created_erc20 = (
                EvmErc20TransferScanner.scan_range_without_cursor(
                    chain=chain,
                    rpc_client=rpc_client,
                    watch_set=watch_set,
                    from_block=range_from_block,
                    to_block=range_to_block,
                )
            )
            observed_erc20 += len(logs)
            created_erc20 += range_created_erc20

        return EvmReconcileScanResult(
            from_block=from_block,
            to_block=to_block,
            observed_native=observed_native,
            observed_erc20=observed_erc20,
            created_native=created_native,
            created_erc20=created_erc20,
        )
