from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from typing import TypedDict

import structlog
from django.db import transaction
from django.utils import timezone
from web3 import Web3

from chains.models import Chain
from chains.models import ChainType
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.constants import DEFAULT_NATIVE_SCAN_BATCH_SIZE
from evm.scanner.constants import DEFAULT_NATIVE_SCAN_REPLAY_BLOCKS
from evm.scanner.cursor import bootstrap_cursor_to_latest_for_debug
from evm.scanner.rpc import EvmScannerRpcClient
from evm.scanner.rpc import EvmScannerRpcError
from evm.scanner.watchers import EvmWatchSet
from evm.scanner.watchers import load_watch_set

logger = structlog.get_logger()


class ParsedNativeTransfer(TypedDict):
    """描述一笔已通过过滤的原生币直转交易。"""

    tx_hash: str
    from_address: str
    to_address: str
    value: Decimal
    amount: Decimal


@dataclass(frozen=True)
class EvmNativeScanResult:
    """描述一次原生币直转扫描的结果。"""

    from_block: int
    to_block: int
    latest_block: int
    observed_transfers: int
    created_transfers: int


class EvmNativeDirectScanner:
    """扫描顶层原生币直接转账。

    V1 明确只处理 input 为空的顶层 value transfer，不解析合约内部转账或带 calldata 的合约调用。
    """

    cursor_type = EvmScanCursorType.NATIVE_DIRECT

    @classmethod
    def scan_chain(
        cls,
        *,
        chain: Chain,
        batch_size: int = DEFAULT_NATIVE_SCAN_BATCH_SIZE,
    ) -> EvmNativeScanResult:
        if chain.type != ChainType.EVM:
            raise ValueError(f"仅支持 EVM 链扫描，当前链为 {chain.code}")

        cursor = cls._get_or_create_cursor(chain=chain)
        rpc_client = EvmScannerRpcClient(chain=chain)

        try:
            latest_block = rpc_client.get_latest_block_number()
            Chain.objects.filter(pk=chain.pk).update(latest_block_number=latest_block)
            cursor = bootstrap_cursor_to_latest_for_debug(
                cursor=cursor,
                latest_block=latest_block,
            )

            watch_set = load_watch_set(chain=chain)
            if not watch_set.watched_addresses:
                # 当前无系统 EVM 观察地址时，不需要为尚未纳入系统的历史直转保留积压；
                # 直接把游标推进到链头，避免后台长期显示“未配置导致的伪卡住”。
                cls._advance_cursor(
                    cursor=cursor,
                    latest_block=latest_block,
                    scanned_to_block=latest_block,
                )
                return EvmNativeScanResult(
                    from_block=0,
                    to_block=0,
                    latest_block=latest_block,
                    observed_transfers=0,
                    created_transfers=0,
                )

            from_block, to_block = cls._compute_scan_window(
                cursor=cursor,
                latest_block=latest_block,
                batch_size=batch_size,
            )
            if from_block > to_block:
                cls._mark_cursor_idle(cursor=cursor, latest_block=latest_block)
                return EvmNativeScanResult(
                    from_block=from_block,
                    to_block=to_block,
                    latest_block=latest_block,
                    observed_transfers=0,
                    created_transfers=0,
                )

            observed_transfers, created_transfers = cls._scan_blocks(
                chain=chain,
                rpc_client=rpc_client,
                watch_set=watch_set,
                from_block=from_block,
                to_block=to_block,
            )
        except EvmScannerRpcError as exc:
            cls._mark_cursor_error(cursor=cursor, exc=exc)
            raise

        cls._advance_cursor(
            cursor=cursor,
            latest_block=latest_block,
            scanned_to_block=to_block,
        )
        return EvmNativeScanResult(
            from_block=from_block,
            to_block=to_block,
            latest_block=latest_block,
            observed_transfers=observed_transfers,
            created_transfers=created_transfers,
        )

    @classmethod
    def _get_or_create_cursor(cls, *, chain: Chain) -> EvmScanCursor:
        with transaction.atomic():
            cursor, _ = EvmScanCursor.objects.select_for_update().get_or_create(
                chain=chain,
                scanner_type=cls.cursor_type,
                defaults={
                    "last_scanned_block": 0,
                    "last_safe_block": 0,
                    "enabled": True,
                },
            )
        return cursor

    @staticmethod
    def _compute_scan_window(
        *,
        cursor: EvmScanCursor,
        latest_block: int,
        batch_size: int,
    ) -> tuple[int, int]:
        if latest_block <= 0:
            return 0, -1

        replay_blocks = DEFAULT_NATIVE_SCAN_REPLAY_BLOCKS
        if cursor.last_scanned_block <= 0:
            from_block = 1
        else:
            from_block = max(1, cursor.last_scanned_block + 1 - replay_blocks)

        forward_batch_size = max(1, batch_size)
        if cursor.last_scanned_block > 0:
            # batch_size 表示本轮向前追的新块数；replay_blocks 只扩大旧块复扫范围，
            # 不参与业务确认深度，也不能挤占净推进量。
            to_block = min(
                latest_block,
                cursor.last_scanned_block + forward_batch_size,
            )
        else:
            to_block = min(latest_block, from_block + forward_batch_size - 1)
        return from_block, to_block

    @classmethod
    def _scan_blocks(
        cls,
        *,
        chain: Chain,
        rpc_client: EvmScannerRpcClient,
        watch_set: EvmWatchSet,
        from_block: int,
        to_block: int,
    ) -> tuple[int, int]:
        # 保持薄封装以维持主扫描路径的既有调用语义；真正的逐块扫描逻辑统一下沉到
        # scan_range_without_cursor，兜底复扫可以在不触碰游标的前提下复用。
        return cls.scan_range_without_cursor(
            chain=chain,
            rpc_client=rpc_client,
            watch_set=watch_set,
            from_block=from_block,
            to_block=to_block,
        )

    @classmethod
    def scan_range_without_cursor(
        cls,
        *,
        chain: Chain,
        rpc_client: EvmScannerRpcClient,
        watch_set: EvmWatchSet,
        from_block: int,
        to_block: int,
    ) -> tuple[int, int]:
        """对 [from_block, to_block] 区间执行一次原生币直转扫描。

        该方法只负责产生 OnchainTransfer，不读不写 EvmScanCursor，也不推进链头。
        调用方需要自行确保 watch_set 有效并决定是否跳过空区间，便于兜底复扫在不
        污染主扫描游标的前提下复用完整的解析 + 落库 + 派发管线。
        """
        observed_transfers = 0
        created_transfers = 0
        if from_block > to_block or not watch_set.watched_addresses:
            # watch_set 为空时逐块扫描只会命中 0 笔，但仍会拉全量区块；提前返回避免浪费 RPC。
            return observed_transfers, created_transfers
        decimals = chain.native_coin.get_decimals(chain)

        for block_number in range(from_block, to_block + 1):
            block: dict[str, Any] = rpc_client.get_full_block(block_number=block_number)
            block_hash = cls._normalize_hash(block.get("hash"))
            timestamp = int(block["timestamp"])
            occurred_at = datetime.fromtimestamp(
                timestamp,
                tz=timezone.get_current_timezone(),
            )

            # 先把整块的命中 tx 集中起来；空块不去触发 receipt 拉取，避免在主路径上
            # 给绝大多数无命中的块都多付一次 eth_getBlockReceipts。
            matched_parsed: list[ParsedNativeTransfer] = []
            for tx in block.get("transactions", []) or []:
                parsed = cls._parse_transaction(
                    tx=tx,
                    watched_addresses=watch_set.watched_addresses,
                    decimals=decimals,
                )
                if parsed is not None:
                    matched_parsed.append(parsed)

            if not matched_parsed:
                continue

            # 优先用 eth_getBlockReceipts 整块取回；命中数 ≥1 时单块只 1 次 RPC，
            # 与之前"逐笔 eth_getTransactionReceipt"相比，命中越多收益越大、上限恒定。
            # 节点不支持时（status_map=None）回退到老路径，逐笔确认状态。
            receipt_status_map = rpc_client.get_block_receipts_status(
                block_number=block_number,
            )

            for parsed in matched_parsed:
                # OnchainTransfer 只表示成功链上资产移动；status=0 的失败交易不得落库。
                if receipt_status_map is not None:
                    receipt_status = receipt_status_map.get(parsed["tx_hash"])
                else:
                    receipt_status = rpc_client.get_transaction_receipt_status(
                        tx_hash=parsed["tx_hash"]
                    )
                if receipt_status != 1:
                    continue

                observed_transfers += 1
                result = TransferService.create_observed_transfer(
                    observed=ObservedTransferPayload(
                        chain=chain,
                        block=block_number,
                        tx_hash=parsed["tx_hash"],
                        event_id="native:tx",
                        from_address=parsed["from_address"],
                        to_address=parsed["to_address"],
                        crypto=chain.native_coin,
                        value=parsed["value"],
                        amount=parsed["amount"],
                        timestamp=timestamp,
                        occurred_at=occurred_at,
                        block_hash=block_hash,
                        source="evm-scan",
                    )
                )
                if result.created:
                    created_transfers += 1

        return observed_transfers, created_transfers

    @staticmethod
    def _parse_transaction(
        *,
        tx: dict[str, Any],
        watched_addresses: frozenset[str],
        decimals: int,
    ) -> ParsedNativeTransfer | None:
        raw_input = tx.get("input", "0x")
        # 真实节点返回的空 calldata 可能是字符串、bytes 或 HexBytes；统一转 hex 后再判断直转。
        input_hex = EvmNativeDirectScanner._to_hex(raw_input).lower()
        if input_hex not in ("", "0"):
            return None

        raw_to = tx.get("to")
        raw_from = tx.get("from")
        if not raw_to or not raw_from:
            return None

        to_address = Web3.to_checksum_address(str(raw_to))
        from_address = Web3.to_checksum_address(str(raw_from))
        if (
            to_address not in watched_addresses
            and from_address not in watched_addresses
        ):
            return None

        value = Decimal(int(tx.get("value", 0)))
        if value <= 0:
            return None

        parsed_transfer: ParsedNativeTransfer = {
            "tx_hash": f"0x{EvmNativeDirectScanner._to_hex(tx['hash']).lower()}",
            "from_address": from_address,
            "to_address": to_address,
            "value": value,
            "amount": Decimal(value).scaleb(-decimals),
        }
        return parsed_transfer

    @staticmethod
    def _to_hex(value: object) -> str:
        if hasattr(value, "hex"):
            hex_value = value.hex()
        else:
            hex_value = str(value)
        return hex_value[2:] if hex_value.startswith("0x") else hex_value

    @staticmethod
    def _normalize_hash(value: object | None) -> str | None:
        if value is None:
            return None
        raw_hex = EvmNativeDirectScanner._to_hex(value)
        return f"0x{raw_hex.lower()}" if raw_hex else None

    @staticmethod
    def _mark_cursor_idle(*, cursor: EvmScanCursor, latest_block: int) -> None:
        EvmScanCursor.objects.filter(pk=cursor.pk).update(
            last_safe_block=max(0, latest_block - cursor.chain.confirm_block_count),
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )

    @staticmethod
    def _advance_cursor(
        *,
        cursor: EvmScanCursor,
        latest_block: int,
        scanned_to_block: int,
    ) -> None:
        EvmScanCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=max(cursor.last_scanned_block, scanned_to_block),
            last_safe_block=max(0, latest_block - cursor.chain.confirm_block_count),
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )

    @staticmethod
    def _mark_cursor_error(*, cursor: EvmScanCursor, exc: Exception) -> None:
        logger.warning(
            "EVM 原生币扫描失败",
            chain=cursor.chain.code,
            scanner_type=cursor.scanner_type,
            error=str(exc),
        )
        EvmScanCursor.objects.filter(pk=cursor.pk).update(
            last_error=str(exc),
            last_error_at=timezone.now(),
            updated_at=timezone.now(),
        )
