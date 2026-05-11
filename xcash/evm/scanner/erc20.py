from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from typing import TypedDict

import structlog
from django.db import transaction
from django.db.models import F
from django.db.models.functions import Greatest
from django.utils import timezone
from web3 import Web3

from chains.models import Chain
from chains.models import ChainType
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.constants import DEFAULT_ERC20_SCAN_BATCH_SIZE
from evm.scanner.constants import DEFAULT_ERC20_SCAN_REPLAY_BLOCKS
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0
from evm.scanner.cursor import bootstrap_cursor_to_latest_for_debug
from evm.scanner.rpc import EvmScannerRpcClient
from evm.scanner.rpc import EvmScannerRpcError
from evm.scanner.watchers import EvmWatchSet
from evm.scanner.watchers import load_watch_set

logger = structlog.get_logger()


class ParsedErc20Log(TypedDict):
    """描述一条已通过过滤的 ERC20 OnchainTransfer 日志。"""

    block_number: int
    block_hash: str | None
    tx_hash: str
    event_id: str
    from_address: str
    to_address: str
    crypto: Any
    value: Decimal
    amount: Decimal


@dataclass(frozen=True)
class EvmErc20ScanResult:
    """描述一次 ERC20 聚合扫描的结果，便于任务层记录指标。"""

    from_block: int
    to_block: int
    latest_block: int
    observed_logs: int
    created_transfers: int


class EvmErc20TransferScanner:
    """按链聚合扫描受支持 ERC20 代币的 OnchainTransfer 日志。"""

    cursor_type = EvmScanCursorType.ERC20_TRANSFER

    @classmethod
    def scan_chain(
        cls,
        *,
        chain: Chain,
        batch_size: int = DEFAULT_ERC20_SCAN_BATCH_SIZE,
        rpc_client: EvmScannerRpcClient | None = None,
    ) -> EvmErc20ScanResult:
        if chain.type != ChainType.EVM:
            raise ValueError(f"仅支持 EVM 链扫描，当前链为 {chain.code}")

        cursor = cls._get_or_create_cursor(chain=chain)
        # 服务层会把 client 注入进来供 native + erc20 共用，单 tick 只打一次 eth_blockNumber；
        # 单独调用时自建一个 client 维持原契约。
        if rpc_client is None:
            rpc_client = EvmScannerRpcClient(chain=chain)

        try:
            latest_block = rpc_client.get_latest_block_number()
            # 用 Greatest 把 latest_block_number 锁成单调向前，避免并发 scanner 因 RPC
            # 短暂滞后把链头记录回退到旧高度，confirm 链路读到回退值会误判确认进度。
            Chain.objects.filter(pk=chain.pk).update(
                latest_block_number=Greatest(F("latest_block_number"), latest_block)
            )
            cursor = bootstrap_cursor_to_latest_for_debug(
                cursor=cursor,
                latest_block=latest_block,
            )

            watch_set = load_watch_set(chain=chain)
            if not watch_set.watched_addresses or not watch_set.tokens_by_address:
                # 链上当前无 ERC20 观察集时，不需要为尚未纳入系统的历史事件保留积压；
                # 直接把游标推进到当前链头，避免后台长期显示“未配置导致的伪积压”。
                cls._advance_cursor(
                    cursor=cursor,
                    latest_block=latest_block,
                    scanned_to_block=latest_block,
                )
                return EvmErc20ScanResult(
                    from_block=0,
                    to_block=0,
                    latest_block=latest_block,
                    observed_logs=0,
                    created_transfers=0,
                )

            from_block, to_block = cls._compute_scan_window(
                cursor=cursor,
                latest_block=latest_block,
                batch_size=batch_size,
            )
            if from_block > to_block:
                cls._mark_cursor_idle(cursor=cursor, latest_block=latest_block)
                return EvmErc20ScanResult(
                    from_block=from_block,
                    to_block=to_block,
                    latest_block=latest_block,
                    observed_logs=0,
                    created_transfers=0,
                )

            logs, created_transfers = cls.scan_range_without_cursor(
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
        return EvmErc20ScanResult(
            from_block=from_block,
            to_block=to_block,
            latest_block=latest_block,
            observed_logs=len(logs),
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

        replay_blocks = DEFAULT_ERC20_SCAN_REPLAY_BLOCKS
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
    def scan_range_without_cursor(
        cls,
        *,
        chain: Chain,
        rpc_client: EvmScannerRpcClient,
        watch_set: EvmWatchSet,
        from_block: int,
        to_block: int,
    ) -> tuple[list[dict[str, Any]], int]:
        """对 [from_block, to_block] 区间拉取 + 落库 ERC20 Transfer 日志。

        不触碰任何游标，也不改变链头记录；返回 (原始日志列表, 本次新增 Transfer 数)，
        给兜底复扫提供可观测指标。区间非法或 watch_set 为空时静默返回空结果。
        """
        if (
            from_block > to_block
            or not watch_set.watched_addresses
            or not watch_set.tokens_by_address
        ):
            return [], 0

        logs = rpc_client.get_transfer_logs(
            from_block=from_block,
            to_block=to_block,
            token_addresses=list(watch_set.tokens_by_address.keys()),
            topic0=ERC20_TRANSFER_TOPIC0,
        )
        created = cls._persist_logs(
            chain=chain,
            logs=logs,
            rpc_client=rpc_client,
            watch_set=watch_set,
        )
        return logs, created

    @classmethod
    def _persist_logs(
        cls,
        *,
        chain: Chain,
        logs: list[dict[str, Any]],
        rpc_client: EvmScannerRpcClient,
        watch_set: EvmWatchSet,
    ) -> int:
        if not logs:
            return 0

        timestamp_cache: dict[int, int] = {}
        created_transfers = 0

        for log in logs:
            parsed = cls._parse_log(log=log, watch_set=watch_set)
            if parsed is None:
                continue

            block_number = parsed["block_number"]
            timestamp = timestamp_cache.get(block_number)
            if timestamp is None:
                timestamp = rpc_client.get_block_timestamp(block_number=block_number)
                timestamp_cache[block_number] = timestamp

            observed = ObservedTransferPayload(
                chain=chain,
                block=block_number,
                tx_hash=parsed["tx_hash"],
                event_id=parsed["event_id"],
                from_address=parsed["from_address"],
                to_address=parsed["to_address"],
                crypto=parsed["crypto"],
                value=parsed["value"],
                amount=parsed["amount"],
                timestamp=timestamp,
                occurred_at=datetime.fromtimestamp(
                    timestamp,
                    tz=timezone.get_current_timezone(),
                ),
                block_hash=parsed["block_hash"],
                source="evm-scan",
            )
            result = TransferService.create_observed_transfer(observed=observed)
            if result.created:
                created_transfers += 1

        return created_transfers

    @staticmethod
    def _parse_log(
        *,
        log: dict[str, Any],
        watch_set: EvmWatchSet,
    ) -> ParsedErc20Log | None:
        # 重组期间节点可能返回 removed=true 的日志，跳过以避免创建无效 Transfer。
        if log.get("removed"):
            return None

        topics = list(log.get("topics") or [])
        if len(topics) < 3:
            return None

        token_address = Web3.to_checksum_address(str(log.get("address", "")))
        token = watch_set.tokens_by_address.get(token_address)
        if token is None:
            return None

        from_address = EvmErc20TransferScanner._topic_to_address(topics[1])
        to_address = EvmErc20TransferScanner._topic_to_address(topics[2])
        if (
            from_address not in watch_set.watched_addresses
            and to_address not in watch_set.watched_addresses
        ):
            return None

        raw_data = log.get("data", "0x0")
        raw_hex = EvmErc20TransferScanner._to_hex(raw_data)
        if not raw_hex:
            return None
        value = Decimal(int(raw_hex, 16))
        if value <= 0:
            return None
        # watch_set 已经把 ChainToken 整行加载进来，这里直接复用链特定精度，
        # 避免每条日志再通过 crypto.get_decimals() 触发一次额外数据库查询。
        decimals = (
            token.decimals if token.decimals is not None else token.crypto.decimals
        )
        amount = Decimal(value).scaleb(-decimals)

        parsed_log: ParsedErc20Log = {
            "block_number": int(log["blockNumber"]),
            "block_hash": EvmErc20TransferScanner._normalize_hash(
                log.get("blockHash")
            ),
            # 统一补齐 0x 前缀，保持与现有 EVM OnchainTransfer.hash 存储语义一致。
            "tx_hash": f"0x{EvmErc20TransferScanner._to_hex(log['transactionHash']).lower()}",
            "event_id": f"erc20:{EvmErc20TransferScanner._parse_int(log.get('logIndex', 0))}",
            "from_address": from_address,
            "to_address": to_address,
            "crypto": token.crypto,
            "value": value,
            "amount": amount,
        }
        return parsed_log

    @staticmethod
    def _topic_to_address(topic: object) -> str:
        raw_hex = EvmErc20TransferScanner._to_hex(topic)
        return Web3.to_checksum_address(f"0x{raw_hex[-40:]}")

    @staticmethod
    def _hex_to_int(value: object) -> int:
        raw_hex = EvmErc20TransferScanner._to_hex(value)
        return int(raw_hex, 16)

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
        raw_hex = EvmErc20TransferScanner._to_hex(value)
        return f"0x{raw_hex.lower()}" if raw_hex else None

    @staticmethod
    def _parse_int(raw_value: object) -> int:
        if isinstance(raw_value, int):
            return raw_value
        value = str(raw_value).strip()
        if value.startswith(("0x", "0X")):
            return int(value, 16)
        return int(value) if value else 0

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
            "EVM ERC20 扫描失败",
            chain=cursor.chain.code,
            scanner_type=cursor.scanner_type,
            error=str(exc),
        )
        EvmScanCursor.objects.filter(pk=cursor.pk).update(
            last_error=str(exc),
            last_error_at=timezone.now(),
            updated_at=timezone.now(),
        )
