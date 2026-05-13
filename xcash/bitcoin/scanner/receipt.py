from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import structlog
from django.db import transaction
from django.db.models import F
from django.db.models.functions import Greatest
from django.utils import timezone

from bitcoin.models import BitcoinScanCursor
from bitcoin.rpc import BitcoinBlockInfo
from bitcoin.rpc import BitcoinRpcClient
from bitcoin.rpc import BitcoinRpcError
from bitcoin.rpc import BitcoinTxInfo
from bitcoin.rpc import BitcoinTxVout
from bitcoin.scanner.constants import DEFAULT_REORG_LOOKBACK_BLOCKS
from bitcoin.scanner.constants import DEFAULT_SCAN_BATCH_SIZE
from bitcoin.scanner.watchers import load_watch_set
from bitcoin.utils import btc_to_satoshi
from chains.models import Chain
from chains.models import ChainType
from chains.service import ObservedTransferPayload
from chains.service import TransferService

logger = structlog.get_logger()


class BitcoinReceiptScanner:
    """基于 Bitcoin Core 的标准 BTC 收款扫描器。"""

    REORG_LOOKBACK_BLOCKS = DEFAULT_REORG_LOOKBACK_BLOCKS
    SCAN_BATCH_SIZE = DEFAULT_SCAN_BATCH_SIZE

    @classmethod
    def scan_recent_receipts(cls, chain: Chain) -> int:
        """按持久化游标推进 Bitcoin 扫描，并对近端区块做尾部回扫。"""
        if chain.type != ChainType.BITCOIN:
            msg = f"仅支持扫描 Bitcoin 链，当前链为 {chain.code}"
            raise ValueError(msg)

        cursor = cls._get_or_create_cursor(chain=chain)
        if not cursor.enabled:
            return 0
        watched_addresses = load_watch_set()
        client = BitcoinRpcClient(chain.rpc)

        try:
            latest_height = client.get_block_count()
            # 后台链列表与扫描游标都依赖最新块高，BTC 扫描顺手把链状态同步上来。
            # 用 Greatest 保证单调向前，避免并发 scanner 因 RPC 抖动把 latest_block_number 写回旧值。
            Chain.objects.filter(pk=chain.pk).update(
                latest_block_number=Greatest(F("latest_block_number"), latest_height)
            )

            if not watched_addresses:
                cls._mark_cursor_idle(cursor=cursor, latest_height=latest_height)
                return 0

            from_block, to_block = cls._compute_scan_window(
                cursor=cursor,
                latest_height=latest_height,
                confirm_block_count=chain.confirm_block_count,
                batch_size=cls.SCAN_BATCH_SIZE,
            )
            if from_block > to_block:
                cls._mark_cursor_idle(cursor=cursor, latest_height=latest_height)
                return 0

            tx_cache: dict[str, BitcoinTxInfo] = {}
            created_count = 0

            for height in range(from_block, to_block + 1):
                try:
                    block_hash = client.get_block_hash(height)
                    block = client.get_block(block_hash)
                except BitcoinRpcError as exc:
                    if cls._is_pruned_block_error(exc):
                        # 裁剪节点已删除该区块数据（宕机时间超过保留窗口），
                        # 将游标重置到最近可用区块，放弃中间缺失的区块。
                        reset_to = max(0, latest_height - cls.SCAN_BATCH_SIZE + 1)
                        logger.warning(
                            "Bitcoin 扫描遇到已裁剪区块，游标重置到链头附近",
                            chain=chain.code,
                            pruned_height=height,
                            reset_to=reset_to,
                            latest_height=latest_height,
                        )
                        cls._advance_cursor(
                            cursor=cursor,
                            latest_height=latest_height,
                            scanned_to_block=reset_to - 1,
                        )
                        return created_count
                    raise

                created_count += cls._scan_block(
                    chain=chain,
                    block=block,
                    watched_addresses=watched_addresses,
                    client=client,
                    tx_cache=tx_cache,
                )
        except BitcoinRpcError as exc:
            cls._mark_cursor_error(cursor=cursor, exc=exc)
            raise

        cls._advance_cursor(
            cursor=cursor,
            latest_height=latest_height,
            scanned_to_block=to_block,
        )
        return created_count

    @classmethod
    def _get_or_create_cursor(cls, *, chain: Chain) -> BitcoinScanCursor:
        with transaction.atomic():
            cursor, _ = BitcoinScanCursor.objects.select_for_update().get_or_create(
                chain=chain,
                defaults={
                    "last_scanned_block": 0,
                    "last_safe_block": 0,
                    "enabled": True,
                },
            )
        return cursor

    @classmethod
    def _compute_scan_window(
        cls,
        *,
        cursor: BitcoinScanCursor,
        latest_height: int,
        confirm_block_count: int,
        batch_size: int,
    ) -> tuple[int, int]:
        if latest_height < 0:
            return 0, -1

        reorg_lookback = max(confirm_block_count, cls.REORG_LOOKBACK_BLOCKS)
        if cursor.last_scanned_block <= 0:
            # 首次建游标时不从创世块慢慢追，而是先覆盖最近一段区块，
            # 这样新接入的 watch-only 地址也能在首轮扫描里及时吃到最新入账。
            from_block = max(0, latest_height - batch_size + 1)
        else:
            # 每轮回退一小段已扫区块，依赖 OnchainTransfer 唯一键保证重扫幂等，从而覆盖轻量重组。
            from_block = max(0, cursor.last_scanned_block + 1 - reorg_lookback)

        # 若当前已经接近链头，窗口必须直接扫到最新块；否则补扫会永远追不齐。
        if (
            cursor.last_scanned_block > 0
            and latest_height - cursor.last_scanned_block <= batch_size
        ):
            to_block = latest_height
        else:
            to_block = min(latest_height, from_block + batch_size - 1)
        return from_block, to_block

    @staticmethod
    def _mark_cursor_idle(*, cursor: BitcoinScanCursor, latest_height: int) -> None:
        BitcoinScanCursor.objects.filter(pk=cursor.pk).update(
            last_safe_block=Greatest(
                F("last_safe_block"),
                max(0, latest_height - cursor.chain.confirm_block_count),
            ),
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )

    @staticmethod
    def _advance_cursor(
        *,
        cursor: BitcoinScanCursor,
        latest_height: int,
        scanned_to_block: int,
    ) -> None:
        BitcoinScanCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=Greatest(F("last_scanned_block"), scanned_to_block),
            last_safe_block=Greatest(
                F("last_safe_block"),
                max(0, latest_height - cursor.chain.confirm_block_count),
            ),
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )

    @staticmethod
    def _is_pruned_block_error(exc: BitcoinRpcError) -> bool:
        """判断 RPC 错误是否因为请求了已裁剪的区块数据。"""
        msg = str(exc).lower()
        return "pruned data" in msg or "block not available" in msg

    @staticmethod
    def _mark_cursor_error(*, cursor: BitcoinScanCursor, exc: Exception) -> None:
        BitcoinScanCursor.objects.filter(pk=cursor.pk).update(
            last_error=str(exc)[:255],
            last_error_at=timezone.now(),
            updated_at=timezone.now(),
        )

    @classmethod
    def _scan_block(
        cls,
        *,
        chain: Chain,
        block: BitcoinBlockInfo,
        watched_addresses: frozenset[str],
        client: BitcoinRpcClient,
        tx_cache: dict[str, BitcoinTxInfo],
    ) -> int:
        created_count = 0
        block_height = int(block.get("height", 0))
        block_time = int(block.get("time", 0))
        transactions = block.get("tx", []) or []

        for tx in transactions:
            sender_address = cls._resolve_sender_address(
                tx=tx,
                client=client,
                tx_cache=tx_cache,
            )
            if not sender_address:
                continue

            tx_hash = str(tx.get("txid", "")).lower()
            if not tx_hash:
                continue

            occurred_ts = int(tx.get("blocktime") or tx.get("time") or block_time)
            occurred_at = datetime.fromtimestamp(
                occurred_ts,
                tz=timezone.get_current_timezone(),
            )

            for output in tx.get("vout", []) or []:
                recipient_address = cls._extract_output_address(output)
                if recipient_address not in watched_addresses:
                    continue

                if not cls._should_track_output(
                    sender_address=sender_address,
                    recipient_address=recipient_address,
                ):
                    continue

                amount_btc = Decimal(str(output.get("value", "0")))
                if amount_btc <= 0:
                    continue

                result = TransferService.create_observed_transfer(
                    observed=ObservedTransferPayload(
                        chain=chain,
                        block=block_height,
                        tx_hash=tx_hash,
                        event_id=f"vout:{int(output.get('n', 0))}",
                        from_address=sender_address,
                        to_address=recipient_address,
                        crypto=chain.native_coin,
                        value=Decimal(btc_to_satoshi(amount_btc)),
                        amount=amount_btc,
                        timestamp=occurred_ts,
                        occurred_at=occurred_at,
                        source="bitcoin-core-scan",
                    )
                )
                if result.created:
                    created_count += 1

        return created_count

    @staticmethod
    def _should_track_output(
        *,
        sender_address: str,
        recipient_address: str,
    ) -> bool:
        # 砍掉充提后只需过滤自发自收；sender 为空串时自动放行
        return sender_address != recipient_address

    @classmethod
    def _resolve_sender_address(
        cls,
        *,
        tx: BitcoinTxInfo,
        client: BitcoinRpcClient,
        tx_cache: dict[str, BitcoinTxInfo],
    ) -> str | None:
        for vin in tx.get("vin", []) or []:
            if vin.get("coinbase"):
                return None

            prev_txid = vin.get("txid")
            prev_vout = vin.get("vout")
            if not prev_txid or prev_vout is None:
                continue

            prev_tx = tx_cache.get(prev_txid)
            if prev_tx is None:
                try:
                    prev_tx = client.get_raw_transaction(prev_txid)
                except BitcoinRpcError:
                    # 剪枝节点可能不保留历史交易数据，跳过该输入继续处理。
                    continue
                if prev_tx is None:
                    continue
                tx_cache[prev_txid] = prev_tx

            outputs = prev_tx.get("vout", []) or []
            if not (0 <= int(prev_vout) < len(outputs)):
                continue

            address = cls._extract_output_address(outputs[int(prev_vout)])
            if address:
                return address

        return None

    @staticmethod
    def _extract_output_address(output: BitcoinTxVout) -> str | None:
        script_pub_key = output.get("scriptPubKey", {}) or {}
        address = script_pub_key.get("address")
        if address:
            return str(address)

        addresses = script_pub_key.get("addresses") or []
        if addresses:
            return str(addresses[0])

        return None
