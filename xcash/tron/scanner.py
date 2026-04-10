from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from currencies.models import ChainToken
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0
from projects.models import RecipientAddress
from tron.client import TronClientError
from tron.client import TronHttpClient
from tron.codec import TronAddressCodec
from tron.models import TronWatchCursor


@dataclass(frozen=True)
class TronScanSummary:
    addresses_scanned: int
    events_seen: int
    created_transfers: int


@dataclass(frozen=True)
class ParsedTronTransferEvent:
    fingerprint: str
    observed: ObservedTransferPayload


class TronUsdtPaymentScanner:
    @classmethod
    def scan_chain(cls, *, chain: Chain) -> TronScanSummary:
        if chain.type != ChainType.TRON:
            raise ValueError(f"仅支持 Tron 链扫描，当前链为 {chain.code}")

        client = TronHttpClient(chain=chain)
        previous_latest_block = chain.latest_block_number
        latest_block = client.get_latest_solid_block_number()
        Chain.objects.filter(pk=chain.pk).update(latest_block_number=latest_block)

        usdt_mapping = (
            ChainToken.objects.select_related("crypto")
            .filter(
                chain=chain,
                crypto__symbol="USDT",
                crypto__active=True,
            )
            .get()
        )
        watch_addresses = list(
            RecipientAddress.objects.filter(
                chain_type=chain.type,
                used_for_invoice=True,
            ).values_list("address", flat=True)
        )
        created_transfers = 0
        events_seen = 0

        for watch_address in watch_addresses:
            cursor = cls._get_or_create_cursor(
                chain=chain,
                watch_address=watch_address,
            )
            if not cursor.enabled:
                continue

            try:
                parsed_events = cls._collect_new_events(
                    client=client,
                    chain=chain,
                    cursor=cursor,
                    watch_address=watch_address,
                    usdt_mapping=usdt_mapping,
                )
            except TronClientError as exc:
                cls._mark_cursor_error(cursor=cursor, exc=exc)
                raise

            events_seen += len(parsed_events)
            for event in reversed(parsed_events):
                result = TransferService.create_observed_transfer(observed=event.observed)
                if result.created:
                    created_transfers += 1

            cls._advance_cursor(
                cursor=cursor,
                latest_block=latest_block,
                parsed_events=parsed_events,
            )

        if (
            latest_block > previous_latest_block
            and OnchainTransfer.objects.filter(
                chain=chain,
                status=TransferStatus.CONFIRMING,
                processed_at__isnull=False,
            ).exists()
        ):
            from chains.tasks import block_number_updated

            block_number_updated.apply_async(args=(chain.pk,), countdown=2)

        return TronScanSummary(
            addresses_scanned=len(watch_addresses),
            events_seen=events_seen,
            created_transfers=created_transfers,
        )

    @classmethod
    def _get_or_create_cursor(
        cls,
        *,
        chain: Chain,
        watch_address: str,
    ) -> TronWatchCursor:
        with transaction.atomic():
            cursor, _ = TronWatchCursor.objects.select_for_update().get_or_create(
                chain=chain,
                watch_address=watch_address,
                defaults={
                    "last_scanned_block": 0,
                    "last_safe_block": 0,
                    "enabled": True,
                },
            )
        return cursor

    @classmethod
    def _collect_new_events(
        cls,
        *,
        client: TronHttpClient,
        chain: Chain,
        cursor: TronWatchCursor,
        watch_address: str,
        usdt_mapping: ChainToken,
    ) -> list[ParsedTronTransferEvent]:
        page_fingerprint: str | None = None
        collected: list[ParsedTronTransferEvent] = []
        reached_cursor = False

        while not reached_cursor:
            payload = client.list_confirmed_trc20_history(
                address=watch_address,
                contract_address=usdt_mapping.address,
                fingerprint=page_fingerprint,
            )
            page_rows = payload.get("data") or []
            if not page_rows:
                break

            for row in page_rows:
                tx_id = str(row.get("transaction_id") or "")
                if not tx_id:
                    continue
                tx_info = client.get_transaction_info_by_id(tx_id)
                for event in cls._parse_tx_info(
                    chain=chain,
                    tx_info=tx_info,
                    watch_address=watch_address,
                    usdt_mapping=usdt_mapping,
                ):
                    if event.fingerprint == cursor.last_event_fingerprint:
                        reached_cursor = True
                        break
                    collected.append(event)
                if reached_cursor:
                    break

            page_fingerprint = (payload.get("meta") or {}).get("fingerprint")
            if not page_fingerprint:
                break

        return collected

    @staticmethod
    def _parse_tx_info(
        *,
        chain: Chain,
        tx_info: dict,
        watch_address: str,
        usdt_mapping: ChainToken,
    ) -> list[ParsedTronTransferEvent]:
        receipt = tx_info.get("receipt") or {}
        if receipt.get("result") != "SUCCESS":
            return []

        block_number = int(tx_info.get("blockNumber") or 0)
        timestamp_ms = int(tx_info.get("blockTimeStamp") or 0)
        tx_id = str(tx_info.get("id") or "")
        if not block_number or not timestamp_ms or not tx_id:
            return []

        occurred_at = datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=timezone.get_current_timezone(),
        )
        target_contract_hex = TronAddressCodec.base58_to_hex41(usdt_mapping.address)[
            2:
        ].lower()
        decimals = (
            usdt_mapping.decimals
            if usdt_mapping.decimals is not None
            else usdt_mapping.crypto.decimals
        )
        events: list[ParsedTronTransferEvent] = []

        for log_index, log in enumerate(tx_info.get("log") or []):
            if TronUsdtPaymentScanner._normalize_hex(log.get("address")) != target_contract_hex:
                continue

            topics = list(log.get("topics") or [])
            if len(topics) < 3:
                continue
            if TronUsdtPaymentScanner._normalize_hex(topics[0]) != TronUsdtPaymentScanner._normalize_hex(ERC20_TRANSFER_TOPIC0):
                continue

            try:
                from_address = TronAddressCodec.topic_to_base58(str(topics[1]))
                to_address = TronAddressCodec.topic_to_base58(str(topics[2]))
            except ValueError:
                continue

            if to_address != watch_address:
                continue

            raw_value_hex = TronUsdtPaymentScanner._normalize_hex(log.get("data")) or "0"
            value = Decimal(int(raw_value_hex, 16))
            if value <= 0:
                continue

            events.append(
                ParsedTronTransferEvent(
                    fingerprint=f"{tx_id}:{log_index}",
                    observed=ObservedTransferPayload(
                        chain=chain,
                        block=block_number,
                        tx_hash=tx_id,
                        event_id=f"trc20:{log_index}",
                        from_address=from_address,
                        to_address=to_address,
                        crypto=usdt_mapping.crypto,
                        value=value,
                        amount=Decimal(value).scaleb(-decimals),
                        timestamp=timestamp_ms // 1000,
                        occurred_at=occurred_at,
                        source="tron-scan",
                    ),
                )
            )

        return events

    @staticmethod
    def _advance_cursor(
        *,
        cursor: TronWatchCursor,
        latest_block: int,
        parsed_events: list[ParsedTronTransferEvent],
    ) -> None:
        TronWatchCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=max(cursor.last_scanned_block, latest_block),
            last_safe_block=max(cursor.last_safe_block, latest_block),
            last_event_fingerprint=(
                parsed_events[0].fingerprint
                if parsed_events
                else cursor.last_event_fingerprint
            ),
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )

    @staticmethod
    def _mark_cursor_error(*, cursor: TronWatchCursor, exc: Exception) -> None:
        TronWatchCursor.objects.filter(pk=cursor.pk).update(
            last_error=str(exc)[:255],
            last_error_at=timezone.now(),
            updated_at=timezone.now(),
        )

    @staticmethod
    def _normalize_hex(value: object) -> str:
        return str(value or "").strip().lower().removeprefix("0x")

