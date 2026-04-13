from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from currencies.models import ChainToken
from projects.models import RecipientAddressUsage
from projects.models import RecipientAddress
from tron.client import TronClientError
from tron.client import TronHttpClient
from tron.codec import TronAddressCodec
from tron.models import TronWatchCursor


@dataclass(frozen=True)
class TronScanSummary:
    filter_addresses: int
    blocks_scanned: int
    events_seen: int
    created_transfers: int


@dataclass(frozen=True)
class ParsedTronTransferEvent:
    observed: ObservedTransferPayload


class TronUsdtPaymentScanner:
    _debug_bootstrapped_cursors: set[tuple[int, str]] = set()

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
        filter_addresses = set(
            RecipientAddress.objects.filter(
                chain_type=chain.type,
                usage=RecipientAddressUsage.INVOICE,
            ).values_list("address", flat=True)
        )
        cursor = cls._get_or_create_cursor(
            chain=chain,
            contract_address=usdt_mapping.address,
        )
        created_transfers = 0
        events_seen = 0
        blocks_scanned = 0

        if cursor.enabled:
            try:
                cursor = cls._bootstrap_cursor_if_needed(
                    cursor=cursor,
                    latest_block=latest_block,
                )
                start_block = cursor.last_scanned_block + 1
                for block_number in range(start_block, latest_block + 1):
                    parsed_events = cls._collect_block_events(
                        client=client,
                        chain=chain,
                        block_number=block_number,
                        filter_addresses=filter_addresses,
                        usdt_mapping=usdt_mapping,
                    )
                    events_seen += len(parsed_events)
                    for event in parsed_events:
                        result = TransferService.create_observed_transfer(
                            observed=event.observed
                        )
                        if result.created:
                            created_transfers += 1
                    blocks_scanned += 1
                    cls._advance_cursor(
                        cursor=cursor,
                        latest_block=latest_block,
                        scanned_block=block_number,
                    )
            except TronClientError as exc:
                cls._mark_cursor_error(cursor=cursor, exc=exc)
                raise

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
            filter_addresses=len(filter_addresses),
            blocks_scanned=blocks_scanned,
            events_seen=events_seen,
            created_transfers=created_transfers,
        )

    @classmethod
    def _get_or_create_cursor(
        cls,
        *,
        chain: Chain,
        contract_address: str,
    ) -> TronWatchCursor:
        with transaction.atomic():
            cursor, _ = TronWatchCursor.objects.select_for_update().get_or_create(
                chain=chain,
                contract_address=contract_address,
                defaults={
                    "last_scanned_block": 0,
                    "last_safe_block": 0,
                    "enabled": True,
                },
            )
        return cursor

    @classmethod
    def _bootstrap_cursor_if_needed(
        cls,
        *,
        cursor: TronWatchCursor,
        latest_block: int,
    ) -> TronWatchCursor:
        debug_key = (cursor.chain_id, cursor.contract_address)
        should_reset = False

        if settings.DEBUG:
            if debug_key not in cls._debug_bootstrapped_cursors:
                cls._debug_bootstrapped_cursors.add(debug_key)
                should_reset = True
        elif cursor.last_scanned_block == 0:
            should_reset = True

        if not should_reset:
            return cursor

        TronWatchCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=latest_block,
            last_safe_block=latest_block,
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )
        cursor.last_scanned_block = latest_block
        cursor.last_safe_block = latest_block
        cursor.last_error = ""
        cursor.last_error_at = None
        return cursor

    @classmethod
    def _collect_block_events(
        cls,
        *,
        client: TronHttpClient,
        chain: Chain,
        block_number: int,
        filter_addresses: set[str],
        usdt_mapping: ChainToken,
    ) -> list[ParsedTronTransferEvent]:
        page_fingerprint: str | None = None
        collected: list[ParsedTronTransferEvent] = []
        seen_fingerprints: set[str] = set()

        while True:
            payload = client.list_confirmed_contract_events(
                contract_address=usdt_mapping.address,
                event_name="Transfer",
                block_number=block_number,
                fingerprint=page_fingerprint,
            )
            if not isinstance(payload, dict):
                raise TronClientError(
                    f"invalid contract events payload from {chain.code}"
                )
            data = payload.get("data")
            meta = payload.get("meta") or {}
            if data is None:
                data = []
            if not isinstance(data, list) or not isinstance(meta, dict):
                raise TronClientError(
                    f"invalid contract events payload from {chain.code}"
                )
            if not data:
                break

            for row in data:
                event = cls._parse_contract_event(
                    chain=chain,
                    row=row,
                    expected_block_number=block_number,
                    filter_addresses=filter_addresses,
                    usdt_mapping=usdt_mapping,
                )
                if event is not None:
                    collected.append(event)

            page_fingerprint = meta.get("fingerprint")
            if not page_fingerprint:
                break
            if not isinstance(page_fingerprint, str):
                raise TronClientError(
                    f"invalid contract events fingerprint from {chain.code}"
                )
            if page_fingerprint in seen_fingerprints:
                raise TronClientError(
                    f"duplicate contract events fingerprint from {chain.code}"
                )
            seen_fingerprints.add(page_fingerprint)

        return collected

    @classmethod
    def _parse_contract_event(
        cls,
        *,
        chain: Chain,
        row: dict,
        expected_block_number: int,
        filter_addresses: set[str],
        usdt_mapping: ChainToken,
    ) -> ParsedTronTransferEvent | None:
        if not isinstance(row, dict):
            return None

        tx_id = str(row.get("transaction_id") or "")
        raw_event_index = row.get("event_index")
        if raw_event_index in (None, ""):
            return None
        try:
            block_number = int(row.get("block_number") or 0)
            timestamp_ms = int(row.get("block_timestamp") or 0)
            event_index = int(raw_event_index)
        except (TypeError, ValueError):
            return None
        if not tx_id or not block_number or not timestamp_ms:
            return None
        if block_number != expected_block_number:
            return None
        if str(row.get("event_name") or "") != "Transfer":
            return None

        contract_address = str(row.get("contract_address") or "")
        if not contract_address:
            return None
        try:
            normalized_contract_address = cls._event_address_to_base58(
                contract_address
            )
        except ValueError:
            return None
        if normalized_contract_address != usdt_mapping.address:
            return None

        result = row.get("result") or {}
        if not isinstance(result, dict):
            return None

        try:
            from_address = cls._event_address_to_base58(result.get("from"))
            to_address = cls._event_address_to_base58(result.get("to"))
        except ValueError:
            return None

        if to_address not in filter_addresses:
            return None

        try:
            value = Decimal(str(result.get("value") or "0"))
        except Exception:  # noqa: BLE001
            return None
        if value <= 0:
            return None

        occurred_at = datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=timezone.get_current_timezone(),
        )
        decimals = (
            usdt_mapping.decimals
            if usdt_mapping.decimals is not None
            else usdt_mapping.crypto.decimals
        )
        return ParsedTronTransferEvent(
            observed=ObservedTransferPayload(
                chain=chain,
                block=block_number,
                tx_hash=tx_id,
                event_id=f"trc20:{event_index}",
                from_address=from_address,
                to_address=to_address,
                crypto=usdt_mapping.crypto,
                value=value,
                amount=value.scaleb(-decimals),
                timestamp=timestamp_ms // 1000,
                occurred_at=occurred_at,
                source="tron-scan",
            )
        )

    @staticmethod
    def _advance_cursor(
        *,
        cursor: TronWatchCursor,
        latest_block: int,
        scanned_block: int,
    ) -> None:
        target_block = min(scanned_block, latest_block)
        TronWatchCursor.objects.filter(pk=cursor.pk).update(
            last_scanned_block=max(cursor.last_scanned_block, target_block),
            last_safe_block=max(cursor.last_safe_block, target_block),
            last_error="",
            last_error_at=None,
            updated_at=timezone.now(),
        )
        cursor.last_scanned_block = max(cursor.last_scanned_block, target_block)
        cursor.last_safe_block = max(cursor.last_safe_block, target_block)
        cursor.last_error = ""
        cursor.last_error_at = None

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

    @classmethod
    def _event_address_to_base58(cls, value: object) -> str:
        raw_value = str(value or "").strip()
        if not raw_value:
            raise ValueError("empty tron event address")
        if TronAddressCodec.is_valid_base58(raw_value):
            return TronAddressCodec.normalize_base58(raw_value)

        normalized = cls._normalize_hex(raw_value)
        if len(normalized) == 40:
            normalized = f"{TronAddressCodec.ADDRESS_HEX_PREFIX}{normalized}"
        elif len(normalized) == 64:
            normalized = (
                f"{TronAddressCodec.ADDRESS_HEX_PREFIX}{normalized[-40:]}"
            )

        return TronAddressCodec.hex41_to_base58(normalized)
