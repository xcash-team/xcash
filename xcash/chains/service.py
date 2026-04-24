from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog
from django.db import IntegrityError
from django.db import transaction

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import Chain
from chains.models import ChainType
from chains.models import ConfirmMode
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.models import TransferType
from chains.models import Wallet

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal

    from django.db.models import QuerySet

    from currencies.models import Crypto

logger = structlog.get_logger()


class ChainService:
    """Read-only accessors for chain metadata."""

    @staticmethod
    def get_active_chains() -> QuerySet[Chain]:
        return Chain.objects.filter(active=True)

    @staticmethod
    def get_by_code(code: str, *, active_only: bool = True) -> Chain:
        qs = Chain.objects.filter(code=code)
        if active_only:
            qs = qs.filter(active=True)
        return qs.get()

    @staticmethod
    def get_by_id(chain_id: int) -> Chain:
        return Chain.objects.get(id=chain_id)

    @staticmethod
    def codes_of_types(chain_types: set[str]) -> set[str]:
        return set(
            Chain.objects.filter(type__in=chain_types, active=True).values_list(
                "code", flat=True
            )
        )


class WalletService:
    """Helpers around wallet and HD address derivation."""

    @staticmethod
    def generate_wallet() -> Wallet:
        return Wallet.generate()

    @staticmethod
    def ensure_address(
        wallet: Wallet,
        chain_type: ChainType | str,
        usage: AddressUsage,
        address_index: int = 0,
    ) -> Address:
        return wallet.get_address(
            chain_type=chain_type,
            usage=usage,
            address_index=address_index,
        )


class AddressService:
    """Query and mutation helpers for chain addresses."""

    @staticmethod
    def find_by_address(
        *,
        address: str,
        chain_type: ChainType | str | None = None,
        usage: AddressUsage | str | None = None,
    ) -> Address | None:
        qs = Address.objects.filter(address=address)
        if chain_type:
            qs = qs.filter(chain_type=chain_type)
        if usage:
            qs = qs.filter(usage=usage)
        return qs.first()

    @staticmethod
    def get_by_address(
        *,
        address: str,
        chain_type: ChainType | str | None = None,
        usage: AddressUsage | str | None = None,
    ) -> Address:
        qs = Address.objects.filter(address=address)
        if chain_type:
            qs = qs.filter(chain_type=chain_type)
        if usage:
            qs = qs.filter(usage=usage)
        return qs.get()



@dataclass(frozen=True)
class ObservedTransferPayload:
    """统一描述"从链上观察到的一条转账事件"。

    不同扫描器或外部观察器看到的本质都是同一类事件。
    先把输入模型收口，后续新增扫描器时只需负责解析，不再重复定义 Transfer 落库字段。
    """

    chain: Chain
    block: int
    tx_hash: str
    event_id: str
    from_address: str
    to_address: str
    crypto: Crypto
    value: Decimal
    amount: Decimal
    timestamp: int
    occurred_at: datetime
    source: str = "observer"


@dataclass(frozen=True)
class ObservedTransferCreateResult:
    """统一描述链上转账落库结果。

    created=True:
        本次首次创建成功
    created=False & conflict=False:
        幂等重放，已存在且内容一致
    created=False & conflict=True:
        命中了相同唯一键，但关键内容不一致，需要上层记录异常
    """

    transfer: OnchainTransfer | None
    created: bool
    conflict: bool = False


class TransferService:
    """Centralized mutators for OnchainTransfer to limit cross-app coupling."""

    @staticmethod
    def enqueue_processing(transfer: OnchainTransfer) -> None:
        """在事务提交后异步处理 OnchainTransfer，替代隐式 post_save signal。"""
        from chains.tasks import process_transfer

        transaction.on_commit(
            lambda transfer_id=transfer.pk: process_transfer.apply_async(
                (transfer_id,), countdown=1
            )
        )

    @staticmethod
    def _mark_broadcast_task_pending_confirm(*, chain: Chain, tx_hash: str) -> None:
        # 一旦链上已经观察到真实交易，统一父任务就进入"确认中"阶段。
        BroadcastTask.mark_pending_confirm(chain=chain, tx_hash=tx_hash)

    @staticmethod
    def _build_observed_transfer_kwargs(
        observed: ObservedTransferPayload,
    ) -> dict[str, object]:
        return {
            "chain": observed.chain,
            "block": observed.block,
            "hash": observed.tx_hash,
            "event_id": observed.event_id,
            "from_address": observed.from_address,
            "to_address": observed.to_address,
            "crypto": observed.crypto,
            "value": observed.value,
            "amount": observed.amount,
            "timestamp": observed.timestamp,
            "datetime": observed.occurred_at,
        }

    @staticmethod
    def _compare_observed_transfer(
        existing: OnchainTransfer,
        observed: ObservedTransferPayload,
    ) -> tuple[bool, dict[str, tuple[object, object]]]:
        compared_values = {
            "crypto_id": (existing.crypto_id, observed.crypto.id),
            "from_address": (existing.from_address, observed.from_address),
            "to_address": (existing.to_address, observed.to_address),
            "value": (existing.value, observed.value),
        }
        differences = {
            field: vals for field, vals in compared_values.items() if vals[0] != vals[1]
        }
        return not differences, differences

    @staticmethod
    def _refresh_observed_transfer_chain_position(
        existing: OnchainTransfer,
        observed: ObservedTransferPayload,
    ) -> None:
        if observed.block < existing.block:
            logger.warning(
                "Observed transfer replay with older block ignored",
                source=observed.source,
                chain=observed.chain.code,
                tx_hash=observed.tx_hash,
                event_id=observed.event_id,
                existing_transfer_id=existing.pk,
                existing_block=existing.block,
                incoming_block=observed.block,
            )
            return

        update_fields = []
        if existing.block != observed.block:
            existing.block = observed.block
            update_fields.append("block")
        if existing.timestamp != observed.timestamp:
            existing.timestamp = observed.timestamp
            update_fields.append("timestamp")
        if existing.datetime != observed.occurred_at:
            existing.datetime = observed.occurred_at
            update_fields.append("datetime")
        if not update_fields:
            return

        OnchainTransfer.objects.filter(pk=existing.pk).update(
            **{field: getattr(existing, field) for field in update_fields}
        )

    @staticmethod
    def _log_observed_transfer_conflict(
        *,
        existing: OnchainTransfer,
        observed: ObservedTransferPayload,
        differences: dict[str, tuple[object, object]],
    ) -> None:
        logger.error(
            "Observed transfer conflict",
            source=observed.source,
            chain=observed.chain.code,
            tx_hash=observed.tx_hash,
            event_id=observed.event_id,
            existing_transfer_id=existing.pk,
            differences={
                field_name: {
                    "existing": str(existing_value),
                    "incoming": str(incoming_value),
                }
                for field_name, (existing_value, incoming_value) in differences.items()
            },
        )

    @staticmethod
    def create_observed_transfer(
        *,
        observed: ObservedTransferPayload,
    ) -> ObservedTransferCreateResult:
        """统一入口：将"外部服务商 / 内部扫描"观察到的链上转账写入 OnchainTransfer。

        OnchainTransfer 创建不能分散在各链 provider/scanner 内部各写各的。
        后续无论是 EVM 自扫、Bitcoin 自扫还是其他链监听，都应通过这个入口落库，
        以统一幂等语义、唯一键冲突判定和后续扩展能力。
        """
        create_kwargs = TransferService._build_observed_transfer_kwargs(observed)
        try:
            # 唯一键冲突会触发 IntegrityError；用内层 savepoint 包住，避免外层事务直接进入 broken 状态。
            with transaction.atomic():
                transfer = OnchainTransfer.objects.create(**create_kwargs)
            # 只有首次真正落库成功的观测转账才需要派发一次业务处理任务。
            TransferService.enqueue_processing(transfer)
            TransferService._mark_broadcast_task_pending_confirm(
                chain=observed.chain,
                tx_hash=observed.tx_hash,
            )
            return ObservedTransferCreateResult(transfer=transfer, created=True)
        except IntegrityError:
            existing = OnchainTransfer.objects.filter(
                chain=observed.chain,
                hash=observed.tx_hash,
                event_id=observed.event_id,
            ).first()
            if existing is None:
                logger.warning(
                    "Observed transfer integrity conflict without existing row",
                    source=observed.source,
                    chain=observed.chain.code,
                    tx_hash=observed.tx_hash,
                    event_id=observed.event_id,
                )
                return ObservedTransferCreateResult(
                    transfer=None,
                    created=False,
                    conflict=True,
                )

            TransferService._mark_broadcast_task_pending_confirm(
                chain=observed.chain,
                tx_hash=observed.tx_hash,
            )

            is_same_transfer, differences = TransferService._compare_observed_transfer(
                existing=existing,
                observed=observed,
            )
            if is_same_transfer:
                TransferService._refresh_observed_transfer_chain_position(
                    existing=existing,
                    observed=observed,
                )
                logger.debug(
                    "Observed transfer replay ignored",
                    source=observed.source,
                    chain=observed.chain.code,
                    tx_hash=observed.tx_hash,
                    event_id=observed.event_id,
                    transfer_id=existing.pk,
                )
            else:
                TransferService._log_observed_transfer_conflict(
                    existing=existing,
                    observed=observed,
                    differences=differences,
                )
            return ObservedTransferCreateResult(
                transfer=existing,
                created=False,
                conflict=not is_same_transfer,
            )

    @staticmethod
    def assign_type_and_mode(
        transfer: OnchainTransfer,
        transfer_type: TransferType,
        confirm_mode: ConfirmMode,
    ) -> OnchainTransfer:
        transfer.type = transfer_type
        transfer.confirm_mode = confirm_mode
        transfer.save(update_fields=["type", "confirm_mode"])
        return transfer

    @staticmethod
    def mark_confirmed(transfer: OnchainTransfer) -> OnchainTransfer:
        if transfer.status == TransferStatus.CONFIRMED:
            return transfer
        transfer.status = TransferStatus.CONFIRMED
        transfer.save(update_fields=["status"])
        return transfer
