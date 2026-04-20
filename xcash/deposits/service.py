from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import structlog
from django.db import transaction as db_transaction
from django.utils import timezone

from chains.adapters import AdapterFactory  # noqa: F401
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from common.internal_callback import send_internal_callback
from common.utils.math import format_decimal_stripped
from deposits.exceptions import DepositStatusError
from deposits.models import CollectSchedule
from deposits.models import Deposit
from deposits.models import DepositAddress
from deposits.models import DepositCollection
from deposits.models import DepositStatus
from deposits.models import GasRecharge
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage
from webhooks.service import WebhookService

logger = structlog.get_logger()


class GasRechargeService:
    """Vault → 充币地址的 gas 预充服务。

    抽离职责：把"向充币地址补 native gas"的完整动作收敛成一个幂等方法，
    当前由 EVM 广播 pre-flight（EvmBroadcastTask.broadcast）统一调用。
    任何调用方都不应自行拼装 GasRecharge 记录或 Vault 交易调度。
    """

    @staticmethod
    def request_recharge(
        *,
        deposit_address: DepositAddress,
        chain,
        erc20_gas_cost: int,
    ) -> bool:
        """幂等地从 Vault 发起一笔 native 币补充到 deposit_address。

        入参：
        - deposit_address: 目标 DepositAddress 记录（对应某客户某链上的充币地址）。
        - chain: Chain 对象，需能取到 native_coin、wallet 及单次转账 gas 消耗。
        - erc20_gas_cost: 由调用方基于当前 gas_price 预估的单次 ERC-20 转账
          总 gas 成本（wei）。

        补给量固定为 10 × erc20_gas_cost，足够支撑后续约 10 次 ERC-20 归集，
        避免为单次归集反复触发补给。

        返回：
        - True 表示已成功创建一笔 Vault → 地址的 gas 补充任务，或已有尚未广播的
          pending GasRecharge（幂等跳过，代表本轮请求"已经生效"）。
        - False 表示 gas 参数非法（<= 0）或补充交易调度失败，本轮无法补充。

        并发幂等：同地址若已存在 stage=QUEUED 且 result=UNKNOWN 的
        GasRecharge（尚未上链且尚未到账），直接返回 True 跳过新建。
        已广播或已终局（result != UNKNOWN 或 stage 流转到后续阶段）的
        GasRecharge 不阻塞新请求，视作历史补充已完成或失败，需要再起一笔新的。
        """
        recharge_raw = 10 * erc20_gas_cost
        if recharge_raw <= 0:
            return False

        # 防重复：用广播任务的语义状态判定"尚未上链的 gas 补充"。
        # 历史实现曾用 tx_hash="" 过滤，但 BroadcastTask.tx_hash 是 HashField(null=True)，
        # 默认落 NULL 而非空串，导致过滤永远空集、幂等失效。
        has_pending_recharge = GasRecharge.objects.filter(
            deposit_address=deposit_address,
            recharged_at__isnull=True,
            broadcast_task__stage=BroadcastTaskStage.QUEUED,
            broadcast_task__result=BroadcastTaskResult.UNKNOWN,
        ).exists()
        if has_pending_recharge:
            return True

        vault_addr = deposit_address.customer.project.wallet.get_address(
            chain_type=chain.type,
            usage=AddressUsage.Vault,
        )
        try:
            from evm.models import EvmBroadcastTask

            task = EvmBroadcastTask.schedule_transfer(
                address=vault_addr,
                chain=chain,
                crypto=chain.native_coin,
                to=deposit_address.address.address,
                value_raw=recharge_raw,
                transfer_type=TransferType.GasRecharge,
            )
            GasRecharge.objects.create(
                deposit_address=deposit_address,
                broadcast_task=task.base_task,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "Gas 补充交易调度失败",
                deposit_address_id=deposit_address.pk,
                chain=chain.code,
            )
            return False
        return True


class DepositService:
    """High level orchestration around deposit lifecycle and collection."""

    @staticmethod
    def build_webhook_payload(
        deposit: Deposit, *, confirmed: bool | None = None
    ) -> dict:
        """统一构造 deposit webhook payload，避免业务层各自拼装。"""
        if confirmed is None:
            confirmed = deposit.status == DepositStatus.COMPLETED

        customer = getattr(deposit, "customer", None)
        return {
            "type": "deposit",
            "data": {
                "sys_no": deposit.sys_no,
                "uid": customer.uid if customer else None,
                "chain": deposit.transfer.chain.code,
                "block": deposit.transfer.block,
                "hash": deposit.transfer.hash,
                "crypto": deposit.transfer.crypto.symbol,
                "amount": format_decimal_stripped(deposit.transfer.amount),
                "confirmed": confirmed,
            },
        }

    @staticmethod
    def refresh_worth(deposit: Deposit) -> None:
        """显式计算 Deposit worth，避免继续依赖 post_save signal。"""
        try:
            worth = deposit.transfer.crypto.usd_amount(deposit.transfer.amount)
        except Exception:
            logger.exception(
                "calculate_worth 失败，worth 保持默认值 0", deposit_id=deposit.pk
            )
            return

        Deposit.objects.filter(pk=deposit.pk).update(
            worth=worth,
            updated_at=timezone.now(),
        )
        deposit.worth = worth

    @classmethod
    def _notify(cls, deposit: Deposit, status: str) -> None:
        """发送 deposit webhook 通知，统一经 service builder 生成 payload。"""
        payload = cls.build_webhook_payload(
            deposit, confirmed=status == DepositStatus.COMPLETED
        )
        try:
            WebhookService.create_event(
                project=deposit.customer.project, payload=payload
            )
        except Exception:
            logger.exception("发送充币 webhook 通知失败", deposit_id=deposit.pk)

    @classmethod
    def _pre_notify(cls, deposit: Deposit) -> None:
        # 预通知：链上刚出块，尚未达到确认数。
        if deposit.customer.project.pre_notify:
            cls._notify(deposit, DepositStatus.CONFIRMING)

    @classmethod
    def notify_completed(cls, deposit: Deposit) -> None:
        cls._notify(deposit, DepositStatus.COMPLETED)

    @classmethod
    def initialize_deposit(cls, deposit: Deposit) -> Deposit:
        """显式执行 Deposit 创建后的初始化。"""
        cls.refresh_worth(deposit)
        cls._pre_notify(deposit)
        return deposit

    @classmethod
    def try_create_deposit(cls, transfer: OnchainTransfer) -> bool:
        # inactive 占位币允许生成 OnchainTransfer 以便统计余额，但不能继续进入商户充值业务流。
        if not transfer.crypto.active:
            return False

        try:
            customer = DepositAddress.objects.get(
                chain_type=transfer.chain.type,
                address__address=transfer.to_address,
            ).customer
        except DepositAddress.DoesNotExist:
            return False

        transfer.type = TransferType.Deposit
        transfer.save(update_fields=["type"])

        deposit = Deposit.objects.create(
            customer=customer,
            transfer=transfer,
            status=DepositStatus.CONFIRMING,
        )
        cls.initialize_deposit(deposit)
        return True

    @classmethod
    @db_transaction.atomic
    def _transition_status(cls, deposit: Deposit, target: str) -> bool:
        """
        加行锁执行状态转换：CONFIRMING -> target。

        并发安全：select_for_update 防止重复确认。
        幂等：已处于目标状态则返回 False（跳过），非 CONFIRMING 则抛异常。
        """
        Deposit.objects.select_for_update().filter(pk=deposit.pk).first()
        deposit.refresh_from_db()

        if deposit.status == target:
            return False
        if deposit.status != DepositStatus.CONFIRMING:
            raise DepositStatusError("Deposit status must be CONFIRMING")

        deposit.status = target
        deposit.save(update_fields=["status", "updated_at"])
        return True

    @classmethod
    def confirm_deposit(cls, deposit: Deposit) -> None:
        if cls._transition_status(deposit, DepositStatus.COMPLETED):
            cls.notify_completed(deposit)
            send_internal_callback(
                event="deposit.confirmed",
                appid=deposit.customer.project.appid,
                sys_no=deposit.sys_no,
                worth=str(deposit.worth),
                currency=deposit.transfer.crypto.symbol,
            )
            cls.schedule_collection_after_confirm(deposit)

    @classmethod
    @db_transaction.atomic
    def drop_deposit(cls, deposit: Deposit) -> None:
        """删除 CONFIRMING 状态的充值记录，释放数据以便 reorg 后扫描器自然重建。"""
        if not Deposit.objects.select_for_update().filter(pk=deposit.pk).exists():
            return  # 已删除，幂等跳过
        deposit.refresh_from_db()
        if deposit.status != DepositStatus.CONFIRMING:
            raise DepositStatusError("Deposit status must be CONFIRMING")
        deposit.delete()

    @classmethod
    def prepare_collection(cls, deposit: Deposit) -> dict | None:
        """
        归集准备阶段（事务外调用）：读取候选组并生成 execute_collection 所需参数。

        本方法不加数据库锁，execute_collection 会在事务内对候选组再次加锁校验，
        避免 prepare 与 execute 之间的状态漂移引起重复归集。

        返回 dict 表示归集参数；返回 None 表示无需归集。
        """
        grouped_deposits, deposit = cls._resolve_collection_group(deposit)
        if deposit is None:
            return None
        return cls._build_collection_params(grouped_deposits, deposit)

    @classmethod
    @db_transaction.atomic
    def execute_collection(cls, params: dict) -> bool:
        """
        归集执行阶段（事务内调用）：加锁再校验 + 原子创建 BroadcastTask、
        DepositCollection、并绑定 Deposit.collection 外键。

        事务内只做本地 DB 操作（不做链上 RPC），保证短事务下的行锁持有时间。
        若任一步失败，整个事务回滚，不留下半成品状态。
        若加锁再校验时发现候选组已被并发处理或状态漂移，本轮放弃（返回 False），
        由下一轮 gather_deposits 重新扫描发起。
        """
        from evm.models import EvmBroadcastTask

        expected_ids = set(params["group_ids"])
        locked_ids = cls._lock_pending_group_ids(params["group_ids"])
        if locked_ids != expected_ids:
            return False

        decimals = params["crypto"].get_decimals(params["chain"])
        value_raw = int(params["amount"] * Decimal(10**decimals))
        task = EvmBroadcastTask.schedule_transfer(
            address=params["address"],
            crypto=params["crypto"],
            chain=params["chain"],
            to=params["recipient_address"],
            value_raw=value_raw,
            transfer_type=TransferType.DepositCollection,
        )
        collection = DepositCollection.objects.create(
            collection_hash=None,
            broadcast_task=task.base_task,
        )
        Deposit.objects.filter(pk__in=params["group_ids"]).update(
            collection=collection,
            updated_at=timezone.now(),
        )
        return True

    @classmethod
    def collect_deposit(cls, deposit: Deposit) -> bool:
        """直接基于某笔 Deposit 的分组创建一笔归集任务。"""
        try:
            params = cls.prepare_collection(deposit)
            if params is None:
                return False
            return cls.execute_collection(params)
        except Exception:  # noqa: BLE001
            logger.exception(
                "归集任务创建失败",
                deposit_id=getattr(deposit, "id", None) or getattr(deposit, "pk", None),
            )
            return False

    @classmethod
    def schedule_collection_after_confirm(cls, deposit: Deposit) -> None:
        now = timezone.now()
        cls._upsert_collect_schedule(
            deposit=deposit,
            next_collect_time=now
            + timedelta(minutes=deposit.customer.project.gather_period),
        )

    @classmethod
    def schedule_collection_after_failure(cls, deposit: Deposit) -> None:
        cls._upsert_collect_schedule(
            deposit=deposit,
            next_collect_time=timezone.now()
            + timedelta(minutes=deposit.customer.project.gather_period),
        )

    @classmethod
    def collect_due_schedule(cls, schedule_id: int) -> bool:
        with db_transaction.atomic():
            schedule = (
                CollectSchedule.objects.select_related(
                    "deposit_address__customer__project",
                    "chain",
                    "crypto",
                )
                .select_for_update(skip_locked=True)
                .filter(pk=schedule_id)
                .first()
            )
            if schedule is None:
                return False

            grouped_deposits = cls._snapshot_schedule_collectible_group(schedule)
            if not grouped_deposits:
                schedule.delete()
                return False

            amount = cls._calculate_collection_amount(grouped_deposits)
            project = schedule.deposit_address.customer.project
            if not cls._should_collect_due_group(
                project=project,
                crypto=schedule.crypto,
                collection_amount=amount,
            ):
                schedule.delete()
                return False

            params = cls._build_collection_params(
                grouped_deposits,
                grouped_deposits[0],
            )
            if params is None:
                return False

            collected = cls.execute_collection(params)
            if collected:
                schedule.delete()
            return collected

    @classmethod
    def _resolve_collection_group(
        cls, deposit: Deposit
    ) -> tuple[list[Deposit], Deposit | None]:
        """
        解析同客户同链同币的待归集分组，返回 (grouped_deposits, representative_deposit)。
        representative_deposit 为 None 表示无需归集。
        """
        if not deposit.pk:
            logger.warning("_resolve_collection_group 收到未持久化实例，跳过")
            return [], None

        grouped = cls._snapshot_collectible_group(deposit)
        if not grouped:
            return [], None
        return grouped, grouped[0]

    @staticmethod
    def _calculate_collection_amount(grouped_deposits: list[Deposit]) -> Decimal:
        """归集金额 = 分组内所有充值金额之和，保证对账一致：充多少归多少。"""
        return sum((d.transfer.amount for d in grouped_deposits), Decimal("0"))

    @classmethod
    def _build_collection_params(
        cls, grouped_deposits: list[Deposit], deposit: Deposit
    ) -> dict | None:
        chain = deposit.transfer.chain
        crypto = deposit.transfer.crypto
        project = deposit.customer.project

        recipient = cls._select_recipient(project_id=project.id, chain_type=chain.type)
        if recipient is None:
            return None

        deposit_addr = DepositAddress.objects.get(
            customer=deposit.customer,
            chain_type=chain.type,
        ).address
        amount = cls._calculate_collection_amount(grouped_deposits)
        return {
            "group_ids": [item.pk for item in grouped_deposits],
            "address": deposit_addr,
            "crypto": crypto,
            "chain": chain,
            "recipient_address": recipient.address,
            "amount": amount,
            "deposit_id": deposit.id,
        }

    @classmethod
    def _upsert_collect_schedule(
        cls,
        *,
        deposit: Deposit,
        next_collect_time,
    ) -> tuple[CollectSchedule, bool]:
        deposit_address = DepositAddress.objects.get(
            customer=deposit.customer,
            chain_type=deposit.transfer.chain.type,
        )
        return CollectSchedule.objects.update_or_create(
            deposit_address=deposit_address,
            chain=deposit.transfer.chain,
            crypto=deposit.transfer.crypto,
            defaults={"next_collect_time": next_collect_time},
        )

    @staticmethod
    def try_match_gas_recharge(
        transfer: OnchainTransfer, broadcast_task: BroadcastTask
    ) -> bool:
        """通过 BroadcastTask 识别 Vault → 充币地址的 Gas 补充转账，并关联到 GasRecharge 记录。"""
        transfer.type = TransferType.GasRecharge
        transfer.save(update_fields=["type"])

        # 将链上转账关联到 GasRecharge 审计记录
        GasRecharge.objects.filter(
            broadcast_task=broadcast_task,
            transfer__isnull=True,
        ).update(transfer=transfer, updated_at=timezone.now())
        return True

    @classmethod
    @db_transaction.atomic
    def try_match_collection(
        cls,
        transfer: OnchainTransfer,
        broadcast_task: BroadcastTask,
    ) -> bool:
        """通过 BroadcastTask 将链上归集转账与 DepositCollection 记录关联。"""
        collection = (
            DepositCollection.objects.select_for_update()
            .filter(broadcast_task=broadcast_task)
            .first()
        )
        if collection is None:
            return False

        transfer.type = TransferType.DepositCollection
        transfer.save(update_fields=["type"])

        collection.collection_hash = transfer.hash
        collection.transfer = transfer
        collection.save(update_fields=["collection_hash", "transfer", "updated_at"])
        return True

    @staticmethod
    @db_transaction.atomic
    def confirm_collection(collection: DepositCollection) -> None:
        """归集交易确认：标记整组充币已归集完成。"""
        # 加行锁后重新读取，防止并发重复确认。
        collection = DepositCollection.objects.select_for_update().get(pk=collection.pk)
        # 幂等：已确认则跳过
        if collection.collected_at:
            return
        collection.collected_at = timezone.now()
        collection.save(update_fields=["collected_at", "updated_at"])

    @staticmethod
    @db_transaction.atomic
    def drop_collection(collection: DepositCollection) -> None:
        """
        归集链上观测失效：保留固定关系，仅清空链上观测字段以等待同一任务重试/重播。

        适用场景：Transfer.drop() 触发的 reorg 场景，此时同一 BroadcastTask
        会被 reset_to_pending_chain 重新广播；只需清空链上观测字段即可由
        try_match_collection 在新 transfer 上链后回写。
        """
        collection = DepositCollection.objects.select_for_update().get(pk=collection.pk)
        if (
            collection.collection_hash is None
            and collection.transfer_id is None
            and collection.collected_at is None
        ):
            return
        collection.collection_hash = None
        collection.transfer = None
        collection.collected_at = None
        collection.save(
            update_fields=["collection_hash", "transfer", "collected_at", "updated_at"]
        )

    @classmethod
    @db_transaction.atomic
    def release_failed_collection(cls, *, broadcast_task) -> None:
        """BroadcastTask 终态失败时调用：解绑 deposits、删除 collection、重建 schedule。"""
        collection = (
            DepositCollection.objects.select_for_update()
            .filter(broadcast_task=broadcast_task)
            .first()
        )
        if collection is None:
            return
        released_deposits = list(
            collection.deposits.select_related(
                "customer__project",
                "transfer__chain",
                "transfer__crypto",
            )
        )
        collection.deposits.update(collection=None, updated_at=timezone.now())
        collection.delete()
        for deposit in released_deposits:
            cls.schedule_collection_after_failure(deposit)

    @staticmethod
    def _select_recipient(*, project_id: int, chain_type: ChainType | str):
        return (
            RecipientAddress.objects.filter(
                project_id=project_id,
                chain_type=chain_type,
                usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
            )
            .order_by("id")
            .first()
        )

    @staticmethod
    def _to_amount(raw_value: int, decimals: int) -> Decimal:
        return Decimal(raw_value).scaleb(-decimals)

    @classmethod
    def _should_collect_due_group(
        cls, *, project, crypto, collection_amount: Decimal
    ) -> bool:
        if project.gather_worth == 0:
            return True
        try:
            worth = collection_amount * crypto.price("USD")
        except KeyError:
            logger.warning(
                "缺少代币价格，直接触发归集",
                crypto=crypto.symbol,
            )
            return True
        return worth >= project.gather_worth

    @staticmethod
    def _lock_pending_group_ids(group_ids: list[int]) -> set[int]:
        """事务内对候选组加 select_for_update(skip_locked=True) 校验。

        返回仍满足'待归集'条件（status=COMPLETED & collection__isnull=True）
        的 deposit ID 集合，调用方必须在事务内调用。"""
        return set(
            Deposit.objects.select_for_update(skip_locked=True)
            .filter(
                pk__in=group_ids,
                status=DepositStatus.COMPLETED,
                collection__isnull=True,
            )
            .values_list("pk", flat=True)
        )

    @staticmethod
    def _snapshot_collectible_group(deposit: Deposit) -> list[Deposit]:
        """读取同客户在同链同币下仍待归集的充币快照（不加锁）。

        仅供事务外的 prepare_collection 使用；execute_collection 在事务内
        会对候选组再加 select_for_update(skip_locked=True) 校验，确保
        并发安全。"""
        return list(
            Deposit.objects.select_related(
                "customer", "customer__project", "transfer__crypto", "transfer__chain"
            )
            .filter(
                customer_id=deposit.customer_id,
                transfer__chain_id=deposit.transfer.chain_id,
                transfer__crypto_id=deposit.transfer.crypto_id,
                status=DepositStatus.COMPLETED,
                collection__isnull=True,
            )
            .order_by("created_at", "pk")
        )

    @staticmethod
    def _snapshot_schedule_collectible_group(schedule: CollectSchedule) -> list[Deposit]:
        customer_id = getattr(
            schedule.deposit_address,
            "customer_id",
            schedule.deposit_address.customer.pk,
        )
        chain_id = getattr(schedule, "chain_id", schedule.chain.pk)
        crypto_id = getattr(schedule, "crypto_id", schedule.crypto.pk)
        return list(
            Deposit.objects.select_related(
                "customer", "customer__project", "transfer__crypto", "transfer__chain"
            )
            .filter(
                customer_id=customer_id,
                transfer__chain_id=chain_id,
                transfer__crypto_id=crypto_id,
                status=DepositStatus.COMPLETED,
                collection__isnull=True,
            )
            .order_by("created_at", "pk")
        )
