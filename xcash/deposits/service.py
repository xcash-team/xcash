from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import structlog
from django.db import transaction as db_transaction
from django.utils import timezone

logger = structlog.get_logger()

from chains.adapters import AdapterFactory
from chains.models import AddressUsage
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from deposits.exceptions import DepositStatusError
from deposits.models import Deposit
from deposits.models import DepositAddress
from deposits.models import DepositCollection
from deposits.models import DepositStatus
from deposits.models import GasRecharge
from projects.models import RecipientAddress
from webhooks.service import WebhookService


class DepositService:
    """High level orchestration around deposit lifecycle and collection."""

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
        """发送 deposit webhook 通知，status 放入 data 层级，与 invoice 格式统一。"""
        content = deposit.content
        content["data"]["status"] = status
        try:
            WebhookService.create_event(
                project=deposit.customer.project, payload=content
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
    def prepare_collection(cls, deposit: Deposit) -> dict | None:  # noqa: PLR0911
        """
        归集准备阶段（必须在事务内调用）：加锁、校验、计算金额，
        预创建 DepositCollection 占位记录并关联到 deposits。

        返回 dict 包含广播所需参数，返回 None 表示无需归集。
        预创建 collection 确保：即使后续链上广播成功但 DB 异常，
        deposits 已被标记 collection 非空，不会被 gather_deposits 重复扫描。
        """
        grouped_deposits, deposit = cls._resolve_collection_group(deposit)
        if deposit is None:
            return None

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
        adapter = AdapterFactory.get_adapter(chain.type)

        # 快速退出：链上余额为 0 不可能归集
        balance_raw = adapter.get_balance(deposit_addr.address, chain, crypto)
        if balance_raw <= 0:
            return None

        # 归集金额 = 充值金额之和（非余额），保证对账一致
        amount = cls._calculate_collection_amount(grouped_deposits)

        if not cls._should_collect(deposit, amount):
            return None

        # Gas 充足性检查：不足时自动补充并跳过本轮，等下一轮 gas 到账后重试
        if not cls._ensure_gas_and_check(
            deposit=deposit,
            deposit_address=deposit_addr,
            adapter=adapter,
            collection_amount=amount,
        ):
            return None

        # 预创建占位 collection（hash 为 NULL），并关联 deposits，
        # 保证 deposits 在事务提交后即标记为"归集中"，避免双重归集。
        collection = DepositCollection.objects.create(collection_hash=None)
        group_ids = [item.pk for item in grouped_deposits]
        Deposit.objects.filter(pk__in=group_ids).update(
            collection=collection,
            updated_at=timezone.now(),
        )

        return {
            "collection_id": collection.pk,
            "address": deposit_addr,
            "crypto": crypto,
            "chain": chain,
            "recipient_address": recipient.address,
            "amount": amount,
            "deposit_id": deposit.id,
        }

    @classmethod
    def execute_collection(cls, params: dict) -> bool:
        """
        归集执行阶段（事务外调用）：广播链上交易并回写 broadcast_task。

        广播失败时清理占位 collection 以便下次重试。
        广播成功后即使后续 DB 更新失败，deposits 仍标记为"归集中"，
        try_match_collection 会在链上交易被节点推送时自动关联。
        """
        try:
            from evm.models import EvmBroadcastTask

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
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "归集充币失败，清理占位 collection",
                deposit_id=params["deposit_id"],
                chain=params["chain"].code,
                crypto=params["crypto"].symbol,
                exc=exc,
            )
            cls._cleanup_placeholder_collection(params["collection_id"])
            return False

        DepositCollection.objects.filter(pk=params["collection_id"]).update(
            broadcast_task=task.base_task,
            updated_at=timezone.now(),
        )
        return True

    @classmethod
    def collect_deposit(cls, deposit: Deposit) -> bool:
        """两阶段归集的便捷封装：prepare（事务内）+ execute（事务外）。
        供测试和需要单方法调用的场景使用。
        注意：调用方需自行确保 prepare 阶段在事务内执行。
        """
        params = cls.prepare_collection(deposit)
        if params is None:
            return False
        return cls.execute_collection(params)

    @classmethod
    def _cleanup_placeholder_collection(cls, collection_id: int) -> None:
        """清理广播失败的占位 collection，解除关联的 deposits 以便下次重试。"""
        try:
            collection = DepositCollection.objects.get(pk=collection_id)
        except DepositCollection.DoesNotExist:
            return
        cls.drop_collection(collection)

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

        grouped = cls._lock_collectible_group(deposit)
        if not grouped:
            return [], None
        return grouped, grouped[0]

    @staticmethod
    def _calculate_collection_amount(grouped_deposits: list[Deposit]) -> Decimal:
        """归集金额 = 分组内所有充值金额之和，保证对账一致：充多少归多少。"""
        return sum((d.transfer.amount for d in grouped_deposits), Decimal("0"))

    @staticmethod
    def try_match_gas_recharge(transfer: OnchainTransfer) -> bool:
        """通过 BroadcastTask 识别 Vault → 充币地址的 Gas 补充转账，并关联到 GasRecharge 记录。"""
        from chains.models import BroadcastTask

        task = BroadcastTask.resolve_by_hash(
            chain=transfer.chain, tx_hash=transfer.hash
        )
        if task is None or task.transfer_type != TransferType.GasRecharge:
            return False
        transfer.type = TransferType.GasRecharge
        transfer.save(update_fields=["type"])

        # 将链上转账关联到 GasRecharge 审计记录
        GasRecharge.objects.filter(
            broadcast_task=task,
            transfer__isnull=True,
        ).update(transfer=transfer, updated_at=timezone.now())
        return True

    @classmethod
    @db_transaction.atomic
    def try_match_collection(cls, transfer: OnchainTransfer) -> bool:
        """通过 BroadcastTask 将链上归集转账与 DepositCollection 记录关联。"""
        from chains.models import BroadcastTask

        broadcast_task = BroadcastTask.resolve_by_hash(
            chain=transfer.chain, tx_hash=transfer.hash
        )
        if broadcast_task is None:
            return False

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
        归集交易失效：解除所有关联 Deposit 的归集记录，以便重新触发归集。

        显式将关联 Deposit 的 collection 置 NULL 并更新 updated_at 时间戳，
        再删除 DepositCollection 记录。显式 update 先于 delete 执行，
        确保 updated_at 被正确刷新（而非依赖 on_delete=SET_NULL 的自动置空，
        后者不会更新 updated_at）。
        """
        collection.deposits.update(collection=None, updated_at=timezone.now())
        collection.delete()

    @staticmethod
    def _select_recipient(*, project_id: int, chain_type: ChainType | str):
        return (
            RecipientAddress.objects.filter(
                project_id=project_id,
                chain_type=chain_type,
                used_for_deposit=True,
            )
            .order_by("id")
            .first()
        )

    @staticmethod
    def _to_amount(raw_value: int, decimals: int) -> Decimal:
        return Decimal(raw_value).scaleb(-decimals)

    @classmethod
    def _should_collect(cls, deposit: Deposit, collection_amount: Decimal) -> bool:
        crypto = deposit.transfer.crypto
        project = deposit.customer.project

        try:
            worth = collection_amount * crypto.price("USD")
        except KeyError:
            logger.warning(
                "缺少代币价格，直接触发归集",
                crypto=crypto.symbol,
            )
            worth = project.gather_worth

        if worth >= project.gather_worth:
            return True

        deadline = deposit.created_at + timedelta(days=project.gather_period)
        return timezone.now() >= deadline

    @classmethod
    def _ensure_gas_and_check(
        cls,
        *,
        deposit: Deposit,
        deposit_address,
        adapter,
        collection_amount: Decimal,
    ) -> bool:
        """
        检查归集 gas 是否充足，不足时自动补充并跳过本轮归集。

        原生币：余额 >= 归集金额 + 2 次原生币转账 gas。
        代币：原生币余额 >= 1 次 ERC-20 转账 gas。
        Gas 补充金额 = min(5 次 ERC-20 转账, 10 次原生币转账)。

        返回 True 表示 gas 充足可立即归集，False 表示已发起补充、本轮跳过。
        """
        chain = deposit.transfer.chain
        crypto = deposit.transfer.crypto

        gas_price = cls._get_gas_price(chain)
        if gas_price <= 0:
            # 非 EVM 或 RPC 异常，直接放行由后续交易自行校验
            return True

        native_gas_cost = gas_price * chain.base_transfer_gas
        erc20_gas_cost = gas_price * chain.erc20_transfer_gas

        # --- 判断 gas 是否充足 ---
        if crypto == chain.native_coin or crypto.is_native:
            # 原生币归集：余额需覆盖归集金额 + 2 次原生币转账 gas
            crypto_decimals = crypto.get_decimals(chain)
            collection_raw = int(collection_amount * Decimal(10**crypto_decimals))
            required_gas_raw = 2 * native_gas_cost
            current_balance = adapter.get_balance(
                deposit_address.address, chain, crypto
            )
            if current_balance >= collection_raw + required_gas_raw:
                return True
        else:
            # 代币归集：需要足够原生币支付 ERC-20 转账 gas
            current_native = adapter.get_balance(
                deposit_address.address, chain, chain.native_coin
            )
            if current_native >= erc20_gas_cost:
                return True

        # --- Gas 不足，发起补充 ---
        recharge_raw = min(5 * erc20_gas_cost, 10 * native_gas_cost)
        if recharge_raw <= 0:
            return False

        vault_addr = deposit.customer.project.wallet.get_address(
            chain_type=chain.type,
            usage=AddressUsage.Vault,
        )
        try:
            from evm.models import EvmBroadcastTask

            task = EvmBroadcastTask.schedule_transfer(
                address=vault_addr,
                chain=chain,
                crypto=chain.native_coin,
                to=deposit_address.address,
                value_raw=recharge_raw,
                transfer_type=TransferType.GasRecharge,
            )
            # 记录 Gas 补充操作，供后续链上匹配和审计追踪
            deposit_addr_record = DepositAddress.objects.get(
                customer=deposit.customer,
                chain_type=chain.type,
            )
            GasRecharge.objects.create(
                deposit_address=deposit_addr_record,
                broadcast_task=task.base_task,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "Gas 补充交易失败，跳过本轮归集",
                deposit_id=deposit.id,
                chain=chain.code,
            )
        # 无论补充成功与否，本轮均跳过，等下一轮 gas 到账后重试
        return False

    @staticmethod
    def _get_gas_price(chain) -> int:
        """获取 EVM 链当前 gas price（wei），非 EVM 返回 0。"""
        if chain.type != ChainType.EVM:
            return 0
        try:
            return chain.w3.eth.gas_price
        except Exception:  # noqa: BLE001
            logger.warning("获取 gas_price 失败", chain=chain.code)
            return 0

    @staticmethod
    def _lock_collectible_group(deposit: Deposit) -> list[Deposit]:
        """锁定同一客户在同链同币下仍待归集的全部完成充币记录。
        使用 skip_locked 与 tasks.py 保持一致，避免并发时阻塞等待或死锁。"""
        return list(
            Deposit.objects.select_for_update(skip_locked=True)
            .select_related(
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
