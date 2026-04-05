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
from common.consts import BASE_TRANSFER_GAS
from common.consts import ERC20_TRANSFER_GAS
from deposits.exceptions import DepositStatusError
from deposits.models import Deposit
from deposits.models import DepositAddress
from deposits.models import DepositCollection
from deposits.models import DepositStatus
from projects.models import RecipientAddress
from webhooks.service import WebhookService


class DepositService:
    """High level orchestration around deposit lifecycle and collection."""

    _NATIVE_BUFFER_RATIO = Decimal("1.2")

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
    def notify_created(cls, deposit: Deposit) -> None:
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
        cls.notify_created(deposit)
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

        并发安全：select_for_update 防止重复确认/丢弃。
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
    def drop_deposit(cls, deposit: Deposit) -> None:
        cls._transition_status(deposit, DepositStatus.DROPPED)

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
        # 归集金额换算必须使用链特定精度，避免覆盖精度的链上资产被错误换算。
        crypto_decimals = crypto.get_decimals(chain)

        recipient = cls._select_recipient(project_id=project.id, chain_type=chain.type)
        if recipient is None:
            return None

        deposit_addr = DepositAddress.objects.get(
            customer=deposit.customer,
            chain_type=chain.type,
        ).address
        adapter = AdapterFactory.get_adapter(chain.type)

        balance_raw = adapter.get_balance(deposit_addr.address, chain, crypto)
        if balance_raw <= 0:
            return None

        if not cls._should_collect(deposit, balance_raw):
            return None

        # Gas 补充交易广播后不等待链上确认即继续归集：若补充尚未到账，归集交易会因
        # Gas 不足而失败，下一轮 gather_deposits 重试时补充已确认即可成功。
        cls._ensure_native_buffer(
            deposit=deposit, deposit_address=deposit_addr, adapter=adapter
        )

        amount = cls._calculate_collection_amount(deposit, balance_raw, crypto_decimals)
        if amount is None:
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
        归集执行阶段（事务外调用）：广播链上交易并回写 collection_hash。

        广播失败时清理占位 collection 以便下次重试。
        广播成功后即使后续 DB 更新 hash 失败，deposits 仍标记为"归集中"，
        try_match_collection 会在链上交易被节点推送时自动关联。
        """
        try:
            if params["chain"].type == ChainType.EVM:
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
                DepositCollection.objects.filter(pk=params["collection_id"]).update(
                    broadcast_task=task.base_task,
                    updated_at=timezone.now(),
                )
                return True

            tx_hash = params["address"].send_crypto(
                crypto=params["crypto"],
                chain=params["chain"],
                to=params["recipient_address"],
                amount=params["amount"],
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
            # 广播失败：清理占位 collection，释放 deposits 以便下次重试
            cls._cleanup_placeholder_collection(params["collection_id"])
            return False

        # 广播成功：回写真实 tx_hash
        DepositCollection.objects.filter(pk=params["collection_id"]).update(
            collection_hash=tx_hash,
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

    @classmethod
    def _calculate_collection_amount(
        cls, deposit: Deposit, balance_raw: int, crypto_decimals: int
    ) -> Decimal | None:
        """
        计算实际可归集金额。

        原生币需扣除 gas 费，ERC-20 由 _ensure_native_buffer 保证 gas 充足，
        直接使用完整余额。余额不足 gas 时返回 None 表示跳过本次归集。
        """
        chain = deposit.transfer.chain
        crypto = deposit.transfer.crypto

        if crypto == chain.native_coin or crypto.is_native:
            # 归集原生币时，需扣除 gas 费用
            fee_raw = cls._estimate_native_fee(chain, crypto)
            net_raw = balance_raw - fee_raw
            if net_raw <= 0:
                logger.warning(
                    "原生币余额不足以支付归集 gas，跳过",
                    deposit_id=deposit.id,
                    chain=chain.code,
                )
                return None
            return cls._to_amount(net_raw, crypto_decimals)

        return cls._to_amount(balance_raw, crypto_decimals)

    @classmethod
    @db_transaction.atomic
    def try_match_collection(cls, transfer: OnchainTransfer) -> bool:
        """将链上归集转账与 DepositCollection 记录关联。"""
        try:
            collection = DepositCollection.objects.select_for_update().get(
                collection_hash=transfer.hash
            )
        except DepositCollection.DoesNotExist:
            from chains.models import BroadcastTask

            broadcast_task = BroadcastTask.resolve_by_hash(
                chain=transfer.chain,
                tx_hash=transfer.hash,
            )
            if broadcast_task is None:
                return False
            try:
                collection = DepositCollection.objects.select_for_update().get(
                    broadcast_task=broadcast_task
                )
            except DepositCollection.DoesNotExist:
                return False
            if collection.collection_hash != transfer.hash:
                collection.collection_hash = transfer.hash

        transfer.type = TransferType.DepositCollection
        transfer.save(update_fields=["type"])

        # 幂等：已关联则跳过，避免重复处理时覆盖已有关联。
        if collection.transfer_id:
            return True

        collection.transfer = transfer
        update_fields = ["transfer", "updated_at"]
        if collection.collection_hash != transfer.hash:
            collection.collection_hash = transfer.hash
            update_fields.insert(0, "collection_hash")
        collection.save(update_fields=update_fields)
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
    def _should_collect(cls, deposit: Deposit, balance_raw: int) -> bool:
        crypto = deposit.transfer.crypto
        chain = deposit.transfer.chain
        project = deposit.customer.project

        # 归集阈值判断必须与实际发送使用同一套链特定精度，避免门槛判断失真。
        amount = cls._to_amount(balance_raw, crypto.get_decimals(chain))

        try:
            worth = amount * crypto.price("USD")
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
    def _ensure_native_buffer(
        cls,
        *,
        deposit: Deposit,
        deposit_address,
        adapter,
    ) -> None:
        chain = deposit.transfer.chain
        crypto = deposit.transfer.crypto

        if crypto == chain.native_coin or crypto.is_native:
            return

        required_native = cls._estimate_native_fee(chain, crypto)
        if required_native <= 0:
            return

        current_native = adapter.get_balance(
            deposit_address.address,
            chain,
            chain.native_coin,
        )

        # 预留 1.2 倍 gas 余量，避免因价格波动导致归集失败
        target_native = int(required_native * cls._NATIVE_BUFFER_RATIO)

        if current_native >= target_native:
            return

        deficit = target_native - current_native
        # Gas 补充金额也按链上原生币真实精度换算，避免误充过多或过少。
        amount = cls._to_amount(deficit, chain.native_coin.get_decimals(chain))
        if amount <= Decimal("0"):
            return

        vault_addr = deposit.customer.project.wallet.get_address(
            chain_type=chain.type,
            usage=AddressUsage.Vault,
        )

        try:
            vault_addr.send_crypto(
                crypto=chain.native_coin,
                chain=chain,
                to=deposit_address.address,
                amount=amount,
                transfer_type=TransferType.GasRecharge,
            )
        except Exception:  # noqa: BLE001
            # Gas 补充失败不应阻断归集流程：充值地址可能已有足够原生币，
            # 或本次补充已广播成功但后续步骤异常。由后续归集交易自行验证余额。
            logger.warning(
                "Gas 补充交易失败，继续尝试归集",
                deposit_id=deposit.id,
                chain=chain.code,
            )

    @staticmethod
    def _estimate_native_fee(chain, crypto) -> int:
        if chain.type == ChainType.EVM:
            try:
                gas_price = chain.w3.eth.gas_price  # noqa: SLF001
            except Exception:  # noqa: BLE001
                # RPC 异常时返回 0，由调用方决定是否继续归集
                logger.warning(
                    "获取 gas_price 失败，返回 0",
                    chain=chain.code,
                )
                return 0
            gas_limit = (
                BASE_TRANSFER_GAS
                if crypto == chain.native_coin or crypto.is_native
                else ERC20_TRANSFER_GAS
            )
            return int(gas_price * gas_limit)

        if chain.type == ChainType.BITCOIN:
            # Bitcoin 归集原生币时，schedule_transfer 内部会从 amount 中额外扣除矿工费，
            # 传入全额余额会导致 select_utxos_for_amount 因 amount + fee > total 而失败。
            # 这里按保守的 1 输入 2 输出 P2PKH 交易体积 × 默认费率预留 fee 空间。
            from bitcoin.constants import BTC_DEFAULT_FEE_RATE_SAT_PER_BYTE
            from bitcoin.constants import BTC_P2PKH_TX_BYTES

            return BTC_P2PKH_TX_BYTES * BTC_DEFAULT_FEE_RATE_SAT_PER_BYTE

        # 防御性兜底：当前系统只支持 EVM / Bitcoin；若出现异常链类型，返回 0 避免归集流程直接崩溃。
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
