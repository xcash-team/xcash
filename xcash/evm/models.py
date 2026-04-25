from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import eth_abi
from django.db import models
from django.db import transaction as db_transaction
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from web3 import Web3

from chains.models import AddressChainState
from chains.models import BroadcastTask
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import TransferType
from chains.models import TxHash
from chains.signer import get_signer_backend
from common.fields import EvmAddressField
from common.models import UndeletableModel
from evm.constants import EVM_PIPELINE_DEPTH

if TYPE_CHECKING:
    from currencies.models import Crypto

# ERC-20 transfer(address,uint256) 函数选择器
_ERC20_TRANSFER_SELECTOR = "0xa9059cbb"


class EvmScanCursorType(models.TextChoices):
    """定义 EVM 自扫描器的游标类型。"""

    NATIVE_DIRECT = "native_direct", _("原生币直转")
    ERC20_TRANSFER = "erc20_transfer", _("ERC20 转账")


class EvmScanCursor(models.Model):
    """记录某条 EVM 链上某类扫描器的推进位置与最近错误。

    设计原则：
    - 游标按"链 + 扫描器类型"维度维护，不按 token 维度膨胀。
    - last_scanned_block 记录主扫描面已经推进到的最高块高。
    - last_safe_block 记录当前安全块高，便于后台观察追平程度。
    """

    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.CASCADE,
        related_name="evm_scan_cursors",
        verbose_name=_("链"),
    )
    scanner_type = models.CharField(
        _("扫描器类型"),
        max_length=32,
        choices=EvmScanCursorType,
    )
    last_scanned_block = models.PositiveIntegerField(_("已扫描到的区块"), default=0)
    last_safe_block = models.PositiveIntegerField(_("安全区块"), default=0)
    enabled = models.BooleanField(_("启用"), default=True)
    last_error = models.CharField(_("最近错误"), max_length=255, blank=True, default="")
    last_error_at = models.DateTimeField(_("最近错误时间"), blank=True, null=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("chain", "scanner_type"),
                name="uniq_evm_scan_cursor_chain_scanner_type",
            ),
        ]
        ordering = ("chain_id", "scanner_type")
        verbose_name = _("EVM 扫描游标")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"{self.chain.code}:{self.scanner_type}"


class EvmBroadcastTask(UndeletableModel):
    # base_task 是跨链统一锚点；EVM 子表继续保存 nonce/gas/data 等链特有执行参数。
    base_task = models.OneToOneField(
        "chains.BroadcastTask",
        on_delete=models.CASCADE,
        related_name="evm_task",
        verbose_name=_("通用链上任务"),
        blank=True,
        null=True,
    )
    address = models.ForeignKey(
        "chains.Address",
        on_delete=models.PROTECT,
        verbose_name=_("地址"),
    )
    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.PROTECT,
        verbose_name=_("网络"),
    )
    nonce = models.PositiveBigIntegerField(_("Nonce"))
    to = EvmAddressField(_("To"))
    value = models.DecimalField(
        _("Value"),
        max_digits=32,
        decimal_places=0,
        default=0,
    )
    data = models.TextField(_("Data"), blank=True, default="")
    gas = models.PositiveIntegerField(_("Gas"))
    gas_price = models.PositiveBigIntegerField(_("Gas Price"), blank=True, null=True)
    signed_payload = models.TextField(_("已签名链上载荷"), blank=True, default="")

    last_attempt_at = models.DateTimeField(_("上次尝试时间"), blank=True, null=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
        constraints = [
            models.UniqueConstraint(
                fields=("address", "chain", "nonce"),
                # 约束名直接采用 BroadcastTask 语义，保持当前模型命名一致。
                name="uniq_evm_broadcast_task_address_chain_nonce",
            ),
        ]
        ordering = ("created_at",)
        # EVM 主执行对象统一命名为 BroadcastTask，避免继续把稳定任务对象写成历史别名。
        verbose_name = _("链上任务")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return (
            self.base_task.tx_hash or f"{self.address_id}:{self.nonce}"
            if self.base_task_id
            else f"{self.address_id}:{self.nonce}"
        )

    @property
    def transaction_dict(self) -> dict:
        if self.gas_price is None:
            raise ValueError("EVM 任务尚未签名，gas_price 不可为空")
        return {
            "chainId": self.chain.chain_id,
            "nonce": self.nonce,
            "from": self.address.address,
            "to": self.to,
            "value": int(self.value),
            # 交易字典要稳定适配 signer 请求载荷和 web3 原始交易格式，空 data 统一使用 0x。
            "data": self.data if self.data else "0x",
            "gas": self.gas,
            "gasPrice": self.gas_price,
        }

    def broadcast(self, *, allow_pending_chain_rebroadcast: bool = False) -> None:
        if not self._can_broadcast_for_current_stage(
            allow_pending_chain_rebroadcast=allow_pending_chain_rebroadcast
        ):
            return
        if self._recover_queued_receipt_if_any():
            return
        if self._is_broadcast_order_blocked(
            allow_pending_chain_rebroadcast=allow_pending_chain_rebroadcast
        ):
            return

        self._ensure_signed_with_latest_gas_price()

        if not self._passes_balance_preflight():
            return
        if not self._passes_execution_preflight():
            return

        self._record_broadcast_attempt()
        self._send_signed_payload()

    def _is_broadcast_order_blocked(
        self, *, allow_pending_chain_rebroadcast: bool
    ) -> bool:
        if self.has_lower_queued_nonce():
            return True
        return not allow_pending_chain_rebroadcast and self.is_pipeline_full()

    def _passes_balance_preflight(self) -> bool:
        # pre-flight 第 1 步：主动阈值检查。
        # 用 *当前* gas_price（不是本任务签名时锁定的 self.gas_price）估算单次
        # ERC-20 转账 gas 成本，再按 value + 2 * erc20_gas_cost 作为预算阈值：
        # - ERC-20 归集：value=0 → 阈值 = 2 * erc20_gas_cost，保留一次重试冗余。
        # - 原生币归集：阈值 = value + 2 * erc20_gas_cost，预留两次 gas 波动空间。
        # 这里刻意用 erc20_transfer_gas（而非 base_transfer_gas）给出更高的安全垫，
        # 避免归集链路在 gas 跳涨时被节点反复拒绝。
        current_native_balance = self.chain.w3.eth.get_balance(self.address.address)  # noqa: SLF001
        current_gas_price = self.chain.w3.eth.gas_price  # noqa: SLF001
        erc20_gas_cost = current_gas_price * self.chain.erc20_transfer_gas
        buffer_required = int(self.value) + 2 * erc20_gas_cost
        if current_native_balance < buffer_required:
            # 仅归集场景补 gas；Withdrawal 的 address 是 Vault 本身，补 gas 无意义，
            # 保持 QUEUED 静默返回，等运营向 Vault 注资即可。
            if self._is_eligible_for_gas_recharge():
                self._request_gas_recharge(erc20_gas_cost=erc20_gas_cost)
            # 不更新 last_attempt_at，避免 reconcile/dispatch 误判为活跃任务。
            return False

        return True

    def _passes_execution_preflight(self) -> bool:
        # pre-flight 第 2 步：estimate_gas 兜底 revert。
        # 主动阈值已覆盖余额不足；这里只防合约 revert / token 余额不足一类
        # 注定失败的交易被反复 send_raw_transaction。通用 RPC 错误原样上抛给 Celery。
        #
        # 注意：estimate_gas 不传 nonce。nonce 顺序由 has_lower_queued_nonce +
        # pipeline_full 保证，与"交易语义能否执行"无关；一旦绑定 nonce，EVM 节点
        # 在同地址前序 tx 未 confirm 时会对本 tx 直接返回 "Nonce too high"
        # （geth/anvil 的 -32003），把所有同地址后续任务打成假失败。
        preflight_tx = self._build_transaction_dict(gas_price=self.gas_price)
        preflight_tx.pop("nonce", None)
        try:
            self.chain.w3.eth.estimate_gas(preflight_tx)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            if self._is_execution_reverted_error(exc):
                # estimate_gas 回退发生在交易广播前，此 nonce 尚未被链上消费。
                # 直接终局会制造 nonce 空洞并放行后续更高 nonce；保持 QUEUED，
                # 仅记录尝试时间让调度节流，等待后续重试、补救或人工处理。
                self._record_broadcast_attempt()
                return False
            raise

        return True

    def _record_broadcast_attempt(self) -> None:
        self.last_attempt_at = timezone.now()
        self.save(update_fields=["last_attempt_at"])

    def _send_signed_payload(self) -> None:
        # pre-flight 通过，真正广播。
        raw_payload = Web3.to_bytes(hexstr=self.signed_payload)
        try:
            self.chain.w3.eth.send_raw_transaction(raw_payload)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            if self._is_nonce_too_low_error(exc):
                if self._recover_queued_receipt_if_any():
                    return
                raise
            if self._is_already_known_error(exc):
                self._mark_pending_chain()
                return
            raise
        self._mark_pending_chain()

    def _known_tx_hashes(self) -> list[str]:
        """返回当前任务所有已知 tx_hash，按新版本优先查询。"""
        if not self.base_task_id:
            return []

        hashes: list[str] = []
        base_tx_hash = (
            BroadcastTask.objects.filter(pk=self.base_task_id)
            .values_list("tx_hash", flat=True)
            .first()
        )
        if base_tx_hash:
            hashes.append(base_tx_hash)

        for tx_hash in (
            TxHash.objects.filter(broadcast_task_id=self.base_task_id)
            .order_by("-version")
            .values_list("hash", flat=True)
        ):
            if tx_hash not in hashes:
                hashes.append(tx_hash)
        return hashes

    def _find_receipt_for_known_hashes(self) -> tuple[str | None, dict | None]:
        from web3.exceptions import TransactionNotFound

        for tx_hash in self._known_tx_hashes():
            try:
                receipt = self.chain.w3.eth.get_transaction_receipt(tx_hash)  # noqa: SLF001
            except TransactionNotFound:
                continue
            except AttributeError:
                return None, None
            if receipt is None:
                continue
            return tx_hash, dict(receipt)
        return None, None

    def _recover_queued_receipt_if_any(self) -> bool:
        """QUEUED 任务若已有 tx_hash，先按链上 receipt 恢复状态。

        send_raw_transaction 可能已被节点接受，但 worker 在 _mark_pending_chain 前
        中断。再次执行时不能盲目重发或让 nonce too low 卡住队列，应先用历史
        hash 观察链上事实，再回到统一 coordinator/业务管线。
        """
        if not self.base_task_id:
            return False

        base_task = BroadcastTask.objects.only("stage", "result", "tx_hash").get(
            pk=self.base_task_id
        )
        if (
            base_task.stage != BroadcastTaskStage.QUEUED
            or base_task.result != BroadcastTaskResult.UNKNOWN
        ):
            return False
        if not self._known_tx_hashes():
            return False

        tx_hash, receipt = self._find_receipt_for_known_hashes()
        if receipt is None or tx_hash is None:
            return False

        from evm.coordinator import InternalEvmTaskCoordinator

        status = receipt.get("status")
        if status == 1:
            self._mark_pending_chain()
            InternalEvmTaskCoordinator._observe_confirmed_transaction(
                evm_task=self,
                tx_hash=tx_hash,
                receipt=receipt,
            )
            return True
        if status == 0:
            self._mark_pending_chain()
            InternalEvmTaskCoordinator._finalize_failed_task(evm_task=self)
            return True
        raise RuntimeError("EVM receipt status missing or invalid")

    @staticmethod
    def _is_execution_reverted_error(exc: Exception) -> bool:
        """识别链上模拟时合约回退 / 交易本身注定失败的错误。"""
        from web3.exceptions import ContractLogicError

        if isinstance(exc, ContractLogicError):
            return True
        msg = str(exc).lower()
        return "execution reverted" in msg

    def _can_broadcast_for_current_stage(
        self, *, allow_pending_chain_rebroadcast: bool
    ) -> bool:
        """校验当前父任务阶段是否允许进入真实广播副作用。"""
        if not self.base_task_id:
            return True

        base_task = BroadcastTask.objects.only("stage", "result").get(
            pk=self.base_task_id
        )
        if base_task.result != BroadcastTaskResult.UNKNOWN:
            return False
        if base_task.stage == BroadcastTaskStage.PENDING_CHAIN:
            return allow_pending_chain_rebroadcast
        return base_task.stage == BroadcastTaskStage.QUEUED

    def _is_eligible_for_gas_recharge(self) -> bool:
        """判断当前任务是否适用"向其 address 补 gas"。

        条件：
        - base_task.transfer_type == DepositCollection（只有归集地址才需要补给）
        - self.address 已登记为有效 DepositAddress（排除其它用途的地址）

        Withdrawal 任务的 address 本身即 Vault，补 gas 会形成 vault→vault 死循环，
        故排除；无 base_task 或非归集类型直接返回 False。
        """
        if not self.base_task_id:
            return False
        if self.base_task.transfer_type != TransferType.DepositCollection:
            return False

        from deposits.models import DepositAddress

        return DepositAddress.objects.filter(address=self.address).exists()

    def _request_gas_recharge(self, *, erc20_gas_cost: int) -> None:
        """pre-flight 阈值不足时，委托 GasRechargeService 幂等补 gas。

        调用此方法前须已通过 _is_eligible_for_gas_recharge 校验，确保
        self.address 存在对应 DepositAddress。内部再 select_related 拉齐
        Vault 派生所需字段；若 DB 关系异常（极端情况）记录日志静默跳过，
        让上层保持 QUEUED 等下一轮再试，而不是把 pre-flight 打成硬错误。
        """
        from deposits.models import DepositAddress
        from deposits.service import GasRechargeService

        try:
            deposit_address = DepositAddress.objects.select_related(
                "customer__project__wallet", "address",
            ).get(address=self.address)
        except DepositAddress.DoesNotExist:
            return

        GasRechargeService.request_recharge(
            deposit_address=deposit_address,
            chain=self.chain,
            erc20_gas_cost=erc20_gas_cost,
        )

    def _ensure_signed_with_latest_gas_price(self) -> None:
        """首次广播时签名并生成首个 tx_hash；重试时仅在 gas 提升时重签。"""
        current_gas_price = self.chain.w3.eth.gas_price  # noqa: SLF001
        if not self.signed_payload or self.gas_price is None:
            signed = get_signer_backend().sign_evm_transaction(
                address=self.address,
                chain=self.chain,
                tx_dict=self._build_transaction_dict(gas_price=current_gas_price),
            )
            self.gas_price = current_gas_price
            self.signed_payload = signed.raw_transaction
            self.save(update_fields=["gas_price", "signed_payload"])
            if self.base_task_id:
                self.base_task.append_tx_hash(signed.tx_hash)
            return

        if current_gas_price <= self.gas_price:
            return

        signed = get_signer_backend().sign_evm_transaction(
            address=self.address,
            chain=self.chain,
            tx_dict=self._build_transaction_dict(gas_price=current_gas_price),
        )
        self.gas_price = current_gas_price
        self.signed_payload = signed.raw_transaction
        self.save(update_fields=["gas_price", "signed_payload"])

        # 重签后 tx_hash 变化，更新父任务并追加历史记录以便链上观测匹配。
        if self.base_task_id:
            self.base_task.append_tx_hash(signed.tx_hash)

    def _build_transaction_dict(self, *, gas_price: int) -> dict:
        return {
            "chainId": self.chain.chain_id,
            "nonce": self.nonce,
            "from": self.address.address,
            "to": self.to,
            "value": int(self.value),
            "data": self.data if self.data else "0x",
            "gas": self.gas,
            "gasPrice": gas_price,
        }

    def _mark_pending_chain(self) -> None:
        if self.base_task_id:
            # 首次成功提交到节点后，统一父任务从"待广播"进入"待上链"。
            BroadcastTask.objects.filter(
                pk=self.base_task_id,
                stage=BroadcastTaskStage.QUEUED,
                result=BroadcastTaskResult.UNKNOWN,
            ).update(
                stage=BroadcastTaskStage.PENDING_CHAIN,
                updated_at=timezone.now(),
            )

    @property
    def status(self) -> str:
        if self.base_task_id:
            return self.base_task.display_status
        return "待执行"

    def has_lower_queued_nonce(self) -> bool:
        """同账户更低 nonce 尚未提交到节点（QUEUED）时阻断，保证 nonce 按顺序进入 mempool。"""
        if not self.address_id or not self.chain_id:
            return False
        return EvmBroadcastTask.objects.filter(
            address=self.address,
            chain=self.chain,
            nonce__lt=self.nonce,
            base_task__stage=BroadcastTaskStage.QUEUED,
            base_task__result=BroadcastTaskResult.UNKNOWN,
        ).exists()

    def is_pipeline_full(self) -> bool:
        """同地址同链已有 >=EVM_PIPELINE_DEPTH 笔在 mempool 中等待确认时阻断。"""
        if not self.address_id or not self.chain_id:
            return False
        return (
            EvmBroadcastTask.objects.filter(
                address=self.address,
                chain=self.chain,
                base_task__stage=BroadcastTaskStage.PENDING_CHAIN,
                base_task__result=BroadcastTaskResult.UNKNOWN,
            ).count()
            >= EVM_PIPELINE_DEPTH
        )

    @staticmethod
    def _is_already_known_error(exc: Exception) -> bool:
        """判断节点返回的错误是否表示"交易已存在于 mempool 或已上链"。

        不同 EVM 客户端返回的措辞各异：
        - Geth / BSC / Bor / coreth / op-geth / Arbitrum: "already known"
        - Nethermind: "AlreadyKnown"（无空格，需单独匹配）
        - Besu: "Known transaction"
        - Parity / OpenEthereum: "Transaction with the same hash was already imported."
        - Anvil (Foundry): "transaction already imported"
        - Erigon: "existing txn with same hash"
        """
        msg = str(exc).lower()
        return (
            "already known" in msg
            or "alreadyknown" in msg
            or "known transaction" in msg
            or "already imported" in msg
            or "existing txn with same hash" in msg
        )

    @staticmethod
    def _is_nonce_too_low_error(exc: Exception) -> bool:
        """nonce too low 只表示该 nonce 已不可用，不能等同本系统交易已知。"""
        return "nonce too low" in str(exc).lower()

    @classmethod
    def _create_broadcast_task(
        cls,
        *,
        address,
        chain,
        to,
        transfer_type,
        gas,
        crypto: Crypto | None,
        recipient,
        amount: Decimal | None,
        value=0,
        data="",
        verify_fn=None,
    ):
        """在数据库行锁内完成 nonce 分配并原子落库待广播任务。

        设计要点：
        - 通过 AddressChainState 行锁对 (address, chain) 串行化，杜绝并发 nonce 冲突。
        - verify_fn 在行锁内、nonce 分配前执行，供调用方注入余额二次验证等逻辑，
          防止 TOCTOU 竞态（Serializer 软检查 → 加锁 → 分配 nonce 之间的窗口期）。
        - 首次签名和首个 tx_hash 生成延后到 broadcast()；内部稳定身份只依赖 (address, chain, nonce)。
        - 行锁跟随事务提交自动释放，不依赖 Redis TTL。
        """
        with db_transaction.atomic():
            state = AddressChainState.acquire_for_update(address=address, chain=chain)

            # 在行锁内执行调用方注入的验证回调（如余额二次确认）
            if verify_fn is not None:
                verify_fn()
            nonce = cls._next_nonce(address, chain, state=state)
            base_task = BroadcastTask.objects.create(
                chain=chain,
                address=address,
                transfer_type=transfer_type,
                crypto=crypto,
                recipient=recipient,
                amount=amount,
                stage=BroadcastTaskStage.QUEUED,
                result=BroadcastTaskResult.UNKNOWN,
            )

            # 稳定执行对象统一命名为 broadcast_task，避免继续把"任务"误解成某次签名尝试。
            broadcast_task = EvmBroadcastTask.objects.create(
                base_task=base_task,
                address=address,
                chain=chain,
                to=to,
                value=value,
                nonce=nonce,
                data=data,
                gas=gas,
            )
            state.next_nonce = nonce + 1
            state.save()
            return broadcast_task

    @staticmethod
    def _next_nonce(address, chain, *, state: AddressChainState) -> int:
        """为 (address, chain) 维度分配严格递增的下一个 nonce。

        调用方必须已通过 AddressChainState.acquire_for_update() 持有行锁，
        并将锁定的 state 实例传入，确保 nonce 分配在串行化保护下进行。
        """
        latest_nonce = (
            EvmBroadcastTask.objects.filter(address=address, chain=chain)
            .aggregate(max_nonce=models.Max("nonce"))
            .get("max_nonce")
        )
        derived_next_nonce = 0 if latest_nonce is None else int(latest_nonce) + 1
        if state.next_nonce is None:
            next_nonce = derived_next_nonce
        else:
            next_nonce = max(int(state.next_nonce), derived_next_nonce)
        if state.next_nonce != next_nonce:
            state.next_nonce = next_nonce
            state.save()
        return next_nonce

    @classmethod
    def schedule_native(
        cls,
        *,
        address,
        chain,
        to,
        value,
        transfer_type,
        verify_fn=None,
    ):
        """发送原生币（ETH/BNB 等）转账，写入队列等待链上观测。"""
        return cls._create_broadcast_task(
            address=address,
            chain=chain,
            to=to,
            value=value,
            transfer_type=transfer_type,
            gas=chain.base_transfer_gas,
            crypto=chain.native_coin,
            recipient=to,
            amount=cls._normalize_amount(value, chain.native_coin.decimals),
            verify_fn=verify_fn,
        )

    @classmethod
    def schedule_erc20(
        cls,
        *,
        address,
        chain,
        contract_address,
        data,
        transfer_type,
        crypto,
        recipient,
        token_value: int,
        verify_fn=None,
    ):
        """发送 ERC-20 代币转账，写入队列等待链上观测。"""
        # ERC-20 展示金额也要使用链特定精度，避免后台看到错误数量。
        amount = cls._normalize_amount(token_value, crypto.get_decimals(chain))
        return cls._create_broadcast_task(
            address=address,
            chain=chain,
            to=contract_address,
            transfer_type=transfer_type,
            gas=chain.erc20_transfer_gas,
            crypto=crypto,
            recipient=recipient,
            data=data,
            amount=amount,
            verify_fn=verify_fn,
        )

    @classmethod
    def schedule_transfer(
        cls,
        *,
        address,
        chain,
        crypto: Crypto,
        to: str,
        value_raw: int,
        transfer_type,
        verify_fn=None,
    ) -> EvmBroadcastTask:
        """统一入口：根据代币类型自动路由到 native 或 ERC-20 路径。

        value_raw 为链上原始整数单位（已乘以 10^decimals）。
        to 为收款地址（自动转为 checksum 格式）。
        verify_fn 透传到 _create_broadcast_task，在账户锁内执行（见该方法注释）。
        """
        to_checksum = Web3.to_checksum_address(to)

        if crypto == chain.native_coin or crypto.is_native:
            return cls.schedule_native(
                address=address,
                chain=chain,
                to=to_checksum,
                value=value_raw,
                transfer_type=transfer_type,
                verify_fn=verify_fn,
            )

        token_addr = crypto.address(chain)
        if not token_addr:
            raise ValueError(
                f"Crypto {crypto.symbol} is not deployed on chain {chain.code}"
            )
        token_addr_checksum = Web3.to_checksum_address(token_addr)

        # 构造 ERC-20 transfer(address,uint256) calldata
        encoded = eth_abi.encode(["address", "uint256"], [to_checksum, value_raw])
        data = _ERC20_TRANSFER_SELECTOR + encoded.hex()

        return cls.schedule_erc20(
            address=address,
            chain=chain,
            contract_address=token_addr_checksum,
            data=data,
            transfer_type=transfer_type,
            crypto=crypto,
            recipient=to_checksum,
            token_value=value_raw,
            verify_fn=verify_fn,
        )

    @staticmethod
    def _normalize_amount(value: int, decimals: int) -> Decimal:
        return Decimal(value).scaleb(-decimals)
