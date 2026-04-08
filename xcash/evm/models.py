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

    # completed 仅表示整笔 EVM 交易生命周期已经结束（确认成功或明确失败）。
    completed = models.BooleanField(_("已完成"), default=False)
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

    def broadcast(self) -> None:
        if self.has_lower_queued_nonce() or self.is_pipeline_full():
            return None
        self._ensure_signed_with_latest_gas_price()

        self.last_attempt_at = timezone.now()
        self.save(update_fields=["last_attempt_at"])

        raw_payload = Web3.to_bytes(hexstr=self.signed_payload)
        try:
            self.chain.w3.eth.send_raw_transaction(raw_payload)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            if self._is_already_known_error(exc):
                return self._mark_pending_chain()
            raise
        self._mark_pending_chain()
        return None

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
            # 首次成功提交到节点后，统一父任务从"待执行"进入"待上链"。
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
        # 对外优先展示统一父任务组合状态；广播细节继续保留在子表内部，不上浮到领域模型。
        if self.base_task_id:
            return self.base_task.display_status
        if self.completed:
            return "已完成"
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
        - 所有客户端 nonce 已上链: "nonce too low"
        """
        msg = str(exc).lower()
        return (
            "already known" in msg
            or "alreadyknown" in msg
            or "known transaction" in msg
            or "already imported" in msg
            or "existing txn with same hash" in msg
            or "nonce too low" in msg
        )

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
    def _next_nonce(address, chain, *, state: AddressChainState | None = None) -> int:
        """为 (address, chain) 维度分配严格递增的下一个 nonce。"""
        if state is None:
            state = AddressChainState.acquire_for_update(address=address, chain=chain)
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
