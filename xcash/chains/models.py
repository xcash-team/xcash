from __future__ import annotations

import contextlib
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING

import environ
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import models
from django.db import transaction as db_transaction
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from common.fields import AddressField
from common.fields import HashField
from common.models import UndeletableModel

if TYPE_CHECKING:
    from currencies.models import Crypto
    from deposits.models import Deposit
    from deposits.models import DepositCollection
    from invoices.models import Invoice
    from withdrawals.models import Withdrawal

env = environ.Env()


# Create your models here.
class ChainType(models.TextChoices):
    EVM = "evm", "EVM"
    BITCOIN = "btc", "Bitcoin"
    TRON = "tron", "Tron"


class Chain(models.Model):
    name = models.CharField(
        _("名称"),
        unique=True,
    )
    code = models.CharField(
        _("代码"),
        unique=True,
    )
    type = models.CharField(
        _("类型"),
        choices=ChainType,
    )
    native_coin = models.ForeignKey(
        "currencies.Crypto",
        verbose_name="原生币",
        on_delete=models.PROTECT,
        related_name="chains_as_native_coin",
    )
    confirm_block_count = models.PositiveIntegerField(
        default=10,
        verbose_name=_("区块确认数"),
    )
    latest_block_number = models.PositiveIntegerField(
        default=0, verbose_name=_("最新区块")
    )

    active = models.BooleanField(default=False, verbose_name=_("启用"))

    # For EVM
    base_transfer_gas = models.PositiveIntegerField(
        _("原生币转账 Gas Limit"),
        default=50_000,
        help_text=_("原生币（ETH/BNB 等）转账的 gas 上限"),
    )
    erc20_transfer_gas = models.PositiveIntegerField(
        _("ERC20 转账 Gas Limit"),
        default=100_000,
        help_text=_("ERC-20 代币 transfer 调用的 gas 上限"),
    )
    chain_id = models.PositiveIntegerField(
        _("Chain ID"),
        unique=True,
        blank=True,
        null=True,
    )
    rpc = models.CharField(_("RPC"), blank=True, default="")
    is_poa = models.BooleanField(_("POA"), blank=True, null=True)

    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        verbose_name = _("链")
        verbose_name_plural = _("链")

    # 当前产品允许创建 EVM / Bitcoin / Tron 三类链。
    PRODUCT_ENABLED_TYPES = (ChainType.EVM, ChainType.BITCOIN, ChainType.TRON)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        # 链创建属于系统级配置，统一在模型层执行校验，避免后台和脚本绕过限制。
        self.full_clean()

        # EVM 链在 RPC 变更时自动检测 chain_id 和 POA
        if self.type == ChainType.EVM and self.rpc:
            rpc_changed = self.pk is None  # 新建时视为变更
            if not rpc_changed:
                old_rpc = (
                    Chain.objects.filter(pk=self.pk)
                    .values_list("rpc", flat=True)
                    .first()
                )
                rpc_changed = old_rpc != self.rpc
            if rpc_changed:
                # 仅在 chain_id 未显式指定时才自动检测，避免覆盖调用方明确传入的值
                if not self.chain_id:
                    self.chain_id = self._detect_chain_id()
                self.is_poa = self._detect_poa()
                # 清除 w3 缓存，确保下次访问使用新 RPC 和 POA 配置
                self.__dict__.pop("w3", None)
        elif self.type == ChainType.TRON:
            self.chain_id = None
            self.is_poa = None
            self.confirm_block_count = 0
        elif self.type != ChainType.EVM:
            self.chain_id = None
            self.is_poa = None

        return super().save(*args, **kwargs)

    def clean(self) -> None:
        """限制后台和脚本只能创建当前产品阶段启用的链类型。"""
        super().clean()
        if self.type and self.type not in self.PRODUCT_ENABLED_TYPES:
            raise ValidationError(
                {"type": _("当前版本仅支持创建 EVM / Bitcoin / Tron 链。")}
            )

    def _detect_chain_id(self) -> int | None:
        """通过 RPC 获取链的 chain_id。"""
        try:
            w3 = Web3(Web3.HTTPProvider(self.rpc, request_kwargs={"timeout": 8}))
            chain_id = w3.eth.chain_id
        except Exception:
            return self.chain_id
        else:
            return chain_id

    def _detect_poa(self) -> bool:
        """通过获取最新区块的 extraData 长度判断是否为 POA 链（如 BSC）。"""
        try:
            w3 = Web3(Web3.HTTPProvider(self.rpc, request_kwargs={"timeout": 8}))
            block = w3.eth.get_block("latest")
            # POA 链的 extraData（proofOfAuthorityData）通常远超 32 字节
            extra = block.get("proofOfAuthorityData") or block.get("extraData", b"")
            return len(extra) > 32
        except Exception:
            return False

    def content(self):
        return {
            "name": self.name,
            "code": self.code,
            "type": self.type,
            "chain_id": self.chain_id if self.chain_id else None,
            "native_coin": self.native_coin.symbol,
        }

    @cached_property
    def w3(self):
        w3 = Web3(Web3.HTTPProvider(self.rpc, request_kwargs={"timeout": 8}))
        if self.is_poa:
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        return w3

    @property
    def adapter(self) -> "AdapterInterface":  # noqa
        from chains.adapters import AdapterFactory

        return AdapterFactory.get_adapter(chain_type=self.type)

    @property
    def get_latest_block_number(self) -> int:
        if self.type == ChainType.EVM:
            return self.w3.eth.block_number
        if self.type == ChainType.BITCOIN:
            from bitcoin.rpc import BitcoinRpcClient

            return BitcoinRpcClient(self.rpc).get_block_count()
        if self.type == ChainType.TRON:
            # Tron 区块轮询尚未接入专用 RPC 适配器。
            # 过渡期对公共 update_latest_block 仅返回数据库中已知高度，
            # 保证 active Tron 链不会在定时任务中抛异常或误推进确认流程。
            return self.latest_block_number
        msg = f"Unsupported chain type: {self.type}"
        raise NotImplementedError(msg)


class AddressUsage(models.TextChoices):
    Deposit = "deposit", _("充币帐户")
    Vault = "vault", _("金库账户")


class AddressChainState(models.Model):
    """按 (address, chain) 维护串行化状态。

    - EVM: `next_nonce` 表示下一笔待分配的 nonce
    - Bitcoin: 当前仅把这行当作数据库级互斥点使用
    """

    address = models.ForeignKey(
        "Address",
        on_delete=models.CASCADE,
        related_name="chain_states",
        verbose_name=_("地址"),
    )
    chain = models.ForeignKey(
        "Chain",
        on_delete=models.CASCADE,
        related_name="address_states",
        verbose_name=_("链"),
    )
    next_nonce = models.PositiveBigIntegerField(
        _("下一个 nonce"),
        blank=True,
        null=True,
    )
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("address", "chain"),
                name="uniq_address_chain_state_address_chain",
            ),
        ]
        verbose_name = _("地址链状态")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"{self.address_id}:{self.chain_id}"

    @classmethod
    def acquire_for_update(
        cls,
        *,
        address: Address,
        chain: Chain,
    ) -> AddressChainState:
        try:
            state, _created = cls.objects.get_or_create(address=address, chain=chain)
        except IntegrityError:
            state = cls.objects.get(address=address, chain=chain)
        return cls.objects.select_for_update().get(pk=state.pk)


# BIP44 account' 层级与业务用途的映射，直接用于派生路径 m/44'/coin'/account'/0/address_index。
BIP44_ACCOUNT_MAP: dict[str, int] = {
    AddressUsage.Vault: 0,
    AddressUsage.Deposit: 1,
}


class Wallet(UndeletableModel):
    class Meta:
        verbose_name = _("钱包")
        verbose_name_plural = _("钱包")

    def __str__(self):
        # 钱包展示名必须是稳定标识，不能把所有"非项目钱包"都误判成 Core。
        if hasattr(self, "project"):
            return f"Wallet-{self.project.appid}"
        wallet_identifier = self.pk if self.pk is not None else "unsaved"
        return f"Wallet-{wallet_identifier}"

    @classmethod
    def generate(cls) -> Wallet:
        """
        创建钱包引用并委托独立 signer 生成远端钱包。
        :return:
        """
        from chains.signer import SignerServiceError
        from chains.signer import get_signer_backend

        signer_backend = get_signer_backend()
        try:
            with db_transaction.atomic():
                # 主应用本地只保存钱包引用，密钥材料完全由 signer 托管。
                wallet = cls.objects.create()
                signer_backend.create_wallet(wallet_id=wallet.pk)
        except SignerServiceError as exc:
            raise RuntimeError("signer 服务不可用，无法创建新钱包") from exc
        return wallet

    @staticmethod
    def get_bip44_account(usage: AddressUsage | str) -> int:
        """将业务用途映射为 BIP44 account' 层级。"""
        bip44_account = BIP44_ACCOUNT_MAP.get(usage)  # type: ignore[arg-type]
        if bip44_account is None:
            raise ValueError(f"未知的 AddressUsage: {usage}")
        return bip44_account

    def get_address(
        self,
        chain_type: ChainType | str,
        usage: AddressUsage,
        address_index: int = 0,
    ) -> Address:
        """
        从当前 Wallet 派生指定链类型和用途的地址。
        先查数据库，不存在时通过 BIP44 推导地址并创建；
        并发安全：若并发创建触发唯一约束则重新查询。
        """
        from django.db import IntegrityError

        bip44_account = self.get_bip44_account(usage)

        from chains.signer import SignerServiceError
        from chains.signer import get_signer_backend
        try:
            expected_address = get_signer_backend().derive_address(
                wallet=self,
                chain_type=chain_type,
                bip44_account=bip44_account,
                address_index=address_index,
            )
        except SignerServiceError as exc:
            raise RuntimeError(
                f"signer 服务不可用，无法为钱包 {self.pk} 派生地址"
            ) from exc

        created = False
        try:
            addr_obj, created = Address.objects.get_or_create(
                wallet=self,
                chain_type=chain_type,
                usage=usage,
                address_index=address_index,
                defaults={
                    "bip44_account": bip44_account,
                    "address": expected_address,
                },
            )
        except Address.MultipleObjectsReturned as exc:
            raise RuntimeError(
                "Address 身份数据已损坏："
                f"wallet_id={self.pk} chain_type={chain_type} "
                f"usage={usage} address_index={address_index} 存在多条记录"
            ) from exc
        except IntegrityError as exc:
            # 只有地址身份唯一键竞争时才允许回查；其他唯一约束错误要继续暴露。
            try:
                addr_obj = Address.objects.get(
                    wallet=self,
                    chain_type=chain_type,
                    usage=usage,
                    address_index=address_index,
                )
            except Address.DoesNotExist as not_exist_exc:
                raise exc from not_exist_exc

        # 地址身份一旦确定，bip44_account 和 address 都必须与 HD 推导结果一致，否则就是脏数据。
        if (
            addr_obj.bip44_account != bip44_account
            or addr_obj.address != expected_address
        ):
            raise RuntimeError(
                "Address 身份数据已损坏："
                f"wallet_id={self.pk} chain_type={chain_type} "
                f"usage={usage} address_index={address_index} "
                f"expected_bip44_account={bip44_account} actual_bip44_account={addr_obj.bip44_account} "
                f"expected_address={expected_address} actual_address={addr_obj.address}"
            )

        return addr_obj


class Address(UndeletableModel):
    wallet = models.ForeignKey(
        "Wallet", on_delete=models.CASCADE, verbose_name=_("钱包")
    )
    chain_type = models.CharField(choices=ChainType, verbose_name=_("链类型"))
    usage = models.CharField(_("用途"), choices=AddressUsage)
    # BIP44 account' 层级，由 usage 决定（冗余存储用于查询优化和数据校验）。
    bip44_account = models.PositiveIntegerField(_("BIP44 账户层级"))
    # BIP44 address_index 层级，该用途下的地址序号。
    address_index = models.PositiveIntegerField(_("地址索引"), default=0)
    address = AddressField(_("地址"), unique=True)

    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        constraints = [
            # (wallet, chain_type, usage, address_index) 是 BIP44 派生地址的唯一身份。
            # 同一 address_index 可以在不同 usage 下各存在一条（因 bip44_account 不同）。
            models.UniqueConstraint(
                fields=("wallet", "chain_type", "usage", "address_index"),
                name="uniq_address_wallet_chain_usage_addridx",
            ),
        ]
        verbose_name = _("地址")
        verbose_name_plural = _("地址")

    def __str__(self):
        return f"{self.address}"

    def send_crypto(
        self,
        crypto: Crypto,
        chain: Chain,
        to: str,
        amount: Decimal,
        transfer_type: TransferType,
    ) -> str:
        """使用本账户私钥签名并发送转账，返回 tx hash / signature。

        EVM：仅创建内部任务，首次广播时才生成首个 tx_hash。
        """
        if chain.type == ChainType.EVM:
            # EvmBroadcastTask 内部管理锁，不在此处获取，避免双重加锁。
            from evm.models import EvmBroadcastTask

            decimals = crypto.get_decimals(chain)
            value_raw = int(amount * Decimal(10**decimals))
            task = EvmBroadcastTask.schedule_transfer(
                address=self,
                chain=chain,
                crypto=crypto,
                to=to,
                value_raw=value_raw,
                transfer_type=transfer_type,
            )
            return task.base_task.tx_hash

        msg = f"Unsupported chain type for send_crypto: {chain.type}"
        raise NotImplementedError(msg)



class TransferType(models.TextChoices):
    Invoice = "iv", _("💳 支付")
    Deposit = "deposit", "💰 充币"
    Withdrawal = "withdrawal", "🏧 提币"

    GasRecharge = "gas-recharge", "⛽ Gas分发"
    DepositCollection = "deposit-collection", "💵 归集充币"
    Prefunding = "prefunding", "🏦 注入金库资金"


class BroadcastTaskStage(models.TextChoices):
    QUEUED = "queued", _("待广播")
    PENDING_CHAIN = "pending_chain", _("待上链")
    PENDING_CONFIRM = "pending_confirm", _("确认中")
    FINALIZED = "finalized", _("已终结")


class BroadcastTaskResult(models.TextChoices):
    UNKNOWN = "unknown", _("未知")
    SUCCESS = "success", _("成功")
    FAILED = "failed", _("失败")


class BroadcastTaskFailureReason(models.TextChoices):
    # 通用失败原因：适用于 EVM / Bitcoin 共享的提交与调度失败路径。
    RPC_REJECTED = "rpc_rejected", _("节点拒绝")
    INSUFFICIENT_BALANCE = "insufficient_balance", _("余额不足")
    FEE_TOO_LOW = "fee_too_low", _("手续费过低")

    # EVM 特有失败原因：当前只保留真正可能落到已创建 BroadcastTask 终局的链上失败原因。
    EXECUTION_REVERTED = "execution_reverted", _("链上执行回退")

    # Bitcoin 特有失败原因：UTXO 冲突与双花只会出现在 UTXO 模型下。
    UTXO_CONFLICT = "utxo_conflict", _("UTXO 冲突")
    DOUBLE_SPEND = "double_spend", _("双花冲突")


class TxHash(models.Model):
    broadcast_task = models.ForeignKey(
        "BroadcastTask",
        on_delete=models.CASCADE,
        related_name="tx_hashes",
        verbose_name=_("链上任务"),
    )
    chain = models.ForeignKey(
        "Chain",
        on_delete=models.PROTECT,
        related_name="tx_hashes",
        verbose_name=_("链"),
    )
    hash = HashField(unique=False, verbose_name=_("交易哈希"))
    version = models.PositiveIntegerField(_("版本"))
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        ordering = ("broadcast_task_id", "version")
        constraints = [
            models.UniqueConstraint(
                fields=("chain", "hash"),
                name="uniq_tx_hash_chain_hash",
            ),
            models.UniqueConstraint(
                fields=("broadcast_task", "version"),
                name="uniq_tx_hash_task_version",
            ),
        ]
        verbose_name = _("交易哈希历史")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return self.hash

    def clean(self) -> None:
        super().clean()
        if self.broadcast_task_id and self.chain_id != self.broadcast_task.chain_id:
            raise ValidationError(
                {"chain": _("TxHash.chain 必须与 BroadcastTask.chain 保持一致。")}
            )


class BroadcastTask(UndeletableModel):
    """跨链统一的链上任务锚点。

    设计原则：
    - stage 只描述当前所处阶段：待广播 / 待上链 / 确认中 / 已终结。
    - result 只描述终局结果：未知 / 成功 / 失败。
    - 广播重试等实现细节继续留在各链子表，避免把"是否广播"污染到统一领域模型。
    - Withdrawal 等业务对象统一外键到该模型，不再直接依赖具体链实现或 tx hash。
    """

    chain = models.ForeignKey(
        "Chain",
        on_delete=models.PROTECT,
        verbose_name=_("链"),
    )
    address = models.ForeignKey(
        "Address",
        on_delete=models.PROTECT,
        verbose_name=_("地址"),
    )
    transfer_type = models.CharField(
        _("类型"),
        choices=TransferType,
    )
    crypto = models.ForeignKey(
        "currencies.Crypto",
        on_delete=models.PROTECT,
        verbose_name=_("代币"),
        blank=True,
        null=True,
    )
    recipient = AddressField(_("收款地址"), blank=True, null=True)
    amount = models.DecimalField(
        _("数量"),
        max_digits=48,
        decimal_places=18,
        blank=True,
        null=True,
    )
    tx_hash = HashField(
        unique=False,
        verbose_name=_("交易哈希"),
        blank=True,
        null=True,
    )
    stage = models.CharField(
        _("阶段"),
        choices=BroadcastTaskStage,
        default=BroadcastTaskStage.QUEUED,
    )
    result = models.CharField(
        _("结果"),
        choices=BroadcastTaskResult,
        default=BroadcastTaskResult.UNKNOWN,
    )
    # failure_reason 使用统一枚举，便于跨链统计失败来源与后台筛选。
    failure_reason = models.CharField(
        _("失败原因"),
        max_length=64,
        choices=BroadcastTaskFailureReason,
        blank=True,
        default="",
    )
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        ordering = ("-created_at",)
        constraints = [
            models.UniqueConstraint(
                fields=("chain", "tx_hash"),
                name="uniq_broadcast_task_chain_hash",
            ),
            models.CheckConstraint(
                # 只要出现成功/失败终局结果，就必须已经进入已终结阶段。
                condition=models.Q(result=BroadcastTaskResult.UNKNOWN)
                | models.Q(stage=BroadcastTaskStage.FINALIZED),
                name="ck_broadcast_task_result_requires_finalized_stage",
            ),
            models.CheckConstraint(
                # 已终结任务不能继续保留未知结果，否则会把阶段和结果语义混在一起。
                condition=~models.Q(stage=BroadcastTaskStage.FINALIZED)
                | ~models.Q(result=BroadcastTaskResult.UNKNOWN),
                name="ck_broadcast_task_finalized_requires_known_result",
            ),
            models.CheckConstraint(
                # 失败时必须明确记录失败原因；非失败任务不得携带失败原因。
                condition=(
                    models.Q(result=BroadcastTaskResult.FAILED)
                    & ~models.Q(failure_reason="")
                )
                | (
                    ~models.Q(result=BroadcastTaskResult.FAILED)
                    & models.Q(failure_reason="")
                ),
                name="ck_broadcast_task_failure_reason_matches_result",
            ),
        ]
        verbose_name = _("链上任务")
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.tx_hash or f"broadcast-task-{self.pk or 'unsaved'}"

    def clean(self) -> None:
        """在模型层显式约束阶段、结果、失败原因三者的一致性。"""
        super().clean()
        errors = {}

        if self.result in {BroadcastTaskResult.SUCCESS, BroadcastTaskResult.FAILED}:
            if self.stage != BroadcastTaskStage.FINALIZED:
                errors["stage"] = _("成功/失败结果只能出现在已终结阶段。")

        if (
            self.stage == BroadcastTaskStage.FINALIZED
            and self.result == BroadcastTaskResult.UNKNOWN
        ):
            errors["result"] = _("已终结任务必须给出成功或失败结果。")

        if self.result == BroadcastTaskResult.FAILED:
            if not self.failure_reason:
                errors["failure_reason"] = _("失败任务必须填写失败原因。")
        elif self.failure_reason:
            errors["failure_reason"] = _("仅失败任务允许填写失败原因。")

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        # BroadcastTask 是跨链主锚点，统一在保存前执行 full_clean，避免后台和脚本写入脏状态。
        self.full_clean()
        return super().save(*args, **kwargs)

    @property
    def display_status(self) -> str:
        """把阶段与结果合成为一个稳定的人类可读状态。"""
        # 使用 Django 自动生成的 get_FOO_display()，既能保留 choices 语义，
        # 也能避免 IDE 对 TextChoices.label 静态推断不准确导致的误报。
        if self.stage != BroadcastTaskStage.FINALIZED:
            return self.get_stage_display()
        return self.get_result_display()

    @property
    def is_confirmed(self) -> bool:
        return (
            self.stage == BroadcastTaskStage.FINALIZED
            and self.result == BroadcastTaskResult.SUCCESS
        )

    @db_transaction.atomic
    def append_tx_hash(self, tx_hash: str) -> TxHash:
        locked_task = BroadcastTask.objects.select_for_update().get(pk=self.pk)
        # 并发广播可能产生相同 tx_hash（相同 nonce + gas_price 签名结果相同），
        # 若已存在则视为幂等，直接返回。
        existing = TxHash.objects.filter(
            chain=locked_task.chain, hash=tx_hash
        ).first()
        if existing:
            self.tx_hash = tx_hash
            return existing
        max_version = (
            TxHash.objects.filter(broadcast_task=locked_task)
            .aggregate(max_version=models.Max("version"))
            .get("max_version")
        )
        next_version = 1 if max_version is None else int(max_version) + 1
        created = TxHash.objects.create(
            broadcast_task=locked_task,
            chain=locked_task.chain,
            hash=tx_hash,
            version=next_version,
        )
        BroadcastTask.objects.filter(pk=locked_task.pk).update(
            tx_hash=tx_hash,
            updated_at=timezone.now(),
        )
        if locked_task.transfer_type == TransferType.Withdrawal:
            from withdrawals.models import Withdrawal

            Withdrawal.objects.filter(broadcast_task=locked_task).update(
                hash=tx_hash,
                updated_at=timezone.now(),
            )
        self.tx_hash = tx_hash
        return created

    @staticmethod
    def resolve_by_hash(*, chain: Chain, tx_hash: str) -> BroadcastTask | None:
        """通过 tx_hash 查找对应的 BroadcastTask。

        优先从 TxHash 历史记录匹配（覆盖 gas 重签后的旧 hash），
        未命中时回退到 BroadcastTask.tx_hash（当前 hash）。
        两张表均有 (chain, hash) 唯一约束，无需额外去重。
        """
        if not tx_hash:
            return None
        # 优先查历史记录（gas 重签后旧 hash 只存在于 TxHash 表）
        history = (
            TxHash.objects.select_related("broadcast_task")
            .filter(chain=chain, hash=tx_hash)
            .first()
        )
        if history is not None:
            return history.broadcast_task
        # 回退到当前 tx_hash
        return BroadcastTask.objects.filter(chain=chain, tx_hash=tx_hash).first()

    @staticmethod
    def mark_finalized_success(*, chain: Chain, tx_hash: str) -> int:
        """将匹配的任务标记为成功终局。

        使用 .update() 绕过 save()/full_clean() 以避免逐行加载，
        依赖 DB CheckConstraint 保证状态三元组一致性。
        """
        if not tx_hash:
            return 0
        task = BroadcastTask.resolve_by_hash(chain=chain, tx_hash=tx_hash)
        if task is None:
            return 0
        updated = (
            BroadcastTask.objects.filter(pk=task.pk, result=BroadcastTaskResult.UNKNOWN)
            .exclude(stage=BroadcastTaskStage.FINALIZED)
            .update(
                tx_hash=tx_hash,
                stage=BroadcastTaskStage.FINALIZED,
                result=BroadcastTaskResult.SUCCESS,
                failure_reason="",
                updated_at=timezone.now(),
            )
        )
        if updated and task.transfer_type == TransferType.Withdrawal:
            from withdrawals.models import Withdrawal

            Withdrawal.objects.filter(broadcast_task=task).update(
                hash=tx_hash,
                updated_at=timezone.now(),
            )
        return updated

    @staticmethod
    def mark_finalized_failed(
        *,
        task_id: int,
        reason: BroadcastTaskFailureReason,
    ) -> int:
        """将匹配的任务标记为失败终局。

        失败终局必须保留失败原因，便于后续按失败类型统计和排查。
        """
        return (
            BroadcastTask.objects.filter(
                pk=task_id,
                result=BroadcastTaskResult.UNKNOWN,
            )
            .exclude(stage=BroadcastTaskStage.FINALIZED)
            .update(
                stage=BroadcastTaskStage.FINALIZED,
                result=BroadcastTaskResult.FAILED,
                failure_reason=reason,
                updated_at=timezone.now(),
            )
        )

    @staticmethod
    def reset_to_pending_chain(*, chain: Chain, tx_hash: str) -> int:
        """将匹配的任务回退到待上链阶段（用于 Transfer drop / reorg 恢复）。

        使用 .update() 绕过 save()/full_clean() 以避免逐行加载，
        依赖 DB CheckConstraint 保证状态三元组一致性。
        """
        task = BroadcastTask.resolve_by_hash(chain=chain, tx_hash=tx_hash)
        if task is None:
            return 0
        updated = BroadcastTask.objects.filter(
            pk=task.pk,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
        ).update(
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
            failure_reason="",
            updated_at=timezone.now(),
        )
        if updated and task.transfer_type == TransferType.Withdrawal:
            from withdrawals.models import Withdrawal

            Withdrawal.objects.filter(broadcast_task=task).update(
                hash=tx_hash,
                updated_at=timezone.now(),
            )
        return updated

    @staticmethod
    def mark_pending_confirm(*, chain: Chain, tx_hash: str) -> int:
        """链上已观察到交易后，将未终结的任务推进到确认中阶段。

        使用 .update() 绕过 save()/full_clean() 以避免逐行加载，
        依赖 DB CheckConstraint 保证状态三元组一致性。
        """
        if not tx_hash:
            return 0
        task = BroadcastTask.resolve_by_hash(chain=chain, tx_hash=tx_hash)
        if task is None:
            return 0
        updated = (
            BroadcastTask.objects.filter(pk=task.pk)
            .exclude(stage=BroadcastTaskStage.FINALIZED)
            .update(
                tx_hash=tx_hash,
                stage=BroadcastTaskStage.PENDING_CONFIRM,
                result=BroadcastTaskResult.UNKNOWN,
                failure_reason="",
                updated_at=timezone.now(),
            )
        )
        if updated and task.transfer_type == TransferType.Withdrawal:
            from withdrawals.models import Withdrawal

            Withdrawal.objects.filter(broadcast_task=task).update(
                hash=tx_hash,
                updated_at=timezone.now(),
            )
        return updated


class TransferStatus(models.TextChoices):
    CONFIRMING = "confirming", _("确认中")
    CONFIRMED = "confirmed", _("已确认")


class ConfirmMode(models.TextChoices):
    FULL = "full", _("完全")
    QUICK = "quick", _("快速")


class OnchainTransfer(models.Model):
    if TYPE_CHECKING:
        # Django 反向 OneToOne 描述符在运行时动态挂载；这里显式声明给 IDE 做静态解析。
        invoice: Invoice
        deposit: Deposit
        deposit_collection: DepositCollection
        withdrawal: Withdrawal

    chain = models.ForeignKey(Chain, on_delete=models.CASCADE, verbose_name=_("链"))
    block = models.IntegerField(_("区块"))
    # 修复：真实链上 tx hash 与"同 tx 内事件明细"拆分建模，避免继续依赖 `hash:logIndex` 字符串协议。
    hash = HashField(unique=False, verbose_name=_("哈希"))
    event_id = models.CharField(
        _("事件标识"),
        max_length=32,
        blank=True,
        default="",
        db_index=True,
    )

    crypto = models.ForeignKey(
        "currencies.Crypto", on_delete=models.CASCADE, verbose_name=_("加密货币")
    )
    from_address = AddressField(_("发送地址"))
    to_address = AddressField(_("目的地址"))
    value = models.DecimalField(_("数值"), max_digits=32, decimal_places=0)
    amount = models.DecimalField(_("数量"), max_digits=32, decimal_places=8)

    type = models.CharField(
        _("类型"),
        choices=TransferType,
        blank=True,
        default="",
    )
    confirm_mode = models.CharField(
        choices=ConfirmMode,
        default=ConfirmMode.FULL,
        max_length=8,
        verbose_name=_("确认模式"),
    )
    timestamp = models.PositiveIntegerField(verbose_name=_("时间戳"), db_index=True)
    datetime = models.DateTimeField(verbose_name=_("日期"))
    status = models.CharField(
        choices=TransferStatus,
        default=TransferStatus.CONFIRMING,
        verbose_name=_("状态"),
    )

    processed_at = models.DateTimeField(_("处理时间"), blank=True, null=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        ordering = ("-timestamp",)
        verbose_name = _("转账")
        verbose_name_plural = _("转账")
        constraints = [
            models.UniqueConstraint(
                fields=("chain", "hash", "event_id"),
                name="uniq_transfer_chain_hash_event",
            ),
        ]

    def __str__(self):
        return self.hash

    @property
    def content(self):
        return {
            "chain": self.chain.code,
            "block": self.block,
            "hash": self.hash,
            "status": self.status,
            "is_confirmed": self.status == TransferStatus.CONFIRMED,
        }

    @db_transaction.atomic
    def process(self):
        # 先加行锁再刷新，防止两个 Celery worker 并发处理同一笔转账
        OnchainTransfer.objects.select_for_update().get(pk=self.pk)
        self.refresh_from_db()
        if self.processed_at:
            return

        from deposits.service import DepositService
        from invoices.service import InvoiceService
        from withdrawals.service import WithdrawalService

        # 资金类转账应在首个业务成功匹配后立即停止，避免同一笔转账被多条业务路径重复消费。
        (
            InvoiceService.try_match_invoice(self)
            or DepositService.try_create_deposit(self)
            or DepositService.try_match_gas_recharge(self)
            or DepositService.try_match_collection(self)
            or WithdrawalService.try_match_withdrawal(self)
            or WithdrawalService.try_match_withdrawal_funding(self)
        )
        self.processed_at = timezone.now()
        # Transfer 处理完成标记不依赖 save() 信号，直接 update 可减少无关字段回写。
        OnchainTransfer.objects.filter(pk=self.pk).update(
            processed_at=self.processed_at
        )

        if self.confirm_mode == ConfirmMode.QUICK:
            from .tasks import confirm_transfer

            confirm_transfer.delay(self.pk)

    @db_transaction.atomic
    def confirm(self):
        # 修复：确认前先加行锁并刷新，避免多个 worker 对同一笔转账重复确认和重复计费。
        OnchainTransfer.objects.select_for_update().get(pk=self.pk)
        self.refresh_from_db()
        if self.status == TransferStatus.CONFIRMED:
            return

        self.status = TransferStatus.CONFIRMED
        # Transfer 状态推进不依赖 post_save 更新逻辑，直接 update 可减少并发覆盖面。
        OnchainTransfer.objects.filter(pk=self.pk).update(
            status=TransferStatus.CONFIRMED
        )
        # 统一父任务在确认后进入稳定成功终局；业务层不需要感知广播细节。
        BroadcastTask.mark_finalized_success(chain=self.chain, tx_hash=self.hash)

        # Transfer 确认后更新系统账户余额（与业务类型无关，所有确认转账均触发）
        Balance.update_from_transfer(self)

        self._dispatch_business_confirm()

    @db_transaction.atomic
    def drop(self):
        """回退关联业务状态，然后删除 Transfer 记录。

        删除记录以释放唯一约束 (chain, hash, event_id),
        使 reorg 后同一笔 tx 被重新打包时, 扫描器可以自然重建 Transfer。
        """
        # 先加行锁，防止并发处理；已删除的 Transfer 直接跳过。
        if not OnchainTransfer.objects.select_for_update().filter(pk=self.pk).exists():
            return
        self.refresh_from_db()

        self._dispatch_business_drop()

        # 当确认前已观察到的交易后来又查不到时, 按"回退到待上链"处理;
        # 让任务继续通过重广播自愈, 而不是直接进入失败终局。
        BroadcastTask.reset_to_pending_chain(chain=self.chain, tx_hash=self.hash)

        self.delete()

    @property
    def confirm_progress(self):
        has = max(self.chain.latest_block_number - self.block, 1)
        need = self.chain.confirm_block_count

        if need <= 0:
            progress = 100
        else:
            progress = int(min(100.0, (has / need) * 100))

        if self.confirm_mode == ConfirmMode.QUICK:
            return {
                "has_confirmed_count": has,
                "need_confirmed_count": 1,
                "progress": 100,  # 统一返回 0-100 整数
            }

        return {
            "has_confirmed_count": has,
            "need_confirmed_count": need,
            "progress": progress,  # 统一返回 0-100 整数
        }

    def _dispatch_business_confirm(self) -> None:
        """统一按 TransferType 分发确认动作，confirm() 专用。"""
        from deposits.service import DepositService
        from invoices.service import InvoiceService
        from withdrawals.service import WithdrawalService

        if self.type == TransferType.Invoice:
            InvoiceService.confirm_invoice(self.invoice)
        elif self.type == TransferType.Deposit:
            DepositService.confirm_deposit(self.deposit)
        elif self.type == TransferType.DepositCollection:
            from deposits.models import DepositCollection  # noqa: WPS433

            with contextlib.suppress(DepositCollection.DoesNotExist):
                DepositService.confirm_collection(self.deposit_collection)
        elif self.type == TransferType.Withdrawal:
            from withdrawals.models import Withdrawal  # noqa: WPS433

            with contextlib.suppress(Withdrawal.DoesNotExist):
                WithdrawalService.confirm_withdrawal(self)

    def _dispatch_business_drop(self) -> None:
        """统一按 TransferType 分发回退动作，drop() 专用。"""
        from deposits.service import DepositService
        from invoices.service import InvoiceService
        from withdrawals.service import WithdrawalService

        if self.type == TransferType.Invoice:
            InvoiceService.drop_invoice(self.invoice)
        elif self.type == TransferType.Deposit:
            DepositService.drop_deposit(self.deposit)
        elif self.type == TransferType.DepositCollection:
            from deposits.models import DepositCollection  # noqa: WPS433

            with contextlib.suppress(DepositCollection.DoesNotExist):
                DepositService.drop_collection(self.deposit_collection)
        elif self.type == TransferType.Withdrawal:
            from withdrawals.models import Withdrawal  # noqa: WPS433

            with contextlib.suppress(Withdrawal.DoesNotExist):
                WithdrawalService.drop_withdrawal(self)


class Balance(models.Model):
    """记录每个系统账户在某条链上持有某代币的实时余额，随 OnchainTransfer 确认后自动更新。

    以 ChainToken (currencies.ChainToken) 为粒度，天然同时包含 crypto 和 chain 信息，
    避免同一代币在不同链上的余额被混为一条记录。
    value 为权威链上原始整数单位；amount 仅保留给后台和报表做展示。
    """

    address = models.ForeignKey(
        "Address",
        on_delete=models.CASCADE,
        related_name="balances",
        verbose_name=_("地址"),
    )
    chain_token = models.ForeignKey(
        "currencies.ChainToken",
        on_delete=models.CASCADE,
        related_name="balances",
        verbose_name=_("代币部署"),
    )
    # 权威余额使用链上原始整数单位（wei/satoshi/...），避免展示精度截断污染记账结果。
    value = models.DecimalField(
        _("原始余额"),
        max_digits=48,
        decimal_places=0,
        default=Decimal("0"),
    )
    # amount 仅用于人类可读展示；真实记账与累计一律以 value 为准。
    amount = models.DecimalField(
        _("余额"),
        max_digits=50,
        decimal_places=30,
        default=Decimal("0"),
    )
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
        constraints = [
            models.UniqueConstraint(
                fields=("address", "chain_token"),
                name="uniq_balance_address_chain_token",
            ),
        ]
        verbose_name = _("余额")
        verbose_name_plural = _("余额")

    def __str__(self):
        from common.utils.math import format_decimal_stripped

        # 余额字符串主要用于后台和排障，展示时去掉末尾无意义的 0。
        return f"{self.address} | {self.chain_token} | {format_decimal_stripped(self.amount)}"

    @classmethod
    def _to_amount(cls, value: Decimal, *, decimals: int) -> Decimal:
        """把链上原始整数余额转换为人类可读余额。"""
        return Decimal(value) / (Decimal(10) ** decimals)

    @classmethod
    def adjust(
        cls,
        address: Address,
        chain_token,
        delta_value: Decimal,
    ) -> None:
        """原子性地调整账户在指定链/代币上的余额：delta_value 为原始整数单位增量。

        采用 update-then-insert 模式保证并发安全：
        1. 先尝试 UPDATE 已有行，若 Balance 记录不存在则无行被更新；
        2. 无行更新时执行 INSERT 创建初始记录；
        3. 若 INSERT 遇到唯一约束冲突（并发创建），则回退为重试 UPDATE。
        """
        from django.db.models import F

        decimals = chain_token.decimals
        if decimals is None:
            decimals = chain_token.crypto.decimals
        delta_amount = cls._to_amount(delta_value, decimals=decimals)

        rows_updated = cls.objects.filter(
            address=address, chain_token=chain_token
        ).update(
            value=F("value") + delta_value,
            amount=F("amount") + delta_amount,
        )
        if rows_updated == 0:
            try:
                # 用 savepoint 隔离 INSERT，避免 IntegrityError 污染外层事务
                with db_transaction.atomic():
                    cls.objects.create(
                        address=address,
                        chain_token=chain_token,
                        value=delta_value,
                        amount=delta_amount,
                    )
            except IntegrityError:
                # 并发场景：另一事务已先创建该记录，重试 UPDATE
                cls.objects.filter(address=address, chain_token=chain_token).update(
                    value=F("value") + delta_value,
                    amount=F("amount") + delta_amount,
                )

    @classmethod
    def update_from_transfer(cls, transfer: OnchainTransfer) -> None:
        """OnchainTransfer 确认后，更新涉及系统账户的余额。

        - to_address 对应的系统账户：余额 +value（收款）
        - from_address 对应的系统账户：余额 -value（付款）
        非系统账户（用户/外部地址）不在 Address 表中，DoesNotExist 时静默跳过。
        OnchainTransfer 关联的 ChainToken 不存在时记录警告并跳过（理论上不应发生）。
        """
        import structlog

        from currencies.models import ChainToken

        logger = structlog.get_logger()

        try:
            ct = ChainToken.objects.get(crypto=transfer.crypto, chain=transfer.chain)
        except ChainToken.DoesNotExist:
            logger.warning(
                "Balance.update_from_transfer: ChainToken 不存在，跳过余额更新",
                crypto=transfer.crypto_id,
                chain=transfer.chain_id,
                transfer=transfer.pk,
            )
            return

        try:
            to_addr = Address.objects.get(address=transfer.to_address)
            cls.adjust(to_addr, ct, transfer.value)
        except Address.DoesNotExist:
            pass

        try:
            from_addr = Address.objects.get(address=transfer.from_address)
            cls.adjust(from_addr, ct, -transfer.value)
        except Address.DoesNotExist:
            pass
