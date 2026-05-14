from django.db import models
from django.utils.translation import gettext_lazy as _
from risk.models import RiskLevel

from chains.models import Address
from chains.models import AddressUsage
from chains.models import Chain
from chains.models import ChainType
from chains.types import AddressStr
from common.fields import HashField
from common.fields import SysNoField
from users.models import Customer


class DepositAddress(models.Model):
    customer = models.ForeignKey(
        Customer, on_delete=models.CASCADE, verbose_name=_("客户")
    )
    chain_type = models.CharField(choices=ChainType, verbose_name=_("链类型"))
    address = models.OneToOneField(
        Address, on_delete=models.CASCADE, verbose_name=_("地址")
    )

    class Meta:
        # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
        constraints = [
            models.UniqueConstraint(
                fields=("customer", "chain_type"),
                name="uniq_deposit_address_customer_chain_type",
            ),
        ]
        verbose_name = _("充币地址")
        verbose_name_plural = _("充币地址")

    def __str__(self):
        return self.address.address

    @staticmethod
    def _get_address_by_chain_type(
        *, chain_type: ChainType | str, customer: Customer
    ) -> AddressStr:
        """
        获取（或首次创建）某客户在指定链类型上的充币地址。

        并发安全说明：
        - 快速路径命中时直接返回，无竞态。
        - 慢路径中 get_address 内部使用 get_or_create + IntegrityError 捕获，
          保证同参数幂等返回同一个 Address（不会产生孤儿记录）。
        - DepositAddress 的 get_or_create 由唯一约束 (customer, chain_type) 保证安全。
        - 并发时 signer 可能被多次调用（幂等但有额外 RPC 开销），属于可接受的代价。

        L2 防御：在创建/返回充币地址之前必须校验 project 已配置对应链的
        DEPOSIT_COLLECTION recipient。否则用户充进来的资金永远归集不出去，
        会在 gather_deposits 队列里反复抢占调度名额，构成 DoS 攻击面。
        校验放在 model 静态方法内（service 层），让两个 API viewset 入口
        （public 与 internal_api）共享，不必在每个 viewset 重复实现。

        快速路径仍允许命中：已经存在的 deposit address 不再次校验，避免
        历史地址在 recipient 短暂被删除时返回失败；新建地址才必须有 recipient。
        """
        # 快速路径：已存在直接返回（历史地址不重复校验，避免 recipient 临时移除时影响存量用户）
        try:
            return DepositAddress.objects.get(
                chain_type=chain_type, customer=customer
            ).address.address
        except DepositAddress.DoesNotExist:
            pass

        # L2：新建充币地址前强校验 recipient 已配置；
        # 漏配时拒绝创建，让商户/运营先在后台补配，杜绝"用户已充值但归集不出去"的死局。
        # 局部 import 避开模块循环依赖（projects → deposits 链路反向引用）。
        from common.error_codes import ErrorCode
        from common.exceptions import APIError
        from projects.models import RecipientAddress
        from projects.models import RecipientAddressUsage

        if not RecipientAddress.objects.filter(
            project=customer.project,
            chain_type=chain_type,
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        ).exists():
            raise APIError(ErrorCode.RECIPIENT_NOT_CONFIGURED)

        # 从项目钱包派生该客户专属账户（get_address 内部 get_or_create 保证幂等）
        addr = customer.project.wallet.get_address(
            chain_type=chain_type,
            usage=AddressUsage.Deposit,
            address_index=customer.address_index,
        )

        # get_or_create 保证唯一约束 (customer, chain_type) 下的并发创建安全
        deposit_addr, _ = DepositAddress.objects.get_or_create(
            chain_type=chain_type,
            customer=customer,
            defaults={"address": addr},
        )
        return deposit_addr.address.address

    @staticmethod
    def get_address(chain: Chain, customer: Customer) -> AddressStr:
        return DepositAddress._get_address_by_chain_type(
            chain_type=chain.type, customer=customer
        )

    @staticmethod
    def get_address_by_chain_type(
        *, chain_type: ChainType | str, customer: Customer
    ) -> AddressStr:
        return DepositAddress._get_address_by_chain_type(
            chain_type=chain_type, customer=customer
        )


class DepositStatus(models.TextChoices):
    # 状态1: 交易已上链，等待区块链确认数达标
    CONFIRMING = "confirming", _("确认中")
    # 状态2: 交易确认数达标，充值成功
    COMPLETED = "completed", _("已完成")


class CollectSchedule(models.Model):
    """按充币地址维度维护下一次归集建单时间。"""

    deposit_address = models.ForeignKey(
        "deposits.DepositAddress",
        on_delete=models.CASCADE,
        related_name="collect_schedules",
        verbose_name=_("充币地址"),
    )
    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.CASCADE,
        verbose_name=_("链"),
    )
    crypto = models.ForeignKey(
        "currencies.Crypto",
        on_delete=models.CASCADE,
        verbose_name=_("代币"),
    )
    next_collect_time = models.DateTimeField(
        db_index=True,
        verbose_name=_("下次归集时间"),
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("deposit_address", "chain", "crypto"),
                name="uniq_collect_schedule_address_chain_crypto",
            ),
        ]
        verbose_name = _("归集调度")
        verbose_name_plural = _("归集调度")

    def __str__(self) -> str:
        return (
            f"CollectSchedule({self.deposit_address_id}, "
            f"{self.chain_id}, {self.crypto_id})"
        )


class GasRecharge(models.Model):
    """Gas 补充记录：归集前 Vault → 充币地址 的原生币预充。"""

    deposit_address = models.ForeignKey(
        "deposits.DepositAddress",
        on_delete=models.PROTECT,
        related_name="gas_recharges",
        verbose_name=_("充币地址"),
    )
    broadcast_task = models.OneToOneField(
        "chains.BroadcastTask",
        on_delete=models.PROTECT,
        related_name="gas_recharge",
        verbose_name=_("链上任务"),
    )
    transfer = models.OneToOneField(
        "chains.OnchainTransfer",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="gas_recharge",
        verbose_name=_("链上转账"),
    )
    recharged_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name=_("到账时间"),
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("Gas 补充")
        verbose_name_plural = _("Gas 补充")

    def __str__(self) -> str:
        return f"GasRecharge({self.deposit_address_id}→{self.broadcast_task_id})"


class DepositCollection(models.Model):
    """
    归集记录：代表一次从充值地址到金库地址的链上归集交易。

    设计说明：
    一笔链上归集交易可以归集同一客户在同链同币下的多笔 Deposit（一对多关系）。
    将归集记录独立建模，避免在 Deposit 上冗余存储 collection_hash + collection_transfer
    两个字段造成的语义重叠和数据不一致风险。
    """

    # unique=True + null=True：Collection 在创建任务后即可落库，但真实链上 tx hash
    # 要等到扫描器观察到归集转账时才会回填，因此需要允许暂时为 NULL。
    collection_hash = HashField(
        max_length=66,
        unique=True,
        null=True,
        blank=True,
        verbose_name=_("归集哈希"),
    )
    transfer = models.OneToOneField(
        "chains.OnchainTransfer",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="deposit_collection",
        verbose_name=_("归集转账"),
    )
    broadcast_task = models.OneToOneField(
        "chains.BroadcastTask",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="deposit_collection",
        verbose_name=_("链上任务"),
    )
    collected_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name=_("归集确认时间"),
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("归集记录")
        verbose_name_plural = _("归集记录")

    def __str__(self) -> str:
        hash_display = self.collection_hash[:10] if self.collection_hash else "pending"
        return f"DepositCollection({hash_display})"


class Deposit(models.Model):
    sys_no = SysNoField(prefix="DXC")
    customer = models.ForeignKey(
        "users.Customer",
        on_delete=models.PROTECT,
        verbose_name=_("客户"),
    )
    transfer = models.OneToOneField(
        "chains.OnchainTransfer",
        on_delete=models.CASCADE,
        verbose_name=_("链上转账"),
    )
    worth = models.DecimalField(
        _("价值(USD)"),
        max_digits=16,
        decimal_places=6,
        default=0,
    )
    status = models.CharField(
        choices=DepositStatus,
        verbose_name=_("状态"),
        default=DepositStatus.CONFIRMING,
    )
    risk_level = models.CharField(  # noqa: DJ001
        _("风险等级"),
        choices=RiskLevel,
        max_length=16,
        null=True,
        blank=True,
        db_index=True,
    )
    risk_score = models.DecimalField(
        _("风险分数"),
        max_digits=6,
        decimal_places=2,
        null=True,
        blank=True,
    )
    # 多笔 Deposit 可共享同一笔归集交易（DepositCollection）；
    # 一旦进入归集流程，该关系即固定为该次归集记录。
    collection = models.ForeignKey(
        "deposits.DepositCollection",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="deposits",
        verbose_name=_("归集记录"),
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("充币")
        verbose_name_plural = _("充币")

    def __str__(self) -> str:
        return f"Deposit({self.sys_no}, status={self.status})"

    @property
    def content(self):
        from deposits.service import DepositService

        return DepositService.build_webhook_payload(self)
