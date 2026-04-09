from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import models
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from shortuuid.django_fields import ShortUUIDField
from simple_history.models import HistoricalRecords

from chains.capabilities import ChainProductCapabilityService
from chains.models import Chain
from chains.models import ChainType
from common.consts import UPPER_ALPHABET
from common.fields import AddressField


class Project(models.Model):
    appid = ShortUUIDField(
        verbose_name=_("Appid"),
        prefix="XC-",
        alphabet=UPPER_ALPHABET,
        db_index=True,
        editable=False,
        unique=True,
        length=8,
    )
    name = models.CharField(
        verbose_name=_("项目名称"),
        help_text=_("对外作为商户名展示"),
        unique=True,
    )
    wallet = models.OneToOneField("chains.Wallet", on_delete=models.CASCADE)
    ip_white_list = models.TextField(
        _("IP白名单"),
        help_text=mark_safe(  # noqa: S308 — admin help_text，内容为硬编码中文字符串，无 XSS 风险
            _("只有符合白名单的 IP 才可以与本网关交互，支持 IP 地址或 IP 网段")
            + "<br>"
            + _("可同时设置多个，中间用英文逗号 ',' 分割")
            + "<br>"
            + _("* 代表允许所有 IP 访问")
        ),
    )
    webhook = models.URLField(
        _("通知地址"),
        help_text=_("用于本网关发送通知到项目后端"),
    )
    webhook_open = models.BooleanField(verbose_name=_("通知状态"), default=True)
    failed_count = models.PositiveIntegerField(
        default=0,
        verbose_name=_("连续失败次数"),
    )
    pre_notify = models.BooleanField(
        _("开启预通知"),
        default=False,
        help_text="刚出块(尚未达到区块确认数)，就发送一次预通知",
    )
    fast_confirm_threshold = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        default=Decimal("10"),
        verbose_name=_("快速确认阈值（USD）"),
        help_text=_("低于该金额的账单无需等待，立即确认"),
    )
    hmac_key = ShortUUIDField(
        verbose_name=_("HMAC密钥"),
        length=32,
    )

    gather_worth = models.DecimalField(
        max_digits=16,
        decimal_places=2,
        verbose_name="自动归集价值(USD)",
        default=10,
        help_text="某充币地址中,若某代币的价值大于此,则自动归集",
        blank=True,
    )
    gather_period = models.PositiveIntegerField(
        verbose_name="自动归集周期(日)",
        default=32,
        help_text="某充币地址中,若某代币大于此周期未进行归集操作,则自动归集",
        blank=True,
    )
    withdrawal_review_required = models.BooleanField(
        _("提币需审核"),
        default=True,
        help_text=_(
            "开启后，新提币请求会先进入审核中，需后台批准后才会进入链上发送队列"
        ),
    )
    withdrawal_review_exempt_limit = models.DecimalField(
        _("免审核门槛(USD)"),
        max_digits=16,
        decimal_places=2,
        null=True,
        blank=True,
        help_text=_(
            "仅在开启提币审核时生效；低于该金额的提币可直接进入链上发送队列，留空表示全部需要审核"
        ),
    )
    withdrawal_single_limit = models.DecimalField(
        _("单笔提币限额(USD)"),
        max_digits=16,
        decimal_places=2,
        null=True,
        blank=True,
        help_text=_("留空表示不限额；超出时直接拒绝创建提币请求"),
    )
    withdrawal_daily_limit = models.DecimalField(
        _("单日提币限额(USD)"),
        max_digits=16,
        decimal_places=2,
        null=True,
        blank=True,
        help_text=_("留空表示不限额；当天已创建的提币请求也会占用额度"),
    )

    active = models.BooleanField(verbose_name=_("启用"), default=True)
    history = HistoricalRecords()
    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("创建时间"))

    class Meta:
        verbose_name = _("项目")
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.name

    @classmethod
    def retrieve(cls, appid: str):
        try:
            return cls.objects.get(appid=appid)
        except cls.DoesNotExist:
            return None

    @property
    def is_ready(self) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if not self.ip_white_list:
            errors.append(_("项目IP白名单未设置"))
        if not self.webhook:
            errors.append(_("项目通知地址未设置"))
        if not self.webhook_open:
            errors.append(_("项目通知已关闭"))
        if not self.active:
            errors.append(_("项目未启用"))
        if not RecipientAddress.objects.filter(
            project=self, used_for_invoice=True
        ).exists():
            errors.append(_("本项目未设置支付地址"))
        return (not errors), errors

    def recipients(self, chain: Chain):
        return set(
            RecipientAddress.objects.filter(
                project=self,
                chain_type=chain.type,
                used_for_invoice=True,
            ).values_list(
                "address",
                flat=True,
            ),
        )


def status(request):
    return ""


class RecipientAddress(models.Model):
    name = models.CharField(verbose_name=_("名称"))
    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.CASCADE,
        verbose_name=_("项目"),
    )
    chain_type = models.CharField(
        _("地址格式"),
        choices=ChainType,
        help_text="EVM: Ethereum, BSC, Polygon, Base...<br>Bitcoin: Bitcoin",
    )
    address = AddressField(verbose_name=_("收币地址"))
    used_for_invoice = models.BooleanField(_("用于账单收款"), default=True)
    used_for_deposit = models.BooleanField(_("用于归集充币"), default=False)
    history = HistoricalRecords()
    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("创建时间"))

    class Meta:
        # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
        constraints = [
            models.UniqueConstraint(
                fields=("chain_type", "address"),
                name="uniq_recipient_address_chain_type_address",
            ),
            models.CheckConstraint(
                condition=(
                    models.Q(used_for_invoice=True, used_for_deposit=False)
                    | models.Q(used_for_invoice=False, used_for_deposit=True)
                ),
                name="recipient_address_single_usage",
            ),
        ]
        verbose_name = _("收币地址")
        verbose_name_plural = _("收币地址")

    def __str__(self):
        return self.address

    def save(self, *args, **kwargs):
        # 收币地址的链上发现完全由内部扫描器负责；模型层只保留数据校验，不再派发外部订阅同步。
        previous = None
        if self.pk is not None:
            previous = (
                RecipientAddress.objects.filter(pk=self.pk)
                .values("chain_type", "address")
                .first()
            )
        self.full_clean()
        result = super().save(*args, **kwargs)

        should_schedule_sync = self.chain_type == ChainType.BITCOIN and (
            previous is None
            or previous["chain_type"] != self.chain_type
            or previous["address"] != self.address
        )
        if should_schedule_sync:
            from bitcoin.watch_sync import schedule_watch_address_sync_on_commit

            # 管理后台保存项目 BTC 收款地址后，提交事务即可把 watch-only 同步到节点钱包。
            schedule_watch_address_sync_on_commit()

        return result

    def clean(self) -> None:
        """按用途校验项目收币地址允许进入的链类型。"""
        super().clean()
        if self.used_for_invoice == self.used_for_deposit:
            raise ValidationError(
                _("项目地址必须且只能用于一种用途：支付地址或归集地址。")
            )
        if not self.chain_type:
            return

        if self.used_for_invoice:
            allowed_chain_types = (
                ChainProductCapabilityService.INVOICE_RECIPIENT_CHAIN_TYPES
            )
            error_message = _("当前版本支付地址仅支持 EVM / Bitcoin / Tron。")
        else:
            allowed_chain_types = (
                ChainProductCapabilityService.COLLECTION_RECIPIENT_CHAIN_TYPES
            )
            error_message = _("当前版本归集地址仅支持 EVM / Bitcoin。")

        if self.chain_type not in allowed_chain_types:
            raise ValidationError({"chain_type": error_message})
