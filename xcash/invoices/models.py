from decimal import Decimal
from typing import TYPE_CHECKING

import structlog
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import models
from django.db import transaction as db_transaction
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

logger = structlog.get_logger()

from common.fields import AddressField
from common.fields import SysNoField
from common.permission_check import filter_saas_allowed_methods
from currencies.service import CryptoService
from currencies.service import FiatService
from projects.models import Project
from projects.service import ProjectService

from .exceptions import InvoiceAllocationError

if TYPE_CHECKING:
    from chains.models import Chain
    from currencies.models import Crypto


class InvoiceStatus(models.TextChoices):
    WAITING = "waiting", _("待支付")
    CONFIRMING = "confirming", _("确认中")
    COMPLETED = "completed", _("已完成")
    EXPIRED = "expired", _("已超时")


class InvoiceProtocol(models.TextChoices):
    NATIVE = "native", _("Xcash 原生")
    EPAY_V1 = "epay_v1", _("EPay V1")


class InvoicePaySlotStatus(models.TextChoices):
    ACTIVE = "active", _("生效中")
    MATCHED = "matched", _("已命中")
    DISCARDED = "discarded", _("已丢弃")


class InvoicePaySlotDiscardReason(models.TextChoices):
    OVERFLOW = "overflow", _("超上限淘汰")
    EXPIRED = "expired", _("账单过期")
    SETTLED = "settled", _("已被其他付款占用")


class Invoice(models.Model):
    MAX_ALLOCATION_RETRY = 5
    MAX_ACTIVE_PAY_SLOTS = 2

    # 保留类属性别名，使 Invoice.InvoiceAllocationError 继续可用。
    InvoiceAllocationError = InvoiceAllocationError

    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.CASCADE,
        verbose_name=_("项目"),
    )
    sys_no = SysNoField(prefix="INV")

    out_no = models.CharField(
        verbose_name=_("商户单号"),
        db_index=True,
    )
    title = models.CharField(_("标题"))
    currency = models.CharField(_("计价货币"))
    amount = models.DecimalField(
        verbose_name=_("金额"),
        max_digits=32,
        decimal_places=8,
    )
    methods = models.JSONField(
        default=dict,
        verbose_name=_("支持的支付方式"),
    )
    email = models.EmailField(
        _("邮箱"),
        blank=True,
        default="",
        help_text=_("如果设置邮箱，在Invoice创建后会自动发送支付链接到此邮箱地址。"),
    )

    crypto = models.ForeignKey(
        "currencies.Crypto",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        verbose_name=_("加密货币"),
    )
    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
        verbose_name=_("链"),
    )
    pay_amount = models.DecimalField(
        verbose_name=_("支付加密货币数量"),
        max_digits=32,
        decimal_places=8,
        blank=True,
        null=True,
        help_text=_("支付加密货币数量可能在原始数量上差额浮动"),
    )
    pay_address = AddressField(
        verbose_name=_("支付地址"),
        blank=True,
        null=True,
        db_index=True,
    )

    started_at = models.DateTimeField(_("支付开始时间"), auto_now_add=True)
    expires_at = models.DateTimeField(_("支付截止时间"))
    return_url = models.URLField(_("支付成功后同步跳转地址"), blank=True)
    worth = models.DecimalField(
        _("价值(USD)"),
        max_digits=16,
        decimal_places=6,
        default=0,
    )
    transfer = models.OneToOneField(
        "chains.OnchainTransfer",
        on_delete=models.SET_NULL,
        verbose_name=_("链上转账"),
        blank=True,
        null=True,
    )
    status = models.CharField(
        choices=InvoiceStatus,
        default=InvoiceStatus.WAITING,
        verbose_name=_("状态"),
    )
    protocol = models.CharField(
        choices=InvoiceProtocol,
        default=InvoiceProtocol.NATIVE,
        max_length=16,
        db_index=True,
        verbose_name=_("接入协议"),
    )

    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        ordering = ("-created_at",)
        verbose_name = _("账单")
        verbose_name_plural = _("账单")
        constraints = [
            # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
            models.UniqueConstraint(
                fields=("project", "out_no"),
                name="uniq_invoice_project_out_no",
            ),
        ]

    def __str__(self):
        return f"{self.sys_no}"

    @property
    def is_crypto_fixed(self):
        """是否为固定加密货币模式"""
        return CryptoService.exists(self.currency)

    @property
    def active_pay_slots(self):
        # 账单允许保留多个历史支付槽位；当前仍可命中的集合统一以 active 状态为准。
        return self.pay_slots.filter(status=InvoicePaySlotStatus.ACTIVE)

    @property
    def current_pay_slot(self):
        # 展示层快照优先跟随最近一次仍生效的支付槽位。
        return self.active_pay_slots.order_by("-version", "-created_at", "-pk").first()

    @classmethod
    def available_methods(cls, project: Project) -> dict[str, list[str]]:
        """返回项目当前可用的 crypto→链列表。

        逻辑 = 系统支持的 invoice (crypto, chain) 组合 ∩ 项目已配置收币地址的链 ∩ SaaS 白名单。
        """
        receivable_codes = ProjectService.receivable_chain_codes(project)
        allowed = CryptoService.allowed_methods(chain_codes=receivable_codes)

        methods = {
            symbol: sorted(allowed[symbol] & receivable_codes)
            for symbol in allowed
            if allowed[symbol] & receivable_codes
        }

        return filter_saas_allowed_methods(
            appid=project.appid,
            methods=methods,
        )

    @db_transaction.atomic
    def select_method(self, crypto: "Crypto", chain: "Chain"):
        # 先锁账单行，保证同一账单的多次切链/切币不会并发写出超过上限的活跃槽位。
        Invoice.objects.select_for_update().get(pk=self.pk)
        self.refresh_from_db()

        current_slot = self.current_pay_slot
        if (
            current_slot is not None
            and current_slot.crypto_id == crypto.id
            and current_slot.chain_id == chain.id
        ):
            self._sync_snapshot_from_slot(current_slot)
            return True

        if self.is_crypto_fixed:
            crypto_amount = self.amount
        else:
            fiat = FiatService.get_by_code(self.currency)
            crypto_amount = FiatService.to_crypto(
                fiat=fiat, crypto=crypto, amount=self.amount
            )

        detail = (
            f"project={self.project_id}, crypto={crypto.symbol}, chain={chain.code}"
        )

        for _retry in range(self.MAX_ALLOCATION_RETRY):
            try:
                with db_transaction.atomic():
                    pay_address, pay_amount = Invoice.get_pay_differ(
                        project=self.project,
                        crypto=crypto,
                        chain=chain,
                        crypto_amount=crypto_amount,
                    )

                    if not (pay_address and pay_amount):
                        logger.warning(
                            "Invoice pay method allocation failed",
                            detail=detail,
                        )
                        raise self.InvoiceAllocationError(detail)

                    created_at = timezone.now()
                    pay_slot = InvoicePaySlot.objects.create(
                        invoice=self,
                        project=self.project,
                        version=self._next_pay_slot_version(),
                        crypto=crypto,
                        chain=chain,
                        pay_address=pay_address,
                        pay_amount=pay_amount,
                        status=InvoicePaySlotStatus.ACTIVE,
                    )
                    self._discard_excess_active_slots(created_slot=pay_slot)
                    self._sync_snapshot_from_slot(
                        pay_slot,
                        started_at=created_at,
                    )
                    return True
            except IntegrityError:
                logger.warning(
                    "Invoice pay slot conflicted, retrying",
                    detail=detail,
                )
                continue

        raise self.InvoiceAllocationError(f"{detail} (alloc retry exceeded)")

    @classmethod
    def get_pay_differ(
        cls,
        project: Project,
        crypto: "Crypto",
        chain: "Chain",
        crypto_amount: Decimal,
    ) -> tuple[str | None, Decimal | None]:
        """Differ amount 分配算法：为每张 Invoice 找到一个唯一的 (地址, 金额) 组合。

        核心思路：
        1. 从基础金额起步，以 crypto.differ_step 为单位生成 101 个候选金额（差额 0~100 档），
           浮动范围极小（通常 < 0.001%），对买家感知影响可忽略。
        2. 先遍历金额再遍历地址（双层循环），保证同一账单产生的差额尽量小——
           优先在最小差额下轮换地址，地址不够时才升档。
        3. 并发安全由 InvoicePaySlot 的部分唯一约束（uniq_invoice_pay_slot_active）保证，
           冲突时由 select_method() 的 IntegrityError 重试循环处理。
           不使用 SELECT FOR UPDATE——悲观锁范围过广（101×N 行）会与 FK 约束检查
           形成环形等待，导致死锁。
        4. 若 101 × N 个组合全部被占用，返回 (None, None) 表示分配失败。
        """
        now = timezone.now()

        amounts = [crypto_amount + step * crypto.differ_step for step in range(101)]
        addresses = list(
            ProjectService.invoice_recipient_addresses(
                project,
                chain_type=chain.type,
            )
        )
        if not addresses:
            return None, None

        # 乐观并发：普通 SELECT 读取已占用的槽位，不加行锁。
        # 并发事务可能同时选中同一空隙，由 uniq_invoice_pay_slot_active 约束拦截，
        # 外层 select_method() 捕获 IntegrityError 后重试即可。
        existing = set(
            InvoicePaySlot.objects.filter(
                project=project,
                crypto=crypto,
                chain=chain,
                status=InvoicePaySlotStatus.ACTIVE,
                pay_amount__in=amounts,
                pay_address__in=addresses,
                invoice__expires_at__gte=now,
            ).values_list("pay_address", "pay_amount")
        )

        for amount in amounts:
            for address in addresses:
                if (address, amount) not in existing:
                    return address, amount

        return None, None

    @property
    def crypto_address(self):
        if self.crypto:
            return self.crypto.address(self.chain)
        return None

    def _next_pay_slot_version(self) -> int:
        # 版本号只在当前账单内单调递增，用于稳定表达"支付指引被更新了多少次"。
        latest_version = (
            self.pay_slots.order_by("-version")
            .values_list(
                "version",
                flat=True,
            )
            .first()
        )
        return (latest_version or 0) + 1

    def _sync_snapshot_from_slot(
        self,
        pay_slot: "InvoicePaySlot",
        *,
        started_at=None,
    ) -> None:
        # 主表继续保留最新展示快照，避免现有 API / admin 一次性大范围改动。
        try:
            worth = pay_slot.crypto.to_fiat(
                fiat=FiatService.get_by_code("USD"),
                amount=pay_slot.pay_amount,
            )
        except (KeyError, TypeError):
            # 价格数据不完整时（如新上线币种尚未同步 USD 价格），
            # 降级为 0 而非中断整个匹配/选方式流程。
            logger.warning(
                "crypto price missing for USD",
                crypto=pay_slot.crypto_id,
            )
            worth = Decimal("0")
        updated_values = {
            "crypto_id": pay_slot.crypto_id,
            "chain_id": pay_slot.chain_id,
            "pay_address": pay_slot.pay_address,
            "pay_amount": pay_slot.pay_amount,
            "worth": worth,
            "updated_at": timezone.now(),
        }
        if started_at is not None:
            updated_values["started_at"] = started_at

        Invoice.objects.filter(pk=self.pk).update(**updated_values)
        self.refresh_from_db()

    def _discard_excess_active_slots(self, *, created_slot: "InvoicePaySlot") -> None:
        # 产品策略：每张账单最多保留两个仍可命中的支付槽位，超出的最旧槽位立即失效。
        active_slot_ids = list(
            self.active_pay_slots.order_by("created_at", "pk").values_list(
                "pk", flat=True
            )
        )
        overflow = len(active_slot_ids) - self.MAX_ACTIVE_PAY_SLOTS
        if overflow <= 0:
            return

        discarded_at = timezone.now()
        slots_to_discard = active_slot_ids[:overflow]
        InvoicePaySlot.objects.filter(pk__in=slots_to_discard).update(
            status=InvoicePaySlotStatus.DISCARDED,
            discard_reason=InvoicePaySlotDiscardReason.OVERFLOW,
            discarded_at=discarded_at,
            updated_at=discarded_at,
        )
        if created_slot.pk in slots_to_discard:
            raise self.InvoiceAllocationError(
                "newly created slot was unexpectedly discarded"
            )


class InvoicePaySlot(models.Model):
    # project 冗余存储到槽位表，用于把"全项目活跃支付槽唯一"下沉到数据库约束层。
    # db_constraint=False: 去掉 DB 层 FK 约束, 避免 INSERT 时的 FOR KEY SHARE
    # 锁定 Project 行导致高并发死锁. 数据完整性由 select_method 的 Invoice->Project
    # 引用链保证, PaySlot.project 始终等于 Invoice.project.
    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.CASCADE,
        db_constraint=False,
        editable=False,
        verbose_name=_("项目"),
    )
    invoice = models.ForeignKey(
        "invoices.Invoice",
        on_delete=models.CASCADE,
        related_name="pay_slots",
        verbose_name=_("账单"),
    )
    version = models.PositiveIntegerField(_("版本"))
    crypto = models.ForeignKey(
        "currencies.Crypto",
        on_delete=models.PROTECT,
        verbose_name=_("加密货币"),
    )
    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.CASCADE,
        verbose_name=_("链"),
    )
    pay_amount = models.DecimalField(
        verbose_name=_("支付加密货币数量"),
        max_digits=32,
        decimal_places=8,
    )
    pay_address = AddressField(
        verbose_name=_("支付地址"),
        db_index=True,
    )
    status = models.CharField(
        choices=InvoicePaySlotStatus,
        default=InvoicePaySlotStatus.ACTIVE,
        max_length=16,
        verbose_name=_("状态"),
    )
    discard_reason = models.CharField(  # noqa: DJ001
        choices=InvoicePaySlotDiscardReason,
        max_length=16,
        blank=True,
        null=True,  # None 表示"未丢弃"，语义上与 "" 不同，故保留 null=True
        verbose_name=_("丢弃原因"),
    )
    matched_at = models.DateTimeField(_("命中时间"), blank=True, null=True)
    discarded_at = models.DateTimeField(_("丢弃时间"), blank=True, null=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        ordering = ("-version", "-created_at", "-pk")
        verbose_name = _("账单支付槽位")
        verbose_name_plural = _("账单支付槽位")
        constraints = [
            models.UniqueConstraint(
                fields=("invoice", "version"),
                name="uniq_invoice_pay_slot_version",
            ),
            models.UniqueConstraint(
                fields=("project", "crypto", "chain", "pay_address", "pay_amount"),
                # mypy 对 UniqueConstraint.condition 的泛型推断有已知误报，实际运行正确
                condition=models.Q(status=InvoicePaySlotStatus.ACTIVE),  # type: ignore[attr-defined]
                name="uniq_invoice_pay_slot_active",
            ),
        ]

    def __str__(self):
        return f"{self.invoice.sys_no}-v{self.version}"

    def save(self, *args, **kwargs):
        # 冗余 project 必须与所属 invoice 一致，否则活跃槽位唯一约束会失真。
        invoice_project_id = getattr(self.invoice, "project_id", None)
        if invoice_project_id is None and self.invoice_id is not None:
            invoice_project_id = (
                Invoice.objects.only("project_id").get(pk=self.invoice_id).project_id
            )
        if invoice_project_id is not None:
            if self.project_id is not None and self.project_id != invoice_project_id:
                raise ValueError("InvoicePaySlot.project must match invoice.project")
            self.project_id = invoice_project_id
        return super().save(*args, **kwargs)


class EpayDefaultCurrency(models.TextChoices):
    CNY = "CNY", _("人民币")
    USD = "USD", _("美元")


class EpayMerchant(models.Model):
    project = models.OneToOneField(
        "projects.Project",
        on_delete=models.CASCADE,
        related_name="epay_merchant",
        verbose_name=_("项目"),
    )
    pid = models.PositiveBigIntegerField(_("EPay 商户 ID"), unique=True)
    secret_key = models.CharField(
        _("EPay 密钥"),
        max_length=128,
        help_text=_("EPay 协议签名密钥。建议使用强随机字符串，不要与项目 HMAC 密钥重用。"),
    )
    default_currency = models.CharField(
        _("默认计价货币"),
        max_length=8,
        choices=EpayDefaultCurrency.choices,
        default=EpayDefaultCurrency.CNY,
    )
    active = models.BooleanField(_("启用"), default=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        verbose_name = _("EPay 商户")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"{self.pid} / {self.project_id}"

    @property
    def signing_key(self) -> str:
        """EPay 协议签名密钥（独立于项目 HMAC 密钥）。"""
        # 纵深防御：即使 admin 表单和 migration 0005 都拦截了空 secret_key，
        # fixtures、bulk_create、test fixture 等仍可能绕过校验直接写入空串。
        # 此处 fail-fast，避免下游用空 KEY 算签名导致任何人都能伪造合法 sign。
        if not self.secret_key:
            raise ValueError(
                f"EpayMerchant(pid={self.pid}) secret_key 为空，"
                "无法用于 EPay 协议签名。"
            )
        return self.secret_key


class EpayOrder(models.Model):
    invoice = models.OneToOneField(
        "invoices.Invoice",
        on_delete=models.CASCADE,
        related_name="epay_order",
        verbose_name=_("账单"),
    )
    merchant = models.ForeignKey(
        "invoices.EpayMerchant",
        on_delete=models.PROTECT,
        related_name="orders",
        verbose_name=_("EPay 商户"),
    )
    pid = models.CharField(_("EPay 商户 ID"), max_length=32)
    trade_no = models.CharField(_("EPay 平台订单号"), max_length=64, db_index=True)
    out_trade_no = models.CharField(_("商户订单号"), max_length=64, db_index=True)
    type = models.CharField(_("支付类型"), max_length=32, blank=True, default="")
    name = models.CharField(_("商品名称"), max_length=128)
    money = models.DecimalField(_("订单金额"), max_digits=32, decimal_places=2)
    notify_url = models.URLField(_("异步通知地址"))
    return_url = models.URLField(_("同步跳转地址"), blank=True, default="")
    param = models.CharField(_("业务扩展参数"), max_length=512, blank=True, default="")
    sign_type = models.CharField(_("签名类型"), max_length=16, default="MD5")
    raw_request = models.JSONField(_("原始请求"), default=dict)
    notify_event = models.OneToOneField(
        "webhooks.WebhookEvent",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="+",
        verbose_name=_("通知事件"),
    )
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        verbose_name = _("EPay 订单")
        verbose_name_plural = verbose_name
        constraints = [
            models.UniqueConstraint(
                fields=("merchant", "out_trade_no"),
                name="uniq_epay_order_merchant_out_trade_no",
            ),
            models.UniqueConstraint(
                fields=("merchant", "trade_no"),
                name="uniq_epay_order_merchant_trade_no",
            ),
        ]

    def __str__(self) -> str:
        return self.trade_no

    def clean(self):
        super().clean()
        errors = {}

        if self.invoice_id and self.merchant_id:
            if self.invoice.project_id != self.merchant.project_id:
                errors["invoice"] = _("账单项目必须与 EPay 商户项目一致。")

        if self.merchant_id and self.pid != str(self.merchant.pid):
            errors["pid"] = _("EPay 商户 ID 必须与所属商户一致。")

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.clean()
        return super().save(*args, **kwargs)
