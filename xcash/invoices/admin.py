from __future__ import annotations

from django.contrib import admin
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from unfold.admin import StackedInline
from unfold.decorators import display

from common.admin import ReadOnlyModelAdmin
from common.utils.math import format_decimal_stripped

from .models import EpayOrder
from .models import Invoice
from .models import InvoicePaySlot
from .models import InvoiceProtocol


class InvoicePaySlotInline(admin.TabularInline):
    model = InvoicePaySlot
    extra = 0
    can_delete = False
    verbose_name = _("支付槽位")
    verbose_name_plural = _("支付槽位")
    fields = (
        "version",
        "crypto",
        "chain",
        "pay_address",
        "pay_amount",
        "status",
        "discard_reason",
        "matched_at",
        "discarded_at",
        "created_at",
    )
    readonly_fields = fields
    ordering = ("-version",)

    def has_add_permission(self, request, obj=None):
        return False


class EpayOrderInline(StackedInline):
    # EpayOrder 与 Invoice 是 OneToOne，限制 max_num=1 让表单语义对齐数据约束。
    model = EpayOrder
    extra = 0
    max_num = 1
    can_delete = False
    verbose_name = _("EPay 订单")
    verbose_name_plural = _("EPay 订单")
    fields = (
        "trade_no",
        "out_trade_no",
        "merchant",
        "pid",
        "type",
        "money",
        "sign_type",
        "notify_url",
        "return_url",
        "param",
        "notify_event",
        "created_at",
    )
    readonly_fields = fields

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(Invoice)
class InvoiceAdmin(ReadOnlyModelAdmin):
    inlines = (InvoicePaySlotInline, EpayOrderInline)

    list_display = (
        "sys_no",
        "project",
        "out_no",
        "currency_amount_display",
        "display_pay_url",
        "display_crypto",
        "pay_amount_display",
        "expires_at",
        "display_protocol",
        "display_status",
    )
    search_fields = (
        "sys_no",
        "out_no",
        "transfer__hash",
    )
    list_filter = (
        "chain",
        "crypto",
        "status",
        "protocol",
    )
    fieldsets = (
        (
            _("基本信息"),
            {
                "fields": (
                    "project",
                    "sys_no",
                    "out_no",
                    "title",
                    "currency",
                    "amount",
                    "worth",
                    "methods",
                    "notify_url",
                    "return_url",
                    "created_at",
                    "expires_at",
                    "status",
                    "protocol",
                )
            },
        ),
        (
            _("支付信息"),
            {
                "fields": (
                    "display_crypto",  # noqa
                    "display_chain",  # noqa
                    "pay_amount",
                    "pay_address",
                )
            },
        ),
        (
            _("交易收据"),
            {"fields": ("transfer",)},
        ),
    )

    def get_inline_instances(self, request, obj=None):
        # 非 EPay 协议账单没有 EpayOrder 数据，隐藏空 inline 避免界面噪音。
        inline_instances = super().get_inline_instances(request, obj)
        if obj is None or obj.protocol != InvoiceProtocol.EPAY_V1:
            inline_instances = [
                inline
                for inline in inline_instances
                if not isinstance(inline, EpayOrderInline)
            ]
        return inline_instances

    @display(
        description=_("状态"),  # noqa
        label={  # noqa
            "待支付": "warning",
            "确认中": "info",
            "已完成": "success",
            "已超时": "",
            "Pending payment": "warning",
            "Confirming": "info",
            "Completed": "success",
            "Timed out": "",
        },
    )
    def display_status(self, instance: Invoice):
        return instance.get_status_display()

    @display(
        description=_("协议"),  # noqa
        label={  # noqa
            "Xcash 原生": "info",
            "EPay V1": "primary",
            "Xcash native": "info",
        },
    )
    def display_protocol(self, instance: Invoice):
        return instance.get_protocol_display()

    @display(
        description=_("金额"),  # noqa
    )
    def currency_amount_display(self, instance: Invoice):
        # 后台金额展示统一去掉末尾无意义的 0，避免高精度 Decimal 显得冗长。
        return f"{format_decimal_stripped(instance.amount)} {instance.currency}"

    @display(
        description=_("加密货币数量"),  # noqa
    )
    def pay_amount_display(self, instance: Invoice):
        return (
            format_decimal_stripped(instance.pay_amount) if instance.pay_amount else "-"
        )

    @display(description=_("支付链接"))  # noqa
    def display_pay_url(self, instance: Invoice):
        return reverse("payment-invoice", kwargs={"sys_no": instance.sys_no})

    @display(description=_("加密货币"))  # noqa
    def display_crypto(self, obj: Invoice):
        return obj.crypto.symbol if obj.crypto else "-"

    @display(description=_("链"))  # noqa
    def display_chain(self, obj: Invoice):
        return obj.chain.name if obj.chain else "-"
