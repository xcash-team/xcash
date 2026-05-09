from __future__ import annotations

from django.contrib import admin
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from unfold.decorators import display

from common.admin import ModelAdmin
from common.admin import ReadOnlyModelAdmin
from common.utils.math import format_decimal_stripped

from .models import EpayMerchant
from .models import EpayOrder
from .models import Invoice
from .models import InvoicePaySlot


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


@admin.register(Invoice)
class InvoiceAdmin(ReadOnlyModelAdmin):
    inlines = (InvoicePaySlotInline,)

    list_display = (
        "project",
        "sys_no",
        "out_no",
        "currency_amount_display",
        "display_pay_url",
        "display_crypto",
        "pay_amount_display",
        "expires_at",
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
                    "email",
                    "redirect_url",
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


@admin.register(EpayMerchant)
class EpayMerchantAdmin(ModelAdmin):
    list_display = ("pid", "project", "active", "default_currency", "created_at")
    search_fields = ("=pid", "project__name", "project__appid")
    list_filter = ("active", "default_currency")


@admin.register(EpayOrder)
class EpayOrderAdmin(ReadOnlyModelAdmin):
    list_display = (
        "trade_no",
        "out_trade_no",
        "merchant",
        "money",
        "type",
        "created_at",
        "display_notified_at",
    )
    search_fields = ("trade_no", "out_trade_no", "invoice__sys_no", "pid")
    list_filter = ("type", "sign_type")
    raw_id_fields = ("invoice", "merchant", "notify_event")

    @admin.display(description=_("通知成功时间"))
    def display_notified_at(self, obj):
        if obj.notify_event_id:
            return obj.notify_event.delivered_at
        return None
