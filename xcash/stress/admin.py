# xcash/stress/admin.py
from django.contrib import admin
from django.contrib import messages
from django.db import transaction
from django.utils.translation import gettext_lazy as _
from unfold.admin import ModelAdmin
from unfold.admin import TabularInline

from .models import InvoiceStressCase
from .models import StressRun
from .models import StressRunStatus
from .models import WithdrawalStressCase
from .models import WithdrawalStressCaseStatus
from .service import StressService
from .tasks import prepare_stress


class InvoiceStressCaseInline(TabularInline):
    model = InvoiceStressCase
    fields = (
        "sequence",
        "status",
        "crypto",
        "chain",
        "invoice_sys_no",
        "tx_hash",
        "webhook_signature_ok",
        "webhook_payload_ok",
        "webhook_nonce_ok",
        "webhook_timestamp_ok",
        "error",
    )
    readonly_fields = fields
    extra = 0
    can_delete = False
    show_change_link = True

    def has_add_permission(self, request, obj=None):
        return False


class WithdrawalStressCaseInline(TabularInline):
    model = WithdrawalStressCase
    fields = (
        "sequence",
        "status",
        "crypto",
        "chain",
        "withdrawal_sys_no",
        "to_address",
        "amount",
        "tx_hash",
        "webhook_signature_ok",
        "webhook_payload_ok",
        "webhook_nonce_ok",
        "webhook_timestamp_ok",
        "error",
    )
    readonly_fields = fields
    extra = 0
    can_delete = False
    show_change_link = True

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(StressRun)
class StressRunAdmin(ModelAdmin):
    inlines = ()

    list_display = (
        "name",
        "count",
        "withdrawal_count",
        "status",
        "succeeded",
        "failed",
        "skipped",
        "created_at",
        "started_at",
        "finished_at",
    )
    list_filter = ("status",)
    search_fields = ("name",)
    readonly_fields = (
        "status",
        "project",
        "succeeded",
        "failed",
        "skipped",
        "error",
        "started_at",
        "finished_at",
    )
    actions = ["start_stress"]

    def get_fields(self, request, obj=None):
        if obj is None:
            return ("name", "count", "withdrawal_count")
        return (
            "name",
            "count",
            "withdrawal_count",
            "status",
            "project",
            "succeeded",
            "failed",
            "skipped",
            "error",
            "started_at",
            "finished_at",
        )

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        if not change and obj.status == StressRunStatus.DRAFT:
            obj.status = StressRunStatus.PREPARING
            obj.save(update_fields=["status"])
            transaction.on_commit(lambda: prepare_stress.delay(obj.pk))
            messages.success(request, _("测试数据正在后台准备，稍后刷新页面查看状态"))

    @admin.action(description=_("开始执行"))
    def start_stress(self, request, queryset):
        started = 0
        for stress in queryset:
            if stress.status != StressRunStatus.READY:
                messages.warning(
                    request,
                    _("%(name)s 状态为 %(status)s，只有就绪状态才能执行")
                    % {"name": stress.name, "status": stress.get_status_display()},
                )
                continue
            StressService.start(stress)
            started += 1

        if started:
            messages.success(request, _("已启动 %(count)d 个测试") % {"count": started})


@admin.register(WithdrawalStressCase)
class WithdrawalStressCaseAdmin(ModelAdmin):
    list_display = (
        "stress_run",
        "sequence",
        "status",
        "crypto",
        "chain",
        "withdrawal_sys_no",
        "to_address",
        "tx_hash",
        "webhook_received",
        "webhook_signature_ok",
        "webhook_payload_ok",
        "webhook_nonce_ok",
        "webhook_timestamp_ok",
    )
    list_filter = ("stress_run", "status")
    search_fields = ("withdrawal_sys_no", "withdrawal_out_no", "tx_hash")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(InvoiceStressCase)
class InvoiceStressCaseAdmin(ModelAdmin):
    list_display = (
        "stress_run",
        "sequence",
        "status",
        "crypto",
        "chain",
        "invoice_sys_no",
        "tx_hash",
        "webhook_received",
        "webhook_signature_ok",
        "webhook_payload_ok",
        "webhook_nonce_ok",
        "webhook_timestamp_ok",
    )
    list_filter = ("stress_run", "status")
    search_fields = ("invoice_sys_no", "invoice_out_no", "tx_hash")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False
