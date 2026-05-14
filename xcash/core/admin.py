from django.contrib import admin
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect
from django.urls import reverse
from django_celery_results.models import TaskResult
from unfold.admin import ModelAdmin

from core.models import PlatformSettings

admin.site.unregister(TaskResult)


@admin.register(TaskResult)
class TaskResultAdmin(ModelAdmin):
    list_display = ("task_id", "task_name", "status", "date_done")
    list_filter = ("status", "task_name", "date_done")


@admin.register(PlatformSettings)
class PlatformSettingsAdmin(ModelAdmin):
    fieldsets = (
        (
            "后台安全",
            {
                "fields": (
                    "admin_session_timeout_minutes",
                    "admin_sensitive_action_otp_max_age_seconds",
                )
            },
        ),
        (
            "告警策略",
            {"fields": ("alerts_repeat_interval_minutes",)},
        ),
        (
            "链上扫描",
            {"fields": ("open_native_scanner",)},
        ),
        (
            "Webhook 投递",
            {
                "fields": (
                    "webhook_delivery_breaker_threshold",
                    "webhook_delivery_max_retries",
                    "webhook_delivery_max_backoff_seconds",
                )
            },
        ),
        (
            "异常巡检",
            {
                "fields": (
                    "reviewing_withdrawal_timeout_minutes",
                    "pending_withdrawal_timeout_minutes",
                    "confirming_withdrawal_timeout_minutes",
                    "deposit_collection_timeout_minutes",
                    "webhook_event_timeout_minutes",
                )
            },
        ),
        (
            "风控系统",
            {
                "fields": (
                    "risk_marking_enabled",
                    "risk_marking_threshold_usd",
                    "risk_marking_cache_seconds",
                    "risk_marking_force_refresh_threshold_usd",
                    "misttrack_openapi_api_key",
                    "quicknode_misttrack_endpoint_url",
                )
            },
        ),
        (
            "审计",
            {
                "fields": (
                    "created_by",
                    "updated_by",
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )
    readonly_fields = ("created_by", "updated_by", "created_at", "updated_at")
    list_display = (
        "id",
        "open_native_scanner",
        "risk_marking_enabled",
        "admin_sensitive_action_otp_max_age_seconds",
        "alerts_repeat_interval_minutes",
        "updated_by",
        "updated_at",
    )

    def has_module_permission(self, request):
        # 平台运行参数属于系统级治理能力，只向超管暴露模块入口。
        return bool(request.user.is_active and request.user.is_superuser)

    def has_view_permission(self, request, obj=None):
        return self.has_module_permission(request)

    def has_change_permission(self, request, obj=None):
        return self.has_module_permission(request)

    def has_add_permission(self, request):
        return (
            self.has_module_permission(request)
            and not PlatformSettings.objects.exists()
        )

    def has_delete_permission(self, request, obj=None):
        return False

    def changelist_view(self, request, extra_context=None):
        if not self.has_view_permission(request):
            raise PermissionDenied
        # 平台参数中心天然是单例，列表页直接收口到唯一那一份配置。
        config = PlatformSettings.objects.order_by("pk").first()
        if config is not None:
            return redirect(
                reverse("admin:core_platformsettings_change", args=[config.pk])
            )
        return redirect(reverse("admin:core_platformsettings_add"))

    def save_model(self, request, obj, form, change):
        if change:
            obj.updated_by = request.user
        else:
            obj.created_by = request.user
            obj.updated_by = request.user
        # 平台运行参数需要保留明确的操作者审计，避免关键阈值被静默修改。
        super().save_model(request, obj, form, change)
