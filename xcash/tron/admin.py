from django.contrib import admin
from unfold.decorators import display

from common.admin import ReadOnlyModelAdmin
from common.admin_scan_cursor import SyncScanCursorToLatestActionMixin
from tron.models import TronWatchCursor


@admin.register(TronWatchCursor)
class TronWatchCursorAdmin(SyncScanCursorToLatestActionMixin, ReadOnlyModelAdmin):
    actions = ("sync_selected_to_latest",)
    ordering = ("chain__name", "watch_address")
    list_display = (
        "display_chain",
        "watch_address",
        "display_enabled",
        "display_lag_state",
        "display_chain_latest_block",
        "last_scanned_block",
        "last_safe_block",
        "display_scan_gap",
        "last_event_fingerprint",
        "display_error_state",
        "display_error_summary",
        "updated_at",
    )
    list_filter = ("enabled", "chain")
    search_fields = ("chain__name", "chain__code", "watch_address", "last_error")
    list_select_related = ("chain",)
    readonly_fields = (
        "chain",
        "watch_address",
        "display_enabled",
        "last_scanned_block",
        "last_safe_block",
        "display_chain_latest_block",
        "display_scan_gap",
        "display_lag_state",
        "last_event_fingerprint",
        "last_error",
        "display_error_summary",
        "last_error_at",
        "updated_at",
        "created_at",
    )
    fields = readonly_fields

    @admin.display(ordering="chain__name", description="网络")
    def display_chain(self, obj: TronWatchCursor):  # pragma: no cover
        return obj.chain

    @display(
        description="启用",
        label={
            "是": "success",
            "否": "danger",
        },
    )
    def display_enabled(self, obj: TronWatchCursor) -> str:
        return "是" if obj.enabled else "否"

    @admin.display(description="链上最新块")
    def display_chain_latest_block(self, obj: TronWatchCursor) -> int:  # pragma: no cover
        return obj.chain.latest_block_number

    @admin.display(description="落后区块")
    def display_scan_gap(self, obj: TronWatchCursor) -> int:
        return max(obj.chain.latest_block_number - obj.last_scanned_block, 0)

    @display(
        description="积压",
        label={
            "正常": "success",
            "轻微": "warning",
            "严重": "danger",
        },
    )
    def display_lag_state(self, obj: TronWatchCursor) -> str:
        gap = self.display_scan_gap(obj)
        if gap >= 128:
            return "严重"
        if gap >= 16:
            return "轻微"
        return "正常"

    @display(
        description="扫描状态",
        label={
            "正常": "success",
            "异常": "danger",
        },
    )
    def display_error_state(self, obj: TronWatchCursor) -> str:
        return "异常" if obj.last_error else "正常"

    @admin.display(description="错误摘要")
    def display_error_summary(self, obj: TronWatchCursor) -> str:
        if not obj.last_error:
            return "—"
        return obj.last_error[:60]
