from django.contrib import admin
from tron.client import TronHttpClient
from tron.models import TronWatchCursor
from unfold.decorators import display

from chains.models import Chain
from common.admin import ReadOnlyModelAdmin
from common.admin_scan_cursor import SyncScanCursorToLatestActionMixin


@admin.register(TronWatchCursor)
class TronWatchCursorAdmin(SyncScanCursorToLatestActionMixin, ReadOnlyModelAdmin):
    actions = (
        "enable_selected_scanners",
        "disable_selected_scanners",
        "sync_selected_to_latest",
    )
    ordering = ("chain__name", "contract_address")
    list_display = (
        "display_chain",
        "contract_address",
        "display_enabled",
        "display_lag_state",
        "display_chain_latest_block",
        "last_scanned_block",
        "last_safe_block",
        "display_scan_gap",
        "display_error_state",
        "display_error_summary",
        "updated_at",
    )
    list_filter = ("enabled", "chain")
    search_fields = ("chain__name", "chain__code", "contract_address", "last_error")
    list_select_related = ("chain",)
    readonly_fields = (
        "chain",
        "contract_address",
        "display_enabled",
        "last_scanned_block",
        "last_safe_block",
        "display_chain_latest_block",
        "display_scan_gap",
        "display_lag_state",
        "last_error",
        "display_error_summary",
        "last_error_at",
        "updated_at",
        "created_at",
    )
    fields = readonly_fields

    def get_sync_latest_block(self, *, chain: Chain) -> int:
        latest_block = TronHttpClient(chain=chain).get_latest_solid_block_number()
        Chain.objects.filter(pk=chain.pk).update(latest_block_number=latest_block)
        chain.latest_block_number = latest_block
        return latest_block

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
