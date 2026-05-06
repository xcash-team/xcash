from django.contrib import admin
from unfold.decorators import display

from common.admin import ReadOnlyModelAdmin
from common.admin_scan_cursor import SyncScanCursorToLatestActionMixin
from common.utils.math import format_decimal_stripped
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor


@admin.register(EvmBroadcastTask)
class EvmBroadcastTaskAdmin(ReadOnlyModelAdmin):
    ordering = ("-created_at",)
    list_display = (
        "display_address",
        "display_chain",
        "transfer_type",
        "display_crypto",
        "display_recipient",
        "display_amount",
        "display_nonce",
        "display_status",
        "created_at",
        "formatted_last_attempt_at",
    )
    # BroadcastTask 已吸收 EVM 业务元数据，后台直接查询主任务对象即可。
    # 状态展示优先读取统一父任务，后台查询一并预加载，避免 N+1。
    list_select_related = ("base_task", "base_task__crypto", "address", "chain")
    search_fields = ("base_task__tx_hash", "address__address", "base_task__recipient")

    @admin.display(ordering="last_attempt_at", description="执行时间")
    def formatted_last_attempt_at(self, obj: EvmBroadcastTask):
        if obj.last_attempt_at:
            return obj.last_attempt_at.strftime("%-m月%-d日 %H:%M:%S")
        return None

    @display(
        description="状态",
        label={
            "待广播": "warning",
            "待上链": "warning",
            "确认中": "info",
            "成功": "success",
            "失败": "danger",
            "已终结": "info",
        },
    )
    def display_status(self, instance: EvmBroadcastTask):
        return instance.status

    @admin.display(description="类型", ordering="base_task__transfer_type")
    def transfer_type(self, obj: EvmBroadcastTask):  # pragma: no cover
        return obj.base_task.get_transfer_type_display() if obj.base_task_id else "—"

    @admin.display(description="代币", ordering="base_task__crypto__symbol")
    def display_crypto(self, obj: EvmBroadcastTask):  # pragma: no cover
        return (
            obj.base_task.crypto
            if obj.base_task_id and obj.base_task.crypto_id
            else "—"
        )

    @admin.display(description="收款地址", ordering="base_task__recipient")
    def display_recipient(self, obj: EvmBroadcastTask):  # pragma: no cover
        if obj.base_task_id and obj.base_task.recipient:
            return obj.base_task.recipient
        return obj.to

    @admin.display(description="数量")
    def display_amount(self, obj: EvmBroadcastTask):  # pragma: no cover
        if obj.base_task_id and obj.base_task.amount is not None:
            return format_decimal_stripped(obj.base_task.amount)
        return "—"

    @admin.display(ordering="address__address", description="地址")
    def display_address(self, obj: EvmBroadcastTask):  # pragma: no cover
        return obj.address

    @admin.display(ordering="chain__name", description="网络")
    def display_chain(self, obj: EvmBroadcastTask):  # pragma: no cover
        return obj.chain

    @admin.display(ordering="nonce", description="Nonce")
    def display_nonce(self, obj: EvmBroadcastTask):  # pragma: no cover
        return obj.nonce


@admin.register(EvmScanCursor)
class EvmScanCursorAdmin(SyncScanCursorToLatestActionMixin, ReadOnlyModelAdmin):
    # 自扫描游标只承担观测与排障职责；后台统一只读展示，避免人工改游标破坏扫描连续性。
    actions = (
        "enable_selected_scanners",
        "disable_selected_scanners",
        "sync_selected_to_latest",
    )
    ordering = ("chain__name", "scanner_type")
    list_display = (
        "display_chain",
        "scanner_type",
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
    list_filter = ("scanner_type", "enabled", "chain")
    search_fields = ("chain__name", "chain__code", "last_error")
    list_select_related = ("chain",)
    readonly_fields = (
        "chain",
        "scanner_type",
        "display_enabled",
        "last_scanned_block",
        "last_safe_block",
        "display_chain_latest_block",
        "display_scan_gap",
        "display_lag_state",
        "last_error",
        "last_error_at",
        "updated_at",
        "created_at",
    )
    fields = readonly_fields

    @admin.display(ordering="chain__name", description="网络")
    def display_chain(self, obj: EvmScanCursor):  # pragma: no cover
        return obj.chain

    @display(
        description="启用",
        label={
            "是": "success",
            "否": "danger",
        },
    )
    def display_enabled(self, obj: EvmScanCursor) -> str:
        return "是" if obj.enabled else "否"

    @admin.display(description="链上最新块")
    def display_chain_latest_block(self, obj: EvmScanCursor) -> int:  # pragma: no cover
        return obj.chain.latest_block_number

    @display(
        description="扫描状态",
        label={
            "正常": "success",
            "异常": "danger",
        },
    )
    def display_error_state(self, obj: EvmScanCursor) -> str:
        return "异常" if obj.last_error else "正常"

    @admin.display(description="落后区块")
    def display_scan_gap(self, obj: EvmScanCursor) -> int:
        # 以链上当前最新高度对比主扫描游标，便于快速判断该链是否积压。
        return max(obj.chain.latest_block_number - obj.last_scanned_block, 0)

    @display(
        description="积压",
        label={
            "正常": "success",
            "轻微": "warning",
            "严重": "danger",
        },
    )
    def display_lag_state(self, obj: EvmScanCursor) -> str:
        gap = self.display_scan_gap(obj)
        if gap >= 128:
            return "严重"
        if gap >= 16:
            return "轻微"
        return "正常"

    @admin.display(description="错误摘要")
    def display_error_summary(self, obj: EvmScanCursor) -> str:
        if not obj.last_error:
            return "—"
        # 列表页只展示摘要，详情页保留完整 last_error 原文。
        return obj.last_error[:60]
