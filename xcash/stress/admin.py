# xcash/stress/admin.py
from django.contrib import admin
from django.contrib import messages
from django.db import connection
from django.db import transaction
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from unfold.admin import ModelAdmin
from unfold.admin import TabularInline

from .models import DepositStressCase
from .models import DepositStressCaseStatus
from .models import InvoiceStressCase
from .models import InvoiceStressCaseStatus
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


class DepositStressCaseInline(TabularInline):
    model = DepositStressCase
    fields = (
        "sequence",
        "status",
        "customer_uid",
        "crypto",
        "chain",
        "amount",
        "tx_hash",
        "webhook_signature_ok",
        "webhook_payload_ok",
        "webhook_nonce_ok",
        "webhook_timestamp_ok",
        "collection_verified",
        "error",
    )
    readonly_fields = fields
    extra = 0
    can_delete = False
    show_change_link = True

    def has_add_permission(self, request, obj=None):
        return False


def _percentile_metrics(
    table: str,
    stress_run_id: int,
    status_value: str,
    stages: list[tuple[str, str, str]],
) -> list[dict]:
    """聚合各阶段耗时的 P50 / P95 / P99 / max（毫秒）。

    只统计 SUCCEEDED 的 case；end 或 start 字段为 NULL 的样本会被
    PostgreSQL 的 EXTRACT(EPOCH FROM ...) 自然忽略（NULL 不参与聚合）。
    通过 PostgreSQL 的 percentile_cont 在数据库层完成分位数计算，避免
    把所有 case 拉到内存再排序。

    stages: [(label, start_field, end_field), ...]
    返回每个阶段一个 dict：
        {label, count, p50, p95, p99, max}
    若 count == 0（即所有样本两端都有 NULL），跳过该阶段。
    """
    if not stages:
        return []

    # 每个阶段构造 4 个聚合表达式 + 1 个非空计数；一次 SQL 取齐
    select_parts: list[str] = []
    for idx, (_label, start_f, end_f) in enumerate(stages):
        diff_secs = f"EXTRACT(EPOCH FROM ({end_f} - {start_f}))"
        # 仅统计两端都非空的样本
        valid = f"({start_f} IS NOT NULL AND {end_f} IS NOT NULL)"
        select_parts.extend(
            [
                f"COUNT(*) FILTER (WHERE {valid}) AS cnt_{idx}",
                f"PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {diff_secs}) "
                f"FILTER (WHERE {valid}) AS p50_{idx}",
                f"PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {diff_secs}) "
                f"FILTER (WHERE {valid}) AS p95_{idx}",
                f"PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {diff_secs}) "
                f"FILTER (WHERE {valid}) AS p99_{idx}",
                f"MAX({diff_secs}) FILTER (WHERE {valid}) AS max_{idx}",
            ]
        )
    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {table} "
        f"WHERE stress_run_id = %s AND status = %s"
    )

    with connection.cursor() as cur:
        cur.execute(sql, [stress_run_id, status_value])
        row = cur.fetchone()

    if row is None:
        return []

    results: list[dict] = []
    cols_per_stage = 5
    for idx, (label, _start_f, _end_f) in enumerate(stages):
        base = idx * cols_per_stage
        cnt = row[base]
        if not cnt:
            continue
        # secs -> ms; PERCENTILE_CONT 返回 float（秒），MAX 返回 interval
        # 上面用 EXTRACT(EPOCH FROM ...) 已转成秒
        results.append(
            {
                "label": label,
                "count": cnt,
                "p50_ms": round((row[base + 1] or 0) * 1000, 2),
                "p95_ms": round((row[base + 2] or 0) * 1000, 2),
                "p99_ms": round((row[base + 3] or 0) * 1000, 2),
                "max_ms": round((row[base + 4] or 0) * 1000, 2),
            }
        )
    return results


@admin.register(StressRun)
class StressRunAdmin(ModelAdmin):
    inlines = ()

    list_display = (
        "name",
        "count",
        "withdrawal_count",
        "deposit_count",
        "deposit_customer_count",
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
        "metrics_summary",
    )
    actions = ["start_stress"]

    def get_fields(self, request, obj=None):
        if obj is None:
            return ("name", "count", "withdrawal_count", "deposit_count", "deposit_customer_count")
        return (
            "name",
            "count",
            "withdrawal_count",
            "deposit_count",
            "deposit_customer_count",
            "status",
            "project",
            "succeeded",
            "failed",
            "skipped",
            "error",
            "started_at",
            "finished_at",
            "metrics_summary",
        )

    @admin.display(description=_("各阶段耗时分位数"))
    def metrics_summary(self, obj: StressRun):
        """展示三类业务关键阶段延迟的 P50/P95/P99/max（毫秒）。

        只对状态为 SUCCEEDED 的 case 聚合；任何阶段两端时间戳为空的样本
        会自然被排除。无可用数据时返回 "无数据可聚合"。
        """
        if obj is None or obj.pk is None:
            return _("无数据可聚合")

        invoice_stages = [
            ("api_create_ms", "started_at", "invoice_created_at"),
            ("api_select_method_ms", "invoice_created_at", "api_done_at"),
            ("chain_pay_ms", "api_done_at", "chain_paid_at"),
            ("webhook_wait_ms", "chain_paid_at", "webhook_received_at"),
            ("total_ms", "started_at", "finished_at"),
        ]
        withdrawal_stages = [
            ("api_ms", "started_at", "api_done_at"),
            ("webhook_wait_ms", "api_done_at", "webhook_received_at"),
            ("total_ms", "started_at", "finished_at"),
        ]
        deposit_stages = [
            ("api_ms", "started_at", "api_done_at"),
            ("chain_pay_ms", "api_done_at", "chain_paid_at"),
            ("webhook_wait_ms", "chain_paid_at", "webhook_received_at"),
            ("collection_wait_ms", "webhook_received_at", "collection_done_at"),
            ("total_ms", "started_at", "finished_at"),
        ]

        sections = [
            (
                _("Invoice"),
                _percentile_metrics(
                    InvoiceStressCase._meta.db_table,
                    obj.pk,
                    InvoiceStressCaseStatus.SUCCEEDED,
                    invoice_stages,
                ),
            ),
            (
                _("Withdrawal"),
                _percentile_metrics(
                    WithdrawalStressCase._meta.db_table,
                    obj.pk,
                    WithdrawalStressCaseStatus.SUCCEEDED,
                    withdrawal_stages,
                ),
            ),
            (
                _("Deposit"),
                _percentile_metrics(
                    DepositStressCase._meta.db_table,
                    obj.pk,
                    DepositStressCaseStatus.SUCCEEDED,
                    deposit_stages,
                ),
            ),
        ]

        # 过滤掉完全无数据的 section
        sections = [(title, rows) for title, rows in sections if rows]
        if not sections:
            return _("无数据可聚合")

        parts: list[str] = []
        for title, rows in sections:
            parts.append(f"<h4 style='margin:8px 0 4px 0'>{title}</h4>")
            parts.append(
                "<table style='border-collapse:collapse;margin-bottom:8px'>"
                "<thead><tr>"
                "<th style='text-align:left;padding:2px 12px 2px 0'>stage</th>"
                "<th style='text-align:right;padding:2px 12px 2px 0'>n</th>"
                "<th style='text-align:right;padding:2px 12px 2px 0'>P50 (ms)</th>"
                "<th style='text-align:right;padding:2px 12px 2px 0'>P95 (ms)</th>"
                "<th style='text-align:right;padding:2px 12px 2px 0'>P99 (ms)</th>"
                "<th style='text-align:right;padding:2px 12px 2px 0'>max (ms)</th>"
                "</tr></thead><tbody>"
            )
            for r in rows:
                parts.append(
                    "<tr>"
                    f"<td style='padding:2px 12px 2px 0'>{r['label']}</td>"
                    f"<td style='text-align:right;padding:2px 12px 2px 0'>{r['count']}</td>"
                    f"<td style='text-align:right;padding:2px 12px 2px 0'>{r['p50_ms']}</td>"
                    f"<td style='text-align:right;padding:2px 12px 2px 0'>{r['p95_ms']}</td>"
                    f"<td style='text-align:right;padding:2px 12px 2px 0'>{r['p99_ms']}</td>"
                    f"<td style='text-align:right;padding:2px 12px 2px 0'>{r['max_ms']}</td>"
                    "</tr>"
                )
            parts.append("</tbody></table>")

        # 整个 HTML 由我们自行拼接，所有数据来自 PostgreSQL 聚合的数值/常量标签，
        # 没有用户可控字符串，可安全标记为 safe。
        return mark_safe("".join(parts))

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


@admin.register(DepositStressCase)
class DepositStressCaseAdmin(ModelAdmin):
    list_display = (
        "stress_run",
        "sequence",
        "status",
        "customer_uid",
        "crypto",
        "chain",
        "amount",
        "tx_hash",
        "webhook_received",
        "webhook_signature_ok",
        "webhook_payload_ok",
        "webhook_nonce_ok",
        "webhook_timestamp_ok",
        "collection_verified",
    )
    list_filter = ("stress_run", "status")
    search_fields = ("customer_uid", "tx_hash", "collection_hash")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False
