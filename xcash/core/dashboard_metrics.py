from __future__ import annotations

from datetime import datetime
from datetime import time
from datetime import timedelta
from decimal import Decimal

from django.db.models import Count
from django.db.models import Q
from django.db.models import Sum
from django.db.models import Value
from django.db.models.fields import DecimalField
from django.db.models.functions import Coalesce
from django.db.models.functions import TruncDate
from django.utils import timezone

from chains.signer import SignerServiceError
from chains.signer import get_signer_backend
from core.monitoring import OperationalRiskService
from invoices.models import Invoice
from invoices.models import InvoiceStatus
from webhooks.models import DeliveryAttempt
from webhooks.models import WebhookEvent
from withdrawals.models import Withdrawal
from withdrawals.models import WithdrawalStatus

ZERO_DECIMAL = Value(
    Decimal("0"),
    output_field=DecimalField(max_digits=16, decimal_places=6),
)


def _day_start(target_day) -> datetime:
    """将日期转换为当前时区下的 00:00:00，便于统一做日维度聚合。"""
    current_tz = timezone.get_current_timezone()
    return timezone.make_aware(datetime.combine(target_day, time.min), current_tz)


def _rate(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return (Decimal(numerator) / Decimal(denominator) * Decimal("100")).quantize(
        Decimal("0.1")
    )


def build_signer_dashboard_summary() -> dict | None:
    try:
        summary = get_signer_backend().fetch_admin_summary()
    except SignerServiceError as exc:
        return {
            "available": False,
            "detail": str(exc),
        }

    return {
        "available": True,
        "health": summary.health,
        "wallets": summary.wallets,
        "requests_last_hour": summary.requests_last_hour,
        "recent_anomalies": summary.recent_anomalies,
    }


def build_dashboard_metrics() -> dict:
    """构建后台首页统计数据。

    analytics app 已退役，首页看板的实时聚合能力迁入 core，
    避免继续维护一个只剩单一服务函数的独立 app。
    """
    now = timezone.now()
    today_date = timezone.localdate()
    today_start = _day_start(today_date)
    last_7d_start = today_start - timedelta(days=6)
    last_30d_start = today_start - timedelta(days=29)
    last_24h = now - timedelta(hours=24)

    invoice_queryset = Invoice.objects.all()
    withdrawal_queryset = Withdrawal.objects.all()
    delivery_attempt_queryset = DeliveryAttempt.objects.all()
    event_queryset = WebhookEvent.objects.all()

    invoice_today = invoice_queryset.filter(created_at__gte=today_start)
    invoice_7d = invoice_queryset.filter(created_at__gte=last_7d_start)
    invoice_30d = invoice_queryset.filter(created_at__gte=last_30d_start)

    today_completed = invoice_today.filter(status=InvoiceStatus.COMPLETED).aggregate(
        count=Count("id"),
        worth=Coalesce(Sum("worth"), ZERO_DECIMAL),
    )
    rolling_7d = invoice_7d.filter(status=InvoiceStatus.COMPLETED).aggregate(
        count=Count("id"),
        worth=Coalesce(Sum("worth"), ZERO_DECIMAL),
    )
    rolling_30d = invoice_30d.filter(status=InvoiceStatus.COMPLETED).aggregate(
        count=Count("id"),
        worth=Coalesce(Sum("worth"), ZERO_DECIMAL),
    )

    created_30d_count = invoice_30d.count()
    completed_30d_count = int(rolling_30d["count"] or 0)

    active_invoice_metrics = invoice_queryset.aggregate(
        waiting_count=Count("id", filter=Q(status=InvoiceStatus.WAITING)),
        waiting_worth=Coalesce(
            Sum("worth", filter=Q(status=InvoiceStatus.WAITING)),
            ZERO_DECIMAL,
        ),
        confirming_count=Count("id", filter=Q(status=InvoiceStatus.CONFIRMING)),
        confirming_worth=Coalesce(
            Sum("worth", filter=Q(status=InvoiceStatus.CONFIRMING)),
            ZERO_DECIMAL,
        ),
    )
    expiring_soon_count = invoice_queryset.filter(
        status=InvoiceStatus.WAITING,
        expires_at__gte=now,
        expires_at__lte=now + timedelta(minutes=30),
    ).count()

    withdrawal_30d = withdrawal_queryset.filter(created_at__gte=last_30d_start)
    withdrawal_metrics = withdrawal_queryset.aggregate(
        reviewing_count=Count("id", filter=Q(status=WithdrawalStatus.REVIEWING)),
        pending_count=Count("id", filter=Q(status=WithdrawalStatus.PENDING)),
        confirming_count=Count("id", filter=Q(status=WithdrawalStatus.CONFIRMING)),
    )
    withdrawal_30d_completed = withdrawal_30d.filter(
        status=WithdrawalStatus.COMPLETED
    ).aggregate(
        count=Count("id"),
        worth=Coalesce(Sum("worth"), ZERO_DECIMAL),
    )
    withdrawal_30d_rejected = withdrawal_30d.filter(
        status=WithdrawalStatus.REJECTED
    ).count()
    withdrawal_30d_failed = withdrawal_30d.filter(
        status=WithdrawalStatus.FAILED
    ).count()

    delivery_7d = delivery_attempt_queryset.filter(created_at__gte=last_7d_start)
    delivery_metrics = delivery_7d.aggregate(
        total=Count("id"),
        ok=Count("id", filter=Q(ok=True)),
    )
    failed_events_count = event_queryset.filter(
        status=WebhookEvent.Status.FAILED
    ).count()
    pending_events_count = event_queryset.filter(
        status=WebhookEvent.Status.PENDING
    ).count()
    operational_risk = OperationalRiskService.build_summary()
    signer_summary = build_signer_dashboard_summary()

    daily_rows = (
        invoice_30d.annotate(day=TruncDate("created_at"))
        .values("day")
        .annotate(
            created_count=Count("id"),
            completed_count=Count("id", filter=Q(status=InvoiceStatus.COMPLETED)),
            expired_count=Count("id", filter=Q(status=InvoiceStatus.EXPIRED)),
            completed_worth=Coalesce(
                Sum("worth", filter=Q(status=InvoiceStatus.COMPLETED)),
                ZERO_DECIMAL,
            ),
        )
    )
    daily_map = {row["day"]: row for row in daily_rows}
    chart_rows = []
    for offset in range(30):
        day = today_date - timedelta(days=29 - offset)
        row = daily_map.get(day, {})
        chart_rows.append(
            {
                "label": day.strftime("%m-%d"),
                "created_count": int(row.get("created_count") or 0),
                "completed_count": int(row.get("completed_count") or 0),
                "expired_count": int(row.get("expired_count") or 0),
                "completed_worth": Decimal(row.get("completed_worth") or 0),
            }
        )

    top_projects = list(
        invoice_30d.values("project_id", "project__name")
        .annotate(
            total_orders=Count("id"),
            completed_orders=Count("id", filter=Q(status=InvoiceStatus.COMPLETED)),
            gmv=Coalesce(
                Sum("worth", filter=Q(status=InvoiceStatus.COMPLETED)),
                ZERO_DECIMAL,
            ),
            waiting_orders=Count("id", filter=Q(status=InvoiceStatus.WAITING)),
            confirming_orders=Count("id", filter=Q(status=InvoiceStatus.CONFIRMING)),
        )
        .order_by("-gmv", "-completed_orders")[:5]
    )

    payment_methods = list(
        invoice_30d.filter(
            status=InvoiceStatus.COMPLETED,
            crypto__isnull=False,
            chain__isnull=False,
        )
        .values("crypto__symbol", "chain__code")
        .annotate(
            order_count=Count("id"),
            gmv=Coalesce(Sum("worth"), ZERO_DECIMAL),
        )
        .order_by("-gmv", "-order_count")[:6]
    )

    recent_failed_attempts = list(
        delivery_attempt_queryset.filter(ok=False, created_at__gte=last_24h)
        .select_related("event", "event__project")
        .order_by("-created_at")[:4]
    )
    recent_stalled_invoices = list(
        invoice_queryset.filter(
            status=InvoiceStatus.CONFIRMING,
            updated_at__lte=now - timedelta(minutes=30),
        )
        .select_related("project", "crypto", "chain")
        .order_by("updated_at")[:4]
    )
    return {
        "snapshot": {
            "today_completed_count": int(today_completed["count"] or 0),
            "today_completed_worth": Decimal(today_completed["worth"] or 0),
            "rolling_7d_completed_count": int(rolling_7d["count"] or 0),
            "rolling_7d_completed_worth": Decimal(rolling_7d["worth"] or 0),
            "rolling_30d_completed_count": completed_30d_count,
            "rolling_30d_completed_worth": Decimal(rolling_30d["worth"] or 0),
            "created_30d_count": created_30d_count,
            "conversion_rate_30d": _rate(completed_30d_count, created_30d_count),
            "waiting_count": int(active_invoice_metrics["waiting_count"] or 0),
            "waiting_worth": Decimal(active_invoice_metrics["waiting_worth"] or 0),
            "confirming_count": int(active_invoice_metrics["confirming_count"] or 0),
            "confirming_worth": Decimal(
                active_invoice_metrics["confirming_worth"] or 0
            ),
            "expiring_soon_count": expiring_soon_count,
            "reviewing_withdrawal_count": int(
                withdrawal_metrics["reviewing_count"] or 0
            ),
            "pending_withdrawal_count": int(withdrawal_metrics["pending_count"] or 0),
            "confirming_withdrawal_count": int(
                withdrawal_metrics["confirming_count"] or 0
            ),
            "stalled_withdrawal_count": operational_risk["stalled_withdrawal_count"],
            "stalled_deposit_collection_count": operational_risk[
                "stalled_deposit_collection_count"
            ],
            "stalled_webhook_event_count": operational_risk[
                "stalled_webhook_event_count"
            ],
            "completed_withdrawal_count_30d": int(
                withdrawal_30d_completed["count"] or 0
            ),
            "completed_withdrawal_worth_30d": Decimal(
                withdrawal_30d_completed["worth"] or 0
            ),
            "rejected_withdrawal_count_30d": withdrawal_30d_rejected,
            "failed_withdrawal_count_30d": withdrawal_30d_failed,
            "webhook_attempt_total_7d": int(delivery_metrics["total"] or 0),
            "webhook_attempt_ok_7d": int(delivery_metrics["ok"] or 0),
            "webhook_attempt_failed_7d": int(delivery_metrics["total"] or 0)
            - int(delivery_metrics["ok"] or 0),
            "webhook_success_rate_7d": _rate(
                int(delivery_metrics["ok"] or 0),
                int(delivery_metrics["total"] or 0),
            ),
            "failed_events_count": failed_events_count,
            "pending_events_count": pending_events_count,
        },
        "chart_rows": chart_rows,
        "top_projects": top_projects,
        "payment_methods": payment_methods,
        "recent_failed_attempts": recent_failed_attempts,
        "recent_stalled_invoices": recent_stalled_invoices,
        "recent_stalled_withdrawals": operational_risk["recent_stalled_withdrawals"],
        "recent_stalled_deposit_collections": operational_risk[
            "recent_stalled_deposit_collections"
        ],
        "recent_stalled_webhook_events": operational_risk[
            "recent_stalled_webhook_events"
        ],
        "signer_summary": signer_summary,
    }
