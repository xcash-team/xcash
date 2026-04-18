"""internal_api 聚合端点：/projects/{appid}/stats/

- summary: 当期 + 上期两个窗口的 GMV 与账单数聚合
- daily:   最近 N 天的每日 GMV 时序

窗口边界由调用方（SaaS）传入；xcash 只负责数据库聚合，不自行决定"本月"语义。
"""
from datetime import datetime as _dt
from datetime import timedelta
from datetime import timezone as dt_tz
from decimal import Decimal

from django.db.models import Count, Sum
from django.db.models.functions import TruncDate
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from internal_api.authentication import InternalTokenAuthentication
from invoices.models import Invoice, InvoiceStatus
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from common.error_codes import ErrorCode
from common.exceptions import APIError
from projects.models import Project


def _parse_iso(value: str | None, name: str):
    """解析 ISO8601 时间字符串，缺失或格式错误时抛出 400。"""
    if not value:
        raise APIError(ErrorCode.PARAMETER_ERROR, f"{name} 必填")
    dt = parse_datetime(value)
    if dt is None:
        raise APIError(ErrorCode.PARAMETER_ERROR, f"{name} 不是合法的 ISO8601 时间")
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.utc)
    return dt


class StatsViewSet(GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]

    def _project(self):
        project = Project.retrieve(self.kwargs["project_appid"])
        if project is None:
            raise APIError(ErrorCode.PROJECT_NOT_FOUND)
        return project

    @action(detail=False, methods=["get"])
    def summary(self, request, project_appid=None):
        cur_start = _parse_iso(request.query_params.get("cur_start"), "cur_start")
        cur_end = _parse_iso(request.query_params.get("cur_end"), "cur_end")
        prev_start = _parse_iso(request.query_params.get("prev_start"), "prev_start")
        prev_end = _parse_iso(request.query_params.get("prev_end"), "prev_end")
        project = self._project()

        invoices = Invoice.objects.filter(project=project)

        def gmv_in(start, end) -> Decimal:
            v = invoices.filter(
                status=InvoiceStatus.COMPLETED,
                updated_at__gte=start,
                updated_at__lt=end,
            ).aggregate(total=Sum("worth"))["total"]
            return v or Decimal("0")

        def invoice_count_in(start, end) -> int:
            return invoices.filter(
                started_at__gte=start, started_at__lt=end,
            ).aggregate(n=Count("pk"))["n"] or 0

        def completed_count_in(start, end) -> int:
            return invoices.filter(
                status=InvoiceStatus.COMPLETED,
                updated_at__gte=start, updated_at__lt=end,
            ).aggregate(n=Count("pk"))["n"] or 0

        return Response({
            "gmv_usd": f"{gmv_in(cur_start, cur_end):.6f}",
            "prev_gmv_usd": f"{gmv_in(prev_start, prev_end):.6f}",
            "invoice_count": invoice_count_in(cur_start, cur_end),
            "prev_invoice_count": invoice_count_in(prev_start, prev_end),
            "completed_invoice_count": completed_count_in(cur_start, cur_end),
        })

    @action(detail=False, methods=["get"])
    def daily(self, request, project_appid=None):
        # 校验 metric 参数，目前只支持 gmv
        metric = request.query_params.get("metric", "gmv")
        if metric != "gmv":
            raise APIError(ErrorCode.PARAMETER_ERROR, "仅支持 metric=gmv")

        # 校验 days 参数，只允许 7 / 30 / 90
        try:
            days = int(request.query_params.get("days", ""))
        except ValueError:
            raise APIError(ErrorCode.PARAMETER_ERROR, "days 必须是整数")
        if days not in (7, 30, 90):
            raise APIError(ErrorCode.PARAMETER_ERROR, "days 必须是 7 / 30 / 90")

        project = self._project()

        # 计算时间窗口：[start_date, end_date] 共 days 天（含今天）
        now = timezone.now()
        end_date = now.date()
        start_date = end_date - timedelta(days=days - 1)
        start_dt = _dt.combine(start_date, _dt.min.time()).replace(tzinfo=dt_tz.utc)
        end_dt = _dt.combine(end_date + timedelta(days=1), _dt.min.time()).replace(tzinfo=dt_tz.utc)

        # 按 UTC 日期分组，汇总 completed 账单的 worth
        rows = (
            Invoice.objects
            .filter(
                project=project,
                status=InvoiceStatus.COMPLETED,
                updated_at__gte=start_dt,
                updated_at__lt=end_dt,
            )
            .annotate(day=TruncDate("updated_at"))
            .values("day")
            .annotate(total=Sum("worth"))
            .order_by("day")
        )
        by_day = {r["day"]: r["total"] or Decimal("0") for r in rows}

        # 构造完整序列，缺失日期补零，保证长度恒为 days
        series = []
        for i in range(days):
            d = start_date + timedelta(days=i)
            series.append({
                "date": d.isoformat(),
                "value": f"{by_day.get(d, Decimal('0')):.6f}",
            })

        return Response({"metric": "gmv", "days": days, "series": series})
