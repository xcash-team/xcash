"""internal_api /projects/{appid}/stats/ 路由与鉴权。"""
import itertools
from datetime import datetime, timedelta
from datetime import timezone as dt_tz
from decimal import Decimal

import pytest
from chains.models import Wallet
from invoices.models import Invoice, InvoiceStatus
from projects.models import Project

AUTH = "Bearer test-internal-token"


@pytest.fixture
def project(db):
    wallet = Wallet.objects.create()
    return Project.objects.create(name="stats-test", wallet=wallet)


@pytest.mark.django_db
class TestStatsRouting:
    def _url(self, project, action):
        return f"/internal/v1/projects/{project.appid}/stats/{action}"

    def test_summary_requires_auth(self, client, project):
        resp = client.get(
            self._url(project, "summary")
            + "?cur_start=2026-04-01T00:00:00Z&cur_end=2026-04-18T00:00:00Z"
            + "&prev_start=2026-03-01T00:00:00Z&prev_end=2026-03-18T00:00:00Z"
        )
        assert resp.status_code in (401, 403)

    def test_summary_happy_path_empty(self, client, project):
        resp = client.get(
            self._url(project, "summary")
            + "?cur_start=2026-04-01T00:00:00Z&cur_end=2026-04-18T00:00:00Z"
            + "&prev_start=2026-03-01T00:00:00Z&prev_end=2026-03-18T00:00:00Z",
            HTTP_AUTHORIZATION=AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["gmv_usd"] == "0.000000"
        assert body["prev_gmv_usd"] == "0.000000"
        assert body["invoice_count"] == 0
        assert body["prev_invoice_count"] == 0
        assert body["completed_invoice_count"] == 0


# ─── 辅助函数 ────────────────────────────────────────────────────────────────

# 用于生成唯一的商户单号，避免 project+out_no 唯一约束冲突
_out_no_counter = itertools.count(1)


def _make_invoice(project, *, worth, status, started_at, updated_at):
    """创建一张账单并覆盖 auto 字段。Invoice.started_at 是 auto_now_add，
    updated_at 是 auto_now，需要用 QuerySet.update() 绕过才能控制测试时间。"""
    inv = Invoice.objects.create(
        project=project,
        worth=Decimal(worth),
        status=status,
        expires_at=started_at,  # 字段 non-null，给个占位值
        out_no=f"TEST-{next(_out_no_counter)}",
        title="Test Invoice",
        currency="USD",
        amount=Decimal(worth),
    )
    Invoice.objects.filter(pk=inv.pk).update(
        started_at=started_at,
        updated_at=updated_at,
    )
    inv.refresh_from_db()
    return inv


# ─── 聚合正确性测试 ───────────────────────────────────────────────────────────


@pytest.mark.django_db
class TestStatsSummaryAggregation:
    CUR_START = "2026-04-01T00:00:00Z"
    CUR_END = "2026-04-18T00:00:00Z"
    PREV_START = "2026-03-01T00:00:00Z"
    PREV_END = "2026-03-18T00:00:00Z"

    def _url(self, project):
        qs = (
            f"?cur_start={self.CUR_START}&cur_end={self.CUR_END}"
            f"&prev_start={self.PREV_START}&prev_end={self.PREV_END}"
        )
        return f"/internal/v1/projects/{project.appid}/stats/summary{qs}"

    def test_gmv_sums_completed_invoices_in_window(self, client, project):
        # 当期 4 月 completed → 计入
        _make_invoice(project, worth="100.50", status=InvoiceStatus.COMPLETED,
                      started_at=datetime(2026, 4, 5, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 4, 10, tzinfo=dt_tz.utc))
        _make_invoice(project, worth="200.25", status=InvoiceStatus.COMPLETED,
                      started_at=datetime(2026, 4, 6, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 4, 15, tzinfo=dt_tz.utc))
        # waiting 不计入 GMV
        _make_invoice(project, worth="999.00", status=InvoiceStatus.WAITING,
                      started_at=datetime(2026, 4, 7, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 4, 7, tzinfo=dt_tz.utc))
        # 上期 3 月 completed → 计入 prev
        _make_invoice(project, worth="50.00", status=InvoiceStatus.COMPLETED,
                      started_at=datetime(2026, 3, 5, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 3, 10, tzinfo=dt_tz.utc))

        resp = client.get(self._url(project), HTTP_AUTHORIZATION=AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert Decimal(body["gmv_usd"]) == Decimal("300.75")
        assert Decimal(body["prev_gmv_usd"]) == Decimal("50.00")

    def test_invoice_count_uses_started_at_all_status(self, client, project):
        # 4 种状态的账单均在当期窗口内（按 started_at），全部计入 invoice_count
        for i, status in enumerate([
            InvoiceStatus.WAITING, InvoiceStatus.COMPLETED,
            InvoiceStatus.EXPIRED, InvoiceStatus.CONFIRMING,
        ]):
            _make_invoice(project, worth="1.00", status=status,
                          started_at=datetime(2026, 4, 2 + i, tzinfo=dt_tz.utc),
                          updated_at=datetime(2026, 4, 2 + i, tzinfo=dt_tz.utc))
        # 上期 3 月 1 张 completed
        _make_invoice(project, worth="1.00", status=InvoiceStatus.COMPLETED,
                      started_at=datetime(2026, 3, 5, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 3, 5, tzinfo=dt_tz.utc))

        resp = client.get(self._url(project), HTTP_AUTHORIZATION=AUTH).json()
        assert resp["invoice_count"] == 4
        assert resp["prev_invoice_count"] == 1
        # completed_invoice_count 只数当期 updated_at 在窗口内的 completed 状态账单
        assert resp["completed_invoice_count"] == 1

    def test_boundaries_are_half_open(self, client, project):
        # 正好 cur_start 的账单应被包含，正好 cur_end 的应被排除
        _make_invoice(project, worth="10.00", status=InvoiceStatus.COMPLETED,
                      started_at=datetime(2026, 4, 1, 0, 0, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 4, 1, 0, 0, tzinfo=dt_tz.utc))
        _make_invoice(project, worth="20.00", status=InvoiceStatus.COMPLETED,
                      started_at=datetime(2026, 4, 18, 0, 0, tzinfo=dt_tz.utc),
                      updated_at=datetime(2026, 4, 18, 0, 0, tzinfo=dt_tz.utc))
        resp = client.get(self._url(project), HTTP_AUTHORIZATION=AUTH).json()
        # GMV 只计 updated_at 在 [cur_start, cur_end) 内的，cur_end 边界被排除
        assert Decimal(resp["gmv_usd"]) == Decimal("10.00")

    def test_invalid_timestamp_returns_400(self, client, project):
        url = (
            f"/internal/v1/projects/{project.appid}/stats/summary"
            "?cur_start=not-a-date&cur_end=2026-04-18T00:00:00Z"
            "&prev_start=2026-03-01T00:00:00Z&prev_end=2026-03-18T00:00:00Z"
        )
        resp = client.get(url, HTTP_AUTHORIZATION=AUTH)
        assert resp.status_code == 400

    def test_missing_timestamp_returns_400(self, client, project):
        url = (
            f"/internal/v1/projects/{project.appid}/stats/summary"
            "?cur_end=2026-04-18T00:00:00Z"
            "&prev_start=2026-03-01T00:00:00Z&prev_end=2026-03-18T00:00:00Z"
        )
        resp = client.get(url, HTTP_AUTHORIZATION=AUTH)
        assert resp.status_code == 400

    def test_unknown_project_returns_not_found(self, client):
        url = (
            "/internal/v1/projects/nonexistent-appid/stats/summary"
            "?cur_start=2026-04-01T00:00:00Z&cur_end=2026-04-18T00:00:00Z"
            "&prev_start=2026-03-01T00:00:00Z&prev_end=2026-03-18T00:00:00Z"
        )
        resp = client.get(url, HTTP_AUTHORIZATION=AUTH)
        assert resp.status_code in (400, 404)


# ─── daily 每日时序测试 ────────────────────────────────────────────────────────


@pytest.mark.django_db
class TestStatsDaily:
    def _url(self, project, days):
        return f"/internal/v1/projects/{project.appid}/stats/daily?days={days}&metric=gmv"

    def test_rejects_unknown_days(self, client, project):
        resp = client.get(self._url(project, 42), HTTP_AUTHORIZATION=AUTH)
        assert resp.status_code == 400

    def test_rejects_unknown_metric(self, client, project):
        resp = client.get(
            f"/internal/v1/projects/{project.appid}/stats/daily?days=7&metric=foo",
            HTTP_AUTHORIZATION=AUTH,
        )
        assert resp.status_code == 400

    def test_empty_series_fills_zero_for_each_day(self, client, project):
        resp = client.get(self._url(project, 7), HTTP_AUTHORIZATION=AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["metric"] == "gmv"
        assert body["days"] == 7
        assert len(body["series"]) == 7
        for item in body["series"]:
            assert item["value"] == "0.000000"
            assert len(item["date"]) == 10  # YYYY-MM-DD

    def test_groups_completed_invoices_by_utc_day(self, client, project):
        from django.utils import timezone as django_tz
        # 用相对"今天"的日期，避免测试在未来某日过期
        today = django_tz.now().astimezone(dt_tz.utc).date()
        base = datetime(today.year, today.month, today.day, 10, 0, tzinfo=dt_tz.utc) - timedelta(days=3)

        for offset, worth in [(0, "100.00"), (1, "50.50"), (2, "25.00")]:
            _make_invoice(
                project, worth=worth, status=InvoiceStatus.COMPLETED,
                started_at=base + timedelta(days=offset),
                updated_at=base + timedelta(days=offset),
            )
        resp = client.get(self._url(project, 7), HTTP_AUTHORIZATION=AUTH).json()
        by_date = {item["date"]: Decimal(item["value"]) for item in resp["series"]}

        expected_dates = [(base + timedelta(days=i)).date().isoformat() for i in range(3)]
        assert by_date.get(expected_dates[0]) == Decimal("100.00")
        assert by_date.get(expected_dates[1]) == Decimal("50.50")
        assert by_date.get(expected_dates[2]) == Decimal("25.00")
