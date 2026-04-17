"""internal_api 列表端点的分页行为验证。

验证通过全局 REST_FRAMEWORK.DEFAULT_PAGINATION_CLASS 接入的
common.pagination.PageNumberSizePagination 真的作用于内网 API 列表端点：
- 默认 page_size = 20
- 支持 size 查询参数覆盖
- 支持 page 查询参数翻页
- size 超过 max_page_size (100) 会被截断
- 响应结构包含 {count, next, previous, results}

使用 webhook-events 端点作为载体：它是 ListModelMixin + GenericViewSet，
依赖最少（仅 Project + WebhookEvent），是验证分页的典型用例。
"""

import pytest
from chains.models import Wallet
from common.pagination import PageNumberSizePagination
from projects.models import Project
from webhooks.models import WebhookEvent

AUTH_HEADER = "Bearer test-internal-token"


@pytest.fixture
def project(db):
    # 本地创建 Wallet 记录即可，不走 Wallet.generate()（那会调用远端 signer）。
    wallet = Wallet.objects.create()
    return Project.objects.create(name="pagination-test-project", wallet=wallet)


@pytest.fixture
def webhook_events(project):
    # 创建 25 条事件以同时覆盖“默认 20 条/页 → 第二页 5 条”和“size=5 → 翻页”。
    events = [
        WebhookEvent(
            project=project,
            payload={"index": i},
            status=WebhookEvent.Status.PENDING,
        )
        for i in range(25)
    ]
    WebhookEvent.objects.bulk_create(events)
    return WebhookEvent.objects.filter(project=project)


class TestPageNumberSizePagination:
    """分页类本身的默认值。"""

    def test_defaults(self):
        paginator = PageNumberSizePagination()
        assert paginator.page_size == 20
        assert paginator.max_page_size == 100
        assert paginator.page_query_param == "page"
        assert paginator.page_size_query_param == "size"


@pytest.mark.django_db
class TestInternalApiPagination:
    """通过 webhook-events 端点验证全局分页实际生效。"""

    endpoint_template = "/internal/v1/projects/{appid}/webhook-events"

    def _url(self, project):
        return self.endpoint_template.format(appid=project.appid)

    def test_response_has_pagination_envelope(self, client, project, webhook_events):
        response = client.get(self._url(project), HTTP_AUTHORIZATION=AUTH_HEADER)
        assert response.status_code == 200
        body = response.json()
        # 至少这四个字段，前端分页 UI 依赖它们。
        assert set(body.keys()) >= {"count", "next", "previous", "results"}
        assert body["count"] == 25
        assert body["previous"] is None

    def test_default_page_size_is_20(self, client, project, webhook_events):
        response = client.get(self._url(project), HTTP_AUTHORIZATION=AUTH_HEADER)
        assert response.status_code == 200
        body = response.json()
        # 默认每页 20 条：25 条数据首页应该返回 20 条，并且存在 next 链接。
        assert len(body["results"]) == 20
        assert body["next"] is not None

    def test_size_query_param_overrides_default(self, client, project, webhook_events):
        response = client.get(
            self._url(project) + "?size=5", HTTP_AUTHORIZATION=AUTH_HEADER
        )
        assert response.status_code == 200
        body = response.json()
        assert len(body["results"]) == 5
        assert body["count"] == 25
        assert body["next"] is not None

    def test_page_query_param_paginates(self, client, project, webhook_events):
        first = client.get(
            self._url(project) + "?page=1&size=5", HTTP_AUTHORIZATION=AUTH_HEADER
        ).json()
        second = client.get(
            self._url(project) + "?page=2&size=5", HTTP_AUTHORIZATION=AUTH_HEADER
        ).json()
        assert len(second["results"]) == 5
        # 第 2 页上一页应当指向第 1 页（且不能是 None）。
        assert second["previous"] is not None
        # 分页应返回不同记录，而非重复第一页。
        first_ids = {row["id"] for row in first["results"]}
        second_ids = {row["id"] for row in second["results"]}
        assert first_ids.isdisjoint(second_ids)

    def test_size_is_capped_by_max_page_size(self, client, project, webhook_events):
        response = client.get(
            self._url(project) + "?size=1000", HTTP_AUTHORIZATION=AUTH_HEADER
        )
        assert response.status_code == 200
        body = response.json()
        # 只有 25 条数据，但关键验证是：响应没被 1000 撑爆（被 max_page_size=100 截断）。
        # 25 < 100 所以返回 25 条，且 next 为 None。
        assert len(body["results"]) == 25
        assert body["next"] is None

    def test_size_cap_applies_when_data_exceeds_cap(
        self, client, project, django_db_blocker
    ):
        """数据超出 max_page_size 时，单页结果数被严格限制为 100。"""
        # 构造 120 条数据，直接请求 size=1000，响应最多给 100 条。
        events = [
            WebhookEvent(
                project=project,
                payload={"i": i},
                status=WebhookEvent.Status.PENDING,
            )
            for i in range(120)
        ]
        WebhookEvent.objects.bulk_create(events)
        response = client.get(
            self._url(project) + "?size=1000", HTTP_AUTHORIZATION=AUTH_HEADER
        )
        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 120
        assert len(body["results"]) == 100
        assert body["next"] is not None
