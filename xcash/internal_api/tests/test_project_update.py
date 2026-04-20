"""ProjectViewSet PATCH 字段白名单与 PUT/DELETE 拦截的安全回归测试。

覆盖：
- PUT/DELETE 被 http_method_names 拦截（405）
- PATCH 只能修改白名单字段，非白名单字段被 DRF 忽略而非抛错
- 每个字段的业务校验（webhook scheme、hmac 长度、IP/CIDR、数值范围）
- 跨字段校验（单笔限额 ≤ 日限额）
- 合法 PATCH 正常入库
"""

from decimal import Decimal

import pytest
from chains.models import Wallet
from projects.models import Project

AUTH_HEADER = "Bearer test-internal-token"


@pytest.fixture
def project(db):
    # 直接本地创建 Wallet 记录，避免 Wallet.generate() 调用远端 signer。
    wallet = Wallet.objects.create()
    return Project.objects.create(
        name="patch-test-project",
        wallet=wallet,
        ip_white_list="*",
        webhook="",
        hmac_key="ORIG-HMAC-KEY-ORIGINAL-32CHARS00",
    )


def _url(project):
    return f"/internal/v1/projects/{project.appid}"


# ---------- HTTP 方法拦截 ----------


@pytest.mark.django_db
class TestMethodRestrictions:
    def test_put_returns_405(self, client, project):
        response = client.put(
            _url(project),
            data={"name": "hacked"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 405

    def test_delete_returns_405(self, client, project):
        response = client.delete(
            _url(project),
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 405
        # 项目不应被删除。
        assert Project.objects.filter(pk=project.pk).exists()

    def test_get_detail_allowed(self, client, project):
        response = client.get(
            _url(project),
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_patch_allowed(self, client, project):
        response = client.patch(
            _url(project),
            data={"webhook_open": False},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200


# ---------- PATCH 字段白名单 ----------


@pytest.mark.django_db
class TestPatchFieldWhitelist:
    def test_non_whitelisted_fields_are_ignored(self, client, project):
        """非白名单字段（name/active/appid）不会被写入，ModelSerializer 默认忽略额外字段。"""
        original_name = project.name
        original_active = project.active
        original_appid = project.appid

        response = client.patch(
            _url(project),
            data={
                "name": "malicious-rename",
                "active": False,
                "appid": "XC-HACKED0",
            },
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

        project.refresh_from_db()
        assert project.name == original_name
        assert project.active == original_active
        assert project.appid == original_appid

    def test_happy_path_multiple_fields(self, client, project):
        """合法 PATCH：同时修改多个白名单字段，均正确入库。"""
        payload = {
            "webhook": "https://example.com/cb",
            "webhook_open": False,
            "pre_notify": True,
            "fast_confirm_threshold": "25.50",
            "gather_period": 30,
        }
        response = client.patch(
            _url(project),
            data=payload,
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200, response.content

        project.refresh_from_db()
        assert project.webhook == "https://example.com/cb"
        assert project.webhook_open is False
        assert project.pre_notify is True
        assert project.fast_confirm_threshold == Decimal("25.50")
        assert project.gather_period == 30


# ---------- 单字段校验 ----------


@pytest.mark.django_db
class TestWebhookValidation:
    def test_rejects_javascript_scheme(self, client, project):
        response = client.patch(
            _url(project),
            data={"webhook": "javascript:alert(1)"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        # URLField 的内置校验或自定义 scheme 校验都会阻止；接受任一 400。
        assert response.status_code == 400

    def test_accepts_http_url(self, client, project):
        response = client.patch(
            _url(project),
            data={"webhook": "http://example.com/cb"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_accepts_https_url(self, client, project):
        response = client.patch(
            _url(project),
            data={"webhook": "https://example.com/cb"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200


@pytest.mark.django_db
class TestHmacKeyValidation:
    def test_too_short(self, client, project):
        response = client.patch(
            _url(project),
            data={"hmac_key": "short"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400

    def test_min_length_ok(self, client, project):
        value = "a" * 16
        response = client.patch(
            _url(project),
            data={"hmac_key": value},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_max_length_ok(self, client, project):
        # 模型层 ShortUUIDField(length=32) 将 max_length 限制为 32。
        value = "a" * 32
        response = client.patch(
            _url(project),
            data={"hmac_key": value},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_too_long(self, client, project):
        value = "a" * 33
        response = client.patch(
            _url(project),
            data={"hmac_key": value},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400


@pytest.mark.django_db
class TestIpWhiteListValidation:
    def test_rejects_garbage(self, client, project):
        response = client.patch(
            _url(project),
            data={"ip_white_list": "abc"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400

    def test_accepts_wildcard(self, client, project):
        response = client.patch(
            _url(project),
            data={"ip_white_list": "*"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_accepts_cidr_list(self, client, project):
        response = client.patch(
            _url(project),
            data={"ip_white_list": "192.168.1.0/24, 10.0.0.1"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200
        project.refresh_from_db()
        assert project.ip_white_list == "192.168.1.0/24, 10.0.0.1"

    def test_rejects_over_max_entries(self, client, project):
        # 101 条 IP，超出 100 上限。
        entries = ",".join(f"10.0.0.{i % 255}" for i in range(101))
        response = client.patch(
            _url(project),
            data={"ip_white_list": entries},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400


@pytest.mark.django_db
class TestFastConfirmThresholdValidation:
    def test_rejects_negative(self, client, project):
        response = client.patch(
            _url(project),
            data={"fast_confirm_threshold": "-1"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400

    def test_accepts_zero(self, client, project):
        response = client.patch(
            _url(project),
            data={"fast_confirm_threshold": "0"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_rejects_over_cap(self, client, project):
        response = client.patch(
            _url(project),
            data={"fast_confirm_threshold": "9999999999"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400


@pytest.mark.django_db
class TestWithdrawalSingleLimitValidation:
    def test_rejects_zero(self, client, project):
        response = client.patch(
            _url(project),
            data={"withdrawal_single_limit": "0"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400

    def test_rejects_negative(self, client, project):
        response = client.patch(
            _url(project),
            data={"withdrawal_single_limit": "-1"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400

    def test_null_allowed(self, client, project):
        # null 表示不限额；通过。
        response = client.patch(
            _url(project),
            data={"withdrawal_single_limit": None},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_single_greater_than_daily_rejected(self, client, project):
        """跨字段：单笔 10 > 日限额 5 应被拒。"""
        response = client.patch(
            _url(project),
            data={
                "withdrawal_single_limit": "10",
                "withdrawal_daily_limit": "5",
            },
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400


@pytest.mark.django_db
class TestGatherPeriodValidation:
    def test_rejects_zero(self, client, project):
        response = client.patch(
            _url(project),
            data={"gather_period": 0},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400

    def test_accepts_min(self, client, project):
        response = client.patch(
            _url(project),
            data={"gather_period": 1},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_accepts_max(self, client, project):
        response = client.patch(
            _url(project),
            data={"gather_period": 525600},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 200

    def test_rejects_over_max(self, client, project):
        response = client.patch(
            _url(project),
            data={"gather_period": 525601},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 400
