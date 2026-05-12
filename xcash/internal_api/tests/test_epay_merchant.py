"""EpayMerchant internal_api 单例资源接口的回归测试。

行为契约：
- 系统级 lazy create：GET 永远返回 200，没有"未配置"概念
- pid 由系统在 PID_BASELINE=1688 基础上单调递增分配，外部不可写
- 用户仅能修改 active 与 secret_key
- 不支持 DELETE（每个项目必须始终拥有 EpayMerchant）
"""

from unittest.mock import Mock
from unittest.mock import patch

import pytest
from chains.models import Wallet
from invoices.models import EpayMerchant
from projects.models import Project

AUTH_HEADER = "Bearer test-internal-token"


@pytest.fixture
def project(db):
    wallet = Wallet.objects.create()
    return Project.objects.create(
        name="epay-test-project",
        wallet=wallet,
        ip_white_list="*",
        webhook="",
        hmac_key="ORIG-HMAC-KEY-ORIGINAL-32CHARS00",
    )


def _other_project(name: str = "other-project") -> Project:
    return Project.objects.create(
        name=name,
        wallet=Wallet.objects.create(),
        ip_white_list="*",
        hmac_key="OTHER-HMAC-KEY-32CHARS-ABCDEFGHI",
    )


def _url(project):
    return f"/internal/v1/projects/{project.appid}/epay-merchant"


VALID_NEW_SECRET = "user-rotated-secret-key-32chars-x"


@pytest.mark.django_db
class TestEpayMerchantLazyCreate:
    def test_get_creates_merchant_on_first_call_at_baseline_pid(self, client, project):
        # 系统中尚无 EpayMerchant：首个 pid 从 PID_BASELINE 起步。
        assert not EpayMerchant.objects.filter(project=project).exists()

        response = client.get(_url(project), HTTP_AUTHORIZATION=AUTH_HEADER)

        assert response.status_code == 200
        body = response.json()
        assert body["pid"] == EpayMerchant.PID_BASELINE
        assert body["active"] is True
        assert len(body["secret_key"]) == EpayMerchant.SECRET_KEY_LENGTH

        # 二次 GET 应当幂等，不再产生新的记录或漂移 pid。
        again = client.get(_url(project), HTTP_AUTHORIZATION=AUTH_HEADER)
        assert again.json()["pid"] == body["pid"]
        assert EpayMerchant.objects.filter(project=project).count() == 1

    def test_get_returns_existing_merchant_without_mutation(self, client, project):
        existing = EpayMerchant.objects.create(
            project=project, pid=9999, secret_key="preexisting-secret-32chars-abcdef",
        )
        response = client.get(_url(project), HTTP_AUTHORIZATION=AUTH_HEADER)
        assert response.status_code == 200
        body = response.json()
        assert body["pid"] == 9999
        assert body["secret_key"] == existing.secret_key

    def test_pid_increments_above_baseline_when_existing_max_above(self, client, project):
        # 模拟系统里已有大于 baseline 的商户：下一次分配应当为 max + 1。
        EpayMerchant.objects.create(
            project=_other_project(), pid=5000, secret_key="seed-secret-32chars-padding00",
        )

        response = client.get(_url(project), HTTP_AUTHORIZATION=AUTH_HEADER)

        assert response.status_code == 200
        assert response.json()["pid"] == 5001

    def test_pid_resets_to_baseline_when_existing_max_below(self, client, project):
        # 历史数据里有 pid=100 这类小于 baseline 的记录，新项目仍应跳到 baseline。
        EpayMerchant.objects.create(
            project=_other_project(), pid=100, secret_key="legacy-secret-32chars-padding00",
        )

        response = client.get(_url(project), HTTP_AUTHORIZATION=AUTH_HEADER)

        assert response.status_code == 200
        assert response.json()["pid"] == EpayMerchant.PID_BASELINE


@pytest.mark.django_db
class TestEpayMerchantUpdate:
    def test_patch_updates_active(self, client, project):
        EpayMerchant.ensure_for_project(project)

        response = client.patch(
            _url(project),
            data={"active": False},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )

        assert response.status_code == 200
        assert response.json()["active"] is False

    def test_patch_rotates_secret_key(self, client, project):
        merchant = EpayMerchant.ensure_for_project(project)
        original_pid = merchant.pid

        response = client.patch(
            _url(project),
            data={"secret_key": VALID_NEW_SECRET},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )

        assert response.status_code == 200
        body = response.json()
        assert body["secret_key"] == VALID_NEW_SECRET
        # pid 不应因 PATCH 而漂移
        assert body["pid"] == original_pid

    def test_patch_ignores_pid_writes(self, client, project):
        merchant = EpayMerchant.ensure_for_project(project)
        original_pid = merchant.pid

        # 显式尝试改 pid 应被静默丢弃（read_only_fields），而非 400。
        response = client.patch(
            _url(project),
            data={"pid": 99999},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )

        assert response.status_code == 200
        merchant.refresh_from_db()
        assert merchant.pid == original_pid

    def test_patch_rejects_short_secret_key(self, client, project):
        EpayMerchant.ensure_for_project(project)

        response = client.patch(
            _url(project),
            data={"secret_key": "short"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )

        assert response.status_code == 400
        assert "secret_key" in response.json()

    def test_patch_creates_merchant_lazily_if_missing(self, client, project):
        # 即使外部直接 PATCH，也应先 lazy create 再应用更新。
        assert not EpayMerchant.objects.filter(project=project).exists()

        response = client.patch(
            _url(project),
            data={"active": False},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )

        assert response.status_code == 200
        assert response.json()["active"] is False
        assert EpayMerchant.objects.filter(project=project).count() == 1


@pytest.mark.django_db
class TestEpayMerchantSafety:
    def test_delete_is_not_allowed(self, client, project):
        EpayMerchant.ensure_for_project(project)

        response = client.delete(_url(project), HTTP_AUTHORIZATION=AUTH_HEADER)

        assert response.status_code == 405
        assert EpayMerchant.objects.filter(project=project).exists()

    def test_requires_internal_token(self, client, project):
        response = client.get(_url(project))
        assert response.status_code == 401

    def test_returns_404_for_unknown_appid(self, client):
        response = client.get(
            "/internal/v1/projects/unknown-appid/epay-merchant",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 404


@pytest.mark.django_db
class TestProjectCreateAutoProvisions:
    @patch("chains.signer.get_signer_backend")
    def test_project_create_auto_provisions_epay_merchant(
        self, get_signer_backend_mock, client,
    ):
        # Wallet.generate 会调用远端 signer，测试里 mock 掉避免发真请求。
        signer_backend = Mock()
        signer_backend.create_wallet.return_value = None
        get_signer_backend_mock.return_value = signer_backend

        # 通过 internal_api 创建 Project，应自动配套创建 EpayMerchant。
        response = client.post(
            "/internal/v1/projects",
            data={"name": "auto-provision-project"},
            content_type="application/json",
            HTTP_AUTHORIZATION=AUTH_HEADER,
        )
        assert response.status_code == 201
        appid = response.json()["appid"]

        project = Project.objects.get(appid=appid)
        assert EpayMerchant.objects.filter(project=project).exists()
        merchant = project.epay_merchant
        assert merchant.pid >= EpayMerchant.PID_BASELINE
        assert merchant.active is True
        assert len(merchant.secret_key) == EpayMerchant.SECRET_KEY_LENGTH


@pytest.mark.django_db
class TestEnsureForProject:
    def test_first_project_gets_baseline_pid(self, project):
        merchant = EpayMerchant.ensure_for_project(project)
        assert merchant.pid == EpayMerchant.PID_BASELINE

    def test_idempotent_on_second_call(self, project):
        first = EpayMerchant.ensure_for_project(project)
        second = EpayMerchant.ensure_for_project(project)
        assert first.pk == second.pk
        assert EpayMerchant.objects.filter(project=project).count() == 1

    def test_pid_resets_to_baseline_when_max_below(self, project):
        EpayMerchant.objects.create(
            project=_other_project(), pid=99, secret_key="legacy-secret-32chars-padding00",
        )
        merchant = EpayMerchant.ensure_for_project(project)
        assert merchant.pid == EpayMerchant.PID_BASELINE

    def test_pid_increments_when_max_above_baseline(self, project):
        EpayMerchant.objects.create(
            project=_other_project(), pid=3000, secret_key="seed-secret-32chars-padding00",
        )
        merchant = EpayMerchant.ensure_for_project(project)
        assert merchant.pid == 3001
