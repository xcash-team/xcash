from unittest.mock import patch

from django.contrib.sessions.middleware import SessionMiddleware
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.test import Client
from django.test import TestCase
from django.test import override_settings
from django.test.client import RequestFactory
from django.urls import reverse
from django.utils import timezone
from django_otp.oath import TOTP
from django_otp.plugins.otp_totp.models import TOTPDevice

from django.core.cache import cache as _cache
from chains.test_signer import build_test_remote_signer_backend
from projects.models import RecipientAddress
from users.models import AdminAccessLog
from users.models import Customer
from users.models import User
from users.otp import ADMIN_OTP_PENDING_USER_ID_SESSION_KEY
from users.otp import ADMIN_OTP_VERIFIED_AT_SESSION_KEY
from users.otp import get_admin_otp_ratelimit_key

_USERS_TEST_PATCHERS = []


def setUpModule():
    # 用户初始化会自动创建项目与钱包；测试阶段统一切到进程内 signer 假体，避免额外依赖外部 HTTP 服务。
    _cache.clear()
    backend = build_test_remote_signer_backend()
    for target in ("chains.signer.get_signer_backend",):
        patcher = patch(target, return_value=backend)
        patcher.start()
        _USERS_TEST_PATCHERS.append(patcher)


def tearDownModule():
    while _USERS_TEST_PATCHERS:
        _USERS_TEST_PATCHERS.pop().stop()
    _cache.clear()


class CustomerRawAccIdxTests(TestCase):
    def test_address_index_can_repeat_across_projects(self):
        # 不同项目的客户索引都会从 0 开始分配，不能被全局唯一约束拦住。
        from projects.models import Project

        first_project = Project.objects.create(name="Project A")
        second_project = Project.objects.create(name="Project B")

        first_customer = Customer.objects.create(project=first_project, uid="u-1")
        second_customer = Customer.objects.create(project=second_project, uid="u-1")

        self.assertEqual(first_customer.address_index, 0)
        self.assertEqual(second_customer.address_index, 0)


class TestEnsureDefaultSuperuserCommand(TestCase):
    def test_creates_default_superuser_when_none_exists(self):
        out = []

        with patch("sys.stdout.write", side_effect=out.append):
            call_command("ensure_default_superuser")

        admin_user = User.objects.get(username="admin")
        self.assertTrue(admin_user.is_superuser)
        self.assertTrue(admin_user.is_staff)
        self.assertTrue(admin_user.check_password("Admin@123456"))
        self.assertIn("已创建默认管理员账号", "".join(out))

    def test_skips_creation_when_superuser_already_exists(self):
        existing = User.objects.create_superuser(
            username="existing-admin",
            password="secret",
        )
        out = []

        with patch("sys.stdout.write", side_effect=out.append):
            call_command("ensure_default_superuser")

        self.assertEqual(User.objects.filter(is_superuser=True).count(), 1)
        self.assertTrue(User.objects.filter(pk=existing.pk).exists())
        self.assertFalse(User.objects.filter(username="admin").exists())
        self.assertIn("已存在管理员账号", "".join(out))


@override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"])
class AdminOTPTests(TestCase):
    def _current_token(self, device: TOTPDevice) -> str:
        # 测试中用设备当前参数实时生成一次有效验证码，避免对固定时间戳产生脆弱依赖。
        return str(
            TOTP(
                device.bin_key, device.step, device.t0, device.digits, device.drift
            ).token()
        ).zfill(device.digits)

    def _force_verified_admin_login(
        self,
        user: User,
        *,
        client: Client | None = None,
        device: TOTPDevice | None = None,
    ):
        client = client or self.client
        device = device or TOTPDevice.objects.create(user=user, name="Admin TOTP")
        client.force_login(user)
        session = client.session
        # admin 用户测试自定义页面时，也必须补齐 OTP 已验证会话，否则会被中间件重定向回登录链路。
        session["otp_device_id"] = device.persistent_id
        session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = timezone.now().isoformat()
        session.save()
        return device

    def test_signup_route_is_removed(self):
        # 账户仅允许后台内部创建后，公开注册路径必须彻底下线。
        response = self.client.get("/signup")

        self.assertEqual(response.status_code, 404)

    def test_admin_otp_ratelimit_key_uses_pending_user_and_session_instead_of_ip_only(
        self,
    ):
        # OTP 绑定/验证不能只按出口 IP 限流，否则同一办公网或本机调试会互相污染。
        request = RequestFactory().post("/otp/setup")
        SessionMiddleware(lambda req: None).process_request(request)
        request.session[ADMIN_OTP_PENDING_USER_ID_SESSION_KEY] = 42
        request.session.save()

        key = get_admin_otp_ratelimit_key(request)
        self.assertIn("/otp/setup", key)
        self.assertIn("user:42", key)
        self.assertIn(f"session:{request.session.session_key}", key)

    def test_password_login_redirects_to_otp_setup_when_device_missing(self):
        user = User.objects.create_user(
            username="otp_setup_user", password="secret", is_staff=True
        )
        client = Client()
        extra = {"REMOTE_ADDR": "10.0.0.11"}

        response = client.post(
            "/login?next=/", {"username": user.username, "password": "secret"}, **extra
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/otp/setup")
        session = client.session
        self.assertEqual(session["admin_otp_pending_user_id"], user.pk)
        self.assertEqual(
            AdminAccessLog.objects.filter(
                user=user,
                action=AdminAccessLog.Action.PASSWORD_LOGIN,
                result=AdminAccessLog.Result.SUCCEEDED,
            ).count(),
            1,
        )

    def test_otp_setup_confirms_device_and_allows_admin_access(self):
        user = User.objects.create_user(
            username="otp_bind_user", password="secret", is_staff=True
        )
        client = Client()
        extra = {"REMOTE_ADDR": "10.0.0.12"}
        client.post(
            "/login?next=/", {"username": user.username, "password": "secret"}, **extra
        )
        setup_page = client.get("/otp/setup", **extra)

        self.assertEqual(setup_page.status_code, 200)

        device = TOTPDevice.objects.get(user=user, confirmed=False)
        response = client.post(
            "/otp/setup",
            {"device_name": "Primary Admin OTP", "token": self._current_token(device)},
            **extra,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/")
        device.refresh_from_db()
        self.assertTrue(device.confirmed)
        admin_response = client.get("/")
        self.assertEqual(admin_response.status_code, 200)
        self.assertTrue(client.session.get("otp_device_id"))
        # OTP 成功后必须记录验证时间，后续高风险动作才有可校验的新鲜度依据。
        self.assertTrue(client.session.get(ADMIN_OTP_VERIFIED_AT_SESSION_KEY))

    def test_login_with_existing_device_redirects_to_otp_verify(self):
        user = User.objects.create_user(
            username="otp_verify_user", password="secret", is_staff=True
        )
        device = TOTPDevice.objects.create(user=user, name="Admin TOTP")
        client = Client()
        extra = {"REMOTE_ADDR": "10.0.0.13"}

        response = client.post(
            "/login?next=/", {"username": user.username, "password": "secret"}, **extra
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/otp/verify")
        self.assertEqual(client.get("/otp/verify", **extra).status_code, 200)
        verify_response = client.post(
            "/otp/verify", {"token": self._current_token(device)}, **extra
        )
        self.assertEqual(verify_response.status_code, 302)
        self.assertEqual(verify_response["Location"], "/")
        self.assertTrue(client.session.get("otp_device_id"))
        self.assertTrue(client.session.get(ADMIN_OTP_VERIFIED_AT_SESSION_KEY))

    def test_unverified_staff_session_is_downgraded_before_admin_access(self):
        user = User.objects.create_user(
            username="otp_guard_user", password="secret", is_staff=True
        )
        TOTPDevice.objects.create(user=user, name="Admin TOTP")
        client = Client()
        client.force_login(user)
        extra = {"REMOTE_ADDR": "10.0.0.14"}

        response = client.get("/", **extra)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/otp/verify")
        session = client.session
        self.assertEqual(session["admin_otp_pending_user_id"], user.pk)
        self.assertFalse("_auth_user_id" in session)
        self.assertEqual(client.get("/otp/verify", **extra).status_code, 200)

    def test_user_admin_can_rotate_otp_secret_from_custom_page(self):
        admin_user = User.objects.create_superuser(
            username="otp_admin_rotate", password="secret"
        )
        target_user = User.objects.create_user(
            username="otp_target_rotate",
            password="secret",
        )
        old_device = TOTPDevice.objects.create(
            user=target_user,
            name="Legacy OTP",
            confirmed=True,
        )
        self._force_verified_admin_login(admin_user)
        change_url = reverse("admin:users_user_otp_change", args=[target_user.pk])

        get_response = self.client.get(change_url)

        self.assertEqual(get_response.status_code, 200)
        pending_device = TOTPDevice.objects.get(user=target_user, confirmed=False)

        post_response = self.client.post(
            change_url,
            {
                "device_name": "Rotated Admin OTP",
                "token": self._current_token(pending_device),
            },
            follow=True,
        )

        self.assertEqual(post_response.status_code, 200)
        self.assertEqual(TOTPDevice.objects.filter(user=target_user).count(), 1)
        new_device = TOTPDevice.objects.get(user=target_user)
        self.assertTrue(new_device.confirmed)
        self.assertEqual(new_device.name, "Rotated Admin OTP")
        self.assertNotEqual(new_device.pk, old_device.pk)

    def test_self_service_otp_rotation_requires_password_and_current_otp(self):
        admin_user = User.objects.create_superuser(
            username="otp_admin_self_rotate",
            password="secret",
        )
        current_device = TOTPDevice.objects.create(
            user=admin_user,
            name="Current Admin OTP",
            confirmed=True,
        )
        self._force_verified_admin_login(admin_user, device=current_device)
        change_url = reverse("admin:users_user_otp_change", args=[admin_user.pk])

        get_response = self.client.get(change_url)

        self.assertEqual(get_response.status_code, 200)
        pending_device = TOTPDevice.objects.get(user=admin_user, confirmed=False)
        invalid_response = self.client.post(
            change_url,
            {
                "device_name": "New Self OTP",
                "token": self._current_token(pending_device),
            },
        )
        self.assertEqual(invalid_response.status_code, 200)
        self.assertIn("current_password", invalid_response.context["form"].errors)
        self.assertIn("current_token", invalid_response.context["form"].errors)

        valid_response = self.client.post(
            change_url,
            {
                "current_password": "secret",
                "current_token": self._current_token(current_device),
                "device_name": "New Self OTP",
                "token": self._current_token(pending_device),
            },
            follow=True,
        )

        self.assertEqual(valid_response.status_code, 200)
        self.assertEqual(TOTPDevice.objects.filter(user=admin_user).count(), 1)
        new_device = TOTPDevice.objects.get(user=admin_user)
        self.assertEqual(new_device.name, "New Self OTP")
        session = self.client.session
        self.assertEqual(session.get("otp_device_id"), new_device.persistent_id)

    def test_non_superuser_cannot_rotate_other_user_otp(self):
        operator = User.objects.create_user(
            username="otp_operator_denied",
            password="secret",
            is_staff=True,
        )
        target_user = User.objects.create_user(
            username="otp_target_denied",
            password="secret",
        )
        self._force_verified_admin_login(operator)

        response = self.client.get(
            reverse("admin:users_user_otp_change", args=[target_user.pk])
        )

        self.assertEqual(response.status_code, 403)


@override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"])
class RecipientAddressAdminTests(TestCase):
    def test_recipient_address_rejects_unknown_usage(self):
        from projects.models import Project

        project = Project.objects.create(name="Recipient Validate Project")

        with self.assertRaises(ValidationError):
            RecipientAddress.objects.create(
                name="非法双用途地址",
                project=project,
                chain_type="evm",
                address="0x5AAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
                usage="unknown",
            )
