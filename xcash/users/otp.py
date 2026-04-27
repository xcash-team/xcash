from __future__ import annotations

import logging

from base64 import b32encode
from base64 import b64encode
from datetime import UTC
from datetime import timedelta
from io import BytesIO

from django.conf import settings
from django.contrib.auth import login as auth_login
from django.contrib.auth import logout as auth_logout
from django.db import transaction
from django.shortcuts import redirect
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.utils.http import url_has_allowed_host_and_scheme
from django_otp import login as otp_login
from django_otp.plugins.otp_totp.models import TOTPDevice
from qrcode import QRCode
from qrcode.constants import ERROR_CORRECT_M
from qrcode.image.svg import SvgPathImage

from core.runtime_settings import (
    get_admin_sensitive_action_otp_max_age_seconds as get_runtime_admin_otp_max_age_seconds,
)
from users.models import AdminAccessLog
from users.models import User

logger = logging.getLogger(__name__)

ADMIN_OTP_PENDING_USER_ID_SESSION_KEY = "admin_otp_pending_user_id"
ADMIN_OTP_NEXT_PATH_SESSION_KEY = "admin_otp_next_path"
ADMIN_OTP_VERIFIED_AT_SESSION_KEY = "admin_otp_verified_at"


class AdminOTPRequiredError(PermissionError):
    """高风险后台动作缺少近期两步验证时抛出。"""


def is_user_otp_verified(user) -> bool:
    verify_fn = getattr(user, "is_verified", None)
    if callable(verify_fn):
        return bool(verify_fn())
    # RequestFactory / 单元测试里 request.user 可能未经过 OTPMiddleware，这里回退到 otp_device 标记。
    return bool(getattr(user, "otp_device", None))


def get_admin_otp_ratelimit_key(request, *_args, **_kwargs) -> str:
    # OTP 阶段优先按“待验证用户 + 会话”限流，避免同一出口 IP 下多个管理员互相打满额度。
    session_key = getattr(request.session, "session_key", "") or "anonymous-session"
    pending_user_id = request.session.get(ADMIN_OTP_PENDING_USER_ID_SESSION_KEY)
    user_id = pending_user_id or getattr(getattr(request, "user", None), "pk", None)
    user_part = f"user:{user_id}" if user_id else "user:anonymous"
    return f"admin-otp:{request.path}:{user_part}:session:{session_key}"


def record_admin_access(
    *, request, action: str, result: str, user=None, reason: str = ""
) -> None:
    # 登录与 OTP 审计要尽量轻量，失败也不能影响主登录链路。
    AdminAccessLog.objects.create(
        user=user,
        username_snapshot=getattr(user, "username", "") or "",
        ip=request.META.get("REMOTE_ADDR") or None,
        user_agent=request.headers.get("user-agent", "")[:1024],
        action=action,
        result=result,
        reason=reason[:1024],
    )


def verify_otp_token(device, token: str) -> bool:
    """校验 TOTP token。settings.DEBUG=True 时无条件通过并打 warning 日志。

    DEBUG bypass 仅用于本地开发，避免每次登录后台都要打开 Authenticator。
    生产配置必须保持 DEBUG=False，否则任何 token 都会被放行。
    """
    if settings.DEBUG:
        logger.warning(
            "OTP token verification bypassed by DEBUG=True (device_id=%s, user_id=%s)",
            getattr(device, "id", None),
            getattr(getattr(device, "user", None), "pk", None),
        )
        return True
    return bool(device.verify_token(token))


def set_pending_admin_otp(request, *, user: User, next_path: str) -> None:
    # 第一步密码校验通过后只记录待验证上下文，不直接发放可进入后台的登录态。
    request.session.pop(ADMIN_OTP_VERIFIED_AT_SESSION_KEY, None)
    request.session[ADMIN_OTP_PENDING_USER_ID_SESSION_KEY] = user.pk
    request.session[ADMIN_OTP_NEXT_PATH_SESSION_KEY] = next_path or "/"


def clear_pending_admin_otp(request) -> None:
    request.session.pop(ADMIN_OTP_PENDING_USER_ID_SESSION_KEY, None)
    request.session.pop(ADMIN_OTP_NEXT_PATH_SESSION_KEY, None)


def get_pending_admin_user(request) -> User | None:
    user_id = request.session.get(ADMIN_OTP_PENDING_USER_ID_SESSION_KEY)
    if not user_id:
        return None
    try:
        return User.objects.get(pk=user_id, is_active=True, is_staff=True)
    except User.DoesNotExist:
        clear_pending_admin_otp(request)
        return None


def get_pending_admin_next_path(request) -> str:
    next_path = request.session.get(ADMIN_OTP_NEXT_PATH_SESSION_KEY, "/")
    # 防止 Open Redirect：仅允许同站相对路径。
    if url_has_allowed_host_and_scheme(next_path, allowed_hosts=None):
        return next_path
    return "/"


def get_admin_sensitive_action_otp_max_age_seconds() -> int:
    # OTP 新鲜度默认仍由 settings 提供，但平台参数中心可在运行期覆盖该阈值。
    return get_runtime_admin_otp_max_age_seconds()


def get_admin_otp_verified_at(request):
    raw_value = request.session.get(ADMIN_OTP_VERIFIED_AT_SESSION_KEY)
    if not raw_value:
        return None
    verified_at = parse_datetime(raw_value)
    if verified_at is None:
        return None
    if timezone.is_naive(verified_at):
        verified_at = timezone.make_aware(verified_at, UTC)
    return verified_at


def build_admin_approval_context(
    *, verified_at=None, source: str = ""
) -> dict[str, object]:
    verified_at = verified_at or timezone.now()
    if timezone.is_naive(verified_at):
        verified_at = timezone.make_aware(verified_at, UTC)
    return {
        "otp_verified": True,
        "otp_verified_at": verified_at.isoformat(),
        "otp_max_age_seconds": get_admin_sensitive_action_otp_max_age_seconds(),
        "source": source or "unknown",
    }


def build_admin_sensitive_action_context(
    *, verified_at=None, source: str = ""
) -> dict[str, object]:
    # 审批只是敏感动作的一种；项目配置、signer 运营页等入口复用同一份上下文结构。
    return build_admin_approval_context(verified_at=verified_at, source=source)


def validate_admin_approval_context(
    *, context: dict[str, object] | None
) -> dict[str, object]:
    if not context or not context.get("otp_verified"):
        raise AdminOTPRequiredError("审批动作缺少两步验证上下文")

    verified_at_raw = str(context.get("otp_verified_at") or "").strip()
    verified_at = parse_datetime(verified_at_raw)
    if verified_at is None:
        raise AdminOTPRequiredError("审批动作缺少有效的两步验证时间")
    if timezone.is_naive(verified_at):
        verified_at = timezone.make_aware(verified_at, UTC)

    max_age_seconds = int(
        context.get("otp_max_age_seconds")
        or get_admin_sensitive_action_otp_max_age_seconds()
    )
    if timezone.now() - verified_at > timedelta(seconds=max_age_seconds):
        raise AdminOTPRequiredError("审批所需的两步验证已过期，请重新验证两步验证码")

    # service 层只信任归一化后的上下文，避免日志里混入无效时间格式。
    return {
        "otp_verified": True,
        "otp_verified_at": verified_at.isoformat(),
        "otp_max_age_seconds": max_age_seconds,
        "source": str(context.get("source") or "unknown"),
    }


def validate_admin_sensitive_action_context(
    *, context: dict[str, object] | None
) -> dict[str, object]:
    return validate_admin_approval_context(context=context)


def get_fresh_admin_approval_context(*, request, source: str) -> dict[str, object]:
    user = getattr(request, "user", None)
    if (
        user is None
        or not user.is_authenticated
        or not user.is_staff
        or not is_user_otp_verified(user)
    ):
        raise AdminOTPRequiredError(
            "当前会话未完成后台两步验证，请先重新验证两步验证码"
        )

    if not hasattr(request, "session"):
        raise AdminOTPRequiredError("当前请求缺少后台会话，请重新验证两步验证码")
    verified_at = get_admin_otp_verified_at(request)
    if verified_at is None:
        raise AdminOTPRequiredError(
            "当前会话缺少近期两步验证记录，请重新验证两步验证码"
        )

    return validate_admin_approval_context(
        context=build_admin_approval_context(verified_at=verified_at, source=source)
    )


def get_fresh_admin_sensitive_action_context(
    *, request, source: str
) -> dict[str, object]:
    return get_fresh_admin_approval_context(request=request, source=source)


def get_primary_totp_device(
    *, user: User, confirmed: bool | None = True
) -> TOTPDevice | None:
    # 第一版只支持一个主 TOTP 设备；后续若支持多设备，再把这里升级为显式设备选择。
    return (
        TOTPDevice.objects.devices_for_user(user, confirmed=confirmed)
        .order_by("id")
        .first()
    )


def get_or_create_pending_totp_device(*, user: User) -> TOTPDevice:
    device = get_primary_totp_device(user=user, confirmed=False)
    if device is not None:
        return device
    return TOTPDevice.objects.create(
        user=user,
        name="后台两步验证",
        confirmed=False,
    )


@transaction.atomic
def activate_pending_totp_device(
    *, user: User, device: TOTPDevice, device_name: str = ""
) -> TOTPDevice:
    # 先锁定该用户名下所有 TOTP 设备，再切换主设备，避免两个后台管理员并发重置时留下多个已确认密钥。
    devices = TOTPDevice.objects.select_for_update().filter(user=user).order_by("id")
    device = devices.filter(pk=device.pk, confirmed=False).first()
    if device is None:
        raise ValueError("待激活的两步验证设备不存在或已确认")

    if device_name:
        device.name = device_name
    device.confirmed = True
    device.save(update_fields=["name", "confirmed"])
    # 新密钥确认成功后再清理旧设备，确保用户不会在确认前失去现有登录能力。
    devices.exclude(pk=device.pk).delete()
    return device


def get_totp_secret(*, device: TOTPDevice) -> str:
    # 向 Google Authenticator 等客户端展示 Base32 密钥，方便扫码失败时手动录入。
    return b32encode(device.bin_key).decode()


def build_totp_qr_data_url(*, config_url: str) -> str:
    # 绑定页直接输出本地 SVG 二维码，避免依赖外部二维码服务导致后台认证链路多一个外部单点。
    qr = QRCode(
        version=None,
        error_correction=ERROR_CORRECT_M,
        box_size=8,
        border=2,
    )
    qr.add_data(config_url)
    qr.make(fit=True)
    image = qr.make_image(image_factory=SvgPathImage)
    buffer = BytesIO()
    image.save(buffer)
    encoded_svg = b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded_svg}"


def complete_admin_otp_login(request, *, user: User, device: TOTPDevice):
    # OTP 成功后才真正建立 Django 会话，并把已验证设备写入 session。
    auth_login(request, user, backend=settings.AUTHENTICATION_BACKENDS[0])
    otp_login(request, device)
    request.session.cycle_key()
    next_path = get_pending_admin_next_path(request)
    clear_pending_admin_otp(request)
    # 高风险动作会基于这次 OTP 完成时间做新鲜度判断，避免长期后台会话直接审批资金。
    request.session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = timezone.now().isoformat()
    return redirect(next_path)


def refresh_admin_otp_verification(request, *, user: User, device: TOTPDevice) -> None:
    # 已登录后台的敏感动作二次验证只需刷新 OTP 时效，不应打断当前会话或重走登录重定向。
    otp_login(request, device)
    request.session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = timezone.now().isoformat()
    clear_pending_admin_otp(request)


def downgrade_unverified_admin_session(request, *, user: User):
    # 若存在仅密码登录但未完成 OTP 的后台会话，先降级为 pending 状态，再跳转到 OTP 页面补验证。
    next_path = request.get_full_path()
    auth_logout(request)
    set_pending_admin_otp(request, user=user, next_path=next_path)
    if get_primary_totp_device(user=user, confirmed=True) is not None:
        return redirect(reverse("users:otp_verify"))
    return redirect(reverse("users:otp_setup"))
