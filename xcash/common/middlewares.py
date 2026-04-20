import logging
import os
import re
from dataclasses import dataclass

from django.conf import settings
from django.http import HttpRequest
from django.utils import timezone
from django_redis import get_redis_connection

logger = logging.getLogger(__name__)

from common.consts import APPID_HEADER
from common.consts import NONCE_HEADER
from common.consts import SIGNATURE_HEADER
from common.consts import TIMESTAMP_HEADER
from common.crypto import verify_hmac
from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.utils.security import is_ip_in_whitelist
from projects.models import Project


class AdminSessionTimeoutMiddleware:
    """根据 PlatformSettings 动态设置后台 session 超时时间。"""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith("/admin/") and request.user.is_authenticated:
            from core.runtime_settings import get_admin_session_timeout_seconds

            request.session.set_expiry(get_admin_session_timeout_seconds())
        return self.get_response(request)


@dataclass
class XcashRequestContext:
    project: Project | None = None
    timestamp: int = 0
    payload: str | None = None
    nonce: str = ""
    signature: str = ""
    client_ip: str | None = None


class XcashMiddleware:
    CONTEXT_ATTR = "_xcash_ctx"
    # 修复：NONCE_TTL_SECONDS 必须 >= TIMESTAMP_TOLERANCE_SECONDS
    # 否则 nonce 在时间窗口内过期后可被重放
    NONCE_TTL_SECONDS = 300
    TIMESTAMP_TOLERANCE_SECONDS = 300  # 5 分钟，兼顾网络延迟与安全

    def __init__(self, get_response):
        self.get_response = get_response
        self._nonce_connection = None

    def _get_context(self, request: HttpRequest) -> XcashRequestContext:
        ctx: XcashRequestContext | None = getattr(request, self.CONTEXT_ATTR, None)
        if ctx is None:
            ctx = XcashRequestContext(
                nonce=request.headers.get(NONCE_HEADER, ""),
                timestamp=self._parse_timestamp(request),
                signature=request.headers.get(SIGNATURE_HEADER, ""),
                payload=self._decode_payload(request),
                client_ip=self._client_ip(request),
                project=Project.retrieve(request.headers.get(APPID_HEADER, "")),
            )
            setattr(request, self.CONTEXT_ATTR, ctx)
        return ctx

    def project(self, request: HttpRequest) -> Project | None:
        return self._get_context(request).project

    def client_ip(self, request: HttpRequest) -> str:
        return self._get_context(request).client_ip

    def timestamp(self, request: HttpRequest) -> int:
        return self._get_context(request).timestamp

    def message(self, request: HttpRequest) -> str:
        ctx = self._get_context(request)
        return ctx.nonce + str(ctx.timestamp) + ctx.payload

    def signature(self, request: HttpRequest) -> str:
        return self._get_context(request).signature

    @staticmethod
    def _is_api_request(request: HttpRequest) -> bool:
        return request.path.startswith("/v1/")

    @staticmethod
    def _is_signer_request(request: HttpRequest) -> bool:
        return request.path.startswith("/v1/sign/") or request.path.startswith(
            "/v1/wallets/"
        )

    @staticmethod
    def _is_no_signature_request(request: HttpRequest) -> bool:
        post_patterns = [
            re.compile(r"^/v1/invoice/[^/]+/select-method$"),
        ]
        get_patterns = [re.compile(r"^/v1/invoice/[^/]+$")]

        if request.method == "POST":
            return any(pattern.match(request.path) for pattern in post_patterns)
        if request.method == "GET":
            return any(pattern.match(request.path) for pattern in get_patterns)
        return False

    def _requires_project(self, request: HttpRequest) -> bool:
        if self._is_signer_request(request):
            # signer API 使用独立共享密钥鉴权，不走商户 AppID/HMAC 链路。
            return False
        return self._is_api_request(request) and not self._is_no_signature_request(
            request
        )

    def _requires_signature(self, request: HttpRequest) -> bool:
        return self._requires_project(request)

    @staticmethod
    def _parse_timestamp(request: HttpRequest) -> int:
        timestamp = request.headers.get(TIMESTAMP_HEADER, "0")
        if isinstance(timestamp, str) and timestamp.isdigit():
            return int(timestamp)
        return 0

    @staticmethod
    def _decode_payload(request: HttpRequest) -> str:
        return request.body.decode("utf-8")

    @staticmethod
    def _client_ip(request: HttpRequest) -> str | None:
        # 只有 TCP 对端本身属于受信代理时，才接受其转发的 X-Real-IP；
        # 否则一律退回 REMOTE_ADDR，避免源站直连时被伪造请求头绕过白名单。
        remote_addr = request.META.get("REMOTE_ADDR")
        x_real_ip = request.headers.get("x-real-ip")
        if (
            x_real_ip
            and remote_addr
            and is_ip_in_whitelist(settings.TRUSTED_PROXY_IPS, remote_addr)
        ):
            return x_real_ip.strip()
        # 无受信代理参与时，直接使用当前 TCP 连接来源 IP。
        return remote_addr

    def _nonce_cache(self):
        if self._nonce_connection is None:
            self._nonce_connection = get_redis_connection("default")
        return self._nonce_connection

    @staticmethod
    def _nonce_cache_key(project: Project, nonce: str) -> str:
        return f"xcash:nonce:{project.appid}:{nonce}"

    def register_nonce(self, request: HttpRequest, project: Project) -> bool | None:
        ctx = self._get_context(request)
        if not ctx.nonce:
            return None

        cache = self._nonce_cache()
        key = self._nonce_cache_key(project, ctx.nonce)

        try:
            # nx=True：仅当 key 不存在时写入并返回 True
            # 若 key 已存在（nonce 已被使用）则返回 None/False → 触发 REPLAY_ATTACK
            # TTL 与时间戳容限一致（NONCE_TTL_SECONDS == TIMESTAMP_TOLERANCE_SECONDS）
            # 保证在有效时间窗口内 nonce 始终存在，防止窗口内重放
            stored = cache.set(
                key,
                1,
                ex=self.NONCE_TTL_SECONDS,
                nx=True,
            )
        except Exception:  # noqa
            # 修复：Redis 不可用时选择拒绝请求（fail-safe），防止通过触发异常绕过防重放
            logger.exception(
                "Nonce 存储失败，为防重放攻击拒绝请求，appid=%s", project.appid
            )
            return False

        return bool(stored)


class ProjectConfigMiddleware(XcashMiddleware):
    def __call__(self, request: HttpRequest):
        if not self._requires_project(request):
            return self.get_response(request)

        project = self.project(request)
        if not project:
            return APIError(ErrorCode.INVALID_APPID).to_response()

        if not settings.DEBUG:
            request_timestamp = self.timestamp(request)
            now_ts = int(timezone.now().timestamp())
            # 修复：容限从 60s 改为 TIMESTAMP_TOLERANCE_SECONDS（300s）
            # 与 NONCE_TTL_SECONDS 保持一致，防止 nonce 在时间窗口内过期后被重放
            if (
                not request_timestamp
                or abs(request_timestamp - now_ts) > self.TIMESTAMP_TOLERANCE_SECONDS
            ):
                return APIError(ErrorCode.EXPIRED).to_response()

        ready, errors = project.is_ready
        if not ready:
            return APIError(
                ErrorCode.PROJECT_NOT_READY, detail="; ".join(str(e) for e in errors)
            ).to_response()

        return self.get_response(request)


class IPWhiteListMiddleware(XcashMiddleware):
    def __call__(self, request: HttpRequest):
        if not self._requires_project(request):
            return self.get_response(request)

        client_ip = self.client_ip(request)
        if not client_ip or not is_ip_in_whitelist(
            whitelist=self.project(request).ip_white_list,
            ip=client_ip,
        ):
            return APIError(ErrorCode.IP_FORBIDDEN).to_response()

        return self.get_response(request)


class HMACMiddleware(XcashMiddleware):
    def __call__(self, request: HttpRequest):
        if not self._requires_signature(request):
            return self.get_response(request)

        project = self.project(request)

        if not self._is_valid_hmac(request, project):
            return APIError(ErrorCode.SIGNATURE_ERROR).to_response()

        nonce_status = self.register_nonce(request, project)
        if nonce_status is None:
            return APIError(ErrorCode.PARAMETER_ERROR).to_response()
        if not nonce_status:
            return APIError(ErrorCode.REPLAY_ATTACK).to_response()

        return self.get_response(request)

    def _is_valid_hmac(self, request: HttpRequest, project: Project) -> bool:
        # 修复：原实现允许客户端通过请求头 "Dev: true" 完全绕过签名验证，高危漏洞
        # 改为：仅在 DEBUG 模式且服务端显式设置 ALLOW_DEV_BYPASS=1 环境变量时跳过
        # 客户端无法控制服务端环境变量，彻底消除此攻击面
        if settings.DEBUG and os.environ.get("ALLOW_DEV_BYPASS") == "1":
            logger.warning(
                "HMAC 验证已跳过（DEBUG + ALLOW_DEV_BYPASS=1），请勿在生产环境使用"
            )
            return True

        return verify_hmac(
            message=self.message(request),
            key=project.hmac_key,
            signature=self.signature(request),
        )


class ExceptionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            return self.get_response(request)
        except APIError as e:
            return e.to_response()
