import ipaddress
import json
import socket
import time
from datetime import timedelta
from urllib.parse import urlsplit

import environ
import httpx
from celery import shared_task
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from common.consts import APPID_HEADER
from common.consts import NONCE_HEADER
from common.consts import SIGNATURE_HEADER
from common.consts import TIMESTAMP_HEADER
from common.crypto import calc_hmac
from common.decorators import singleton_task
from core.runtime_settings import get_webhook_delivery_breaker_threshold
from core.runtime_settings import get_webhook_delivery_max_backoff_seconds
from core.runtime_settings import get_webhook_delivery_max_retries
from projects.models import Project
from webhooks.models import DeliveryAttempt
from webhooks.models import WebhookEvent

EVENT_ATTEMPT_TIMEOUT = 10
DELIVERY_CLAIM_TIMEOUT = EVENT_ATTEMPT_TIMEOUT + 5
# 商户回执通常只有 "ok"/"success" 等几字节。设置 64KB 上限既能兼容偶发的 HTML
# 错误页等场景，又能挡掉恶意商户回包放大 celery worker 内存。
MAX_RESPONSE_BYTES = 64 * 1024

# 出口代理配置（可选）：设置后 webhook 请求通过代理转发，隐藏服务器真实 IP
# XCASH_EGRESS_PROXY      — 代理转发地址（不设则直连商户 webhook URL）
# XCASH_EGRESS_PROXY_KEY  — 代理鉴权密钥
_egress_proxy_url: str | None = environ.Env().str("XCASH_EGRESS_PROXY", default=None)
_egress_proxy_key: str = environ.Env().str("XCASH_EGRESS_PROXY_KEY", default="")


def _is_safe_delivery_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return False

    if parsed.scheme != "https" or not parsed.hostname:
        return False

    hostname = parsed.hostname.strip().lower().rstrip(".")
    if hostname == "localhost" or hostname.endswith(".localhost"):
        return False

    try:
        addresses = [ipaddress.ip_address(hostname)]
    except ValueError:
        try:
            addresses = [
                ipaddress.ip_address(item[4][0])
                for item in socket.getaddrinfo(
                    hostname,
                    parsed.port or 443,
                    type=socket.SOCK_STREAM,
                )
            ]
        except OSError:
            return False

    return bool(addresses) and all(ip.is_global for ip in addresses)


def next_backoff(try_number: int) -> int:
    # Webhook 重试节奏允许通过平台参数中心调节，但仍保持指数退避，避免失败时瞬时洪泛商户端。
    return min(2 ** (try_number + 1), get_webhook_delivery_max_backoff_seconds())


def _claim_event_for_delivery(event_pk) -> bool:
    now = timezone.now()
    claimed = (
        WebhookEvent.objects.filter(
            pk=event_pk,
            status=WebhookEvent.Status.PENDING,
        )
        .filter(
            Q(schedule_locked_until__isnull=True)
            | Q(schedule_locked_until__lte=now)
        )
        .filter(
            Q(delivery_locked_until__isnull=True)
            | Q(delivery_locked_until__lte=now)
        )
        .update(
            delivery_locked_until=now + timedelta(seconds=DELIVERY_CLAIM_TIMEOUT),
        )
    )
    return claimed == 1


def _build_delivery_headers(project, event, body_str: str, timestamp: str) -> dict:
    """组装 Webhook 请求头，包含 HMAC 签名信息。"""
    nonce = event.nonce
    return {
        "Content-Type": "application/json",
        APPID_HEADER: project.appid,
        NONCE_HEADER: nonce,
        TIMESTAMP_HEADER: timestamp,
        SIGNATURE_HEADER: calc_hmac(
            message=f"{nonce}{timestamp}{body_str}",
            key=project.hmac_key,
        ),
    }


def _execute_http_delivery(
    *,
    request_url: str,
    method: str = "POST",
    headers: dict,
    body_str: str = "",
    params: dict | None = None,
    expected_response_body: str = "ok",
) -> tuple[bool, int | None, dict | None, str, str, int]:
    """
    向目标地址发送 Webhook 请求，返回
    (ok, status_code, resp_headers, resp_text, err_text, duration_ms)。
    不抛异常，所有错误均通过返回值传递。
    """
    ok = False
    status_code = None
    resp_headers = None
    resp_text = ""
    err_text = ""

    start = time.perf_counter()
    try:
        with httpx.Client(timeout=5) as client:
            with client.stream(
                method,
                request_url,
                headers=headers,
                params=params,
                content=body_str if method != "GET" else None,
            ) as resp:
                status_code = resp.status_code
                resp_headers = dict(resp.headers)
                # 流式读取并在累计达到 MAX_RESPONSE_BYTES 时截断，避免恶意/异常
                # 商户回执（如 100MB HTML 错误页）撑爆 worker 内存。
                buf = bytearray()
                for chunk in resp.iter_bytes():
                    if len(buf) + len(chunk) > MAX_RESPONSE_BYTES:
                        buf.extend(chunk[: MAX_RESPONSE_BYTES - len(buf)])
                        break
                    buf.extend(chunk)
                resp_text = bytes(buf).decode("utf-8", errors="replace")
            # 商户的 PHP/Java 框架 echo "success" 时常带回车 / BOM / 前后空白，
            # 严格相等会把这些合法响应判为失败、触发重试与误熔断；strip 后精确匹配
            # 兼顾兼容性与匹配严格度（仍区分大小写、不允许中间夹杂内容）。
            ok = status_code == 200 and resp_text.strip() == expected_response_body
    except httpx.RequestError as e:
        err_text = f"{e.__class__.__name__}: {e}"
    except Exception as e:
        err_text = f"UnexpectedError: {type(e).__name__}"
    duration_ms = int((time.perf_counter() - start) * 1000)

    return ok, status_code, resp_headers, resp_text, err_text, duration_ms


@shared_task
@singleton_task(timeout=15, use_params=False)
def schedule_events(batch_size=128):
    qs = (
        WebhookEvent.objects.filter(status=WebhookEvent.Status.PENDING)
        .filter(
            Q(schedule_locked_until__isnull=True)
            | Q(schedule_locked_until__lte=timezone.now())
        )
        .filter(
            Q(delivery_locked_until__isnull=True)
            | Q(delivery_locked_until__lte=timezone.now())
        )
        .order_by("created_at")[:batch_size]
    )

    for ev in qs:
        deliver_event.delay(ev.pk)


@shared_task(
    acks_late=True,
    max_retries=0,
    soft_time_limit=8,  # httpx timeout=5s，额外留 3s 给 DB 写入，避免 SoftTimeLimitExceeded 打断事务
    time_limit=EVENT_ATTEMPT_TIMEOUT,
)
@singleton_task(timeout=EVENT_ATTEMPT_TIMEOUT + 2, use_params=True)
def deliver_event(event_pk):
    if not _claim_event_for_delivery(event_pk):
        return

    event = WebhookEvent.objects.select_related("project").get(pk=event_pk)

    project = event.project
    target_url = event.delivery_url or project.webhook

    # 同时检查熔断开关和投递地址是否已配置
    if not project.webhook_open or not target_url:
        reason = (
            "Endpoint not open."
            if not project.webhook_open
            else "Webhook URL not configured."
        )
        WebhookEvent.objects.filter(pk=event_pk).update(
            status=WebhookEvent.Status.FAILED,
            last_error=reason,
            delivery_locked_until=None,
        )
        return

    if not _is_safe_delivery_url(target_url):
        WebhookEvent.objects.filter(pk=event_pk).update(
            status=WebhookEvent.Status.FAILED,
            last_error="Unsafe webhook URL.",
            delivery_locked_until=None,
        )
        return

    try_number = event.attempts.count() + 1
    body_str = json.dumps(event.payload)
    timestamp = str(int(timezone.now().timestamp()))

    if event.delivery_method == WebhookEvent.DeliveryMethod.GET_QUERY:
        # GET 请求由商户端用自有签名（如 EPay MD5）校验 query string，不附带 HMAC 头
        headers = {}
        http_method = "GET"
        query_params = event.payload
        # GET 实际不发送 body，attempt 记录留空避免误导
        body_str_for_attempt = ""
    else:
        headers = _build_delivery_headers(project, event, body_str, timestamp)
        http_method = "POST"
        query_params = None
        body_str_for_attempt = body_str

    # 出口代理模式：所有 delivery_method 一律走代理。商户配置的 notify_url 由 xcash worker
    # 直连会暴露真实 IP，且无法防御内网/元数据端点的 SSRF 攻击。代理地址未配置时退回直连。
    if _egress_proxy_url:
        request_url = _egress_proxy_url
        headers["CF-Worker-Destination"] = target_url
        headers["CF-Worker-Key"] = _egress_proxy_key
    else:
        request_url = target_url

    ok, status_code, resp_headers, resp_text, err_text, duration_ms = (
        _execute_http_delivery(
            request_url=request_url,
            method=http_method,
            headers=headers,
            body_str=body_str,
            params=query_params,
            expected_response_body=event.expected_response_body,
        )
    )

    # 记录本次 attempt + 更新事件状态（事务保护）
    # 去掉代理鉴权头，避免写入 attempt 日志泄漏密钥
    headers.pop("CF-Worker-Key", None)
    headers.pop("CF-Worker-Destination", None)
    with transaction.atomic():
        DeliveryAttempt.objects.create(
            event=event,
            try_number=try_number,
            request_headers=headers,
            request_body=body_str_for_attempt,
            response_status=status_code,
            response_headers=resp_headers,
            response_body=resp_text[:1024],
            duration_ms=duration_ms,
            ok=ok,
            error=err_text[:1024],
        )

        if ok:
            # 投递成功：标记事件完成，重置熔断计数
            WebhookEvent.objects.filter(pk=event_pk).update(
                status=WebhookEvent.Status.SUCCEEDED,
                last_error="",
                delivered_at=timezone.now(),
                delivery_locked_until=None,
            )
            # 使用 select_for_update 与失败路径保持一致，防止并发投递时成功路径的无锁重置覆盖失败路径的计数累加
            locked_project = (
                Project.objects.select_for_update()
                .only("failed_count", "webhook_open")
                .get(pk=project.pk)
            )
            Project.objects.filter(pk=locked_project.pk).update(
                webhook_open=True, failed_count=0
            )
            return

        # 失败：累加失败计数，超过阈值则触发熔断
        locked_project = (
            Project.objects.select_for_update()
            .only("failed_count", "webhook_open")
            .get(pk=project.pk)
        )
        locked_project.failed_count += 1
        if locked_project.failed_count >= get_webhook_delivery_breaker_threshold():
            locked_project.webhook_open = False
        Project.objects.filter(pk=locked_project.pk).update(
            failed_count=locked_project.failed_count,
            webhook_open=locked_project.webhook_open,
        )

        # 仅 5xx / 网络错误可重试；2xx(非200)、3xx、4xx 均视为不可恢复
        retryable = (
            (status_code is None or status_code >= 500)
            and try_number < get_webhook_delivery_max_retries()
            and locked_project.webhook_open
        )
        error_msg = err_text or f"status={status_code}"
        if retryable:
            WebhookEvent.objects.filter(pk=event_pk).update(
                schedule_locked_until=timezone.now()
                + timedelta(seconds=next_backoff(try_number)),
                last_error=error_msg,
                delivery_locked_until=None,
            )
        else:
            WebhookEvent.objects.filter(pk=event_pk).update(
                status=WebhookEvent.Status.FAILED,
                last_error=error_msg,
                schedule_locked_until=None,
                delivery_locked_until=None,
            )
