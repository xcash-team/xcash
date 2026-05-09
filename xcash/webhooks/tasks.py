import json
import time
from datetime import timedelta

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

# 出口代理配置（可选）：设置后 webhook 请求通过代理转发，隐藏服务器真实 IP
# XCASH_EGRESS_PROXY      — 代理转发地址（不设则直连商户 webhook URL）
# XCASH_EGRESS_PROXY_KEY  — 代理鉴权密钥
_egress_proxy_url: str | None = environ.Env().str("XCASH_EGRESS_PROXY", default=None)
_egress_proxy_key: str = environ.Env().str("XCASH_EGRESS_PROXY_KEY", default="")


def next_backoff(try_number: int) -> int:
    # Webhook 重试节奏允许通过平台参数中心调节，但仍保持指数退避，避免失败时瞬时洪泛商户端。
    return min(2 ** (try_number + 1), get_webhook_delivery_max_backoff_seconds())


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
            if method == "GET":
                resp = client.get(request_url, headers=headers, params=params)
            else:
                resp = client.post(request_url, headers=headers, content=body_str)
            status_code = resp.status_code
            resp_headers = dict(resp.headers)
            resp_text = resp.text
            ok = status_code == 200 and resp_text == expected_response_body
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
    event = WebhookEvent.objects.select_related("project").get(pk=event_pk)

    # 幂等保护：非 PENDING 状态的事件跳过，防止并发或手动触发重复处理
    if not event.is_pending:
        return

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
            status=WebhookEvent.Status.FAILED, last_error=reason
        )
        return

    try_number = event.attempts.count() + 1
    body_str = json.dumps(event.payload)
    timestamp = str(int(timezone.now().timestamp()))

    if event.delivery_method == WebhookEvent.DeliveryMethod.GET_QUERY:
        headers = {}
        http_method = "GET"
        query_params = event.payload
        request_url = target_url
    else:
        headers = _build_delivery_headers(project, event, body_str, timestamp)
        http_method = "POST"
        query_params = None
        # 出口代理模式：将真实目标 URL 放入代理 header，请求发往代理地址；直连模式直接请求商户 URL
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
            request_body=body_str,
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
            )
        else:
            WebhookEvent.objects.filter(pk=event_pk).update(
                status=WebhookEvent.Status.FAILED,
                last_error=error_msg,
                schedule_locked_until=None,
            )
