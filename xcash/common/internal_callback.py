from __future__ import annotations

import httpx
import structlog
from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.utils import timezone

logger = structlog.get_logger()

# SaaS 侧接收回调的固定路径；SAAS_CALLBACK_URL 只配 scheme+host，路径由这里拼
_SAAS_CALLBACK_PATH = "/callbacks/xcash"

# 指数退避序列（秒）：第 N 次重试前等待 _RETRY_BACKOFF[N]，超出长度使用最后一个值
# 覆盖窗口：前 5 次共 ~46 分钟，之后每小时一次，配合 max_retries=20 总计约 15 小时
_RETRY_BACKOFF = (8, 60, 300, 600, 1800, 3600)


def _retry_countdown(retries: int) -> int:
    return _RETRY_BACKOFF[min(retries, len(_RETRY_BACKOFF) - 1)]


def send_internal_callback(
    *,
    event: str,
    appid: str,
    sys_no: str,
    worth: str,
    currency: str,
) -> None:
    """
    在事务提交后异步发送内部回调给 SaaS。
    IS_SAAS=False 视为未对接 SaaS，直接跳过（没 token 也过不了 SaaS 的鉴权）。
    """
    if not settings.IS_SAAS:
        return

    transaction.on_commit(
        lambda: _deliver_internal_callback.delay(
            event=event,
            appid=appid,
            sys_no=sys_no,
            worth=worth,
            currency=currency,
        )
    )


@shared_task(
    bind=True,
    ignore_result=True,
    max_retries=20,
    soft_time_limit=10,
    time_limit=15,
    acks_late=True,
    reject_on_worker_lost=True,
)
def _deliver_internal_callback(
    self,
    *,
    event: str,
    appid: str,
    sys_no: str,
    worth: str,
    currency: str,
) -> None:
    """Celery task：向 SaaS 发送内部回调 POST 请求。"""
    if not settings.IS_SAAS:
        return
    url = f"{settings.SAAS_CALLBACK_URL.rstrip('/')}{_SAAS_CALLBACK_PATH}"

    payload = {
        "event": event,
        "appid": appid,
        "sys_no": sys_no,
        "worth": worth,
        "currency": currency,
        "timestamp": timezone.now().isoformat(),
    }

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {settings.INTERNAL_API_TOKEN}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "internal_callback_failed",
            url=url,
            callback_event=event,
            appid=appid,
            sys_no=sys_no,
            error=str(exc),
            retry=self.request.retries,
        )
        # DEBUG 环境不做指数退避重试，只通知一次
        if settings.DEBUG:
            return
        raise self.retry(countdown=_retry_countdown(self.request.retries), exc=exc)
