from __future__ import annotations

import httpx
import structlog
from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.utils import timezone

logger = structlog.get_logger()


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
    INTERNAL_CALLBACK_URL 为空则跳过。
    """
    if not settings.INTERNAL_CALLBACK_URL:
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
    max_retries=3,
    soft_time_limit=10,
    time_limit=15,
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
    url = settings.INTERNAL_CALLBACK_URL
    if not url:
        return

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
            event=event,
            appid=appid,
            sys_no=sys_no,
            error=str(exc),
            retry=self.request.retries,
        )
        raise self.retry(countdown=2 ** (self.request.retries + 1), exc=exc)
