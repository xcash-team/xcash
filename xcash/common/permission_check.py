"""SaaS 模式下，对锁定操作（deposit/withdrawal）做权限校验。

设计参考：xcash-saas spec §5.3
- INTERNAL_API_TOKEN 为空视为未对接 SaaS（自托管），直接放行
- 缓存正常结果 60 秒，stale 副本 300 秒兜底
- SaaS 不可达且无 stale 缓存时 fail-closed
"""

from __future__ import annotations

import httpx
import structlog
from django.conf import settings
from django.core.cache import cache

from common.error_codes import ErrorCode
from common.exceptions import APIError

logger = structlog.get_logger()

# SaaS 侧 endpoint 路径；SAAS_CALLBACK_URL 只配 scheme+host
_SAAS_PERMISSION_PATH = "/callbacks/xcash/permission"

CACHE_TTL = 60          # 正常缓存 1 分钟
STALE_TTL = 300         # SaaS 不可达时兜底用的过期缓存 5 分钟

_TIMEOUT = httpx.Timeout(connect=2.0, read=3.0, write=3.0, pool=5.0)


def check_saas_permission(*, appid: str, action: str) -> None:
    """对锁定操作做权限校验。

    Args:
        appid: xcash Project appid
        action: 'deposit' / 'withdrawal' 等，对应 SaaS 返回的 enable_<action>

    Raises:
        APIError: 该 tier 未开放该功能 / 用户已 frozen / SaaS 不可达且无缓存

    Returns:
        None — 不抛异常即放行
    """
    # 自托管模式：未对接 SaaS，所有功能默认开放
    if not settings.INTERNAL_API_TOKEN:
        return

    # 防御：appid 缺失（header 没传 / 中间件未过滤）→ 直接 INVALID_APPID
    if not appid:
        raise APIError(ErrorCode.INVALID_APPID)

    cache_key = f"saas:permission:{appid}"
    perm = cache.get(cache_key)

    if perm is None:
        try:
            perm = _fetch_from_saas(appid)
            # 先写 stale 缓存，再写主缓存（防崩溃中间状态）
            cache.set(f"{cache_key}:stale", perm, STALE_TTL)
            cache.set(cache_key, perm, CACHE_TTL)
        except httpx.HTTPError as exc:
            # SaaS 不可达 → 用 stale 缓存兜底
            perm = cache.get(f"{cache_key}:stale")
            if perm is None:
                logger.warning(
                    "saas_permission_unavailable",
                    appid=appid, action=action, error=str(exc),
                )
                raise APIError(ErrorCode.PERMISSION_SERVICE_UNAVAILABLE)
            logger.info("saas_permission_stale_used", appid=appid)

    if perm.get("frozen"):
        raise APIError(ErrorCode.ACCOUNT_FROZEN)

    feature_key = f"enable_{action}"
    if not perm.get(feature_key, False):
        raise APIError(ErrorCode.FEATURE_NOT_ENABLED, detail=action)


def _fetch_from_saas(appid: str) -> dict:
    url = f"{settings.SAAS_CALLBACK_URL.rstrip('/')}{_SAAS_PERMISSION_PATH}"
    with httpx.Client(timeout=_TIMEOUT) as client:
        resp = client.post(
            url,
            json={"appid": appid},
            headers={
                "Authorization": f"Bearer {settings.INTERNAL_API_TOKEN}",
                "Content-Type": "application/json",
            },
        )
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as exc:
            # 非 JSON 响应（502 HTML 网关页等）→ 包成 HTTPError 让调用方按"SaaS 不可达"处理
            raise httpx.HTTPError(f"non-JSON response: {exc}") from exc
        if not isinstance(data, dict):
            raise httpx.HTTPError(f"unexpected response type: {type(data).__name__}")
        return data
