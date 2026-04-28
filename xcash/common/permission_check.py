"""SaaS 模式下，对锁定操作（deposit/withdrawal）做权限校验。

设计原则（高可用优先，availability over consistency）：
- INTERNAL_API_TOKEN 为空视为未对接 SaaS（自托管），直接放行
- 缓存值带 `_fetched_at` 时间戳，永不过期；判定完全基于缓存
- 命中缓存且 fetched_at 落后 > 60s：派发异步刷新任务，本次仍按旧缓存判定
- 未命中缓存：默认放行，并派发异步刷新任务（让下次有数据可用）
- 异步刷新失败只 log，不破坏旧缓存；同一 appid 60s 内只派发一次（去重锁）

这样设计的目的：SaaS 暂时不可用不会阻塞 xcash 主链路；权限变更最多延迟 60s 生效。
"""

from __future__ import annotations

import time

import httpx
import structlog
from celery import shared_task
from django.conf import settings
from django.core.cache import cache

from common.error_codes import ErrorCode
from common.exceptions import APIError

logger = structlog.get_logger()

# SaaS 侧 endpoint 路径；SAAS_CALLBACK_URL 只配 scheme+host
_SAAS_PERMISSION_PATH = "/callbacks/xcash/permission"

# fetched_at 落后超过此秒数即派发异步刷新；同时也是去重锁 TTL
REFRESH_AFTER = 60

_TIMEOUT = httpx.Timeout(connect=2.0, read=3.0, write=3.0, pool=5.0)


def _cache_key(appid: str) -> str:
    return f"saas:permission:{appid}"


def _refresh_lock_key(appid: str) -> str:
    return f"saas:permission:refresh_lock:{appid}"


def _schedule_refresh(appid: str) -> None:
    """派发异步刷新任务；同一 appid 在 REFRESH_AFTER 秒内只派发一次。"""
    # cache.add 是原子操作：仅当 key 不存在时写入并返回 True，避免并发请求重复派发
    if cache.add(_refresh_lock_key(appid), "1", REFRESH_AFTER):
        _refresh_saas_permission.delay(appid=appid)


def check_saas_permission(*, appid: str, action: str) -> None:
    """对锁定操作做权限校验。

    Args:
        appid: xcash Project appid
        action: 'deposit' / 'withdrawal' 等，对应 SaaS 返回的 enable_<action>

    Raises:
        APIError: 该 tier 未开放该功能 / 用户已 frozen / appid 缺失

    Returns:
        None — 不抛异常即放行
    """
    # 自托管模式：未对接 SaaS，所有功能默认开放
    if not settings.INTERNAL_API_TOKEN:
        return

    # 防御：appid 缺失（header 没传 / 中间件未过滤）→ 直接 INVALID_APPID
    if not appid:
        raise APIError(ErrorCode.INVALID_APPID)

    perm = cache.get(_cache_key(appid))

    if perm is None:
        # 冷启动：默认放行，但派发刷新任务，让下次有缓存可用
        _schedule_refresh(appid)
        return

    # 命中缓存：必要时派发后台刷新（不影响本次判定）
    fetched_at = perm.get("_fetched_at", 0)
    if time.time() - fetched_at > REFRESH_AFTER:
        _schedule_refresh(appid)

    if perm.get("frozen"):
        raise APIError(ErrorCode.ACCOUNT_FROZEN)

    feature_key = f"enable_{action}"
    if not perm.get(feature_key, False):
        raise APIError(ErrorCode.FEATURE_NOT_ENABLED, detail=action)


@shared_task(
    ignore_result=True,
    soft_time_limit=8,
    time_limit=12,
)
def _refresh_saas_permission(*, appid: str) -> None:
    """Celery task：从 SaaS 拉取最新 permission 并覆写缓存。

    任务失败只 log，不重试也不清缓存——下一次主调用发现 stale 会再次派发。
    """
    if not settings.INTERNAL_API_TOKEN:
        return

    try:
        perm = _fetch_from_saas(appid)
    except httpx.HTTPError as exc:
        # SaaS 暂时不可达：保留旧缓存继续兜底，下次主调用还会派发新任务
        logger.warning("saas_permission_refresh_failed", appid=appid, error=str(exc))
        return

    perm["_fetched_at"] = time.time()
    cache.set(_cache_key(appid), perm, None)


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
