"""SaaS 模式下，对锁定操作（deposit/withdrawal）和 Invoice 白名单做权限校验。

设计原则（高可用优先，availability over consistency）：
- IS_SAAS=False 视为未对接 SaaS（自托管），直接放行
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
_DEPOSIT_WITHDRAWAL_ACTIONS = {"deposit", "withdrawal"}


def _cache_key(appid: str) -> str:
    return f"saas:permission:{appid}"


def _refresh_lock_key(appid: str) -> str:
    return f"saas:permission:refresh_lock:{appid}"


def _normalize_whitelist(values):
    """SaaS 用 None/[] 表达未限制；非空列表才作为白名单。"""
    return values or None


def _schedule_refresh(appid: str) -> None:
    """派发异步刷新任务；同一 appid 在 REFRESH_AFTER 秒内只派发一次。"""
    # cache.add 是原子操作：仅当 key 不存在时写入并返回 True，避免并发请求重复派发
    if cache.add(_refresh_lock_key(appid), "1", REFRESH_AFTER):
        _refresh_saas_permission.delay(appid=appid)


def _read_saas_perm(appid: str) -> dict | None:
    """读取 SaaS 权限缓存，必要时触发后台刷新。

    Returns:
        None: 自托管、未对接 SaaS、或冷缓存（fail-open 场景）
        dict: 缓存中的权限数据
    """
    if not settings.IS_SAAS or not appid:
        return None

    perm = cache.get(_cache_key(appid))
    if perm is None:
        _schedule_refresh(appid)
        return None

    fetched_at = perm.get("_fetched_at", 0)
    if time.time() - fetched_at > REFRESH_AFTER:
        _schedule_refresh(appid)

    return perm


def check_saas_permission(
    *,
    appid: str,
    action: str,
    chain_code: str | None = None,
    crypto_symbol: str | None = None,
) -> None:
    """对锁定操作做权限校验。

    Args:
        appid: xcash Project appid
        action: 'deposit' / 'withdrawal' 会读取 SaaS 返回的
            enable_deposit_withdrawal；'invoice' 不读取该功能锁，只用于账号冻结
            和链币白名单校验。
        chain_code: 可选，Chain.code。传入时会按 SaaS 返回的 allowed_chain_codes 校验
        crypto_symbol: 可选，Crypto.symbol。传入时会按 SaaS 返回的 allowed_crypto_symbols 校验

    Raises:
        APIError: 该 tier 未开放该功能 / 用户已 frozen / appid 缺失

    Returns:
        None — 不抛异常即放行
    """
    # 自托管模式：未对接 SaaS，所有功能默认开放
    if not settings.IS_SAAS:
        return

    # 防御：appid 缺失（header 没传 / 中间件未过滤）→ 直接 INVALID_APPID
    if not appid:
        raise APIError(ErrorCode.INVALID_APPID)

    perm = _read_saas_perm(appid)
    if perm is None:
        return

    if perm.get("frozen"):
        raise APIError(ErrorCode.ACCOUNT_FROZEN)

    if (
        action in _DEPOSIT_WITHDRAWAL_ACTIONS
        and not perm.get("enable_deposit_withdrawal", False)
    ):
        raise APIError(ErrorCode.FEATURE_NOT_ENABLED, detail=action)

    allowed_chain_codes = _normalize_whitelist(perm.get("allowed_chain_codes"))
    if (
        chain_code is not None
        and allowed_chain_codes is not None
        and chain_code not in allowed_chain_codes
    ):
        raise APIError(ErrorCode.FEATURE_NOT_ENABLED, detail=chain_code)

    allowed_crypto_symbols = _normalize_whitelist(perm.get("allowed_crypto_symbols"))
    if (
        crypto_symbol is not None
        and allowed_crypto_symbols is not None
        and crypto_symbol not in allowed_crypto_symbols
    ):
        raise APIError(ErrorCode.FEATURE_NOT_ENABLED, detail=crypto_symbol)


def filter_saas_allowed_methods(
    *,
    appid: str,
    methods: dict[str, list[str]],
) -> dict[str, list[str]]:
    """按 SaaS 缓存的 Tier 链币白名单收敛可用支付方式。

    与 check_saas_permission 保持同样的可用性策略：自托管、冷缓存、旧缓存缺字段时
    fail-open；命中缓存且包含白名单时只返回交集。
    """
    if not settings.IS_SAAS or not appid:
        return {symbol: list(chain_codes) for symbol, chain_codes in methods.items()}

    perm = _read_saas_perm(appid)
    if perm is None:
        return {symbol: list(chain_codes) for symbol, chain_codes in methods.items()}

    if perm.get("frozen"):
        return {}

    allowed_chain_codes = _normalize_whitelist(perm.get("allowed_chain_codes"))
    allowed_crypto_symbols = _normalize_whitelist(perm.get("allowed_crypto_symbols"))
    allowed_crypto_set = (
        {str(symbol).upper() for symbol in allowed_crypto_symbols}
        if allowed_crypto_symbols is not None
        else None
    )

    filtered: dict[str, list[str]] = {}
    for symbol, chain_codes in methods.items():
        if allowed_crypto_set is not None and symbol.upper() not in allowed_crypto_set:
            continue
        available_chain_codes = [
            code
            for code in chain_codes
            if allowed_chain_codes is None or code in allowed_chain_codes
        ]
        if available_chain_codes:
            filtered[symbol] = available_chain_codes
    return filtered


@shared_task(
    ignore_result=True,
    soft_time_limit=8,
    time_limit=12,
)
def _refresh_saas_permission(*, appid: str) -> None:
    """Celery task：从 SaaS 拉取最新 permission 并覆写缓存。

    任务失败只 log，不重试也不清缓存——下一次主调用发现 stale 会再次派发。
    """
    if not settings.IS_SAAS:
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
