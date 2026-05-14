from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.conf import settings
from django.core.cache import cache

from core.models import PLATFORM_SETTINGS_CACHE_KEY
from core.models import PlatformSettings

_MISSING_PLATFORM_SETTINGS = "__missing_platform_settings__"


def get_platform_settings() -> PlatformSettings | None:
    # 平台参数读取频率高于写入频率，缓存单例记录可以避免每次请求都命中数据库。
    cached_value = cache.get(
        PLATFORM_SETTINGS_CACHE_KEY, default=_MISSING_PLATFORM_SETTINGS
    )
    if cached_value == _MISSING_PLATFORM_SETTINGS:
        platform_settings = PlatformSettings.objects.order_by("pk").first()
        cache.set(
            PLATFORM_SETTINGS_CACHE_KEY,
            platform_settings or _MISSING_PLATFORM_SETTINGS,
            timeout=None,
        )
        return platform_settings
    if cached_value is None:
        return None
    if cached_value != _MISSING_PLATFORM_SETTINGS:
        return cached_value
    return None


def get_admin_session_timeout_seconds() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.admin_session_timeout_minutes) * 60
    return 10 * 60


def get_admin_sensitive_action_otp_max_age_seconds() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.admin_sensitive_action_otp_max_age_seconds)
    return int(settings.ADMIN_SENSITIVE_ACTION_OTP_MAX_AGE_SECONDS)


def get_alerts_repeat_interval_minutes() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.alerts_repeat_interval_minutes)
    return int(settings.ALERTS_REPEAT_INTERVAL_MINUTES)


def get_open_native_scanner() -> bool:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return bool(platform_settings.open_native_scanner)
    return False


def get_webhook_delivery_breaker_threshold() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.webhook_delivery_breaker_threshold)
    return 30


def get_webhook_delivery_max_retries() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.webhook_delivery_max_retries)
    return 5


def get_webhook_delivery_max_backoff_seconds() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.webhook_delivery_max_backoff_seconds)
    return 120


def get_reviewing_withdrawal_timeout() -> timedelta:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return timedelta(minutes=platform_settings.reviewing_withdrawal_timeout_minutes)
    return timedelta(minutes=30)


def get_pending_withdrawal_timeout() -> timedelta:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return timedelta(minutes=platform_settings.pending_withdrawal_timeout_minutes)
    return timedelta(minutes=15)


def get_confirming_withdrawal_timeout() -> timedelta:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return timedelta(
            minutes=platform_settings.confirming_withdrawal_timeout_minutes
        )
    return timedelta(minutes=30)


def get_deposit_collection_timeout() -> timedelta:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return timedelta(minutes=platform_settings.deposit_collection_timeout_minutes)
    return timedelta(minutes=30)


def get_webhook_event_timeout() -> timedelta:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return timedelta(minutes=platform_settings.webhook_event_timeout_minutes)
    return timedelta(minutes=15)


def get_risk_marking_enabled() -> bool:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return bool(platform_settings.risk_marking_enabled)
    return False


def get_risk_marking_threshold_usd() -> Decimal:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return Decimal(platform_settings.risk_marking_threshold_usd)
    return Decimal("0")


def get_risk_marking_cache_seconds() -> int:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return int(platform_settings.risk_marking_cache_seconds)
    return 3600


def get_risk_marking_force_refresh_threshold_usd() -> Decimal:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return Decimal(platform_settings.risk_marking_force_refresh_threshold_usd)
    return Decimal("10000")


def get_quicknode_misttrack_endpoint_url() -> str:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return platform_settings.quicknode_misttrack_endpoint_url.strip()
    return ""


def get_misttrack_openapi_api_key() -> str:
    platform_settings = get_platform_settings()
    if platform_settings is not None:
        return platform_settings.misttrack_openapi_api_key.strip()
    return ""
