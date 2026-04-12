# ruff: noqa: E501, F405
import logging

from .base import *  # noqa
from .base import env
from .base import shared_processors

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env("DJANGO_SECRET_KEY")
DOMAIN = env("SITE_DOMAIN", default="").strip().lower()


def _is_ip(host: str) -> bool:
    """判断 host 是否为 IP 地址（IPv4 / IPv6 / [IPv6]）。"""
    import ipaddress

    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


# SITE_DOMAIN 为域名时自动启用 HTTPS；为 IP 地址或未设置时使用 HTTP。
USE_HTTPS = bool(DOMAIN) and not _is_ip(DOMAIN)
SCHEME = "https" if USE_HTTPS else "http"

# 改动原因：SITE_DOMAIN 允许为空且只提供主机名，生产配置需要分别适配 ALLOWED_HOSTS 与 CSRF_TRUSTED_ORIGINS 的格式要求。
ALLOWED_HOSTS = [host for host in ["127.0.0.1", "localhost", DOMAIN] if host]

# STATIC & MEDIA
# ------------------------
STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

# 基础 URL 设置
USE_X_FORWARDED_HOST = True
ACCOUNT_DEFAULT_HTTP_PROTOCOL = SCHEME

# SECURITY
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secure-proxy-ssl-header
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https") if USE_HTTPS else None
SESSION_COOKIE_SECURE = USE_HTTPS
CSRF_COOKIE_SECURE = USE_HTTPS
SECURE_HSTS_SECONDS = 3600 if USE_HTTPS else 0
SECURE_HSTS_INCLUDE_SUBDOMAINS = USE_HTTPS
SECURE_HSTS_PRELOAD = USE_HTTPS

# Django 要求 CSRF_TRUSTED_ORIGINS 带 scheme。
CSRF_TRUSTED_ORIGINS = [f"{SCHEME}://{DOMAIN}"] if DOMAIN else []

# 生产环境严格限制跨域来源，仅允许自身域名。
CORS_ALLOW_ALL_ORIGINS = False
CORS_ALLOWED_ORIGINS = [f"{SCHEME}://{DOMAIN}"] if DOMAIN else []

# LOGGING
# ------------------------------------------------------------------------------
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": structlog.stdlib.ProcessorFormatter,
            "processors": [
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ],
            "foreign_pre_chain": shared_processors,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",
        },
    },
    "root": {"level": "INFO", "handlers": ["console"]},
    "loggers": {
        "django.security.DisallowedHost": {"level": "ERROR", "handlers": ["console"]},
        "django.request": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False,
        },
    },
}
