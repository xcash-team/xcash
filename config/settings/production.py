# ruff: noqa: E501, F405
import logging

from common.host_access import normalize_ip_host

from .base import *  # noqa
from .base import env
from .base import shared_processors

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env("DJANGO_SECRET_KEY")
DOMAIN = env("SITE_DOMAIN", default="localhost").strip().lower()
SCHEME = "https"
# 未显式配置时默认只为本机保留 internal API Host，避免空配置退化为完全不限制。
INTERNAL_API_ALLOWED_IP = normalize_ip_host(
    env.str("INTERNAL_API_IP", default="127.0.0.1")
)
ALLOWED_HOSTS = ["127.0.0.1", "localhost", DOMAIN]
if INTERNAL_API_ALLOWED_IP:
    ALLOWED_HOSTS.append(INTERNAL_API_ALLOWED_IP)

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
ACCOUNT_DEFAULT_HTTP_PROTOCOL = "https"

# SECURITY
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secure-proxy-ssl-header
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_HSTS_SECONDS = 3600
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True

# Django 要求 CSRF_TRUSTED_ORIGINS 带 scheme。
CSRF_TRUSTED_ORIGINS = [f"{SCHEME}://{DOMAIN}"]

# 生产环境严格限制跨域来源，仅允许自身域名。
CORS_ALLOW_ALL_ORIGINS = False
CORS_ALLOWED_ORIGINS = [f"{SCHEME}://{DOMAIN}"]

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
