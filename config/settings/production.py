# ruff: noqa: E501, F405
import logging

from .base import *  # noqa
from .base import env
from .base import shared_processors

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env("DJANGO_SECRET_KEY")
DOMAIN = env("SITE_DOMAIN", default="localhost").strip().lower()
SCHEME = "https"
# xcash_traefik：同机内部服务（如 saas）通过 Docker 共享网络按容器名访问时的 Host 头。
# 公网 DNS 无法解析此名，不构成对外暴露风险。
ALLOWED_HOSTS = ["127.0.0.1", "localhost", DOMAIN, "xcash_traefik"]

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

# Celery
# ------------------------------------------------------------------------------
# Redis broker 在 worker 收到 TERM 时，需要先进入 soft shutdown 才更容易把未确认任务重新入队。
# 生产 worker 用 30s 软关闭窗口，且保持小于 compose 里的 60s stop_grace_period。
CELERY_WORKER_SOFT_SHUTDOWN_TIMEOUT = env.float(
    "CELERY_WORKER_SOFT_SHUTDOWN_TIMEOUT", default=30.0
)
# 项目存在 ETA 过期检查任务；空闲但已预留 ETA 消息时，也需要进入 soft shutdown。
CELERY_WORKER_ENABLE_SOFT_SHUTDOWN_ON_IDLE = env.bool(
    "CELERY_WORKER_ENABLE_SOFT_SHUTDOWN_ON_IDLE", default=True
)

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
