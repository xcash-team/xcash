"""
测试环境配置。

目标：
1. 默认直连本地/容器内 Postgres + Redis，尽量贴近真实运行环境；
2. 与 base 保持大部分行为一致，避免“测试环境和生产语义完全不同”；
3. 不要求额外手工导出环境变量，直接复用 docker-compose.dev.yml 的默认约定。
"""

import os
import warnings

# base.py 在导入阶段就会解析数据库 / Redis 配置；这里先注入与 docker-compose
# 一致的默认值，保证 `manage.py test` 与 `pytest` 开箱即用地连上本地依赖容器。
os.environ.setdefault("POSTGRES_DB", "xcash")
os.environ.setdefault("POSTGRES_USER", "postgres")
os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_DB", "0")
os.environ.setdefault("TRUSTED_PROXY_IPS", "127.0.0.1,::1")
os.environ.setdefault("SIGNER_BASE_URL", "http://signer.internal")
os.environ.setdefault("SIGNER_SHARED_SECRET", "test-signer-secret")

# web3 7.14.1 仍会在导入阶段触发 websockets.legacy 的上游弃用告警；
# 测试环境先静默该第三方噪音，避免掩盖项目自身 warning。
warnings.filterwarnings(
    "ignore",
    message=r"websockets\.legacy is deprecated;.*",
    category=DeprecationWarning,
)

from .base import *  # noqa: F403
from .base import INSTALLED_APPS
from .base import TEMPLATES
from .base import env

INTERNAL_API_TOKEN = "test-internal-token"

# stress app 仅在开发/测试环境加载，生产环境不包含。
INSTALLED_APPS += ["stress"]

# 测试环境用内存静态文件后端，避免在项目根目录留下静态文件构建产物。
STATIC_ROOT = str(BASE_DIR / "test-staticfiles")  # noqa: F405
STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
}

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env(
    "DJANGO_SECRET_KEY",
    default="fvPNVgLTa2BF9HgH4gwp7AV9aOccqo8dqvKynNanwisKy7oc6tPITD4d7GMjGjIy",
)
# https://docs.djangoproject.com/en/dev/ref/settings/#test-runner
TEST_RUNNER = "django.test.runner.DiscoverRunner"

# PASSWORDS
# ------------------------------------------------------------------------------
# 测试环境使用更快的哈希器，缩短用户/权限相关测试耗时。
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]

# DEBUGGING FOR TEMPLATES
# ------------------------------------------------------------------------------
TEMPLATES[0]["OPTIONS"]["debug"] = True  # type: ignore[index]

# CACHE / EMAIL
# ------------------------------------------------------------------------------
# 邮件仍走内存后端，避免测试期间依赖外部 SMTP。
EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"

# 测试默认关闭启动自动注数，避免污染依赖空库假设的单测。
AUTO_BOOTSTRAP_REFERENCE_DATA = False

# CELERY
# ------------------------------------------------------------------------------
# 测试保持真实的 Redis broker；未启动 worker 时任务只会入队，不会被立即执行。
CELERY_TASK_ALWAYS_EAGER = False
CELERY_TASK_EAGER_PROPAGATES = True

# MEDIA
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#media-url
MEDIA_URL = "http://media.testserver/"
