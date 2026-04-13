from pathlib import Path

import environ
from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent

env = environ.Env()

SECRET_KEY = env.str(
    "SIGNER_SECRET_KEY",
    default="signer-dev-secret-key-change-me",
)
DEBUG = env.bool("SIGNER_DEBUG", default=False)
ALLOWED_HOSTS = env.list("SIGNER_ALLOWED_HOSTS", default=["signer", "127.0.0.1", "localhost"])
LANGUAGE_CODE = "zh-hans"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

# 助记词加密专用密钥，与 Django SECRET_KEY 隔离，缩小 SECRET_KEY 泄露的爆炸半径。
SIGNER_MNEMONIC_ENCRYPTION_KEY = env.str(
    "SIGNER_MNEMONIC_ENCRYPTION_KEY",
    default="dev-mnemonic-encryption-key-change-me",
)

SIGNER_SHARED_SECRET = env.str("SIGNER_SHARED_SECRET", default="")
SIGNER_REQUEST_TTL = env.int("SIGNER_REQUEST_TTL", default=60)
# BIP44 address_index 上界，单个 bip44_account 下最多派生的地址数量。
SIGNER_MAX_ADDRESS_INDEX = env.int("SIGNER_MAX_ADDRESS_INDEX", default=100_000_000)
# BIP44 account 上界，限制可使用的 BIP44 account' 层级数量。
SIGNER_MAX_BIP44_ACCOUNT = env.int("SIGNER_MAX_BIP44_ACCOUNT", default=10)
SIGNER_RATE_LIMIT_WINDOW = env.int("SIGNER_RATE_LIMIT_WINDOW", default=60)
SIGNER_RATE_LIMIT_MAX_REQUESTS = env.int("SIGNER_RATE_LIMIT_MAX_REQUESTS", default=120)
SIGNER_WALLET_SIGN_RATE_LIMIT_WINDOW = env.int(
    "SIGNER_WALLET_SIGN_RATE_LIMIT_WINDOW",
    default=60,
)
SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS = env.int(
    "SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS",
    default=30,
)
BITCOIN_NETWORK = env.str("BITCOIN_NETWORK", default="mainnet")

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env.str("SIGNER_POSTGRES_DB", default="xcash_signer"),
        "USER": env.str("SIGNER_POSTGRES_USER", default="postgres"),
        "PASSWORD": env.str("SIGNER_POSTGRES_PASSWORD", default="postgres"),
        # 本地直接运行 signer 管理命令时优先复用现有 PostgreSQL 主机环境，避免默认容器主机名无法解析。
        "HOST": env.str(
            "SIGNER_POSTGRES_HOST",
            default=env.str("POSTGRES_HOST", default="signer-db"),
        ),
        "PORT": env.int("SIGNER_POSTGRES_PORT", default=5432),
    }
}
DATABASES["default"]["ATOMIC_REQUESTS"] = True
DATABASES["default"]["CONN_MAX_AGE"] = env.int("SIGNER_CONN_MAX_AGE", default=60)

REDIS_HOST = env.str(
    "SIGNER_REDIS_HOST",
    default=env.str("REDIS_HOST", default="redis"),
)
REDIS_PORT = env.int("SIGNER_REDIS_PORT", default=env.int("REDIS_PORT", default=6379))
REDIS_DB = env.int("SIGNER_REDIS_DB", default=env.int("REDIS_DB", default=1))
REDIS_URL = env.str(
    "SIGNER_REDIS_URL",
    default=f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
)

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "rest_framework",
    "wallets",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
        },
    },
}

REST_FRAMEWORK = {
    "UNAUTHENTICATED_USER": None,
}

if not DEBUG and SECRET_KEY == "signer-dev-secret-key-change-me":
    raise ImproperlyConfigured("非开发环境必须显式配置 SIGNER_SECRET_KEY")

if (
    not DEBUG
    and SIGNER_MNEMONIC_ENCRYPTION_KEY == "dev-mnemonic-encryption-key-change-me"
):
    raise ImproperlyConfigured("非开发环境必须显式配置 SIGNER_MNEMONIC_ENCRYPTION_KEY")

if not DEBUG and not SIGNER_SHARED_SECRET:
    raise ImproperlyConfigured("非开发环境必须显式配置 SIGNER_SHARED_SECRET")

# signer 内部可信代理 IP 列表；仅当 REMOTE_ADDR 属于此列表时才读取 X-Forwarded-For。
SIGNER_TRUSTED_PROXY_IPS = env.list("SIGNER_TRUSTED_PROXY_IPS", default=[])
