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
ALLOWED_HOSTS = ["signer", "127.0.0.1", "localhost"]
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
SIGNER_REQUEST_TTL = 60
# BIP44 address_index 上界，单个 bip44_account 下最多派生的地址数量。
SIGNER_MAX_ADDRESS_INDEX = 100_000_000
# BIP44 account 上界，限制可使用的 BIP44 account' 层级数量。
SIGNER_MAX_BIP44_ACCOUNT = 10
SIGNER_RATE_LIMIT_WINDOW = 60
SIGNER_RATE_LIMIT_MAX_REQUESTS = 120
SIGNER_WALLET_SIGN_RATE_LIMIT_WINDOW = 60
SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS = 30
BITCOIN_NETWORK = env.str("BITCOIN_NETWORK", default="mainnet")

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "xcash_signer",
        "USER": "postgres",
        "PASSWORD": env.str("SIGNER_POSTGRES_PASSWORD", default="postgres"),
        "HOST": "signer-db",
        "PORT": 5432,
    }
}
DATABASES["default"]["ATOMIC_REQUESTS"] = True
DATABASES["default"]["CONN_MAX_AGE"] = 60

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 1
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

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
