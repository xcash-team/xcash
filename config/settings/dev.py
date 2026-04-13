# ruff: noqa: E501
import socket
from copy import deepcopy

from .base import *  # noqa: F403
from .base import INSTALLED_APPS
from .base import MIDDLEWARE
from .base import REST_FRAMEWORK as BASE_REST_FRAMEWORK
from .base import UNFOLD
from .base import env

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#debug
DEBUG = True
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
# 开发环境使用固定 SECRET_KEY，避免每次重启后 session 失效需要重新登录。
SECRET_KEY = env(
    "DJANGO_SECRET_KEY", default="django-insecure-dev-only-key-do-not-use-in-production"
)
SESSION_EXPIRE_AT_BROWSER_CLOSE = False  # False表示关闭浏览器后session仍然有效
ALLOWED_HOSTS = ["*"]
# 本地开发常见是 Nginx/Caddy 以 loopback 反代 Django；默认信任本机代理来源。
TRUSTED_PROXY_IPS = ["127.0.0.1", "::1"]

# django-debug-toolbar
# ------------------------------------------------------------------------------
# https://django-debug-toolbar.readthedocs.io/en/latest/installation.html#prerequisites
INSTALLED_APPS += ["debug_toolbar", "stress"]
# https://django-debug-toolbar.readthedocs.io/en/latest/installation.html#middleware
MIDDLEWARE += ["debug_toolbar.middleware.DebugToolbarMiddleware"]
# https://django-debug-toolbar.readthedocs.io/en/latest/configuration.html#debug-toolbar-config
DEBUG_TOOLBAR_CONFIG = {
    "DISABLE_PANELS": [
        "debug_toolbar.panels.redirects.RedirectsPanel",
        # Disable profiling panel due to an issue with Python 3.12:
        # https://github.com/jazzband/django-debug-toolbar/issues/1875
        "debug_toolbar.panels.profiling.ProfilingPanel",
    ],
    "SHOW_TEMPLATE_CONTEXT": True,
}
# https://django-debug-toolbar.readthedocs.io/en/latest/installation.html#internal-ips
INTERNAL_IPS = ["127.0.0.1", "10.0.2.2"]

try:
    # 某些本地环境的 hostname 不可反解；开发配置应回退而不是直接阻断 manage.py。
    _hostname, _, _ips = socket.gethostbyname_ex(socket.gethostname())
except (socket.herror, socket.gaierror):
    _ips = []
INTERNAL_IPS += [".".join(ip.split(".")[:-1] + ["1"]) for ip in _ips]

# Celery
# ------------------------------------------------------------------------------

# https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-eager-propagates
CELERY_TASK_EAGER_PROPAGATES = True

# Your stuff...
# ------------------------------------------------------------------------------

CSRF_TRUSTED_ORIGINS = []

# 开发环境允许所有跨域请求，生产环境必须通过 CORS_ALLOWED_ORIGINS 配置白名单。
CORS_ALLOW_ALL_ORIGINS = True

# Signer
# ------------------------------------------------------------------------------
# 开发环境默认直连本机暴露的 signer 端口，保证 fresh init 时无需先手工补环境变量。
SIGNER_BASE_URL = "http://127.0.0.1:8010"
SIGNER_SHARED_SECRET = ""

# Stress test 配置（仅开发环境）
STRESS_EVM_RPC_URL = "http://localhost:8545"
# Anvil 默认助记词第一个账户私钥
STRESS_EVM_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
STRESS_BTC_RPC_URL = "http://xcash:xcash@localhost:18443"
STRESS_WEBHOOK_BASE_URL = "http://localhost:8000"
# 压力测试允许的链代码；EVM 走 Ethereum Local，BTC 走 Bitcoin Local，非本地链无法支付
STRESS_ALLOWED_CHAINS = ["ethereum-local", "bitcoin-local"]

# 本地压测会以匿名方式高并发访问公开接口，沿用基础配置的 256/minute 很容易在建单阶段触发 429。
# 仅开发环境放宽匿名限流，生产仍继续使用 base.py 中的保守默认值。
REST_FRAMEWORK = deepcopy(BASE_REST_FRAMEWORK)
REST_FRAMEWORK["DEFAULT_THROTTLE_RATES"]["anon"] = "10000/minute"
REST_FRAMEWORK["DEFAULT_THROTTLE_RATES"]["withdrawal_create"] = "10000/minute"
REST_FRAMEWORK["DEFAULT_THROTTLE_RATES"]["deposit_address"] = "10000/minute"

# Stress app 侧边栏注入
# ------------------------------------------------------------------------------
from django.urls import reverse_lazy  # noqa: E402
from django.utils.translation import gettext_lazy as _  # noqa: E402

UNFOLD["SIDEBAR"]["navigation"].insert(
    -1,
    {
        "title": _("压测"),
        "collapsible": True,
        "items": [
            {
                "title": _("测试轮次"),
                "icon": "speed",
                "link": reverse_lazy("admin:stress_stressrun_changelist"),
            },
            {
                "title": _("账单测试"),
                "icon": "checklist",
                "link": reverse_lazy("admin:stress_invoicestresscase_changelist"),
            },
            {
                "title": _("提币测试"),
                "icon": "upload",
                "link": reverse_lazy("admin:stress_withdrawalstresscase_changelist"),
            },
            {
                "title": _("充币测试"),
                "icon": "download",
                "link": reverse_lazy("admin:stress_depositstresscase_changelist"),
            },
        ],
    },
)
