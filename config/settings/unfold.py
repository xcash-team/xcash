from django.templatetags.static import static
from django.urls import reverse_lazy
from django.utils.translation import gettext_lazy as _

BASE_UNFOLD = {
    "SITE_TITLE": "Xcash",
    "SITE_HEADER": "Xcash",
    "SITE_URL": "https://xca.sh/",
    "SITE_SYMBOL": "dashboard",  # symbol from icon set
    "ENVIRONMENT": "core.dashboard.environment_callback",
    "DASHBOARD_CALLBACK": "core.dashboard.dashboard_callback",
    "LOGIN": {
        "image": lambda request: static("login-bg.jpg"),
    },
    "SITE_FAVICONS": [
        {
            "rel": "icon",
            "sizes": "32x32",
            "type": "image/svg+xml",
            "href": lambda request: static("logo.svg"),
        },
    ],
    "SITE_ICON": {
        "light": lambda request: static("logo.svg"),  # light mode
        "dark": lambda request: static("logo.svg"),  # dark mode
    },
    "SHOW_LANGUAGES": True,
    "LANGUAGES": {
        "navigation": [
            {
                "bidi": False,
                "code": "en",
                "name": "English",
                "name_local": "🇺🇸 English",
                "name_translated": "🇺🇸 English",
            },
            {
                "bidi": False,
                "code": "zh-hans",
                "name": "简体中文",
                "name_local": "🇨🇳 简体中文",
                "name_translated": "🇨🇳 简体中文",
            },
        ],
    },
    "SITE_DROPDOWN": [
        {
            "icon": "home",
            "title": "Xcash",
            "link": "https://xca.sh",
            "attrs": {
                "target": "_blank",
            },
        },
        {
            "icon": "docs",
            "title": _("文档"),
            "link": "https://docs.xca.sh",
        },
    ],
    "BORDER_RADIUS": "16px",
    "COLORS": {
        "primary": {
            "50": "239 246 255",
            "100": "219 234 254",
            "200": "191 219 254",
            "300": "147 197 253",
            "400": "96 165 250",
            "500": "37 99 235",
            "600": "29 78 216",
            "700": "30 64 175",
            "800": "30 58 138",
            "900": "23 37 84",
            "950": "15 23 42",
        },
    },
}

SIDEBAR_UNFOLD = {
    "SIDEBAR": {
        "navigation": [
            {
                # 所有后台用户都统一从总览进入，避免继续维护双后台心智模型。
                "title": _("总览"),
                "collapsible": False,
                "items": [
                    {
                        "title": _("经营看板"),
                        "icon": "insert_chart",
                        "link": reverse_lazy("admin:index"),
                    },
                ],
            },
            {
                "title": _("项目"),
                "collapsible": False,
                "items": [
                    {
                        "title": _("项目列表"),
                        "icon": "widgets",
                        "link": reverse_lazy("admin:projects_project_changelist"),
                    },
                ],
            },
            {
                "title": _("支付"),
                "collapsible": False,
                "items": [
                    {
                        "title": _("账单列表"),
                        "icon": "receipt_long",
                        "link": reverse_lazy("admin:invoices_invoice_changelist"),
                    },
                ],
            },
            {
                "title": _("充币"),
                "collapsible": False,
                "items": [
                    {
                        "title": _("充币记录"),
                        "icon": "download",
                        "link": reverse_lazy("admin:deposits_deposit_changelist"),
                    },
                    {
                        "title": _("归集记录"),
                        "icon": "call_split",
                        "link": reverse_lazy(
                            "admin:deposits_depositcollection_changelist"
                        ),
                    },
                    {
                        "title": _("Gas 补充"),
                        "icon": "local_gas_station",
                        "link": reverse_lazy("admin:deposits_gasrecharge_changelist"),
                    },
                    {
                        "title": _("充币地址"),
                        "icon": "add_card",
                        "link": reverse_lazy(
                            "admin:deposits_depositaddress_changelist"
                        ),
                    },
                ],
            },
            {
                "title": _("提币"),
                "collapsible": False,
                "items": [
                    {
                        "title": _("提币列表"),
                        "icon": "upload",
                        "link": reverse_lazy("admin:withdrawals_withdrawal_changelist"),
                    },
                    {
                        "title": _("金库注资"),
                        "icon": "account_balance",
                        "link": reverse_lazy(
                            "admin:withdrawals_vaultfunding_changelist"
                        ),
                    },
                    {
                        "title": _("审核日志"),
                        "icon": "fact_check",
                        "link": reverse_lazy(
                            "admin:withdrawals_withdrawalreviewlog_changelist"
                        ),
                    },
                ],
            },
            {
                "title": _("通知"),
                "collapsible": False,
                "items": [
                    {
                        "title": _("通知事件"),
                        "icon": "notifications_active",
                        "link": reverse_lazy("admin:webhooks_webhookevent_changelist"),
                    },
                    {
                        "title": _("投递日志"),
                        "icon": "send",
                        "link": reverse_lazy(
                            "admin:webhooks_deliveryattempt_changelist"
                        ),
                    },
                ],
            },
            {
                "title": _("运维"),
                "collapsible": True,
                "items": [
                    {
                        "title": _("异常巡检"),
                        "icon": "monitor_heart",
                        "link": reverse_lazy("operational-inspection"),
                    },
                    {
                        "title": _("Signer"),
                        "icon": "shield_lock",
                        "link": reverse_lazy("signer-overview"),
                    },
                    {
                        "title": _("告警通道"),
                        "icon": "campaign",
                        "link": reverse_lazy(
                            "admin:alerts_projecttelegramalertconfig_changelist"
                        ),
                    },
                    {
                        "title": _("告警事件"),
                        "icon": "warning",
                        "link": reverse_lazy(
                            "admin:alerts_projectalertstate_changelist"
                        ),
                    },
                ],
            },
            {
                "title": _("区块链"),
                "collapsible": True,
                "items": [
                    {
                        "title": _("链"),
                        "icon": "memory",
                        "link": reverse_lazy("admin:chains_chain_changelist"),
                    },
                    {
                        "title": _("钱包"),
                        "icon": "account_balance_wallet",
                        "link": reverse_lazy("admin:chains_wallet_changelist"),
                    },
                    {
                        "title": _("地址"),
                        "icon": "wallet",
                        "link": reverse_lazy("admin:chains_address_changelist"),
                    },
                    {
                        "title": _("转账"),
                        "icon": "sync_alt",
                        "link": reverse_lazy("admin:chains_onchaintransfer_changelist"),
                    },
                    {
                        "title": _("EVM 链上任务"),
                        "icon": "bolt",
                        "link": reverse_lazy("admin:evm_evmbroadcasttask_changelist"),
                    },
                    {
                        "title": _("EVM 扫描游标"),
                        "icon": "radar",
                        "link": reverse_lazy("admin:evm_evmscancursor_changelist"),
                    },
                    {
                        "title": _("Bitcoin 扫描游标"),
                        "icon": "track_changes",
                        "link": reverse_lazy(
                            "admin:bitcoin_bitcoinscancursor_changelist"
                        ),
                    },
                    {
                        "title": _("Tron 扫描游标"),
                        "icon": "radar",
                        "link": reverse_lazy(
                            "admin:tron_tronwatchcursor_changelist"
                        ),
                    },
                ],
            },
            {
                "title": _("任务"),
                "collapsible": True,
                "items": [
                    {
                        "title": _("任务日志"),
                        "icon": "task",
                        "link": reverse_lazy(
                            "admin:django_celery_results_taskresult_changelist",
                        ),
                    },
                ],
            },
            {
                "title": _("代币"),
                "collapsible": True,
                "items": [
                    {
                        "title": _("加密货币"),
                        "icon": "currency_bitcoin",
                        "link": reverse_lazy("admin:currencies_crypto_changelist"),
                    },
                    {
                        "title": _("法币"),
                        "icon": "currency_yuan",
                        "link": reverse_lazy("admin:currencies_fiat_changelist"),
                    },
                ],
            },
            {
                "title": _("系统"),
                "collapsible": True,
                "items": [
                    {
                        "title": _("用户"),
                        "icon": "account_circle",
                        "link": reverse_lazy("admin:users_user_changelist"),
                    },
                    {
                        "title": _("平台参数"),
                        "icon": "tune",
                        "link": reverse_lazy("admin:core_platformsettings_changelist"),
                    },
                ],
            },
        ],
    },
}

UNFOLD = {**BASE_UNFOLD, **SIDEBAR_UNFOLD}
