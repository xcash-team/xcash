import os

from celery import Celery
from celery.schedules import crontab

from config.performance import get_int
from config.performance import get_int_default

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")

app = Celery("xcash")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


WEBHOOK_EVENTS_SCHEDULE_SECONDS = get_int_default(
    "CELERY_WEBHOOK_EVENTS_SCHEDULE_SECONDS",
    15,
)
LATEST_BLOCK_SCHEDULE_SECONDS = get_int_default(
    "CELERY_LATEST_BLOCK_SCHEDULE_SECONDS",
    16,
)
FALLBACK_PROCESS_TRANSFER_SCHEDULE_SECONDS = get_int_default(
    "CELERY_FALLBACK_PROCESS_TRANSFER_SCHEDULE_SECONDS",
    20,
)
DEPOSIT_GATHER_SCHEDULE_SECONDS = get_int_default(
    "CELERY_DEPOSIT_GATHER_SCHEDULE_SECONDS",
    20,
)
EVM_BROADCAST_SCHEDULE_SECONDS = get_int_default(
    "CELERY_EVM_BROADCAST_SCHEDULE_SECONDS",
    8,
)
EVM_ERC20_SCAN_SCHEDULE_SECONDS = get_int(
    "CELERY_EVM_ERC20_SCAN_SCHEDULE_SECONDS",
    "evm_scan_seconds",
)
EVM_NATIVE_SCAN_SCHEDULE_SECONDS = get_int(
    "CELERY_EVM_NATIVE_SCAN_SCHEDULE_SECONDS",
    "evm_scan_seconds",
)
EVM_RECONCILE_SCHEDULE_SECONDS = get_int_default(
    "CELERY_EVM_RECONCILE_SCHEDULE_SECONDS",
    45,
)
TRON_SCAN_SCHEDULE_SECONDS = get_int(
    "CELERY_TRON_SCAN_SCHEDULE_SECONDS",
    "tron_scan_seconds",
)
BITCOIN_SCAN_SCHEDULE_SECONDS = get_int(
    "CELERY_BITCOIN_SCAN_SCHEDULE_SECONDS",
    "bitcoin_scan_seconds",
)
BITCOIN_WATCH_SYNC_SCHEDULE_SECONDS = get_int(
    "CELERY_BITCOIN_WATCH_SYNC_SCHEDULE_SECONDS",
    "bitcoin_watch_sync_seconds",
)
INVOICE_EXPIRED_SCHEDULE_SECONDS = get_int_default(
    "CELERY_INVOICE_EXPIRED_SCHEDULE_SECONDS",
    60,
)
OPERATIONAL_RISKS_SCHEDULE_SECONDS = get_int_default(
    "CELERY_OPERATIONAL_RISKS_SCHEDULE_SECONDS",
    120,
)
CRYPTO_PRICE_REFRESH_SCHEDULE_SECONDS = get_int_default(
    "CELERY_CRYPTO_PRICE_REFRESH_SCHEDULE_SECONDS",
    120,
)


# ---------------------------
# webhooks app
# ---------------------------
webhooks_tasks = {
    "schedule_events": {
        "task": "webhooks.tasks.schedule_events",
        "schedule": WEBHOOK_EVENTS_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# chains app
# ---------------------------
chains_tasks = {
    "update_latest_block": {
        "task": "chains.tasks.update_latest_block",
        "schedule": LATEST_BLOCK_SCHEDULE_SECONDS,
    },
    "fallback_process_transfer": {
        "task": "chains.tasks.fallback_process_transfer",
        "schedule": FALLBACK_PROCESS_TRANSFER_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# deposits app
# ---------------------------
deposits_tasks = {
    "gather_deposits": {
        "task": "deposits.tasks.gather_deposits",
        "schedule": DEPOSIT_GATHER_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# evm app
# ---------------------------
evm_tasks = {
    "dispatch_due_evm_broadcast_tasks": {
        "task": "evm.tasks.dispatch_due_evm_broadcast_tasks",
        "schedule": EVM_BROADCAST_SCHEDULE_SECONDS,
    },
    "scan_active_evm_erc20_chains": {
        # ERC20 走 eth_getLogs，RPC 成本低于原生币 full block 扫描，可保持较高频率。
        "task": "evm.tasks.scan_active_evm_erc20_chains",
        "schedule": EVM_ERC20_SCAN_SCHEDULE_SECONDS,
    },
    "scan_active_evm_native_chains": {
        # 原生币直转需要逐块拉完整交易列表，单独调度便于按链和 RPC 能力独立调优。
        "task": "evm.tasks.scan_active_evm_native_chains",
        "schedule": EVM_NATIVE_SCAN_SCHEDULE_SECONDS,
    },
    "reconcile_stale_pending_chain_for_active_evm_chains": {
        # 兜底：主扫描漏扫导致的 PENDING_CHAIN 卡单，周期性按 receipt 主动命中并定点复扫。
        "task": "evm.tasks.reconcile_stale_pending_chain_for_active_evm_chains",
        "schedule": EVM_RECONCILE_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# currencies app
# ---------------------------
currencies_tasks = {
    "refresh_crypto_prices": {
        "task": "currencies.tasks.refresh_crypto_prices",
        "schedule": CRYPTO_PRICE_REFRESH_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# tron app
# ---------------------------
tron_tasks = {
    "scan_active_tron_chains": {
        "task": "tron.tasks.scan_active_tron_chains",
        "schedule": TRON_SCAN_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# bitcoin app
# ---------------------------
bitcoin_tasks = {
    "scan_bitcoin_receipts": {
        # BTC 首版改为内部区块扫描，不再依赖外部流服务商回调。
        "task": "bitcoin.tasks.scan_bitcoin_receipts",
        "schedule": BITCOIN_SCAN_SCHEDULE_SECONDS,
    },
    "sync_bitcoin_watch_addresses": {
        # 定期全量同步 watch-only 地址到 Bitcoin 节点钱包，
        # 确保换节点或节点重建后自动恢复，无需手动干预。
        "task": "bitcoin.tasks.sync_bitcoin_watch_addresses",
        "schedule": BITCOIN_WATCH_SYNC_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# invoices app
# ---------------------------
invoices_tasks = {
    "fallback_invoice_expired": {
        "task": "invoices.tasks.fallback_invoice_expired",
        "schedule": INVOICE_EXPIRED_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# core app
# ---------------------------
core_tasks = {
    "scan_operational_risks": {
        # 巡检提币、归集、Webhook 卡单风险；告警先走结构化日志，后续再接外部通知渠道。
        "task": "core.tasks.scan_operational_risks",
        "schedule": OPERATIONAL_RISKS_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# celery 内置
# ---------------------------
celery_internal_tasks = {
    "backend_cleanup": {
        "task": "celery.backend_cleanup",
        "schedule": crontab(hour=4, minute=0, day_of_week=1),
    },
}

# ---------------------------
# 最终合并
# ---------------------------
app.conf.beat_schedule = {
    **webhooks_tasks,
    **chains_tasks,
    **deposits_tasks,
    **evm_tasks,
    **tron_tasks,
    **bitcoin_tasks,
    **currencies_tasks,
    **celery_internal_tasks,
    **invoices_tasks,
    **core_tasks,
}
