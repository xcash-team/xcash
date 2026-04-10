import os

from celery import Celery
from celery.schedules import crontab

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

# EVM 自扫描属于基础设施级轮询任务；周期独立成环境变量，便于不同环境按节点能力调优。
EVM_SCAN_SCHEDULE_SECONDS = int(os.getenv("EVM_SCAN_SCHEDULE_SECONDS", "5"))
TRON_SCAN_SCHEDULE_SECONDS = int(os.getenv("TRON_SCAN_SCHEDULE_SECONDS", "15"))


# ---------------------------
# webhooks app
# ---------------------------
webhooks_tasks = {
    "schedule_events": {
        "task": "webhooks.tasks.schedule_events",
        "schedule": 10,
    },
}

# ---------------------------
# chains app
# ---------------------------
chains_tasks = {
    "update_latest_block": {
        "task": "chains.tasks.update_latest_block",
        "schedule": 10,
    },
    "fallback_process_transfer": {
        "task": "chains.tasks.fallback_process_transfer",
        "schedule": 10,
    },
}

# ---------------------------
# deposits app
# ---------------------------
deposits_tasks = {
    "gather_deposits": {
        "task": "deposits.tasks.gather_deposits",
        "schedule": 8,
    },
}

# ---------------------------
# evm app
# ---------------------------
evm_tasks = {
    "dispatch_due_evm_broadcast_tasks": {
        "task": "evm.tasks.dispatch_due_evm_broadcast_tasks",
        "schedule": 5,
    },
    "scan_active_evm_chains": {
        # 周期性触发所有启用中的 EVM 链自扫描，负责发现原生币直转与 ERC20 Transfer。
        "task": "evm.tasks.scan_active_evm_chains",
        "schedule": EVM_SCAN_SCHEDULE_SECONDS,
    },
}

# ---------------------------
# currencies app
# ---------------------------
currencies_tasks = {
    "refresh_crypto_prices": {
        "task": "currencies.tasks.refresh_crypto_prices",
        "schedule": 120,
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
        "schedule": 30,
    },
    "sync_bitcoin_watch_addresses": {
        # 定期全量同步 watch-only 地址到 Bitcoin 节点钱包，
        # 确保换节点或节点重建后自动恢复，无需手动干预。
        "task": "bitcoin.tasks.sync_bitcoin_watch_addresses",
        "schedule": 300,
    },
}

# ---------------------------
# invoices app
# ---------------------------
invoices_tasks = {
    "fallback_invoice_expired": {
        "task": "invoices.tasks.fallback_invoice_expired",
        "schedule": 30,
    },
}

# ---------------------------
# core app
# ---------------------------
core_tasks = {
    "scan_operational_risks": {
        # 巡检提币、归集、Webhook 卡单风险；告警先走结构化日志，后续再接外部通知渠道。
        "task": "core.tasks.scan_operational_risks",
        "schedule": 60,
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
