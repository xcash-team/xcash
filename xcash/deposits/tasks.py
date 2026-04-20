import structlog
from celery import shared_task
from django.utils import timezone

from common.decorators import singleton_task
from deposits.models import CollectSchedule
from deposits.service import DepositService

logger = structlog.get_logger()

# 单轮归集名额：保持原有吞吐预期不变。
TOTAL_BATCH_SIZE = 16


@shared_task(ignore_result=True)
@singleton_task(timeout=64)
def gather_deposits() -> None:
    """按 CollectSchedule 扫描到期归集窗口并创建归集任务。"""
    schedule_ids = list(
        CollectSchedule.objects.filter(next_collect_time__lte=timezone.now())
        .order_by("next_collect_time", "pk")
        .values_list("pk", flat=True)[:TOTAL_BATCH_SIZE]
    )

    for schedule_id in schedule_ids:
        try:
            DepositService.collect_due_schedule(schedule_id)
        except Exception:  # noqa: BLE001
            logger.exception("归集调度任务失败", schedule_id=schedule_id)
