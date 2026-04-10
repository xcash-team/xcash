# xcash/stress/tasks.py
import time
from datetime import timedelta

import structlog
from celery import shared_task
from django.db import transaction
from django.utils import timezone

from .models import InvoiceStressCase
from .models import InvoiceStressCaseStatus
from .models import StressRun
from .models import StressRunStatus
from .models import WithdrawalStressCase
from .models import WithdrawalStressCaseStatus
from .payment import simulate_payment
from .service import StressService

logger = structlog.get_logger()


@shared_task(ignore_result=True)
def prepare_stress(stress_run_id: int) -> None:
    """准备 StressRun 测试数据：创建 Project 和 InvoiceStressCase。"""
    try:
        stress_run = StressRun.objects.get(pk=stress_run_id)
    except StressRun.DoesNotExist:
        return
    try:
        StressService.prepare(stress_run)
    except Exception as exc:
        logger.exception("stress.prepare.failed", stress_run_id=stress_run_id)
        StressRun.objects.filter(pk=stress_run_id).update(
            status=StressRunStatus.FAILED,
            error=str(exc)[:2000],
            finished_at=timezone.now(),
        )


@shared_task(ignore_result=True, soft_time_limit=120, time_limit=180)
def execute_stress_case(case_id: int) -> None:
    """执行单个 InvoiceStressCase 的完整流程。"""
    try:
        case = InvoiceStressCase.objects.select_related("stress_run__project").get(
            pk=case_id
        )
    except InvoiceStressCase.DoesNotExist:
        return

    if case.status != InvoiceStressCaseStatus.PENDING:
        return

    case.started_at = timezone.now()
    case.status = InvoiceStressCaseStatus.CREATING
    case.save(update_fields=["started_at", "status"])

    try:
        _execute(case)
    except Exception as exc:
        logger.exception("stress.case.failed", case_id=case.pk)
        case.status = InvoiceStressCaseStatus.FAILED
        case.error = str(exc)[:2000]
        case.finished_at = timezone.now()
        case.save(update_fields=["status", "error", "finished_at"])
        StressService.on_case_finished(case)


def _execute(case: InvoiceStressCase) -> None:
    """InvoiceStressCase 执行核心流程。"""
    # 阶段 1: 创建 Invoice
    resp = StressService.create_invoice(case)
    case.invoice_sys_no = resp["sys_no"]
    case.invoice_out_no = resp.get("out_no", "")
    case.status = InvoiceStressCaseStatus.CREATED
    case.save(update_fields=["invoice_sys_no", "invoice_out_no", "status"])

    # 阶段 2: 选择支付方式
    resp = StressService.select_method(case)
    case.crypto = resp.get("crypto", "")
    case.chain = resp.get("chain", "")
    case.pay_address = resp.get("pay_address", "")
    case.pay_amount = resp.get("pay_amount")
    case.save(update_fields=["crypto", "chain", "pay_address", "pay_amount"])

    # 阶段 3: 链上支付
    # 等待 2 秒，确保链上交易的区块时间戳（秒级精度）晚于 Invoice 的 started_at，
    # 否则 try_match_invoice 的 invoice__started_at__lte=transfer.datetime 条件会因
    # 区块时间戳被截断到同一秒的起点而匹配失败。
    time.sleep(2)
    case.status = InvoiceStressCaseStatus.PAYING
    case.save(update_fields=["status"])

    payment_result = _do_payment(case)
    case.tx_hash = payment_result["tx_hash"]
    case.payer_address = payment_result["payer_address"]
    case.status = InvoiceStressCaseStatus.PAID
    case.save(update_fields=["tx_hash", "payer_address", "status"])

    # 派发超时检查任务（5 分钟后）
    check_webhook_timeout.apply_async(
        args=[case.pk],
        eta=timezone.now() + timedelta(minutes=15),
    )


def _do_payment(case: InvoiceStressCase) -> dict[str, str]:
    """统一调用 stress 链上转币入口，返回 tx_hash 与付款方地址。

    当前场景是“账单支付”，所以 payment_ref 使用 case 维度命名。
    后续如果新增充币测试或其他需要模拟链上转入的测试，应该继续走
    `stress.payment.simulate_payment()`，而不是重新旁路实现发送逻辑。
    """
    return simulate_payment(
        to_address=case.pay_address,
        chain_code=case.chain,
        crypto_symbol=case.crypto,
        amount=case.pay_amount,
        payment_ref=f"case-{case.pk}",
    )


@shared_task(ignore_result=True, soft_time_limit=120, time_limit=180)
def execute_withdrawal_case(case_id: int) -> None:
    """执行单个 WithdrawalStressCase 的完整流程。"""
    try:
        case = WithdrawalStressCase.objects.select_related("stress_run__project").get(
            pk=case_id
        )
    except WithdrawalStressCase.DoesNotExist:
        return

    if case.status != WithdrawalStressCaseStatus.PENDING:
        return

    case.started_at = timezone.now()
    case.status = WithdrawalStressCaseStatus.CREATING
    case.save(update_fields=["started_at", "status"])

    try:
        _execute_withdrawal(case)
    except Exception as exc:
        logger.exception("stress.withdrawal_case.failed", case_id=case.pk)
        case.status = WithdrawalStressCaseStatus.FAILED
        case.error = str(exc)[:2000]
        case.finished_at = timezone.now()
        case.save(update_fields=["status", "error", "finished_at"])
        StressService.on_case_finished(case)


def _execute_withdrawal(case: WithdrawalStressCase) -> None:
    """WithdrawalStressCase 执行核心流程。"""
    # 阶段 1: 调用提币 API
    resp = StressService.create_withdrawal(case)
    case.withdrawal_sys_no = resp["sys_no"]
    case.withdrawal_out_no = f"STRESS-WD-{case.stress_run_id}-{case.sequence}"
    case.tx_hash = resp.get("hash", "")
    case.status = WithdrawalStressCaseStatus.CREATED
    case.save(
        update_fields=["withdrawal_sys_no", "withdrawal_out_no", "tx_hash", "status"]
    )

    # 阶段 2: 等待链上确认（由系统自动处理）
    case.status = WithdrawalStressCaseStatus.CONFIRMING
    case.save(update_fields=["status"])

    # 派发超时检查任务（15 分钟后）
    check_withdrawal_webhook_timeout.apply_async(
        args=[case.pk],
        eta=timezone.now() + timedelta(minutes=15),
    )


@shared_task(ignore_result=True)
def check_withdrawal_webhook_timeout(case_id: int) -> None:
    """检查 WithdrawalStressCase 是否在超时前收到了 webhook。"""
    with transaction.atomic():
        try:
            case = WithdrawalStressCase.objects.select_for_update().get(pk=case_id)
        except WithdrawalStressCase.DoesNotExist:
            return

        if case.status != WithdrawalStressCaseStatus.CONFIRMING:
            return

        case.status = WithdrawalStressCaseStatus.FAILED
        case.error = "webhook 超时未收到（15 分钟）"
        case.finished_at = timezone.now()
        case.save(update_fields=["status", "error", "finished_at"])

    StressService.on_case_finished(case)


@shared_task(ignore_result=True)
def finalize_stress_timeout(stress_run_id: int) -> None:
    """StressRun 级别的兜底超时：将所有未执行的 case 标记为 skipped 并结束整轮压测。

    解决场景：worker 重启 / Django 短暂不可用等导致部分 case 任务丢失，
    StressRun 永远凑不够终态数量而卡在 running。
    """
    with transaction.atomic():
        try:
            stress_run = StressRun.objects.select_for_update().get(pk=stress_run_id)
        except StressRun.DoesNotExist:
            return

        if stress_run.status != StressRunStatus.RUNNING:
            return

        terminal_invoice = {
            InvoiceStressCaseStatus.SUCCEEDED,
            InvoiceStressCaseStatus.FAILED,
            InvoiceStressCaseStatus.SKIPPED,
        }
        terminal_withdrawal = {
            WithdrawalStressCaseStatus.SUCCEEDED,
            WithdrawalStressCaseStatus.FAILED,
            WithdrawalStressCaseStatus.SKIPPED,
        }

        non_terminal_invoices = stress_run.cases.exclude(status__in=terminal_invoice)
        non_terminal_withdrawals = stress_run.withdrawal_cases.exclude(
            status__in=terminal_withdrawal
        )

        skipped_count = non_terminal_invoices.count() + non_terminal_withdrawals.count()
        if skipped_count == 0:
            return

        now = timezone.now()
        non_terminal_invoices.update(
            status=InvoiceStressCaseStatus.SKIPPED,
            error="压测整轮超时，任务未执行",
            finished_at=now,
        )
        non_terminal_withdrawals.update(
            status=WithdrawalStressCaseStatus.SKIPPED,
            error="压测整轮超时，任务未执行",
            finished_at=now,
        )

        stress_run.skipped += skipped_count
        stress_run.status = StressRunStatus.COMPLETED
        stress_run.finished_at = now
        stress_run.save(update_fields=["skipped", "status", "finished_at"])

    logger.info(
        "stress.finalize_timeout",
        stress_run_id=stress_run_id,
        skipped=skipped_count,
    )


@shared_task(ignore_result=True)
def check_webhook_timeout(case_id: int) -> None:
    """检查 InvoiceStressCase 是否在超时前收到了 webhook。"""
    with transaction.atomic():
        try:
            case = InvoiceStressCase.objects.select_for_update().get(pk=case_id)
        except InvoiceStressCase.DoesNotExist:
            return

        if case.status != InvoiceStressCaseStatus.PAID:
            return

        case.status = InvoiceStressCaseStatus.FAILED
        case.error = "webhook 超时未收到（15 分钟）"
        case.finished_at = timezone.now()
        case.save(update_fields=["status", "error", "finished_at"])

    StressService.on_case_finished(case)
