# xcash/stress/tasks.py
import time
from datetime import timedelta

import httpx
import structlog
from celery import shared_task
from django.db import transaction
from django.utils import timezone

from common.decorators import singleton_task

from .models import DepositStressCase
from .models import DepositStressCaseStatus
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


# 瞬态连接错误：Django 服务未就绪、连接被重置等，可安全重试。
_TRANSIENT_EXC = (ConnectionError, httpx.ConnectError, httpx.RemoteProtocolError)

_RETRY_KWARGS = {
    "autoretry_for": _TRANSIENT_EXC,
    "retry_backoff": 3,
    "retry_backoff_max": 30,
    "max_retries": 5,
}


@shared_task(ignore_result=True, soft_time_limit=120, time_limit=180, **_RETRY_KWARGS)
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
    case.invoice_created_at = timezone.now()
    case.save(
        update_fields=[
            "invoice_sys_no",
            "invoice_out_no",
            "status",
            "invoice_created_at",
        ]
    )

    # 阶段 2: 选择支付方式
    resp = StressService.select_method(case)
    case.crypto = resp.get("crypto", "")
    case.chain = resp.get("chain", "")
    case.pay_address = resp.get("pay_address", "")
    case.pay_amount = resp.get("pay_amount")
    case.api_done_at = timezone.now()
    case.save(
        update_fields=[
            "crypto",
            "chain",
            "pay_address",
            "pay_amount",
            "api_done_at",
        ]
    )

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
    case.chain_paid_at = timezone.now()
    case.save(
        update_fields=[
            "tx_hash",
            "payer_address",
            "status",
            "chain_paid_at",
        ]
    )

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


@shared_task(ignore_result=True, soft_time_limit=120, time_limit=180, **_RETRY_KWARGS)
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
    case.api_done_at = timezone.now()
    case.save(
        update_fields=[
            "withdrawal_sys_no",
            "withdrawal_out_no",
            "tx_hash",
            "status",
            "api_done_at",
        ]
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
        terminal_deposit = {
            DepositStressCaseStatus.SUCCEEDED,
            DepositStressCaseStatus.FAILED,
            DepositStressCaseStatus.SKIPPED,
        }

        non_terminal_invoices = stress_run.cases.exclude(status__in=terminal_invoice)
        non_terminal_withdrawals = stress_run.withdrawal_cases.exclude(
            status__in=terminal_withdrawal
        )
        non_terminal_deposits = stress_run.deposit_cases.exclude(
            status__in=terminal_deposit
        )

        skipped_count = (
            non_terminal_invoices.count()
            + non_terminal_withdrawals.count()
            + non_terminal_deposits.count()
        )
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
        non_terminal_deposits.update(
            status=DepositStressCaseStatus.SKIPPED,
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


# ── 充币压测 ──────────────────────────────────────────────────


@shared_task(ignore_result=True, soft_time_limit=120, time_limit=180, **_RETRY_KWARGS)
def execute_deposit_case(case_id: int) -> None:
    """执行单个 DepositStressCase 的完整流程：获取地址 → 模拟充值 → 等待 webhook。"""
    try:
        case = DepositStressCase.objects.select_related("stress_run__project").get(
            pk=case_id
        )
    except DepositStressCase.DoesNotExist:
        return

    if case.status != DepositStressCaseStatus.PENDING:
        return

    case.started_at = timezone.now()
    case.status = DepositStressCaseStatus.CREATING
    case.save(update_fields=["started_at", "status"])

    try:
        _execute_deposit(case)
    except Exception as exc:
        logger.exception("stress.deposit_case.failed", case_id=case.pk)
        case.status = DepositStressCaseStatus.FAILED
        case.error = str(exc)[:2000]
        case.finished_at = timezone.now()
        case.save(update_fields=["status", "error", "finished_at"])
        StressService.on_case_finished(case)


def _execute_deposit(case: DepositStressCase) -> None:
    """DepositStressCase 执行核心流程。"""
    # 阶段 1: 获取充值地址
    deposit_address = StressService.get_deposit_address(case)
    case.deposit_address = deposit_address
    case.api_done_at = timezone.now()
    case.save(update_fields=["deposit_address", "api_done_at"])

    # 阶段 2: 模拟链上充值
    # 等待 2 秒，确保区块时间戳晚于充值地址创建时间
    time.sleep(2)
    case.status = DepositStressCaseStatus.PAYING
    case.save(update_fields=["status"])

    payment_result = simulate_payment(
        to_address=case.deposit_address,
        chain_code=case.chain,
        crypto_symbol=case.crypto,
        amount=case.amount,
        payment_ref=f"deposit-{case.pk}",
    )
    case.tx_hash = payment_result["tx_hash"]
    case.payer_address = payment_result["payer_address"]
    case.status = DepositStressCaseStatus.PAID
    case.chain_paid_at = timezone.now()
    case.save(
        update_fields=[
            "tx_hash",
            "payer_address",
            "status",
            "chain_paid_at",
        ]
    )

    # 派发超时检查任务（15 分钟后）
    check_deposit_webhook_timeout.apply_async(
        args=[case.pk],
        eta=timezone.now() + timedelta(minutes=15),
    )


@shared_task(ignore_result=True)
def check_deposit_webhook_timeout(case_id: int) -> None:
    """检查 DepositStressCase 是否在超时前收到了 webhook。"""
    with transaction.atomic():
        try:
            case = DepositStressCase.objects.select_for_update().get(pk=case_id)
        except DepositStressCase.DoesNotExist:
            return

        if case.status != DepositStressCaseStatus.PAID:
            return

        case.status = DepositStressCaseStatus.FAILED
        case.error = "webhook 超时未收到（15 分钟）"
        case.finished_at = timezone.now()
        case.save(update_fields=["status", "error", "finished_at"])

    StressService.on_case_finished(case)
    _maybe_trigger_collection_verification(case.stress_run_id)


def _maybe_trigger_collection_verification(stress_run_id: int) -> None:
    """延迟调度归集验证任务。

    并发 webhook 处理时存在竞态：每个 handler 提交自己的 case 后检查
    其他 case 状态，但其他 handler 的事务可能尚未提交，导致所有 handler
    都认为还有 case 在 PAID 状态而不触发验证。

    解决方案：无条件延迟 15 秒调度。verify_deposit_collection 本身
    幂等且会检查前置条件，多次调度不会重复执行。
    """
    verify_deposit_collection.apply_async(
        args=[stress_run_id],
        countdown=15,
    )


@shared_task(ignore_result=True, soft_time_limit=1740, time_limit=1800)
@singleton_task(timeout=1800, use_params=True)
def verify_deposit_collection(stress_run_id: int) -> None:
    """Phase 2：触发归集并验证所有 WEBHOOK_OK 的 deposit cases 是否被正确归集。

    time_limit / singleton timeout 统一为 30 分钟：等待策略改成"uncollected 连续
    2 轮不下降"后，实际等待时长由归集流水线的真实吞吐决定；7 分钟硬顶会在
    5000+ 笔规模下抢先于进度判定触发，因此放宽到 30 分钟（按 1000 笔约 ~4 分钟
    的吞吐，30 分钟可覆盖约 ~7500 笔，足够覆盖当前压测规模）。singleton timeout
    与 hard time_limit 对齐，避免锁提前释放导致任务重入。
    """
    from deposits.models import Deposit
    from deposits.tasks import gather_deposits as _gather_deposits_task

    # 前置条件：如果还有 case 尚未通过 webhook 阶段，提前返回
    pre_webhook_states = {
        DepositStressCaseStatus.PENDING,
        DepositStressCaseStatus.CREATING,
        DepositStressCaseStatus.PAYING,
        DepositStressCaseStatus.PAID,
    }
    if DepositStressCase.objects.filter(
        stress_run_id=stress_run_id,
        status__in=pre_webhook_states,
    ).exists():
        return

    try:
        stress = StressRun.objects.select_related("project").get(pk=stress_run_id)
    except StressRun.DoesNotExist:
        return

    webhook_ok_cases = list(
        DepositStressCase.objects.filter(
            stress_run=stress,
            status=DepositStressCaseStatus.WEBHOOK_OK,
        )
    )
    if not webhook_ok_cases:
        logger.info(
            "stress.deposit_collection.no_webhook_ok_cases",
            stress_run_id=stress_run_id,
        )
        return

    # Transfer.hash 带 0x 前缀，case.tx_hash 可能不带，构建两种形式用于匹配
    tx_hashes = []
    for c in webhook_ok_cases:
        h = c.tx_hash
        tx_hashes.append(h)
        if not h.startswith("0x"):
            tx_hashes.append(f"0x{h}")
        else:
            tx_hashes.append(h.removeprefix("0x"))
    # 基于分阶段进度判定：区分"未匹配到 Deposit"、"已匹配但尚未建单"、
    # "已建单待链上确认" 三个阶段。CollectSchedule 重构后，归集任务会先经历
    # "等待 schedule 到期 -> 创建 DepositCollection -> 链上广播/确认" 三步，
    # 只看最终 uncollected 数会把"已建单但待确认"误判成无进展。
    expected_case_hashes = {c.tx_hash.removeprefix("0x") for c in webhook_ok_cases}
    stall_rounds = 0
    prev_progress_key: tuple[int, int, int] | None = None
    while True:
        # 同步调用归集逻辑
        try:
            _gather_deposits_task()
        except Exception:
            logger.warning("stress.deposit_collection.gather_failed", exc_info=True)

        deposits = list(
            Deposit.objects.filter(
                transfer__hash__in=tx_hashes,
                customer__project=stress.project,
                status="completed",
            )
            .select_related("transfer", "collection")
            .order_by("pk")
        )
        matched_hashes = {deposit.transfer.hash.removeprefix("0x") for deposit in deposits}
        missing_deposit_count = len(expected_case_hashes - matched_hashes)
        no_collection_count = sum(
            1 for deposit in deposits if deposit.collection_id is None
        )
        pending_confirm_count = sum(
            1
            for deposit in deposits
            if deposit.collection_id is not None
            and deposit.collection.collected_at is None
        )
        progress_key = (
            missing_deposit_count,
            no_collection_count,
            pending_confirm_count,
        )

        if progress_key == (0, 0, 0):
            logger.info(
                "stress.deposit_collection.all_collected",
                stress_run_id=stress_run_id,
            )
            break

        # 第 1 轮无参考值；从第 2 轮开始比较阶段进度。只要任一 case 从
        # "无 Deposit -> 无 Collection -> 待确认" 继续向前推进，就重置停滞计数。
        if prev_progress_key is not None and progress_key >= prev_progress_key:
            stall_rounds += 1
            if stall_rounds >= 2:
                logger.warning(
                    "stress.deposit_collection.stalled",
                    stress_run_id=stress_run_id,
                    missing_deposit_count=missing_deposit_count,
                    no_collection_count=no_collection_count,
                    pending_confirm_count=pending_confirm_count,
                )
                break
        else:
            stall_rounds = 0

        prev_progress_key = progress_key
        time.sleep(30)

    # 逐个验证归集结果
    for case in webhook_ok_cases:
        # Transfer.hash 带 0x，case.tx_hash 可能不带
        h = case.tx_hash
        hash_variants = [h, f"0x{h}"] if not h.startswith("0x") else [h, h.removeprefix("0x")]
        deposit = (
            Deposit.objects.filter(
                transfer__hash__in=hash_variants,
                customer__project=stress.project,
            )
            .select_related("collection")
            .first()
        )

        update_fields = [
            "collection_verified",
            "collection_hash",
            "status",
            "error",
            "finished_at",
        ]
        if (
            deposit
            and deposit.collection
            and deposit.collection.collected_at is not None
        ):
            case.collection_verified = True
            case.collection_hash = deposit.collection.collection_hash or ""
            case.status = DepositStressCaseStatus.SUCCEEDED
            case.collection_done_at = timezone.now()
            update_fields.append("collection_done_at")
        else:
            case.status = DepositStressCaseStatus.FAILED
            reason = "未找到 Deposit 记录" if not deposit else "归集未完成"
            case.error = reason

        case.finished_at = timezone.now()
        case.save(update_fields=update_fields)
        StressService.on_case_finished(case)
