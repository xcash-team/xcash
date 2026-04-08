import structlog
from celery import shared_task
from django.db import transaction as db_transaction
from django.db.models import Count
from django.db.models import F
from django.db.models import Min
from django.db.models import OuterRef
from django.db.models import Q
from django.db.models import Subquery
from django.db.models import Value
from django.db.models.functions import Coalesce

from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from common.decorators import singleton_task
from common.time import ago
from evm.constants import EVM_PIPELINE_DEPTH
from evm.coordinator import InternalEvmTaskCoordinator
from evm.models import EvmBroadcastTask
from evm.scanner.rpc import EvmScannerRpcError
from evm.scanner.service import EvmChainScannerService

logger = structlog.get_logger()


@shared_task(ignore_result=True)
def broadcast_evm_task(pk: int) -> None:
    # 任务入口统一使用 BroadcastTask 命名，避免继续暴露旧的广播载荷概念。
    broadcast_task = EvmBroadcastTask.objects.select_related("base_task").get(pk=pk)
    if broadcast_task.base_task_id:
        # 已进入待确认/已结束的任务不应再重复广播。
        if (
            broadcast_task.base_task.result != BroadcastTaskResult.UNKNOWN
            or broadcast_task.base_task.stage
            not in (BroadcastTaskStage.QUEUED, BroadcastTaskStage.PENDING_CHAIN)
        ):
            return
    if broadcast_task.has_lower_queued_nonce() or broadcast_task.is_pipeline_full():
        logger.info(
            "EVM 广播被阻断",
            task_pk=broadcast_task.pk,
            address=broadcast_task.address.address,
            chain=broadcast_task.chain.code,
            nonce=broadcast_task.nonce,
            reason="lower_queued_nonce"
            if broadcast_task.has_lower_queued_nonce()
            else "pipeline_full",
        )
        return
    broadcast_task.broadcast()
    # 广播成功后，链式调度同地址下一个 QUEUED nonce，快速填充 pipeline。
    _chain_dispatch_next(broadcast_task)


def _chain_dispatch_next(completed_task: EvmBroadcastTask) -> None:
    """广播成功后立即调度同地址下一个 QUEUED nonce，避免等待下一轮 dispatch 周期。"""
    if completed_task.is_pipeline_full():
        return
    next_task = (
        EvmBroadcastTask.objects.select_related("base_task")
        .filter(
            address=completed_task.address,
            chain=completed_task.chain,
            base_task__stage=BroadcastTaskStage.QUEUED,
            base_task__result=BroadcastTaskResult.UNKNOWN,
        )
        .order_by("nonce")
        .first()
    )
    if next_task is not None:
        broadcast_evm_task.delay(next_task.pk)


@shared_task(ignore_result=True)
@singleton_task(timeout=64)
@db_transaction.atomic
def dispatch_due_evm_broadcast_tasks() -> None:
    """定时调度 QUEUED 状态的 EVM 广播任务。

    由 Celery Beat 每 5 秒执行一次，从队列中挑选到期任务投递给 broadcast_evm_task 实际广播。
    调度规则：
    - 每个 (address, chain) 只放行最低 nonce 的任务，保证 nonce 按顺序进入 mempool
    - pipeline 未满（同地址 PENDING_CHAIN < EVM_PIPELINE_DEPTH）才放行
    - 4 分钟内已尝试过的不重复投递
    - 每轮最多投递 8 笔
    """
    # 每个 (address, chain) 中最低 QUEUED nonce——保证 nonce 按顺序提交到 mempool。
    min_queued_nonce_subquery = (
        EvmBroadcastTask.objects.filter(
            address_id=OuterRef("address_id"),
            chain_id=OuterRef("chain_id"),
            base_task__stage=BroadcastTaskStage.QUEUED,
            base_task__result=BroadcastTaskResult.UNKNOWN,
        )
        .order_by()
        .values("address_id", "chain_id")
        .annotate(min_nonce=Min("nonce"))
        .values("min_nonce")[:1]
    )
    # 每个 (address, chain) 当前在 mempool 中等待确认的交易数量。
    pending_count_subquery = (
        EvmBroadcastTask.objects.filter(
            address_id=OuterRef("address_id"),
            chain_id=OuterRef("chain_id"),
            base_task__stage=BroadcastTaskStage.PENDING_CHAIN,
            base_task__result=BroadcastTaskResult.UNKNOWN,
        )
        .order_by()
        .values("address_id", "chain_id")
        .annotate(cnt=Count("id"))
        .values("cnt")[:1]
    )
    queryset = (
        EvmBroadcastTask.objects.select_for_update()
        .select_related("base_task")
        .annotate(
            min_queued_nonce=Subquery(min_queued_nonce_subquery),
            pending_count=Coalesce(Subquery(pending_count_subquery), Value(0)),
        )
        .filter(
            Q(last_attempt_at__isnull=True) | Q(last_attempt_at__lt=ago(minutes=4)),
            created_at__lt=ago(seconds=4),
            # 只放行 QUEUED 任务；PENDING_CHAIN 的超时重广播由 reconcile_chain 负责。
            base_task__stage=BroadcastTaskStage.QUEUED,
            base_task__result=BroadcastTaskResult.UNKNOWN,
            nonce=F("min_queued_nonce"),
        )
        .exclude(pending_count__gte=EVM_PIPELINE_DEPTH)
        .order_by("created_at")[:8]
    )

    for broadcast_task in queryset:
        if broadcast_task.has_lower_queued_nonce() or broadcast_task.is_pipeline_full():
            continue
        # 事务提交后再投递广播任务，避免事务回滚时子任务执行"已回滚"的状态（与 Bitcoin 路径对齐）。
        task_pk = broadcast_task.pk
        db_transaction.on_commit(
            lambda pk=task_pk: broadcast_evm_task.delay(pk)
        )


@shared_task(ignore_result=True)
@singleton_task(timeout=48, use_params=True)
def scan_evm_chain(chain_pk: int) -> None:
    """按链执行一次 EVM 自扫描，同时扫描原生币直转和 ERC20 Transfer。"""
    from chains.models import Chain

    chain = Chain.objects.get(pk=chain_pk)
    if not chain.active:
        return

    try:
        summary = EvmChainScannerService.scan_chain(chain=chain)
    except EvmScannerRpcError:
        # RPC 失败已在游标层记录，任务层只保留简洁日志，避免重复堆叠异常噪音。
        logger.warning("EVM 自扫描 RPC 失败", chain=chain.code)
        return

    internal_failed = InternalEvmTaskCoordinator.reconcile_chain(chain=chain)

    logger.info(
        "EVM 自扫描完成",
        chain=chain.code,
        native_from=summary.native.from_block,
        native_to=summary.native.to_block,
        native_observed=summary.native.observed_transfers,
        native_created=summary.native.created_transfers,
        erc20_from=summary.erc20.from_block,
        erc20_to=summary.erc20.to_block,
        erc20_logs=summary.erc20.observed_logs,
        erc20_created=summary.erc20.created_transfers,
        internal_failed=internal_failed,
    )


@shared_task(ignore_result=True)
def scan_active_evm_chains() -> None:
    """批量调度所有启用中的 EVM 链自扫描任务。"""
    from chains.models import Chain
    from chains.models import ChainType

    for chain_pk in Chain.objects.filter(
        active=True,
        type=ChainType.EVM,
    ).values_list("pk", flat=True):
        scan_evm_chain.delay(chain_pk)
