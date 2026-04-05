import structlog
from celery import shared_task
from django.db import transaction as db_transaction
from django.db.models import F
from django.db.models import Min
from django.db.models import OuterRef
from django.db.models import Q
from django.db.models import Subquery

from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from common.decorators import singleton_task
from common.time import ago
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
    if broadcast_task.has_lower_unsettled_nonce():
        logger.info(
            "EVM 广播被更低 nonce 阻断",
            task_pk=broadcast_task.pk,
            address=broadcast_task.address.address,
            chain=broadcast_task.chain.code,
            nonce=broadcast_task.nonce,
        )
        return
    broadcast_task.broadcast()


@shared_task(ignore_result=True)
@singleton_task(timeout=64)
@db_transaction.atomic
def dispatch_due_evm_broadcast_tasks() -> None:
    min_unsettled_nonce_subquery = (
        EvmBroadcastTask.objects.filter(
            address_id=OuterRef("address_id"),
            chain_id=OuterRef("chain_id"),
            base_task__stage__in=(
                BroadcastTaskStage.QUEUED,
                BroadcastTaskStage.PENDING_CHAIN,
            ),
            base_task__result=BroadcastTaskResult.UNKNOWN,
        )
        .order_by()
        .values("address_id", "chain_id")
        .annotate(min_nonce=Min("nonce"))
        .values("min_nonce")[:1]
    )
    queryset = (
        EvmBroadcastTask.objects.select_for_update()
        .select_related("base_task")
        .annotate(min_unsettled_nonce=Subquery(min_unsettled_nonce_subquery))
        .filter(
            Q(last_attempt_at__isnull=True) | Q(last_attempt_at__lt=ago(minutes=4)),
            created_at__lt=ago(seconds=4),
            # 只有“待执行/待上链”的未知任务才允许继续重试广播。
            base_task__stage__in=(
                BroadcastTaskStage.QUEUED,
                BroadcastTaskStage.PENDING_CHAIN,
            ),
            base_task__result=BroadcastTaskResult.UNKNOWN,
            nonce=F("min_unsettled_nonce"),
        )
        .order_by("created_at")[:8]
    )

    for broadcast_task in queryset:
        if broadcast_task.has_lower_unsettled_nonce():
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
