from celery import shared_task

from chains.adapters import AdapterFactory
from chains.adapters import TxCheckStatus
from chains.models import Chain
from chains.models import ConfirmMode
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from common.decorators import singleton_task
from common.time import ago


@shared_task(ignore_result=True)
@singleton_task(timeout=5, use_params=True)
def process_transfer(pk):
    transfer = OnchainTransfer.objects.get(pk=pk)
    transfer.process()


@shared_task
def fallback_process_transfer():
    for transfer in OnchainTransfer.objects.filter(
        processed_at__isnull=True,
        created_at__lte=ago(seconds=30),
    ):
        process_transfer.delay(transfer.pk)


@shared_task(
    ignore_result=True,
    bind=True,
    max_retries=5,
    time_limit=10,
)
@singleton_task(timeout=5, use_params=True)
def confirm_transfer(self, pk):
    try:
        transfer = OnchainTransfer.objects.get(pk=pk)
    except OnchainTransfer.DoesNotExist:
        # OnchainTransfer 已被 drop() 删除，无需再处理
        return
    if transfer.status == TransferStatus.CONFIRMED:
        return

    adapter = AdapterFactory.get_adapter(transfer.chain.type)
    result = adapter.tx_result(chain=transfer.chain, tx_hash=transfer.hash)

    if isinstance(result, Exception):
        # 指数退避：8s → 16s → 32s → 64s → 128s，避免节点抖动时密集重试。
        countdown = 8 * (2**self.request.retries)
        raise self.retry(exc=result, countdown=countdown)
    if result == TxCheckStatus.CONFIRMED:
        transfer.confirm()
    elif result == TxCheckStatus.CONFIRMING:
        if self.request.retries >= self.max_retries:
            transfer.drop()
            return
        countdown = 8 * (2**self.request.retries)
        raise self.retry(
            exc=RuntimeError(f"交易 receipt 暂不可见: {transfer.hash}"),
            countdown=countdown,
        )
    elif result == TxCheckStatus.DROPPED:
        transfer.drop()
    elif result == TxCheckStatus.FAILED:
        raise RuntimeError(
            "失败交易不应存在 OnchainTransfer 记录；请检查扫描器与内部任务协调器语义"
        )


@shared_task(ignore_result=True)
def block_number_updated(chain_pk):
    batch_size = 16
    chain = Chain.objects.only("confirm_block_count", "latest_block_number").get(
        pk=chain_pk
    )
    base_qs = OnchainTransfer.objects.filter(
        chain=chain,
        status=TransferStatus.CONFIRMING,
        processed_at__isnull=False,
    )

    quick_pks = list(
        base_qs.filter(
            confirm_mode=ConfirmMode.QUICK,
        )
        .order_by("timestamp")[:batch_size]
        .values_list("pk", flat=True)
    )

    full_pks = list(
        base_qs.filter(
            confirm_mode=ConfirmMode.FULL,
            block__lte=chain.latest_block_number - chain.confirm_block_count,
            created_at__lte=ago(seconds=10),
        )
        .order_by("timestamp")[:batch_size]
        .values_list("pk", flat=True)
    )

    dispatched = quick_pks + full_pks
    for pk in dispatched:
        confirm_transfer.delay(pk)

    # 当任一模式满批时，可能还有积压；延迟自调度继续消化，避免大量转账等到下个区块才处理。
    if len(quick_pks) >= batch_size or len(full_pks) >= batch_size:
        block_number_updated.apply_async(args=(chain_pk,), countdown=2)


@shared_task(ignore_result=True, time_limit=40)
@singleton_task(timeout=5, use_params=True)
def update_the_latest_block(pk):
    chain = Chain.objects.get(pk=pk)
    old_latest_block = chain.latest_block_number

    chain.latest_block_number = chain.get_latest_block_number
    # 链高度刷新不依赖 save() 信号，直接 update 可减少实例级整行写入。
    Chain.objects.filter(pk=chain.pk).update(
        latest_block_number=chain.latest_block_number
    )

    if chain.latest_block_number > old_latest_block:
        block_number_updated.delay(chain.pk)


@shared_task
@singleton_task(timeout=5)
def update_latest_block():
    for chain in Chain.objects.filter(active=True):
        update_the_latest_block.delay(chain.pk)
