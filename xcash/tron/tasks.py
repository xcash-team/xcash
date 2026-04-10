import structlog
from celery import shared_task

from chains.models import Chain
from chains.models import ChainType
from common.decorators import singleton_task
from tron.client import TronClientError
from tron.scanner import TronUsdtPaymentScanner

logger = structlog.get_logger()


@shared_task(ignore_result=True)
@singleton_task(timeout=48, use_params=True)
def scan_tron_chain(chain_pk: int) -> None:
    chain = Chain.objects.get(pk=chain_pk)
    if not chain.active:
        return

    try:
        summary = TronUsdtPaymentScanner.scan_chain(chain=chain)
    except TronClientError:
        logger.warning("Tron USDT 扫描 RPC 失败", chain=chain.code)
        return

    logger.info(
        "Tron USDT 扫描完成",
        chain=chain.code,
        addresses_scanned=summary.addresses_scanned,
        events_seen=summary.events_seen,
        created_transfers=summary.created_transfers,
    )


@shared_task(ignore_result=True)
@singleton_task(timeout=64)
def scan_active_tron_chains() -> None:
    for chain_pk in Chain.objects.filter(
        active=True,
        type=ChainType.TRON,
    ).values_list("pk", flat=True):
        scan_tron_chain.delay(chain_pk)

