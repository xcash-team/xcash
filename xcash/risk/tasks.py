from celery import shared_task
from risk.service import RiskMarkingService


@shared_task(ignore_result=True)
def mark_invoice_risk(invoice_id: int) -> None:
    RiskMarkingService.mark_invoice(invoice_id)


@shared_task(ignore_result=True)
def mark_deposit_risk(deposit_id: int) -> None:
    RiskMarkingService.mark_deposit(deposit_id)
