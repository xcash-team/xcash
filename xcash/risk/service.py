from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import Any

import structlog
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone
from risk.clients import MistTrackRiskResult
from risk.clients import QuicknodeMistTrackClient
from risk.models import RiskAssessment
from risk.models import RiskAssessmentStatus
from risk.models import RiskSource
from risk.models import RiskTargetType

from chains.models import Chain
from chains.models import ChainType
from common.permission_check import _read_saas_perm
from core.runtime_settings import get_quicknode_misttrack_endpoint_url
from core.runtime_settings import get_risk_marking_cache_seconds
from core.runtime_settings import get_risk_marking_enabled
from core.runtime_settings import get_risk_marking_force_refresh_threshold_usd
from core.runtime_settings import get_risk_marking_threshold_usd
from deposits.models import Deposit
from invoices.models import Invoice

logger = structlog.get_logger()


class RiskMarkingService:
    @classmethod
    def mark_invoice(cls, invoice_id: int) -> None:
        invoice = (
            Invoice.objects.select_related(
                "transfer", "transfer__chain", "project"
            )
            .filter(pk=invoice_id)
            .first()
        )
        if invoice is None or invoice.transfer_id is None:
            return

        if not cls._is_risk_marking_allowed(invoice):
            cls._mark_skipped_invoice(invoice, "saas tier missing risk_marking permission")
            return

        if not get_risk_marking_enabled():
            cls._mark_skipped_invoice(invoice, "risk marking disabled")
            return

        if invoice.worth <= get_risk_marking_threshold_usd():
            cls._mark_skipped_invoice(invoice, "invoice below risk threshold")
            return

        cls._mark_target(
            target=invoice,
            target_type=RiskTargetType.INVOICE,
            worth=invoice.worth,
        )

    @classmethod
    def mark_deposit(cls, deposit_id: int) -> None:
        deposit = (
            Deposit.objects.select_related(
                "transfer", "transfer__chain", "customer", "customer__project"
            )
            .filter(pk=deposit_id)
            .first()
        )
        if deposit is None:
            return

        if not cls._is_risk_marking_allowed(deposit):
            cls._mark_skipped_deposit(deposit, "saas tier missing risk_marking permission")
            return

        if not get_risk_marking_enabled():
            cls._mark_skipped_deposit(deposit, "risk marking disabled")
            return

        if deposit.worth <= get_risk_marking_threshold_usd():
            cls._mark_skipped_deposit(deposit, "deposit below risk threshold")
            return

        cls._mark_target(
            target=deposit,
            target_type=RiskTargetType.DEPOSIT,
            worth=deposit.worth,
        )

    @classmethod
    def _is_risk_marking_allowed(cls, target: Invoice | Deposit) -> bool:
        """SaaS 模式下按 tier 的 enable_risk_marking 判定；自托管模式直接放行。

        语义（spec：xcash-saas docs/superpowers/specs/2026-05-14-tier-risk-marking-permission-design.md §5）：
        - 自托管（IS_SAAS=False）→ 放行，保持独立部署旧行为。
        - SaaS 模式 + 缓存命中 → 按 enable_risk_marking 判定。
        - SaaS 模式 + 冷缓存 → fail-closed，避免在权限不明时产生 MistTrack 成本。
        """
        if not settings.IS_SAAS:
            return True

        if isinstance(target, Invoice):
            appid = target.project.appid
        else:
            appid = target.customer.project.appid

        perm = _read_saas_perm(appid)
        if perm is None:
            logger.info(
                "risk_marking.saas_perm_unavailable",
                appid=appid,
                target_type=target.__class__.__name__,
                target_id=target.pk,
            )
            return False

        return bool(perm.get("enable_risk_marking", False))

    @classmethod
    def write_cache(
        cls,
        *,
        source: str,
        address: str,
        result: dict[str, Any],
        timeout: int,
    ) -> None:
        cache.set(cls._cache_key(source=source, address=address), result, timeout)

    @classmethod
    def _mark_target(cls, *, target: Invoice | Deposit, target_type: str, worth):
        transfer = target.transfer
        source = RiskSource.QUICKNODE_MISTTRACK
        endpoint_url = get_quicknode_misttrack_endpoint_url()
        if not endpoint_url:
            cls._mark_failed(target, target_type, "QuickNode MistTrack endpoint is empty")
            return

        address = transfer.from_address
        cached_result = None
        if worth <= get_risk_marking_force_refresh_threshold_usd():
            cached_result = cache.get(cls._cache_key(source=source, address=address))

        if cached_result is not None:
            cls._mark_success(target, target_type, cached_result)
            return

        try:
            chain = cls._misttrack_chain(transfer.chain)
            result = QuicknodeMistTrackClient(
                endpoint_url=endpoint_url
            ).address_risk_score(chain=chain, address=address)
        except Exception as exc:
            logger.warning(
                "risk_marking.quicknode_failed",
                target_type=target_type,
                target_id=target.pk,
                address=address,
                error=str(exc),
            )
            cls._mark_failed(target, target_type, str(exc))
            return

        payload = cls._result_to_cache_payload(result)
        cls.write_cache(
            source=source,
            address=address,
            result=payload,
            timeout=get_risk_marking_cache_seconds(),
        )
        cls._mark_success(target, target_type, payload)

    @classmethod
    def _mark_success(
        cls, target: Invoice | Deposit, target_type: str, payload: dict[str, Any]
    ) -> None:
        now = timezone.now()
        risk_score = (
            Decimal(str(payload["risk_score"]))
            if payload.get("risk_score") is not None
            else None
        )
        defaults = {
            "source": RiskSource.QUICKNODE_MISTTRACK,
            "status": RiskAssessmentStatus.SUCCESS,
            "target_type": target_type,
            "address": target.transfer.from_address,
            "tx_hash": target.transfer.hash,
            "risk_level": payload.get("risk_level"),
            "risk_score": risk_score,
            "detail_list": payload.get("detail_list") or [],
            "risk_detail": payload.get("risk_detail") or {},
            "risk_report_url": payload.get("risk_report_url") or "",
            "raw_response": payload.get("raw_response") or {},
            "error_message": "",
            "checked_at": now,
        }
        cls._upsert_assessment(target, target_type, defaults)
        cls._sync_snapshot(target, payload.get("risk_level"), risk_score)

    @classmethod
    def _mark_failed(
        cls, target: Invoice | Deposit, target_type: str, error_message: str
    ) -> None:
        cls._upsert_assessment(
            target,
            target_type,
            {
                "source": RiskSource.QUICKNODE_MISTTRACK,
                "status": RiskAssessmentStatus.FAILED,
                "target_type": target_type,
                "address": target.transfer.from_address,
                "tx_hash": target.transfer.hash,
                "risk_level": None,
                "risk_score": None,
                "detail_list": [],
                "risk_detail": {},
                "risk_report_url": "",
                "raw_response": {},
                "error_message": error_message[:1000],
                "checked_at": timezone.now(),
            },
        )
        cls._sync_snapshot(target, None, None)

    @classmethod
    def _mark_skipped_invoice(cls, invoice: Invoice, reason: str) -> None:
        cls._mark_skipped(invoice, RiskTargetType.INVOICE, reason)

    @classmethod
    def _mark_skipped_deposit(cls, deposit: Deposit, reason: str) -> None:
        cls._mark_skipped(deposit, RiskTargetType.DEPOSIT, reason)

    @classmethod
    def _mark_skipped(
        cls, target: Invoice | Deposit, target_type: str, reason: str
    ) -> None:
        cls._upsert_assessment(
            target,
            target_type,
            {
                "source": RiskSource.QUICKNODE_MISTTRACK,
                "status": RiskAssessmentStatus.SKIPPED,
                "target_type": target_type,
                "address": target.transfer.from_address if target.transfer_id else "",
                "tx_hash": target.transfer.hash if target.transfer_id else "",
                "risk_level": None,
                "risk_score": None,
                "detail_list": [],
                "risk_detail": {},
                "risk_report_url": "",
                "raw_response": {},
                "error_message": reason,
                "checked_at": timezone.now(),
            },
        )
        cls._sync_snapshot(target, None, None)

    @staticmethod
    def _sync_snapshot(
        target: Invoice | Deposit, risk_level: str | None, risk_score: Decimal | None
    ) -> None:
        target.__class__.objects.filter(pk=target.pk).update(
            risk_level=risk_level,
            risk_score=risk_score,
            updated_at=timezone.now(),
        )

    @staticmethod
    @transaction.atomic
    def _upsert_assessment(
        target: Invoice | Deposit, target_type: str, defaults: dict[str, Any]
    ) -> None:
        lookup: dict[str, Any]
        if target_type == RiskTargetType.INVOICE:
            lookup = {"invoice": target}
            defaults["deposit"] = None
        else:
            lookup = {"deposit": target}
            defaults["invoice"] = None
        RiskAssessment.objects.update_or_create(defaults=defaults, **lookup)

    @staticmethod
    def _cache_key(*, source: str, address: str) -> str:
        return f"risk:{source}:{address.strip().lower()}"

    @staticmethod
    def _result_to_cache_payload(result: MistTrackRiskResult) -> dict[str, Any]:
        payload = asdict(result)
        if payload["risk_score"] is not None:
            payload["risk_score"] = str(payload["risk_score"])
        return payload

    @staticmethod
    def _misttrack_chain(chain: Chain) -> str:
        if chain.type == ChainType.BITCOIN:
            return "BTC"
        if chain.type == ChainType.TRON:
            return "TRX"
        if chain.type == ChainType.EVM:
            mapping = {
                1: "ETH",
                56: "BNB",
                137: "POLYGON",
                324: "ZKSYNC",
                4200: "MERLIN",
                4689: "IOTX",
                8453: "BASE",
                42161: "ARBITRUM",
                43114: "AVAX",
                10: "OPTIMISM",
            }
            if chain.chain_id in mapping:
                return mapping[chain.chain_id]
        raise RuntimeError(f"unsupported MistTrack chain: {chain.code}")
