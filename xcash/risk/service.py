from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import TYPE_CHECKING
from typing import Any

import structlog
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone
from risk.clients import MistTrackOpenApiClient
from risk.clients import MistTrackRiskResult
from risk.clients import QuicknodeMistTrackClient
from risk.misttrack_coin_map import OPENAPI_EVM_COIN
from risk.misttrack_coin_map import OPENAPI_TRON_COIN
from risk.misttrack_coin_map import QUICKNODE_EVM_CHAIN
from risk.models import RiskAssessment
from risk.models import RiskAssessmentStatus
from risk.models import RiskSkipReason
from risk.models import RiskSource
from risk.models import RiskTargetType

from chains.models import Chain
from chains.models import ChainType
from common.permission_check import _read_saas_perm
from core.runtime_settings import get_misttrack_openapi_api_key
from core.runtime_settings import get_quicknode_misttrack_endpoint_url
from core.runtime_settings import get_risk_marking_cache_seconds
from core.runtime_settings import get_risk_marking_enabled
from core.runtime_settings import get_risk_marking_force_refresh_threshold_usd
from core.runtime_settings import get_risk_marking_threshold_usd
from deposits.models import Deposit
from invoices.models import Invoice

if TYPE_CHECKING:
    from currencies.models import Crypto

logger = structlog.get_logger()


class UnsupportedRiskProviderChainError(RuntimeError):
    """provider 当前未覆盖该 chain/coin 的映射，应视为 SKIPPED 而非 FAILED。"""


class RiskMarkingService:
    @classmethod
    def mark_invoice(cls, invoice_id: int) -> None:
        invoice = (
            Invoice.objects.select_related(
                "transfer", "transfer__chain", "transfer__crypto", "project"
            )
            .filter(pk=invoice_id)
            .first()
        )
        if invoice is None or invoice.transfer_id is None:
            return

        # 以下三种"业务上根本不该走风控"的场景直接返回，不写 RiskAssessment：
        # - SaaS tier 未开权限：每个商户每笔都写记录会污染审计数据，权限本身在 saas tier 可查。
        # - 风控总开关关闭：会让所有 invoice/deposit 各写一条 SKIPPED，纯垃圾数据。
        # - 价值低于阈值：小额支付占绝大多数，要查"哪些单没风控"看 invoice.risk_level IS NULL 即可。
        if not cls._is_risk_marking_allowed(invoice):
            return
        if not get_risk_marking_enabled():
            return
        if invoice.worth <= get_risk_marking_threshold_usd():
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
                "transfer",
                "transfer__chain",
                "transfer__crypto",
                "customer",
                "customer__project",
            )
            .filter(pk=deposit_id)
            .first()
        )
        if deposit is None:
            return

        # 同 mark_invoice，"业务上不该走风控"的场景直接 return，不写 RiskAssessment。
        if not cls._is_risk_marking_allowed(deposit):
            return
        if not get_risk_marking_enabled():
            return
        if deposit.worth <= get_risk_marking_threshold_usd():
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
        chain: str = "",
        address: str,
        result: dict[str, Any],
        timeout: int,
    ) -> None:
        cache.set(
            cls._cache_key(source=source, chain=chain, address=address),
            result,
            timeout,
        )

    @classmethod
    def _mark_target(cls, *, target: Invoice | Deposit, target_type: str, worth):
        transfer = target.transfer
        provider = cls._select_provider()
        if provider is None:
            cls._mark_skipped(
                target,
                target_type,
                "Risk marking provider config is empty",
                skip_reason=RiskSkipReason.PROVIDER_NOT_CONFIGURED,
            )
            return

        address = transfer.from_address
        cached_result = None
        if worth <= get_risk_marking_force_refresh_threshold_usd():
            cached_result = cache.get(
                cls._cache_key(
                    source=provider["source"],
                    chain=transfer.chain.code,
                    address=address,
                )
            )
            if cached_result is None:
                cached_result = cache.get(
                    cls._cache_key(source=provider["source"], address=address)
                )

        if cached_result is not None:
            cls._mark_success(target, target_type, provider["source"], cached_result)
            return

        try:
            result = cls._query_provider(
                provider=provider,
                chain=transfer.chain,
                crypto=transfer.crypto,
                address=address,
            )
        except UnsupportedRiskProviderChainError as exc:
            cls._mark_skipped(
                target,
                target_type,
                str(exc),
                source=provider["source"],
                skip_reason=RiskSkipReason.UNSUPPORTED_CHAIN,
            )
            return
        except Exception as exc:
            logger.warning(
                "risk_marking.provider_failed",
                source=provider["source"],
                target_type=target_type,
                target_id=target.pk,
                address=address,
                error=str(exc),
            )
            cls._mark_failed(target, target_type, str(exc), source=provider["source"])
            return

        payload = cls._result_to_cache_payload(result)
        cls.write_cache(
            source=provider["source"],
            chain=transfer.chain.code,
            address=address,
            result=payload,
            timeout=get_risk_marking_cache_seconds(),
        )
        cls._mark_success(target, target_type, provider["source"], payload)

    @classmethod
    def _mark_success(
        cls,
        target: Invoice | Deposit,
        target_type: str,
        source: str,
        payload: dict[str, Any],
    ) -> None:
        now = timezone.now()
        risk_score = (
            Decimal(str(payload["risk_score"]))
            if payload.get("risk_score") is not None
            else None
        )
        defaults = {
            "source": source,
            "status": RiskAssessmentStatus.SUCCESS,
            "target_type": target_type,
            "address": target.transfer.from_address,
            "tx_hash": target.transfer.hash,
            "risk_level": payload.get("risk_level"),
            "risk_score": risk_score,
            "detail_list": payload.get("detail_list") or [],
            "risk_detail": payload.get("risk_detail") or [],
            "risk_report_url": payload.get("risk_report_url") or "",
            "raw_response": payload.get("raw_response") or {},
            "skip_reason": "",
            "error_message": "",
            "checked_at": now,
        }
        cls._upsert_assessment(target, target_type, defaults)
        cls._sync_snapshot(target, payload.get("risk_level"), risk_score)

    @classmethod
    def _mark_failed(
        cls,
        target: Invoice | Deposit,
        target_type: str,
        error_message: str,
        *,
        source: str = RiskSource.QUICKNODE_MISTTRACK,
    ) -> None:
        cls._upsert_assessment(
            target,
            target_type,
            {
                "source": source,
                "status": RiskAssessmentStatus.FAILED,
                "target_type": target_type,
                "address": target.transfer.from_address,
                "tx_hash": target.transfer.hash,
                "risk_level": None,
                "risk_score": None,
                "detail_list": [],
                "risk_detail": [],
                "risk_report_url": "",
                "raw_response": {},
                "skip_reason": "",
                "error_message": error_message[:1000],
                "checked_at": timezone.now(),
            },
        )
        cls._sync_snapshot(target, None, None)

    @classmethod
    def _mark_skipped(
        cls,
        target: Invoice | Deposit,
        target_type: str,
        reason: str,
        *,
        skip_reason: str,
        source: str = RiskSource.QUICKNODE_MISTTRACK,
    ) -> None:
        cls._upsert_assessment(
            target,
            target_type,
            {
                "source": source,
                "status": RiskAssessmentStatus.SKIPPED,
                "target_type": target_type,
                "address": target.transfer.from_address if target.transfer_id else "",
                "tx_hash": target.transfer.hash if target.transfer_id else "",
                "risk_level": None,
                "risk_score": None,
                "detail_list": [],
                "risk_detail": [],
                "risk_report_url": "",
                "raw_response": {},
                "skip_reason": skip_reason,
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
    def _cache_key(*, source: str, address: str, chain: str = "") -> str:
        if chain:
            return f"risk:{source}:{chain}:{address.strip().lower()}"
        return f"risk:{source}:{address.strip().lower()}"

    @staticmethod
    def _result_to_cache_payload(result: MistTrackRiskResult) -> dict[str, Any]:
        payload = asdict(result)
        if payload["risk_score"] is not None:
            payload["risk_score"] = str(payload["risk_score"])
        return payload

    @staticmethod
    def _select_provider() -> dict[str, str] | None:
        api_key = get_misttrack_openapi_api_key()
        if api_key:
            return {"source": RiskSource.MISTTRACK_OPENAPI, "api_key": api_key}

        endpoint_url = get_quicknode_misttrack_endpoint_url()
        if endpoint_url:
            return {
                "source": RiskSource.QUICKNODE_MISTTRACK,
                "endpoint_url": endpoint_url,
            }

        return None

    @classmethod
    def _query_provider(
        cls, *, provider: dict[str, str], chain: Chain, crypto: Crypto, address: str
    ) -> MistTrackRiskResult:
        if provider["source"] == RiskSource.MISTTRACK_OPENAPI:
            coin = cls._misttrack_openapi_coin(chain=chain, crypto=crypto)
            return MistTrackOpenApiClient(
                api_key=provider["api_key"]
            ).address_risk_score(coin=coin, address=address)

        quicknode_chain = cls._quicknode_misttrack_chain(chain)
        return QuicknodeMistTrackClient(
            endpoint_url=provider["endpoint_url"]
        ).address_risk_score(chain=quicknode_chain, address=address)

    @staticmethod
    def _quicknode_misttrack_chain(chain: Chain) -> str:
        if chain.type == ChainType.BITCOIN:
            return "BTC"
        if chain.type == ChainType.TRON:
            return "TRX"
        if chain.type == ChainType.EVM and chain.chain_id in QUICKNODE_EVM_CHAIN:
            return QUICKNODE_EVM_CHAIN[chain.chain_id]
        raise UnsupportedRiskProviderChainError(
            f"unsupported QuickNode MistTrack chain: {chain.code}"
        )

    @staticmethod
    def _misttrack_openapi_coin(*, chain: Chain, crypto: Crypto) -> str:
        symbol = crypto.symbol.upper()
        if chain.type == ChainType.BITCOIN and symbol == "BTC":
            return "BTC"
        if chain.type == ChainType.TRON and symbol in OPENAPI_TRON_COIN:
            return OPENAPI_TRON_COIN[symbol]
        if chain.type == ChainType.EVM:
            mapping = OPENAPI_EVM_COIN.get(chain.chain_id)
            if mapping and symbol in mapping:
                return mapping[symbol]
        raise UnsupportedRiskProviderChainError(
            f"unsupported MistTrack OpenAPI coin: {crypto.symbol} on {chain.code}"
        )
