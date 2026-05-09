from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from django.db import IntegrityError
from django.db import transaction
from django.utils import timezone

from common.permission_check import check_saas_permission

from .epay import EPAY_V1_SUCCESS_TEXT
from .epay import EPAY_V1_TRADE_SUCCESS
from .epay import build_epay_v1_sign
from .epay import format_epay_money
from .epay import verify_epay_v1_sign
from .epay_serializers import EpaySubmitSerializer
from .models import EpayMerchant
from .models import EpayOrder
from .models import Invoice
from .models import InvoiceProtocol
from .service import InvoiceService

if TYPE_CHECKING:
    from collections.abc import Mapping


class EpaySubmitError(Exception):
    pass


class EpaySubmitService:
    _IDEMPOTENT_EPAY_FIELDS = (
        "pid",
        "out_trade_no",
        "type",
        "name",
        "money",
        "notify_url",
        "return_url",
        "param",
        "sign_type",
    )

    @classmethod
    def submit(cls, raw_params: Mapping[str, object]) -> Invoice:
        serializer = EpaySubmitSerializer(data=raw_params)
        if not serializer.is_valid():
            raise EpaySubmitError(serializer.errors)

        params = serializer.validated_data
        merchant = cls._get_active_merchant(pid=params["pid"])
        if not verify_epay_v1_sign(
            cls._raw_sign_params(raw_params),
            merchant.signing_key,
        ):
            raise EpaySubmitError("invalid sign")

        check_saas_permission(
            appid=merchant.project.appid,
            action="invoice",
        )

        with transaction.atomic():
            existing_order = (
                cls._get_existing_order_for_update(
                    merchant=merchant,
                    out_trade_no=params["out_trade_no"],
                )
            )
            if existing_order is not None:
                cls._validate_idempotent_order(existing_order, params)
                return existing_order.invoice

            return cls._create_invoice_and_order(
                merchant=merchant,
                params=params,
                raw_request=cls._normalize_raw_request(raw_params),
            )

    @staticmethod
    def _get_active_merchant(*, pid: int) -> EpayMerchant:
        try:
            return EpayMerchant.objects.select_related("project").get(
                pid=pid,
                active=True,
            )
        except EpayMerchant.DoesNotExist as exc:
            raise EpaySubmitError("invalid pid") from exc

    @staticmethod
    def _get_existing_order_for_update(
        *,
        merchant: EpayMerchant,
        out_trade_no: str,
    ) -> EpayOrder | None:
        return (
            EpayOrder.objects.select_for_update()
            .select_related("invoice", "merchant")
            .filter(merchant=merchant, out_trade_no=out_trade_no)
            .first()
        )

    @classmethod
    def _validate_idempotent_order(
        cls,
        order: EpayOrder,
        params: dict,
    ) -> None:
        expected = cls._epay_order_values(params)
        mismatched_fields = [
            field
            for field in cls._IDEMPOTENT_EPAY_FIELDS
            if getattr(order, field) != expected[field]
        ]
        invoice = order.invoice
        if (
            invoice.project_id != order.merchant.project_id
            or invoice.out_no != params["out_trade_no"]
            or invoice.title != params["name"]
            or invoice.currency != order.merchant.default_currency
            or invoice.amount != params["money"]
            or invoice.redirect_url != params["return_url"]
            or invoice.protocol != InvoiceProtocol.EPAY_V1
        ):
            mismatched_fields.append("invoice")

        if mismatched_fields:
            raise EpaySubmitError(
                f"out_trade_no already exists with different metadata: "
                f"{', '.join(sorted(set(mismatched_fields)))}"
            )

    @classmethod
    def _create_invoice_and_order(
        cls,
        *,
        merchant: EpayMerchant,
        params: dict,
        raw_request: dict[str, str],
    ) -> Invoice:
        project = merchant.project
        try:
            # 内层 atomic 建立保存点，避免唯一约束冲突后污染外层幂等事务。
            with transaction.atomic():
                invoice = Invoice.objects.create(
                    project=project,
                    out_no=params["out_trade_no"],
                    title=params["name"],
                    currency=merchant.default_currency,
                    amount=params["money"],
                    methods=Invoice.available_methods(project),
                    redirect_url=params["return_url"],
                    expires_at=timezone.now() + timedelta(minutes=10),
                    protocol=InvoiceProtocol.EPAY_V1,
                )
                EpayOrder.objects.create(
                    invoice=invoice,
                    merchant=merchant,
                    trade_no=invoice.sys_no,
                    raw_request=raw_request,
                    **cls._epay_order_values(params),
                )
        except IntegrityError as exc:
            existing_order = cls._get_existing_order_for_update(
                merchant=merchant,
                out_trade_no=params["out_trade_no"],
            )
            if existing_order is None:
                raise EpaySubmitError("out_trade_no already exists") from exc
            cls._validate_idempotent_order(existing_order, params)
            return existing_order.invoice

        return InvoiceService.initialize_invoice(invoice)

    @staticmethod
    def _epay_order_values(params: dict) -> dict:
        return {
            "pid": str(params["pid"]),
            "out_trade_no": params["out_trade_no"],
            "type": params["type"],
            "name": params["name"],
            "money": params["money"],
            "notify_url": params["notify_url"],
            "return_url": params["return_url"],
            "param": params["param"],
            "sign_type": params["sign_type"],
        }

    @staticmethod
    def _normalize_raw_request(raw_params: Mapping[str, object]) -> dict[str, str]:
        return EpaySubmitService._raw_sign_params(raw_params)

    @staticmethod
    def _raw_sign_params(raw_params: Mapping[str, object]) -> dict[str, str]:
        return {
            str(key): EpaySubmitService._raw_value(value)
            for key, value in raw_params.items()
        }

    @staticmethod
    def _raw_value(value: object) -> str:
        if isinstance(value, (list, tuple)):
            value = value[-1] if value else ""
        elif hasattr(value, "getlist"):
            values = value.getlist()
            value = values[-1] if values else ""
        return str(value)

    # ── EPay 支付成功通知 ──

    @classmethod
    def build_notify_payload(cls, invoice: Invoice) -> dict[str, str]:
        epay_order = invoice.epay_order
        payload: dict[str, str] = {
            "pid": epay_order.pid,
            "trade_no": epay_order.trade_no,
            "out_trade_no": epay_order.out_trade_no,
            "type": epay_order.type,
            "name": epay_order.name,
            "money": format_epay_money(epay_order.money),
            "trade_status": EPAY_V1_TRADE_SUCCESS,
            "sign_type": epay_order.sign_type,
        }
        if epay_order.param:
            payload["param"] = epay_order.param
        payload["sign"] = build_epay_v1_sign(payload, epay_order.merchant.signing_key)
        return payload

    @classmethod
    def enqueue_paid_notify(cls, invoice: Invoice) -> "WebhookEvent":
        from webhooks.models import WebhookEvent
        from webhooks.service import WebhookService

        epay_order = invoice.epay_order
        payload = cls.build_notify_payload(invoice)
        return WebhookService.create_event(
            project=invoice.project,
            payload=payload,
            delivery_url=epay_order.notify_url,
            delivery_method=WebhookEvent.DeliveryMethod.GET_QUERY,
            expected_response_body=EPAY_V1_SUCCESS_TEXT,
        )
