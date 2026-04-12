from decimal import Decimal

from django.core.exceptions import ObjectDoesNotExist
from django.urls import reverse
from django_otp.plugins.otp_email.conf import settings
from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.fields import EmailField
from rest_framework.serializers import Serializer

from chains.serializers import TransferSerializer
from chains.service import ChainService
from common.consts import APPID_HEADER
from common.consts import MAX_INVOICE_DURATION
from common.consts import MIN_INVOICE_DURATION
from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.serializers import StrippedDecimalField
from currencies.service import CryptoService
from currencies.service import FiatService
from projects.service import ProjectService

from .models import Invoice
from .models import InvoiceStatus


class InvoiceSetCryptoChainSerializer(Serializer):
    crypto = serializers.CharField(required=True)
    chain = serializers.CharField(required=True)

    def validate_crypto(self, value):  # noqa
        if value and not CryptoService.exists(value):
            raise ValidationError(detail=ErrorCode.INVALID_CRYPTO.to_payload())
        return value

    def validate_chain(self, value):  # noqa
        if not value:
            return value
        try:
            ChainService.get_by_code(value)
        except ObjectDoesNotExist as exc:
            raise ValidationError(detail=ErrorCode.INVALID_CHAIN.to_payload()) from exc
        return value

    def validate(self, attrs):
        if not self._is_chain_crypto_supported(attrs):
            raise APIError(ErrorCode.CHAIN_CRYPTO_NOT_SUPPORT)
        return attrs

    @staticmethod
    def _is_chain_crypto_supported(attrs) -> bool:
        if not attrs["chain"] or not attrs["crypto"]:
            return False
        try:
            chain = ChainService.get_by_code(attrs["chain"])
            crypto = CryptoService.get_by_symbol(attrs["crypto"])
        except ObjectDoesNotExist:
            return False
        return CryptoService.is_supported_on_chain(crypto, chain=chain)


class InvoiceCreateSerializer(Serializer):
    out_no = serializers.CharField(required=True, max_length=32)
    title = serializers.CharField(required=True, max_length=32)
    currency = serializers.CharField(required=True, max_length=8)
    amount = serializers.DecimalField(
        required=True,
        max_digits=32,
        decimal_places=8,
        min_value=Decimal("0.00000001"),
        max_value=Decimal(
            "1000000"
        ),  # 单笔上限 100 万，防止天文数字金额干扰汇率换算和差额分配
    )
    email = EmailField(required=False)

    duration = serializers.IntegerField(
        required=False,
        default=10,
        min_value=MIN_INVOICE_DURATION,
        max_value=MAX_INVOICE_DURATION,
    )
    methods = serializers.JSONField(required=False, default=dict)
    redirect_url = serializers.URLField(required=False)

    def _get_project(self):
        # 缓存到实例，避免 validate_out_no / validate_methods / validate 三处重复查询。
        if not hasattr(self, "_project"):
            request = self.context["request"]
            self._project = ProjectService.get_by_appid(
                request.headers.get(APPID_HEADER)
            )
        return self._project

    def validate_methods(self, value):  # noqa
        project = self._get_project()

        available_methods = Invoice.available_methods(project)

        if not available_methods:
            raise APIError(ErrorCode.NO_RECIPIENT_ADDRESS)

        if not value:
            return available_methods

        if not isinstance(value, dict):
            raise APIError(ErrorCode.PARAMETER_ERROR, detail="methods")

        sanitized: dict[str, list[str]] = {}
        for crypto_symbol, chain_codes in value.items():
            if not isinstance(chain_codes, (list, tuple)):
                raise APIError(ErrorCode.PARAMETER_ERROR, detail=crypto_symbol)

            try:
                CryptoService.get_by_symbol(crypto_symbol)
            except ObjectDoesNotExist as exc:
                raise APIError(ErrorCode.INVALID_CRYPTO, detail=crypto_symbol) from exc

            available_chains = set(available_methods.get(crypto_symbol, []))
            if not available_chains:
                raise APIError(ErrorCode.NO_RECIPIENT_ADDRESS, detail=crypto_symbol)

            normalized_codes: list[str] = []
            for chain_code in chain_codes:
                if not isinstance(chain_code, str):
                    raise APIError(ErrorCode.PARAMETER_ERROR, detail=f"{crypto_symbol}")

                try:
                    ChainService.get_by_code(chain_code)
                except ObjectDoesNotExist as exc:
                    raise APIError(ErrorCode.INVALID_CHAIN, detail=chain_code) from exc
                if chain_code not in available_chains:
                    raise APIError(
                        ErrorCode.NO_RECIPIENT_ADDRESS,
                        detail=f"{crypto_symbol}:{chain_code}",
                    )
                normalized_codes.append(chain_code)

            if normalized_codes:
                sanitized[crypto_symbol] = normalized_codes

        if not sanitized:
            raise APIError(ErrorCode.NO_RECIPIENT_ADDRESS)

        return sanitized

    def validate_currency(self, value):  # noqa
        if not (CryptoService.exists(value) or FiatService.exists(value)):
            raise APIError(ErrorCode.INVALID_INVOICE_CURRENCY)
        return value

    def validate_out_no(self, value):
        project = self._get_project()
        if Invoice.objects.filter(project=project, out_no=value).exists():
            raise APIError(ErrorCode.DUPLICATE_OUT_NO, detail=value)
        return value

    def validate(self, attrs):
        project = self._get_project()

        if not settings.DEBUG and (
            Invoice.objects.filter(
                project=project, status=InvoiceStatus.WAITING
            ).count()
            >= 100
        ):
            raise APIError(ErrorCode.TOO_MANY_WAITING)

        if CryptoService.exists(attrs["currency"]):
            currency = attrs["currency"]
            methods = attrs["methods"].get(currency, [])
            if not methods:
                raise APIError(ErrorCode.NO_AVAILABLE_METHOD)
            attrs["methods"] = {currency: methods}

        return attrs


class InvoicePublicSerializer(serializers.ModelSerializer):
    """公开 API（无需鉴权的 retrieve 端点）专用序列化器。

    仅暴露买家付款所需的最小字段集，不包含 appid、out_no 等商户内部信息。
    """

    crypto = serializers.CharField(
        source="crypto.symbol", read_only=True, allow_null=True
    )
    chain = serializers.CharField(source="chain.code", read_only=True, allow_null=True)
    amount = StrippedDecimalField(max_digits=32, decimal_places=8)
    pay_amount = StrippedDecimalField(max_digits=32, decimal_places=8)
    pay_url = serializers.SerializerMethodField()
    payment = TransferSerializer(source="transfer", read_only=True)

    def get_pay_url(self, obj: Invoice) -> str:
        pay_path = reverse("payment-invoice", kwargs={"sys_no": obj.sys_no})
        request = self.context.get("request")
        if request is None:
            return pay_path
        django_request = getattr(request, "_request", request)
        return django_request.build_absolute_uri(pay_path)

    class Meta:
        model = Invoice
        fields = (
            "sys_no",
            "title",
            "currency",
            "amount",
            "methods",
            "chain",
            "crypto",
            "crypto_address",
            "pay_address",
            "pay_amount",
            "pay_url",
            "started_at",
            "created_at",
            "expires_at",
            "redirect_url",
            "payment",
            "status",
        )


class InvoiceDisplaySerializer(serializers.ModelSerializer):
    """商户侧（需要鉴权的 create 响应）序列化器，包含完整商户信息。"""

    appid = serializers.CharField(
        source="project.appid", read_only=True, allow_null=True
    )
    crypto = serializers.CharField(
        source="crypto.symbol", read_only=True, allow_null=True
    )
    chain = serializers.CharField(source="chain.code", read_only=True, allow_null=True)
    amount = StrippedDecimalField(max_digits=32, decimal_places=8)
    pay_amount = StrippedDecimalField(max_digits=32, decimal_places=8)

    pay_url = serializers.SerializerMethodField()
    payment = TransferSerializer(source="transfer", read_only=True)

    def get_pay_url(self, obj: Invoice) -> str:
        pay_path = reverse("payment-invoice", kwargs={"sys_no": obj.sys_no})
        request = self.context.get("request")
        if request is None:
            return pay_path
        django_request = getattr(request, "_request", request)
        return django_request.build_absolute_uri(pay_path)

    class Meta:
        model = Invoice
        fields = (
            "appid",
            "sys_no",
            "out_no",
            "title",
            "currency",
            "amount",
            "methods",
            "chain",
            "crypto",
            "crypto_address",
            "pay_address",
            "pay_amount",
            "pay_url",
            "started_at",
            "created_at",
            "expires_at",
            "redirect_url",
            "payment",
            "status",
        )
