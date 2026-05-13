from datetime import timedelta

import structlog
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from django.utils import timezone
from rest_framework import status
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from chains.service import ChainService
from common.consts import APPID_HEADER
from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.permission_check import check_saas_permission
from common.permissions import RejectAll
from common.throttles import InvoiceRetrieveThrottle
from common.throttles import InvoiceSelectMethodThrottle
from currencies.service import CryptoService
from projects.models import Project

from .models import Invoice
from .models import InvoiceStatus
from .serializers import InvoiceCreateSerializer
from .serializers import InvoiceDisplaySerializer
from .serializers import InvoicePublicSerializer
from .serializers import InvoiceSetCryptoChainSerializer
from .service import InvoiceService

logger = structlog.get_logger()


class InvoiceViewSet(viewsets.ModelViewSet):
    queryset = Invoice.objects.all()
    lookup_field = "sys_no"
    # 公开端点不需要身份认证；类级别移除 SessionAuthentication 同时消除 CSRF 强制检查，
    # 避免同源前端页面因携带 session cookie 而触发 CSRF 403。
    # 非公开 action 已由 RejectAll 拦截，无需依赖 authentication_classes。
    authentication_classes = []

    def get_serializer_class(self):
        if self.action == "create":
            return InvoiceCreateSerializer
        elif self.action == "select_method":
            return InvoiceSetCryptoChainSerializer
        elif self.action == "retrieve":
            # 公开端点使用精简序列化器，不暴露 appid/out_no 等商户内部信息。
            return InvoicePublicSerializer
        # 修复：RejectAll 覆盖了其他动作，但 DRF 在生成 schema / OPTIONS 时仍会访问 serializer；
        # 这里返回展示序列化器，避免无意义的 500。
        return InvoiceDisplaySerializer

    def get_permissions(self):
        if self.action in ("create", "select_method", "retrieve"):
            permission_classes = [AllowAny]
        else:
            permission_classes = [RejectAll]
        return [permission() for permission in permission_classes]

    def get_throttles(self):
        # retrieve 是前端公开轮询端点，select_method 则会消耗支付槽位配额。
        # 两者必须拆分限流桶，避免状态轮询意外耗尽切换支付方式额度。
        if self.action == "retrieve":
            return [InvoiceRetrieveThrottle()]
        if self.action == "select_method":
            return [InvoiceSelectMethodThrottle()]
        return super().get_throttles()

    def create(self, request, *args, **kwargs):
        # 不使用 @db_transaction.atomic：让 Invoice INSERT 立即提交，
        # 释放 Project 行上的 FOR KEY SHARE FK 锁，避免高并发下
        # 多事务争夺同一 Project tuple 的 MultiXact 锁元数据导致死锁。
        # IntegrityError（重复 out_no）在 autocommit 模式下仍可正常捕获。

        # SaaS 模式：Invoice 收款不属于充提币功能锁；这里只校验账号状态。
        check_saas_permission(
            appid=request.headers.get(APPID_HEADER),
            action="invoice",
        )

        serializer = self.get_serializer(
            data=request.data, context={"request": request}
        )
        if not serializer.is_valid():
            raise APIError(ErrorCode.PARAMETER_ERROR, detail=serializer.errors)
        validated_data = serializer.validated_data
        for crypto_symbol, chain_codes in validated_data["methods"].items():
            for chain_code in chain_codes:
                check_saas_permission(
                    appid=request.headers.get(APPID_HEADER),
                    action="invoice",
                    chain_code=chain_code,
                    crypto_symbol=crypto_symbol,
                )

        try:
            invoice = Invoice.objects.create(
                project=Project.retrieve(appid=request.headers.get(APPID_HEADER)),
                out_no=validated_data["out_no"],
                title=validated_data["title"],
                currency=validated_data["currency"],
                amount=validated_data["amount"],
                methods=validated_data["methods"],
                notify_url=validated_data.get("notify_url", ""),
                return_url=validated_data.get("return_url", ""),
                expires_at=timezone.now()
                + timedelta(minutes=validated_data["duration"]),
            )
        except IntegrityError as exc:
            # 重复 out_no 的最终幂等边界在数据库，命中唯一约束时要返回稳定业务错误码。
            raise APIError(
                ErrorCode.DUPLICATE_OUT_NO, detail=validated_data["out_no"]
            ) from exc
        # 账单初始化副作用显式走 service，替代隐式 post_save signal。
        InvoiceService.initialize_invoice(invoice)

        return Response(
            InvoiceDisplaySerializer(
                invoice,
                context={
                    "request": request,
                },
            ).data,
            status=status.HTTP_201_CREATED,
        )

    @action(methods=["post"], detail=True, url_path="select-method")
    def select_method(self, request, *args, **kwargs):
        serializer = self.get_serializer(
            data=request.data, context={"request": request}
        )
        if not serializer.is_valid():
            raise APIError(ErrorCode.PARAMETER_ERROR, detail=serializer.errors)
        validated_data = serializer.validated_data

        invoice: Invoice = self.get_object()

        if invoice.status != InvoiceStatus.WAITING:
            raise APIError(ErrorCode.INVALID_INVOICE_STATUS)

        if invoice.expires_at < timezone.now():
            raise APIError(ErrorCode.INVOICE_EXPIRED)

        if validated_data["crypto"] not in invoice.methods:
            raise APIError(ErrorCode.INVALID_CRYPTO)

        if validated_data["chain"] not in invoice.methods[validated_data["crypto"]]:
            raise APIError(ErrorCode.INVALID_CHAIN)

        check_saas_permission(
            appid=invoice.project.appid,
            action="invoice",
            chain_code=validated_data["chain"],
            crypto_symbol=validated_data["crypto"],
        )

        try:
            crypto = CryptoService.get_by_symbol(validated_data["crypto"])
            chain = ChainService.get_by_code(validated_data["chain"])
        except ObjectDoesNotExist as exc:
            raise APIError(ErrorCode.PARAMETER_ERROR) from exc

        try:
            invoice.select_method(crypto, chain)
        except Invoice.InvoiceAllocationError as exc:
            # 不透传异常内部信息给 API 调用方，避免泄露 project/crypto/chain 等内部标识。
            logger.warning("select_method allocation failed", detail=str(exc))
            raise APIError(ErrorCode.NO_RECIPIENT_ADDRESS) from exc
        except (KeyError, ValueError) as exc:
            # 价格数据缺失（KeyError）或精度溢出（ValueError）导致法币换算失败，
            # 属于系统配置问题而非调用方参数错误，记录 error 级别日志便于排查。
            logger.exception(
                "select_method price conversion failed",
                invoice=invoice.sys_no,
                crypto=validated_data["crypto"],
                exc=str(exc),
            )
            raise APIError(
                ErrorCode.PARAMETER_ERROR, detail="price unavailable"
            ) from exc

        return Response(
            InvoiceDisplaySerializer(
                invoice,
                context={
                    "request": request,
                },
            ).data,
            status=status.HTTP_200_OK,
        )
