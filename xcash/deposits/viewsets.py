import re

from django.core.exceptions import ObjectDoesNotExist
from rest_framework import viewsets
from rest_framework.decorators import action as view_action
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response

from chains.models import Chain
from chains.capabilities import ChainProductCapabilityService
from common.consts import APPID_HEADER
from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.throttles import DepositAddressThrottle
from currencies.service import CryptoService
from deposits.models import DepositAddress
from projects.models import Project
from users.models import Customer

# uid 合法字符：字母、数字、下划线、中划线，长度 1~128
_UID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-]{1,128}$")


class DepositViewSet(viewsets.GenericViewSet):
    """
    充币相关接口。仅暴露 address 这一个 action，不注册 ModelViewSet 的默认 CRUD 路由。
    """

    @view_action(
        methods=["get"],
        detail=False,
        permission_classes=[AllowAny],
        throttle_classes=[DepositAddressThrottle],
    )
    def address(self, request: Request):
        """
        获取客户在指定链上的充币地址。

        请求头：XC-Appid
        Query 参数：uid、chain（链代码）、crypto（代币符号）
        """
        appid = request.headers.get(APPID_HEADER)
        project = Project.retrieve(appid=appid)
        if project is None:
            raise APIError(ErrorCode.INVALID_APPID)

        uid = request.GET.get("uid")
        chain_code = request.GET.get("chain")
        crypto_symbol = request.GET.get("crypto")

        if not uid or not _UID_PATTERN.match(uid):
            raise APIError(ErrorCode.INVALID_UID)

        try:
            chain = Chain.objects.get(code=chain_code, active=True)
        except Chain.DoesNotExist as exc:
            raise APIError(ErrorCode.INVALID_CHAIN) from exc

        try:
            crypto = CryptoService.get_by_symbol(crypto_symbol)
        except ObjectDoesNotExist as exc:
            raise APIError(ErrorCode.INVALID_CRYPTO) from exc

        # inactive 占位币不允许申请充币地址，避免用户入金后因币种未激活而没有 Deposit 记录。
        if not crypto.active:
            raise APIError(ErrorCode.INVALID_CRYPTO)

        if not crypto.support_this_chain(chain=chain):
            raise APIError(
                ErrorCode.CHAIN_CRYPTO_NOT_SUPPORT,
                detail=f"{crypto_symbol} 不支持 {chain_code} 链",
            )
        if not ChainProductCapabilityService.supports_deposit_address(
            chain=chain,
            crypto=crypto,
        ):
            raise APIError(ErrorCode.INVALID_CHAIN)

        customer, _ = Customer.objects.get_or_create(project=project, uid=uid)

        deposit_address = DepositAddress.get_address(chain, customer)
        return Response({"deposit_address": deposit_address})
