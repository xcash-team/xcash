from decimal import Decimal

from django.db import IntegrityError
from django.db import transaction as db_transaction
from rest_framework import status
from rest_framework import viewsets
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from chains.models import Chain
from common.consts import APPID_HEADER
from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.permission_check import check_saas_permission
from common.permissions import RejectAll
from common.throttles import WithdrawalCreateThrottle
from currencies.service import CryptoService
from projects.models import Project
from users.models import Customer
from withdrawals.models import Withdrawal
from withdrawals.models import WithdrawalStatus
from withdrawals.serializers import CreateWithdrawalSerializer
from withdrawals.service import WithdrawalService


class WithdrawalViewSet(viewsets.ModelViewSet):
    queryset = Withdrawal.objects.all()

    def get_permissions(self):
        if self.action == "create":
            permission_classes = [AllowAny]
        else:
            permission_classes = [RejectAll]
        return [permission() for permission in permission_classes]

    def get_throttles(self):
        if self.action == "create":
            return [WithdrawalCreateThrottle()]
        return []

    @db_transaction.atomic
    def create(self, request, *args, **kwargs):
        # SaaS 模式：校验该 project 是否有权限发起提币
        check_saas_permission(
            appid=request.headers.get(APPID_HEADER),
            action="withdrawal",
        )

        serializer = CreateWithdrawalSerializer(
            data=request.data,
            context={"request": request},
        )
        if not serializer.is_valid():
            raise APIError(ErrorCode.PARAMETER_ERROR, detail=serializer.errors)
        validated_data = serializer.validated_data

        project = Project.retrieve(appid=request.headers.get(APPID_HEADER))
        if project is None:
            raise APIError(ErrorCode.INVALID_APPID)
        # 项目级风控（尤其日限额）依赖数据库锁复核，避免并发请求在 serializer 之后同时越过额度。
        project = Project.objects.select_for_update().get(pk=project.pk)

        chain = Chain.objects.get(code=validated_data["chain"])
        # 提币入口只能操作正式启用的资产，占位币不能进入出金链路。
        crypto = CryptoService.get_by_symbol(validated_data["crypto"])
        amount = validated_data["amount"]
        worth = WithdrawalService.assert_project_policy(
            project=project,
            chain=chain,
            crypto=crypto,
            to=validated_data["to"],
            amount=amount,
        )
        # 审核是否必需依赖项目配置与本单美元价值；低于免审核门槛时直接进入发送队列。
        should_require_review = WithdrawalService.should_require_review(
            project=project,
            worth=worth,
        )

        if validated_data["uid"] is not None:
            customer, _ = Customer.objects.get_or_create(
                project=project, uid=validated_data["uid"]
            )
        else:
            customer = None

        try:
            withdrawal = Withdrawal.objects.create(
                project=project,
                out_no=validated_data["out_no"],
                to=validated_data["to"],
                customer=customer,
                chain=chain,
                crypto=crypto,
                amount=amount,
                worth=worth,
                status=(
                    WithdrawalStatus.REVIEWING
                    if should_require_review
                    else WithdrawalStatus.PENDING
                ),
            )
        except IntegrityError as exc:
            # 数据库唯一约束才是真正的幂等边界；并发重复 out_no 不能返回 500。
            raise APIError(
                ErrorCode.DUPLICATE_OUT_NO, detail=validated_data["out_no"]
            ) from exc
        if worth == Decimal("0"):
            # 未配置限额时 worth 仍需补齐，保证列表展示与后续风控字段完整。
            WithdrawalService.initialize_withdrawal(withdrawal)

        if not should_require_review:
            # 关闭审核或命中免审核门槛时，推进到链上发送队列。
            withdrawal = WithdrawalService.submit_withdrawal(
                withdrawal=withdrawal
            )

        return Response(
            {
                "sys_no": withdrawal.sys_no,
                "hash": withdrawal.hash or "",
                "status": withdrawal.status,
            },
            status=status.HTTP_200_OK,
        )
