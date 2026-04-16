from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.operations import DepositCollectionSerializer
from internal_api.serializers.operations import GasRechargeSerializer
from internal_api.serializers.operations import VaultFundingSerializer
from internal_api.serializers.operations import WithdrawalReviewLogSerializer
from rest_framework.mixins import ListModelMixin
from rest_framework.mixins import RetrieveModelMixin
from rest_framework.permissions import IsAuthenticated
from rest_framework.viewsets import GenericViewSet

from deposits.models import DepositCollection
from deposits.models import GasRecharge
from withdrawals.models import VaultFunding
from withdrawals.models import WithdrawalReviewLog


class DepositCollectionViewSet(ListModelMixin, RetrieveModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = DepositCollectionSerializer

    def get_queryset(self):
        return DepositCollection.objects.filter(
            deposits__customer__project__appid=self.kwargs["project_appid"]
        ).select_related("transfer__crypto", "transfer__chain").distinct()


class GasRechargeViewSet(ListModelMixin, RetrieveModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = GasRechargeSerializer

    def get_queryset(self):
        return GasRecharge.objects.filter(
            deposit_address__customer__project__appid=self.kwargs["project_appid"]
        ).select_related(
            "deposit_address__address", "transfer__crypto", "transfer__chain"
        )


class VaultFundingViewSet(ListModelMixin, RetrieveModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = VaultFundingSerializer

    def get_queryset(self):
        return VaultFunding.objects.filter(
            project__appid=self.kwargs["project_appid"]
        ).select_related("transfer__crypto", "transfer__chain")


class WithdrawalReviewLogViewSet(ListModelMixin, RetrieveModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = WithdrawalReviewLogSerializer

    def get_queryset(self):
        return WithdrawalReviewLog.objects.filter(
            project__appid=self.kwargs["project_appid"]
        ).select_related("withdrawal", "actor").order_by("-created_at")
