from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from common.error_codes import ErrorCode
from common.exceptions import APIError
from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.epay import EpayMerchantDetailSerializer
from internal_api.serializers.epay import EpayMerchantUpdateSerializer
from invoices.models import EpayMerchant
from projects.models import Project


class EpayMerchantView(APIView):
    """EpayMerchant 在外部视角下是项目的单例配置。

    系统级 lazy create：项目创建时已经自动绑定 EpayMerchant，
    历史项目首次访问也会兜底创建；因此 GET 永远返回 200。
    PATCH 只允许修改 active 与 secret_key；pid 由系统分配，禁止外部修改。
    不暴露 DELETE — 每个项目必须始终具备 EPay 商户身份。
    """

    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    http_method_names = ["get", "patch", "head", "options"]

    def _get_project(self, appid: str) -> Project:
        project = Project.retrieve(appid)
        if project is None:
            raise APIError(ErrorCode.PROJECT_NOT_FOUND)
        return project

    def get(self, request, project_appid: str):
        project = self._get_project(project_appid)
        merchant = EpayMerchant.ensure_for_project(project)
        return Response(EpayMerchantDetailSerializer(merchant).data)

    def patch(self, request, project_appid: str):
        project = self._get_project(project_appid)
        merchant = EpayMerchant.ensure_for_project(project)
        serializer = EpayMerchantUpdateSerializer(
            instance=merchant, data=request.data, partial=True,
        )
        serializer.is_valid(raise_exception=True)
        merchant = serializer.save()
        return Response(EpayMerchantDetailSerializer(merchant).data)
