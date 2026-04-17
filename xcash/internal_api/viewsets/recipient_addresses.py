from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.recipient_addresses import (
    RecipientAddressCreateSerializer,
)
from internal_api.serializers.recipient_addresses import (
    RecipientAddressDetailSerializer,
)
from rest_framework.mixins import CreateModelMixin
from rest_framework.mixins import DestroyModelMixin
from rest_framework.mixins import ListModelMixin
from rest_framework.mixins import RetrieveModelMixin
from rest_framework.permissions import IsAuthenticated
from rest_framework.viewsets import GenericViewSet

from common.error_codes import ErrorCode
from common.exceptions import APIError
from projects.models import Project
from projects.models import RecipientAddress


class RecipientAddressViewSet(
    CreateModelMixin, ListModelMixin, RetrieveModelMixin, DestroyModelMixin, GenericViewSet
):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return RecipientAddress.objects.filter(
            project__appid=self.kwargs["project_appid"]
        ).order_by("-created_at", "-pk")

    def get_serializer_class(self):
        if self.action == "create":
            return RecipientAddressCreateSerializer
        return RecipientAddressDetailSerializer

    def perform_create(self, serializer):
        project = Project.retrieve(self.kwargs["project_appid"])
        if project is None:
            raise APIError(ErrorCode.PROJECT_NOT_FOUND)
        serializer.save(project=project)
