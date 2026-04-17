from rest_framework import status as drf_status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from chains.models import Wallet
from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.projects import ProjectCreateSerializer
from internal_api.serializers.projects import ProjectDetailSerializer
from internal_api.serializers.projects import ProjectUpdateSerializer
from projects.models import Project


class ProjectViewSet(ModelViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    # Project 模型本身未在 Meta 里声明 ordering，启用全局分页后必须显式排序，
    # 否则 DRF 分页器会警告翻页结果可能重复/缺失。按创建时间倒序是列表页直觉顺序。
    queryset = Project.objects.all().order_by("-created_at", "-pk")
    lookup_field = "appid"

    def get_serializer_class(self):
        if self.action == "create":
            return ProjectCreateSerializer
        if self.action == "partial_update":
            return ProjectUpdateSerializer
        return ProjectDetailSerializer

    def perform_create(self, serializer):
        wallet = Wallet.generate()
        serializer.save(wallet=wallet)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        detail = ProjectDetailSerializer(serializer.instance)
        return Response(detail.data, status=drf_status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"])
    def activate(self, request, appid=None):
        project = self.get_object()
        project.active = True
        project.save(update_fields=["active"])
        return Response(ProjectDetailSerializer(project).data)

    @action(detail=True, methods=["post"])
    def deactivate(self, request, appid=None):
        project = self.get_object()
        project.active = False
        project.save(update_fields=["active"])
        return Response(ProjectDetailSerializer(project).data)
