from django.utils import timezone
from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.invoices import InternalInvoiceCreateSerializer
from internal_api.serializers.invoices import InternalInvoiceDetailSerializer
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.permissions import RejectAll
from invoices.models import Invoice
from invoices.models import InvoiceGeneration
from invoices.service import InvoiceService
from projects.models import Project


class InternalInvoiceViewSet(ModelViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    lookup_field = "sys_no"

    def get_queryset(self):
        return Invoice.objects.filter(
            project__appid=self.kwargs["project_appid"]
        ).select_related("crypto", "chain", "transfer")

    def get_serializer_class(self):
        if self.action == "create":
            return InternalInvoiceCreateSerializer
        return InternalInvoiceDetailSerializer

    def get_permissions(self):
        if self.action in ("create", "list", "retrieve"):
            return [IsAuthenticated()]
        return [RejectAll()]

    def get_serializer_context(self):
        context = super().get_serializer_context()
        if self.action == "create":
            context["project"] = Project.retrieve(self.kwargs["project_appid"])
        return context

    def create(self, request, *args, **kwargs):
        project = Project.retrieve(self.kwargs["project_appid"])
        if project is None:
            raise APIError(ErrorCode.PROJECT_NOT_FOUND)

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        duration = data.pop("duration")
        now = timezone.now()
        invoice = serializer.save(
            project=project,
            started_at=now,
            expires_at=now + timezone.timedelta(minutes=duration),
            generated_by=InvoiceGeneration.API,
        )
        InvoiceService.initialize_invoice(invoice)
        return Response(
            InternalInvoiceDetailSerializer(invoice).data,
            status=201,
        )
