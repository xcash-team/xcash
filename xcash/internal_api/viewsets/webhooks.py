from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.webhooks import DeliveryAttemptSerializer
from internal_api.serializers.webhooks import WebhookEventSerializer
from rest_framework.mixins import ListModelMixin
from rest_framework.mixins import RetrieveModelMixin
from rest_framework.permissions import IsAuthenticated
from rest_framework.viewsets import GenericViewSet

from webhooks.models import DeliveryAttempt
from webhooks.models import WebhookEvent


class WebhookEventViewSet(ListModelMixin, RetrieveModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = WebhookEventSerializer

    def get_queryset(self):
        return WebhookEvent.objects.filter(
            project__appid=self.kwargs["project_appid"]
        ).order_by("-created_at")


class DeliveryAttemptViewSet(ListModelMixin, RetrieveModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = DeliveryAttemptSerializer

    def get_queryset(self):
        return DeliveryAttempt.objects.filter(
            event__project__appid=self.kwargs["project_appid"]
        ).select_related("event").order_by("-created_at")
