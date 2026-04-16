from rest_framework import serializers

from webhooks.models import DeliveryAttempt
from webhooks.models import WebhookEvent


class WebhookEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = WebhookEvent
        fields = [
            "id",
            "nonce",
            "payload",
            "status",
            "last_error",
            "delivered_at",
            "created_at",
        ]


class DeliveryAttemptSerializer(serializers.ModelSerializer):
    event_nonce = serializers.CharField(source="event.nonce", read_only=True)

    class Meta:
        model = DeliveryAttempt
        fields = [
            "id",
            "event_nonce",
            "try_number",
            "request_headers",
            "request_body",
            "response_status",
            "response_headers",
            "response_body",
            "duration_ms",
            "ok",
            "error",
            "created_at",
        ]
