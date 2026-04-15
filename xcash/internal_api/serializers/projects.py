from rest_framework import serializers

from projects.models import Project


class ProjectCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = ["name", "webhook"]


class ProjectUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = [
            "webhook",
            "webhook_open",
            "ip_white_list",
            "fast_confirm_threshold",
            "pre_notify",
            "withdrawal_review_required",
            "withdrawal_review_exempt_limit",
            "withdrawal_single_limit",
            "withdrawal_daily_limit",
            "gather_worth",
            "gather_period",
        ]
        extra_kwargs = {field: {"required": False} for field in fields}


class ProjectDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = [
            "appid",
            "name",
            "webhook",
            "webhook_open",
            "ip_white_list",
            "hmac_key",
            "fast_confirm_threshold",
            "pre_notify",
            "withdrawal_review_required",
            "withdrawal_review_exempt_limit",
            "withdrawal_single_limit",
            "withdrawal_daily_limit",
            "gather_worth",
            "gather_period",
            "active",
            "created_at",
        ]
