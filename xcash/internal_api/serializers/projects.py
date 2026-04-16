from rest_framework import serializers

from chains.models import AddressUsage
from chains.models import ChainType
from projects.models import Project


class ProjectCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = ["name", "webhook"]
        extra_kwargs = {"webhook": {"required": False}}


class ProjectUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = [
            "webhook",
            "webhook_open",
            "hmac_key",
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
    vault_address = serializers.SerializerMethodField()
    is_ready = serializers.SerializerMethodField()
    ready_errors = serializers.SerializerMethodField()

    class Meta:
        model = Project
        fields = [
            "appid",
            "name",
            "webhook",
            "webhook_open",
            "failed_count",
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
            "vault_address",
            "is_ready",
            "ready_errors",
            "active",
            "created_at",
        ]

    def get_vault_address(self, obj):
        try:
            addr = obj.wallet.get_address(
                chain_type=ChainType.EVM, usage=AddressUsage.Vault,
            )
            return addr.address
        except Exception:
            return None

    def get_is_ready(self, obj):
        ready, _ = obj.is_ready
        return ready

    def get_ready_errors(self, obj):
        _, errors = obj.is_ready
        return [str(e) for e in errors]
