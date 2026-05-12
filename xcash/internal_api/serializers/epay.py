from rest_framework import serializers

from invoices.models import EpayMerchant


# secret_key 长度上下界。下限与模型 _generate_secret_key 输出长度对齐。
EPAY_SECRET_KEY_MIN_LENGTH = 16
EPAY_SECRET_KEY_MAX_LENGTH = 128


class EpayMerchantDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = EpayMerchant
        fields = [
            "pid",
            "secret_key",
            "active",
            "created_at",
            "updated_at",
        ]
        # pid 由系统在创建时分配并对外只读，避免外部 SaaS 误改导致 EPay 客户端找不到商户。
        read_only_fields = ["pid", "created_at", "updated_at"]


class EpayMerchantUpdateSerializer(serializers.ModelSerializer):
    """商户可写字段白名单：仅允许修改 active 与 secret_key。

    EpayMerchant 由系统在创建项目时自动分配 pid 与初始 secret_key，
    外部 SaaS 不再持有"创建"权限；pid 是 EPay 客户端寻址主键，
    一旦分配就不能再变。
    """

    class Meta:
        model = EpayMerchant
        fields = ["secret_key", "active"]
        extra_kwargs = {field: {"required": False} for field in fields}

    def validate_secret_key(self, value: str) -> str:
        if len(value) < EPAY_SECRET_KEY_MIN_LENGTH:
            raise serializers.ValidationError(
                f"secret_key 长度不能少于 {EPAY_SECRET_KEY_MIN_LENGTH} 位"
            )
        if len(value) > EPAY_SECRET_KEY_MAX_LENGTH:
            raise serializers.ValidationError(
                f"secret_key 长度不能超过 {EPAY_SECRET_KEY_MAX_LENGTH} 位"
            )
        return value
