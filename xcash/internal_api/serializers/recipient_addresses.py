from django.utils.translation import gettext_lazy as _
from rest_framework import serializers

from projects.models import RecipientAddress


class RecipientAddressCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = RecipientAddress
        fields = ["name", "chain_type", "address", "usage"]
        # 禁用自动生成的唯一约束校验器，改用 validate 提供可读错误信息
        validators = []

    def validate(self, attrs):
        if RecipientAddress.objects.filter(
            chain_type=attrs["chain_type"], address=attrs["address"],
        ).exists():
            raise serializers.ValidationError(
                {"address": _("该地址已被使用，同一链类型下地址不能重复。")}
            )
        return attrs


class RecipientAddressDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = RecipientAddress
        fields = ["id", "name", "chain_type", "address", "usage", "created_at"]
