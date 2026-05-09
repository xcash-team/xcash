from __future__ import annotations

import re
from decimal import Decimal

from rest_framework import serializers

_EPAY_MONEY_PATTERN = re.compile(r"^\d+\.\d{2}$")


class EpaySubmitSerializer(serializers.Serializer):
    pid = serializers.IntegerField(min_value=1)
    type = serializers.CharField(max_length=32, allow_blank=True)
    out_trade_no = serializers.CharField(max_length=64)
    notify_url = serializers.URLField()
    return_url = serializers.URLField(allow_blank=True)
    name = serializers.CharField(max_length=128)
    money = serializers.CharField()
    param = serializers.CharField(max_length=512, allow_blank=True)
    sign = serializers.CharField(max_length=128)
    sign_type = serializers.CharField(max_length=16)

    def validate_sign_type(self, value: str) -> str:
        normalized = value.upper()
        if normalized != "MD5":
            raise serializers.ValidationError("EPay v1 only supports MD5 sign_type.")
        return normalized

    def validate_money(self, value: object) -> Decimal:
        if not isinstance(value, str) or not _EPAY_MONEY_PATTERN.fullmatch(value):
            raise serializers.ValidationError(
                "money must be submitted as a two-decimal string."
            )

        integer_part = value.split(".", 1)[0]
        if len(integer_part) > 30:
            raise serializers.ValidationError(
                "Ensure that there are no more than 32 digits in total."
            )

        money = Decimal(value)
        if money < Decimal("0.01"):
            raise serializers.ValidationError(
                "Ensure this value is greater than or equal to 0.01."
            )
        return money
