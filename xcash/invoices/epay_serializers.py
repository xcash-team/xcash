from __future__ import annotations

import re
from decimal import ROUND_HALF_UP
from decimal import Decimal

from rest_framework import serializers

# EPay V1 协议文档原文是 "Amount in yuan with max 2 decimals"，即「最多」两位小数。
# 真实 typecho/wordpress/discuz 等商户插件经常发送 "18" 或 "18.5"，因此放宽校验：
# 允许整数 / 一位小数 / 两位小数三种形式，最终统一 quantize 到两位归一化。
_EPAY_MONEY_PATTERN = re.compile(r"^\d+(?:\.\d{1,2})?$")


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
        # 仍仅接受字符串：签名校验依赖原始字符串形态，
        # 若上游传 Decimal 或其他类型，签名一致性无从谈起。
        if not isinstance(value, str) or not _EPAY_MONEY_PATTERN.fullmatch(value):
            raise serializers.ValidationError(
                "money must be a positive number string with at most 2 decimals."
            )

        # 整数部分上限 26：与 Python decimal 默认 prec=28 对齐
        # （26 整数 + 2 小数 = 28 位有效精度），避免 quantize 抛 InvalidOperation
        # 冒到 view 层 500。业务上「订单金额（元）」也不需要 1e26 级别。
        integer_part = value.split(".", 1)[0]
        if len(integer_part) > 26:
            raise serializers.ValidationError(
                "Ensure that the integer part has no more than 26 digits."
            )

        # 在 quantize 之前比较：当前正则限定 ≤ 2 位小数，quantize 是 no-op；
        # 但若未来正则放宽（例如允许 3 位），"0.005" 会被 quantize 成 "0.01" 绕过此校验。
        # 提前比较是面向未来的防线。
        money = Decimal(value)
        if money < Decimal("0.01"):
            raise serializers.ValidationError(
                "Ensure this value is greater than or equal to 0.01."
            )

        # quantize 统一为两位小数 Decimal，以便 EpayOrder.money 与 Invoice.amount 落库一致。
        return money.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
