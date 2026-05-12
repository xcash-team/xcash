from __future__ import annotations

import re
from decimal import ROUND_HALF_UP
from decimal import Decimal

from rest_framework import serializers

from currencies.models import Fiat

# EPay V1 协议文档原文是 "Amount in yuan with max 2 decimals"，即「最多」两位小数。
# 真实 typecho/wordpress/discuz 等商户插件经常发送 "18" 或 "18.5"，因此放宽校验：
# 允许整数 / 一位小数 / 两位小数三种形式，最终统一 quantize 到两位归一化。
_EPAY_MONEY_PATTERN = re.compile(r"^\d+(?:\.\d{1,2})?$")


class EpaySubmitSerializer(serializers.Serializer):
    pid = serializers.IntegerField(min_value=1)
    type = serializers.CharField(max_length=32, allow_blank=True)
    out_trade_no = serializers.CharField(max_length=64)
    notify_url = serializers.URLField()
    # EPay V1 协议中 return_url（同步跳转地址）与 param（业务扩展参数）均为可选字段，
    # typecho/wordpress/discuz 等主流商户插件经常完全不发送，DRF 默认 required=True 会
    # 直接 400 拒掉合规请求，因此显式 required=False + default=""。
    # 签名层 (epay.py:epay_v1_signing_string) 已经会跳过空值，"" 与「不发」在签名上等价。
    return_url = serializers.URLField(required=False, allow_blank=True, default="")
    name = serializers.CharField(max_length=128)
    money = serializers.CharField()
    # currency 是 xcash 对 EPay V1 的扩展：标准协议没有该字段，默认按 CNY 元计价
    # （typecho/wordpress/discuz 等主流商户插件都不会传），因此设为可选，缺省落库 CNY。
    # 商户若显式传值则进入签名（签名层会自动把所有非 sign/sign_type 字段按字典序纳入），
    # 此时签名必须基于 raw_params 中的 currency 计算才能通过校验。
    # 无论传与不传，最终落库的 currency 必须命中 currencies.Fiat 表，避免下游计价崩溃。
    currency = serializers.CharField(max_length=8, required=False, default="CNY")
    param = serializers.CharField(
        max_length=512, required=False, allow_blank=True, default=""
    )
    sign = serializers.CharField(max_length=128)
    sign_type = serializers.CharField(max_length=16)

    def validate_sign_type(self, value: str) -> str:
        normalized = value.upper()
        if normalized != "MD5":
            raise serializers.ValidationError("EPay v1 only supports MD5 sign_type.")
        return normalized

    def validate_currency(self, value: str) -> str:
        # 大小写规范化到大写，避免商户传 "cny" 与系统数据 "CNY" 字面不一致；
        # 签名校验已在 validate 之前用 raw_params 完成，这里只决定落库形态。
        # 即便走 default="CNY" 分支，DRF 也会调用此方法，所以 Fiat 校验对
        # 「显式传值」与「缺省回退」都生效。
        normalized = value.strip().upper()
        # Fiat.code 是主键，存在即支持；不存在直接 400 拒掉，避免落库后下游计价崩溃。
        if not Fiat.objects.filter(code=normalized).exists():
            raise serializers.ValidationError(
                f"currency '{normalized}' is not a supported fiat code."
            )
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
