import ipaddress
from decimal import Decimal

from rest_framework import serializers

from chains.models import AddressUsage
from chains.models import ChainType
from projects.models import Project


# 业务校验上下界，集中声明便于审计与调整。
HMAC_KEY_MIN_LENGTH = 16
# 模型层 ShortUUIDField(length=32) 硬性限制 max_length=32，
# 这里给出一个不超过模型上限的安全值；DRF 会合并 model 的 max_length 校验。
HMAC_KEY_MAX_LENGTH = 32
IP_WHITE_LIST_MAX_ENTRIES = 100
FAST_CONFIRM_THRESHOLD_MAX = Decimal("1000000")
WITHDRAWAL_LIMIT_MAX = Decimal("10000000")
GATHER_WORTH_MAX = Decimal("10000000")
GATHER_PERIOD_MIN = 1
GATHER_PERIOD_MAX = 525600


class ProjectCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = ["name", "webhook"]
        extra_kwargs = {"webhook": {"required": False}}


class ProjectUpdateSerializer(serializers.ModelSerializer):
    """商户可编辑的项目字段白名单，附带业务校验。

    与 ProjectDetailSerializer（只读展示）分离，严禁让 PATCH 回退到 Detail。
    """

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

    def validate_webhook(self, value: str) -> str:
        # URLField 已校验 URL 格式；此处额外要求必须 http/https（避免 ftp/javascript 等）。
        if value and not value.startswith(("http://", "https://")):
            raise serializers.ValidationError(
                "webhook 必须以 http:// 或 https:// 开头"
            )
        return value

    def validate_hmac_key(self, value: str) -> str:
        if len(value) < HMAC_KEY_MIN_LENGTH or len(value) > HMAC_KEY_MAX_LENGTH:
            raise serializers.ValidationError(
                f"hmac_key 长度需在 {HMAC_KEY_MIN_LENGTH}~{HMAC_KEY_MAX_LENGTH} 之间"
            )
        return value

    def validate_ip_white_list(self, value: str) -> str:
        """校验格式：`*`、空串、或逗号分隔的 IP/CIDR 列表。"""
        stripped = value.strip()
        if stripped == "" or stripped == "*":
            return stripped
        entries = [e.strip() for e in stripped.split(",") if e.strip()]
        if len(entries) > IP_WHITE_LIST_MAX_ENTRIES:
            raise serializers.ValidationError(
                f"IP 白名单最多 {IP_WHITE_LIST_MAX_ENTRIES} 条"
            )
        for entry in entries:
            try:
                # ip_network 同时接受纯 IP 和 CIDR 表示。
                ipaddress.ip_network(entry, strict=False)
            except ValueError:
                raise serializers.ValidationError(
                    f"IP 白名单格式不合法: {entry}"
                ) from None
        return stripped

    def validate_fast_confirm_threshold(self, value: Decimal) -> Decimal:
        if value < 0:
            raise serializers.ValidationError("fast_confirm_threshold 不能为负数")
        if value > FAST_CONFIRM_THRESHOLD_MAX:
            raise serializers.ValidationError(
                f"fast_confirm_threshold 不能超过 {FAST_CONFIRM_THRESHOLD_MAX}"
            )
        return value

    def _validate_positive_limit(
        self, value, field_name: str, max_value: Decimal,
    ):
        if value is None:
            return value
        if value <= 0:
            raise serializers.ValidationError(f"{field_name} 必须大于 0")
        if value > max_value:
            raise serializers.ValidationError(
                f"{field_name} 不能超过 {max_value}"
            )
        return value

    def validate_withdrawal_review_exempt_limit(self, value):
        return self._validate_positive_limit(
            value, "withdrawal_review_exempt_limit", WITHDRAWAL_LIMIT_MAX,
        )

    def validate_withdrawal_single_limit(self, value):
        return self._validate_positive_limit(
            value, "withdrawal_single_limit", WITHDRAWAL_LIMIT_MAX,
        )

    def validate_withdrawal_daily_limit(self, value):
        return self._validate_positive_limit(
            value, "withdrawal_daily_limit", WITHDRAWAL_LIMIT_MAX,
        )

    def validate_gather_worth(self, value: Decimal) -> Decimal:
        if value < 0:
            raise serializers.ValidationError("gather_worth 不能为负数")
        if value > GATHER_WORTH_MAX:
            raise serializers.ValidationError(
                f"gather_worth 不能超过 {GATHER_WORTH_MAX}"
            )
        return value

    def validate_gather_period(self, value: int) -> int:
        if value < GATHER_PERIOD_MIN:
            raise serializers.ValidationError(
                f"gather_period 不能小于 {GATHER_PERIOD_MIN}"
            )
        if value > GATHER_PERIOD_MAX:
            raise serializers.ValidationError(
                f"gather_period 不能超过 {GATHER_PERIOD_MAX}"
            )
        return value

    def validate(self, attrs):
        """跨字段校验：单笔限额不能超过日限额。

        PATCH 是局部更新，未提交的字段要从 instance 读取以拿到真实值。
        """
        instance = self.instance
        single = attrs.get(
            "withdrawal_single_limit",
            getattr(instance, "withdrawal_single_limit", None),
        )
        daily = attrs.get(
            "withdrawal_daily_limit",
            getattr(instance, "withdrawal_daily_limit", None),
        )
        if single is not None and daily is not None and single > daily:
            raise serializers.ValidationError({
                "withdrawal_single_limit": "单笔限额不能大于日限额",
            })
        return attrs


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
