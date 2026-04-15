from rest_framework import serializers

from chains.serializers import TransferSerializer
from common.error_codes import ErrorCode
from common.exceptions import APIError
from invoices.models import Invoice


class InternalInvoiceCreateSerializer(serializers.ModelSerializer):
    """内网 Invoice 创建序列化器。

    project 由 ViewSet 从 URL 中的 appid 注入，不由客户端传入。
    """

    duration = serializers.IntegerField(
        required=False,
        default=10,
        min_value=5,
        max_value=60,
        help_text="支付有效期（分钟），默认 10",
    )

    class Meta:
        model = Invoice
        fields = [
            "out_no",
            "title",
            "currency",
            "amount",
            "methods",
            "email",
            "redirect_url",
            "duration",
        ]

    def validate_out_no(self, value):
        project = self.context.get("project")
        if project and Invoice.objects.filter(project=project, out_no=value).exists():
            raise APIError(ErrorCode.DUPLICATE_OUT_NO)
        return value


class InternalInvoiceDetailSerializer(serializers.ModelSerializer):
    payment = TransferSerializer(source="transfer", read_only=True)
    crypto = serializers.SlugRelatedField(slug_field="symbol", read_only=True)
    chain = serializers.SlugRelatedField(slug_field="code", read_only=True)

    class Meta:
        model = Invoice
        fields = [
            "sys_no",
            "out_no",
            "title",
            "currency",
            "amount",
            "methods",
            "email",
            "crypto",
            "chain",
            "pay_amount",
            "pay_address",
            "worth",
            "status",
            "payment",
            "started_at",
            "expires_at",
            "redirect_url",
            "generated_by",
            "created_at",
            "updated_at",
        ]
