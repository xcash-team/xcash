from rest_framework import serializers

from chains.serializers import TransferSerializer
from common.consts import MAX_INVOICE_DURATION
from common.consts import MIN_INVOICE_DURATION
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
        min_value=MIN_INVOICE_DURATION,
        max_value=MAX_INVOICE_DURATION,
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
            "notify_url",
            "return_url",
            "duration",
        ]

    def validate_out_no(self, value):
        project = self.context.get("project")
        if project and Invoice.objects.filter(project=project, out_no=value).exists():
            raise APIError(ErrorCode.DUPLICATE_OUT_NO)
        return value


class InternalInvoiceDetailSerializer(serializers.ModelSerializer):
    tx = TransferSerializer(source="transfer", read_only=True)
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
            "tx",
            "started_at",
            "expires_at",
            "notify_url",
            "return_url",
            "created_at",
            "updated_at",
        ]
