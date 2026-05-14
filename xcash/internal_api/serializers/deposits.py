from rest_framework import serializers

from chains.serializers import TransferSerializer
from deposits.models import Deposit


class InternalDepositDetailSerializer(serializers.ModelSerializer):
    tx = TransferSerializer(source="transfer", read_only=True)
    uid = serializers.CharField(source="customer.uid", read_only=True)
    crypto = serializers.SlugRelatedField(
        source="transfer.crypto", slug_field="symbol", read_only=True
    )
    chain = serializers.SlugRelatedField(
        source="transfer.chain", slug_field="code", read_only=True
    )

    class Meta:
        model = Deposit
        fields = [
            "sys_no",
            "uid",
            "crypto",
            "chain",
            "worth",
            "status",
            "risk_level",
            "risk_score",
            "tx",
            "created_at",
            "updated_at",
        ]
