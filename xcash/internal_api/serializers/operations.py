from rest_framework import serializers

from chains.serializers import TransferSerializer
from deposits.models import DepositCollection
from deposits.models import GasRecharge
from withdrawals.models import VaultFunding
from withdrawals.models import WithdrawalReviewLog


class DepositCollectionSerializer(serializers.ModelSerializer):
    tx = TransferSerializer(source="transfer", read_only=True)

    class Meta:
        model = DepositCollection
        fields = [
            "id",
            "collection_hash",
            "tx",
            "collected_at",
            "created_at",
            "updated_at",
        ]


class GasRechargeSerializer(serializers.ModelSerializer):
    tx = TransferSerializer(source="transfer", read_only=True)
    deposit_address = serializers.CharField(
        source="deposit_address.address.address", read_only=True
    )

    class Meta:
        model = GasRecharge
        fields = [
            "id",
            "deposit_address",
            "tx",
            "recharged_at",
            "created_at",
            "updated_at",
        ]


class VaultFundingSerializer(serializers.ModelSerializer):
    tx = TransferSerializer(source="transfer", read_only=True)

    class Meta:
        model = VaultFunding
        fields = [
            "id",
            "tx",
        ]


class WithdrawalReviewLogSerializer(serializers.ModelSerializer):
    withdrawal_sys_no = serializers.CharField(source="withdrawal.sys_no", read_only=True)
    actor = serializers.StringRelatedField(read_only=True)

    class Meta:
        model = WithdrawalReviewLog
        fields = [
            "id",
            "withdrawal_sys_no",
            "actor",
            "action",
            "from_status",
            "to_status",
            "note",
            "created_at",
        ]
