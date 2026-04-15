from rest_framework import serializers

from chains.models import Chain
from currencies.models import ChainToken
from currencies.models import Crypto


class ChainTokenSerializer(serializers.ModelSerializer):
    chain = serializers.SlugRelatedField(slug_field="code", read_only=True)

    class Meta:
        model = ChainToken
        fields = ["chain", "address", "decimals"]


class InternalCryptoSerializer(serializers.ModelSerializer):
    chain_tokens = ChainTokenSerializer(
        source="chaintoken_set", many=True, read_only=True
    )

    class Meta:
        model = Crypto
        fields = [
            "name",
            "symbol",
            "decimals",
            "prices",
            "active",
            "chain_tokens",
        ]


class InternalChainSerializer(serializers.ModelSerializer):
    native_coin = serializers.SlugRelatedField(slug_field="symbol", read_only=True)

    class Meta:
        model = Chain
        fields = [
            "name",
            "code",
            "type",
            "native_coin",
            "confirm_block_count",
            "active",
        ]
