from internal_api.authentication import InternalTokenAuthentication
from internal_api.serializers.currencies import InternalChainSerializer
from internal_api.serializers.currencies import InternalCryptoSerializer
from rest_framework.mixins import ListModelMixin
from rest_framework.permissions import IsAuthenticated
from rest_framework.viewsets import GenericViewSet

from chains.models import Chain
from currencies.models import Crypto


class InternalCryptoViewSet(ListModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = InternalCryptoSerializer
    queryset = Crypto.objects.filter(active=True).prefetch_related("chaintoken_set__chain")
    pagination_class = None


class InternalChainViewSet(ListModelMixin, GenericViewSet):
    authentication_classes = [InternalTokenAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = InternalChainSerializer
    queryset = Chain.objects.filter(active=True).select_related("native_coin")
    pagination_class = None
