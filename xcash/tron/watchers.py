from __future__ import annotations

from django.core.cache import cache

from chains.models import ChainType
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage

# Tron 当前仅观测 INVOICE 入账（系统不在 Tron 网络发起交易，无 Vault/DEPOSIT_COLLECTION 出入观测需求）；
# filter_addresses 全局共享同一 Redis key，按链型而非链 pk 维度缓存，省去多 Tron 链的重复缓存项。
TRON_FILTER_ADDRESSES_CACHE_KEY = "tron:scanner:filter_addresses"
TRON_FILTER_ADDRESSES_CACHE_TIMEOUT = None
TRON_FILTER_ADDRESSES_ITERATOR_CHUNK_SIZE = 1_000


def load_tron_filter_addresses(*, refresh: bool = False) -> frozenset[str]:
    """加载 Tron 链上 INVOICE 收款地址集合，命中 Redis 缓存时跳过 DB 查询。"""

    if refresh:
        return refresh_tron_filter_addresses()

    cached = cache.get(TRON_FILTER_ADDRESSES_CACHE_KEY)
    if cached is None:
        return refresh_tron_filter_addresses()
    return cached


def refresh_tron_filter_addresses() -> frozenset[str]:
    addresses = frozenset(_load_tron_filter_addresses_from_db())
    cache.set(
        TRON_FILTER_ADDRESSES_CACHE_KEY,
        addresses,
        timeout=TRON_FILTER_ADDRESSES_CACHE_TIMEOUT,
    )
    return addresses


def clear_tron_filter_addresses_cache() -> None:
    cache.delete(TRON_FILTER_ADDRESSES_CACHE_KEY)


def _load_tron_filter_addresses_from_db():
    return (
        RecipientAddress.objects.filter(
            chain_type=ChainType.TRON,
            usage=RecipientAddressUsage.INVOICE,
        )
        .values_list("address", flat=True)
        .iterator(chunk_size=TRON_FILTER_ADDRESSES_ITERATOR_CHUNK_SIZE)
    )
