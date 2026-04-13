from __future__ import annotations

from chains.models import ChainType
from projects.models import RecipientAddressUsage


def load_watch_set() -> frozenset[str]:
    """加载 Bitcoin 收款扫描需要关注的系统地址集合（仅项目收币地址）。"""
    from projects.models import RecipientAddress

    return frozenset(
        RecipientAddress.objects.filter(
            chain_type=ChainType.BITCOIN,
            usage=RecipientAddressUsage.INVOICE,
        ).values_list("address", flat=True)
    )
