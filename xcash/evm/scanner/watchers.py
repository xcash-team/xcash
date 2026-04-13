from __future__ import annotations

from dataclasses import dataclass

from web3 import Web3

from chains.models import Address
from chains.models import Chain
from chains.models import ChainType
from currencies.models import ChainToken
from projects.models import RecipientAddressUsage
from projects.models import RecipientAddress


@dataclass(frozen=True)
class EvmWatchSet:
    """描述某条 EVM 链当前需要关注的地址和代币集合。"""

    watched_addresses: frozenset[str]
    tokens_by_address: dict[str, ChainToken]


def _normalize_address(address: str) -> str:
    # 扫描器统一将地址标准化为 checksum，保证 DB 数据与 RPC 返回值可直接比对。
    return Web3.to_checksum_address(str(address))


def load_watch_set(*, chain: Chain) -> EvmWatchSet:
    """加载某条链上需要监听的系统地址与受支持 ERC20 合约集合。"""

    system_addresses = Address.objects.filter(
        chain_type=ChainType.EVM,
    ).values_list("address", flat=True)
    recipient_addresses = RecipientAddress.objects.filter(
        chain_type=ChainType.EVM,
        usage__in=(
            RecipientAddressUsage.INVOICE,
            RecipientAddressUsage.DEPOSIT_COLLECTION,
        ),
    ).values_list("address", flat=True)

    watched_addresses = frozenset(
        _normalize_address(address)
        for address in [*system_addresses, *recipient_addresses]
    )

    token_rows = (
        ChainToken.objects.select_related("crypto")
        .filter(
            chain=chain,
            crypto__active=True,
        )
        .exclude(address="")
    )
    tokens_by_address = {
        _normalize_address(token.address): token for token in token_rows
    }

    return EvmWatchSet(
        watched_addresses=watched_addresses,
        tokens_by_address=tokens_by_address,
    )
