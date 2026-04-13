from __future__ import annotations

import structlog
from django.db import transaction

from bitcoin.rpc import BitcoinRpcClient
from bitcoin.rpc import BitcoinRpcError
from chains.models import Chain
from chains.models import ChainType
from projects.models import RecipientAddressUsage
from projects.models import RecipientAddress

logger = structlog.get_logger()

WATCH_ONLY_LABEL = "xcash-watch-only"
LEGACY_WALLET_ONLY_ERROR = "Only legacy wallets are supported by this command"
DESCRIPTOR_PRIVATE_KEY_ERROR = "Cannot import descriptor without private keys"


class BitcoinWatchSyncService:
    """把系统已知的 BTC 地址同步到节点钱包的 watch-only 视图。"""

    @staticmethod
    def load_known_imports() -> list[tuple[str, str]]:
        return [
            (address, f"addr({address})")
            for address in RecipientAddress.objects.filter(
                chain_type=ChainType.BITCOIN,
                usage=RecipientAddressUsage.INVOICE,
            ).values_list(
                "address",
                flat=True,
            )
        ]

    @classmethod
    def sync_chain(cls, chain: Chain) -> int:
        if chain.type != ChainType.BITCOIN:
            msg = f"仅支持同步 Bitcoin 链，当前链为 {chain.code}"
            raise ValueError(msg)

        client = BitcoinRpcClient(chain.rpc)
        imported_count = 0

        for address, descriptor in cls.load_known_imports():
            if cls._import_address(
                client=client,
                address=address,
                descriptor=descriptor,
            ):
                imported_count += 1

        return imported_count

    @staticmethod
    def _import_address(
        *,
        client: BitcoinRpcClient,
        address: str,
        descriptor: str,
    ) -> bool:
        try:
            client.import_address(address, label=WATCH_ONLY_LABEL, rescan=False)
        except BitcoinRpcError as exc:
            error_message = str(exc)
            if LEGACY_WALLET_ONLY_ERROR in error_message:
                try:
                    client.import_descriptor(
                        descriptor=descriptor,
                        label=WATCH_ONLY_LABEL,
                    )
                except BitcoinRpcError as descriptor_exc:
                    if DESCRIPTOR_PRIVATE_KEY_ERROR in str(descriptor_exc):
                        logger.warning(
                            "Bitcoin watch-only descriptor 导入不受支持",
                            address=address,
                        )
                        return False
                    raise
                return True
            if "already exists" in error_message.lower():
                return False
            raise

        return True


def schedule_watch_address_sync_on_commit() -> None:
    """在事务提交后触发一次 BTC watch-only 全量同步。"""
    from bitcoin.tasks import sync_bitcoin_watch_addresses

    transaction.on_commit(
        lambda: sync_bitcoin_watch_addresses.apply_async(countdown=1)
    )
