from __future__ import annotations

from chains.models import ChainType
from core.runtime_settings import get_open_native_scanner


class ChainProductCapabilityService:
    """集中维护链类型在各产品入口中的能力边界。"""

    INVOICE_RECIPIENT_CHAIN_TYPES = frozenset(
        {ChainType.EVM, ChainType.BITCOIN, ChainType.TRON}
    )
    COLLECTION_RECIPIENT_CHAIN_TYPES = frozenset({ChainType.EVM})
    DEPOSIT_CHAIN_TYPES = frozenset({ChainType.EVM})
    WITHDRAWAL_CHAIN_TYPES = frozenset({ChainType.EVM})

    @staticmethod
    def _is_chain_native_crypto(*, chain, crypto) -> bool:
        chain_native_id = getattr(chain, "native_coin_id", None)
        crypto_id = getattr(crypto, "id", None)
        if chain_native_id is not None and crypto_id is not None:
            return chain_native_id == crypto_id
        return getattr(chain, "native_coin", None) == crypto

    @classmethod
    def _allows_evm_native(cls, *, chain, crypto) -> bool:
        if chain.type != ChainType.EVM:
            return True
        if not cls._is_chain_native_crypto(chain=chain, crypto=crypto):
            return True
        return get_open_native_scanner()

    @classmethod
    def supports_invoice_method(cls, *, chain, crypto) -> bool:
        if chain.type not in cls.INVOICE_RECIPIENT_CHAIN_TYPES:
            return False
        if not crypto.support_this_chain(chain):
            return False
        return cls.supports_existing_invoice_method(chain=chain, crypto=crypto)

    @classmethod
    def supports_existing_invoice_method(cls, *, chain, crypto) -> bool:
        """判断已存在 ChainToken 关系的链币组合是否可用于 Invoice。"""
        if chain.type not in cls.INVOICE_RECIPIENT_CHAIN_TYPES:
            return False
        if not cls._allows_evm_native(chain=chain, crypto=crypto):
            return False
        if chain.type == ChainType.TRON:
            return crypto.symbol == "USDT"
        return True

    @classmethod
    def supports_deposit_address(cls, *, chain, crypto) -> bool:
        return (
            chain.type in cls.DEPOSIT_CHAIN_TYPES
            and crypto.support_this_chain(chain)
            and cls._allows_evm_native(chain=chain, crypto=crypto)
        )

    @classmethod
    def supports_withdrawal(cls, *, chain, crypto) -> bool:
        return (
            chain.type in cls.WITHDRAWAL_CHAIN_TYPES
            and crypto.support_this_chain(chain)
            and cls._allows_evm_native(chain=chain, crypto=crypto)
        )
