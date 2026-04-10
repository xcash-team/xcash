from __future__ import annotations

from chains.models import ChainType


class ChainProductCapabilityService:
    """集中维护链类型在各产品入口中的能力边界。"""

    INVOICE_RECIPIENT_CHAIN_TYPES = frozenset(
        {ChainType.EVM, ChainType.BITCOIN, ChainType.TRON}
    )
    COLLECTION_RECIPIENT_CHAIN_TYPES = frozenset({ChainType.EVM})
    DEPOSIT_CHAIN_TYPES = frozenset({ChainType.EVM})
    WITHDRAWAL_CHAIN_TYPES = frozenset({ChainType.EVM})

    @classmethod
    def supports_invoice_method(cls, *, chain, crypto) -> bool:
        if chain.type not in cls.INVOICE_RECIPIENT_CHAIN_TYPES:
            return False
        if not crypto.support_this_chain(chain):
            return False
        if chain.type == ChainType.TRON:
            return crypto.symbol == "USDT"
        return True

    @classmethod
    def supports_deposit_address(cls, *, chain, crypto) -> bool:
        return (
            chain.type in cls.DEPOSIT_CHAIN_TYPES
            and crypto.support_this_chain(chain)
        )

    @classmethod
    def supports_withdrawal(cls, *, chain, crypto) -> bool:
        return (
            chain.type in cls.WITHDRAWAL_CHAIN_TYPES
            and crypto.support_this_chain(chain)
        )
