from __future__ import annotations

from chains.adapters import AdapterInterface
from chains.adapters import TxCheckStatus
from tron.client import TronClientError
from tron.client import TronHttpClient
from tron.codec import TronAddressCodec


class TronAdapter(AdapterInterface):
    @staticmethod
    def validate_address(address: str) -> bool:
        return TronAddressCodec.is_valid_base58(address)

    def is_address(self, chain, address: str) -> bool:
        return self.validate_address(address)

    def is_contract(self, chain, address: str) -> bool:
        return False

    def get_balance(self, address, chain, crypto) -> int:
        raise NotImplementedError("Tron invoice-only adapter does not support balances")

    def tx_result(self, chain, tx_hash: str) -> TxCheckStatus | Exception:
        try:
            payload = TronHttpClient(chain=chain).get_transaction_info_by_id(tx_hash)
        except TronClientError as exc:
            return exc

        receipt = payload.get("receipt") or {}
        if payload.get("id") == tx_hash and receipt.get("result") == "SUCCESS":
            return TxCheckStatus.CONFIRMED
        return RuntimeError(f"tron tx {tx_hash} not confirmed")

