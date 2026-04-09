from abc import ABC
from abc import abstractmethod
from enum import StrEnum

from chains.models import Chain
from chains.types import AddressStr
from currencies.models import Crypto


class TxCheckStatus(StrEnum):
    """链上交易结果查询的内存枚举。

    这里只描述“当前查到的交易结果”，不落库，也不参与业务状态机建模。
    """

    CONFIRMING = "confirming"
    CONFIRMED = "confirmed"
    DROPPED = "dropped"
    FAILED = "failed"


class AdapterInterface(ABC):
    """链适配器接口：负责地址验证、余额查询、交易结果查询。

    交易签名与广播逻辑已从 Adapter 层移除，统一由各链专属的 XxxBroadcastTask 模型负责：
    - EVM：evm.EvmBroadcastTask.schedule_transfer()
    """

    @abstractmethod
    def validate_address(self, address: AddressStr) -> bool:
        pass

    @abstractmethod
    def is_address(self, chain: Chain, address: AddressStr) -> bool:
        pass

    @abstractmethod
    def is_contract(self, chain: Chain, address: AddressStr) -> bool:
        pass

    @abstractmethod
    def get_balance(self, address: AddressStr, chain: Chain, crypto: Crypto) -> int:
        pass

    @abstractmethod
    def tx_result(self, chain, tx_hash: str) -> TxCheckStatus | Exception:
        pass


class AdapterFactory:
    # 各链适配器在首次请求时懒加载，避免类体级别导入产生启动时强依赖。

    @staticmethod
    def get_adapter(chain_type: str) -> AdapterInterface:
        if chain_type == "evm":
            from evm.adapter import EvmAdapter

            return EvmAdapter()
        if chain_type == "btc":
            from bitcoin.adapter import BitcoinAdapter

            return BitcoinAdapter()
        if chain_type == "tron":
            from tron.adapter import TronAdapter

            return TronAdapter()
        raise ValueError(f"Unsupported chain adapter: {chain_type}")
