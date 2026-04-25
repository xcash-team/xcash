from typing import ClassVar

import web3
from eth_typing import ChecksumAddress
from web3.exceptions import TransactionNotFound
from web3.types import HexBytes

from chains.adapters import AdapterInterface
from chains.adapters import TxCheckStatus
from chains.models import Chain
from chains.types import AddressStr
from currencies.models import Crypto


class EvmAdapter(AdapterInterface):
    # ERC-20 balanceOf(address) ABI，用于查询代币余额
    _ERC20_BALANCE_OF_ABI: ClassVar[list[dict]] = [
        {
            "name": "balanceOf",
            "type": "function",
            "constant": True,
            "stateMutability": "view",
            "inputs": [{"name": "_owner", "type": "address"}],
            "outputs": [{"name": "balance", "type": "uint256"}],
        },
    ]

    @classmethod
    def validate_address(cls, address: AddressStr) -> bool:
        return web3.Web3.is_checksum_address(address)

    @classmethod
    def is_address(cls, chain: Chain, address: AddressStr) -> bool:
        if not cls.validate_address(address):
            return False
        # 无合约代码的地址为 EOA（外部账户）
        return cls._is_contract_code_empty(chain, address)

    @classmethod
    def is_contract(cls, chain: Chain, address: AddressStr) -> bool:
        if not cls.validate_address(address):
            return False
        # 有合约代码的地址为合约账户
        return not cls._is_contract_code_empty(chain, address)

    @classmethod
    def get_balance(cls, address: AddressStr, chain: Chain, crypto: Crypto) -> int:
        checksum_address = cls._to_checksum(address)

        if crypto == chain.native_coin or crypto.is_native:
            # 查询原生币余额（ETH/BNB 等）
            return int(chain.w3.eth.get_balance(checksum_address))  # noqa: SLF001

        token_address = crypto.address(chain)
        if not token_address:
            raise ValueError(
                f"Crypto {crypto.symbol} is not deployed on chain {chain.code}."
            )

        # 通过 ERC-20 合约的 balanceOf 查询代币余额
        contract = chain.w3.eth.contract(  # noqa: SLF001
            address=cls._to_checksum(token_address),
            abi=cls._ERC20_BALANCE_OF_ABI,
        )
        return int(contract.functions.balanceOf(checksum_address).call())

    @classmethod
    def tx_result(cls, chain: Chain, tx_hash: str) -> TxCheckStatus | Exception:
        """查询交易哈希的链上状态，返回交易检查结果或异常。"""
        try:
            receipt = chain.w3.eth.get_transaction_receipt(tx_hash)  # noqa: SLF001
        except TransactionNotFound:
            # 负载均衡 RPC 滞后、索引延迟或 pending 交易都可能暂时查不到 receipt。
            # 这里保持确认中，由 confirm_transfer 做退避确认后再判断是否 drop。
            return TxCheckStatus.CONFIRMING
        except Exception as exc:  # noqa: BLE001
            return exc

        if receipt is None:
            # 有些节点对 pending tx 返回 None，不能直接视为 reorg/drop。
            return TxCheckStatus.CONFIRMING

        status = receipt.get("status")
        if status == 1:
            return TxCheckStatus.CONFIRMED

        if status == 0:
            # receipt 存在且明确执行失败，才可终局为 FAILED。
            return TxCheckStatus.FAILED

        return RuntimeError("EVM receipt status missing or invalid")

    @staticmethod
    def _to_checksum(address: AddressStr | str) -> ChecksumAddress:
        """将地址统一转为 EIP-55 校验和格式。"""
        return web3.Web3.to_checksum_address(str(address))

    @classmethod
    def _is_contract_code_empty(cls, chain: Chain, address: AddressStr) -> bool:
        """判断地址是否为 EOA（无合约代码）。"""
        checksum_address = cls._to_checksum(address)
        code: HexBytes = chain.w3.eth.get_code(checksum_address)  # noqa: SLF001
        return code in (b"", HexBytes(b""))
