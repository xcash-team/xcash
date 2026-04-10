# xcash/stress/evm.py
"""EVM 链上支付：直连 Anvil 本地测试链。"""
from decimal import Decimal

import structlog
from django.conf import settings
from evm.local_erc20 import LOCAL_EVM_ERC20_ABI
from web3 import Web3

logger = structlog.get_logger()
_EVM_GAS_BUFFER_WEI = 10**16


def _get_w3() -> Web3:
    return Web3(Web3.HTTPProvider(settings.STRESS_EVM_RPC_URL))


def _create_payer_account(w3: Web3):
    return w3.eth.account.create()


def _set_balance(w3: Web3, address: str, value: int) -> None:
    checksum = Web3.to_checksum_address(address)
    response = w3.provider.make_request(
        "anvil_setBalance",
        [checksum, hex(value)],
    )
    if response.get("error"):
        raise RuntimeError(f"anvil_setBalance failed: {response['error']}")

def _require_contract(w3: Web3, address: str) -> str:
    """要求本地 ERC20 合约已存在；支付链路不负责部署。"""
    checksum = Web3.to_checksum_address(address)
    if len(w3.eth.get_code(checksum)) == 0:
        raise ValueError("本地 ERC20 合约不存在，请先初始化本地链配置")
    return checksum


def send_native(to: str, amount: Decimal | str, decimals: int = 18) -> dict[str, str]:
    """为当前支付生成独立账户并发送原生币（ETH）。"""
    w3 = _get_w3()
    payer = _create_payer_account(w3)
    value = int(Decimal(str(amount)) * (10**decimals))
    _set_balance(w3, payer.address, value + _EVM_GAS_BUFFER_WEI)
    tx = {
        "from": payer.address,
        "to": Web3.to_checksum_address(to),
        "value": value,
        "gas": 21000,
        "gasPrice": w3.eth.gas_price,
        "nonce": w3.eth.get_transaction_count(payer.address, "pending"),
        "chainId": w3.eth.chain_id,
    }
    signed = payer.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    logger.info(
        "stress.evm.native_sent",
        tx_hash=tx_hash.hex(),
        to=to,
        value=value,
        payer_address=payer.address,
    )
    return {"tx_hash": tx_hash.hex(), "payer_address": payer.address}


def send_erc20(
    token_address: str, to: str, amount: Decimal | str, decimals: int = 18
) -> dict[str, str]:
    """为当前支付生成独立账户并发送 ERC20 代币。"""
    w3 = _get_w3()
    payer = _create_payer_account(w3)
    checksum_token = _require_contract(w3, token_address)
    checksum_to = Web3.to_checksum_address(to)

    contract = w3.eth.contract(
        address=checksum_token,
        abi=LOCAL_EVM_ERC20_ABI,
    )
    raw_amount = int(Decimal(str(amount)) * (10**decimals))
    _set_balance(w3, payer.address, _EVM_GAS_BUFFER_WEI)

    mint_tx = contract.functions.mint(payer.address, raw_amount).build_transaction(
        {
            "from": payer.address,
            "nonce": w3.eth.get_transaction_count(payer.address, "pending"),
            "gasPrice": w3.eth.gas_price,
            "chainId": w3.eth.chain_id,
        }
    )
    signed_mint = payer.sign_transaction(mint_tx)
    mint_hash = w3.eth.send_raw_transaction(signed_mint.raw_transaction)
    w3.eth.wait_for_transaction_receipt(mint_hash)

    transfer_tx = contract.functions.transfer(
        checksum_to, raw_amount
    ).build_transaction(
        {
            "from": payer.address,
            "nonce": w3.eth.get_transaction_count(payer.address, "pending"),
            "gasPrice": w3.eth.gas_price,
            "chainId": w3.eth.chain_id,
        }
    )
    signed = payer.sign_transaction(transfer_tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    logger.info(
        "stress.evm.erc20_minted",
        token=checksum_token,
        amount=raw_amount,
        payer_address=payer.address,
        mint_tx_hash=mint_hash.hex(),
    )
    logger.info(
        "stress.evm.erc20_sent",
        tx_hash=tx_hash.hex(),
        token=checksum_token,
        to=to,
        amount=raw_amount,
        payer_address=payer.address,
    )
    return {"tx_hash": tx_hash.hex(), "payer_address": payer.address}
