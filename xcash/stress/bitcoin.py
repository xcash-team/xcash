# xcash/stress/bitcoin.py
"""Bitcoin 链上支付：直连 regtest 节点。"""
from decimal import Decimal
from urllib.parse import quote
from urllib.parse import unquote
from urllib.parse import urlparse
from urllib.parse import urlunparse

import structlog
from bitcoin.rpc import BitcoinRpcClient
from bitcoin.rpc import BitcoinRpcError
from django.conf import settings

logger = structlog.get_logger()
_BTC_FUNDING_BUFFER = Decimal("0.001")
_DEFAULT_BTC_ROOT_WALLET = "xcash-miner"


class BitcoinStressClient:
    """简化版 Bitcoin RPC 客户端，仅用于 stress 测试支付。"""

    def __init__(self, wallet_name: str | None = None):
        self.wallet_name = wallet_name or _root_wallet_name()
        self.root_client, self.root_wallet_client, self.wallet_client = (
            _build_wallet_clients(wallet_name=self.wallet_name)
        )

    def get_new_address(self) -> str:
        return self.wallet_client.get_new_address(
            label=f"stress-recipient-{self.wallet_name}",
            address_type="bech32",
        )

    def generate_to_address(self, count: int, address: str) -> list[str]:
        return self.wallet_client.generate_to_address(count, address)

    def get_balance(self) -> Decimal:
        return self.wallet_client.get_balance()

    def send_to_address(self, address: str, amount: Decimal) -> str:
        return self.wallet_client.send_to_address(address, amount)


def _root_rpc_url() -> str:
    parsed = urlparse(settings.STRESS_BTC_RPC_URL)
    return urlunparse(parsed._replace(path="", params="", query="", fragment="")).rstrip(
        "/"
    )


def _root_wallet_name() -> str:
    path = urlparse(settings.STRESS_BTC_RPC_URL).path.strip("/")
    if path.startswith("wallet/"):
        return unquote(path.split("/", 1)[1]) or _DEFAULT_BTC_ROOT_WALLET
    return _DEFAULT_BTC_ROOT_WALLET


def _wallet_rpc_url(root_rpc_url: str, wallet_name: str) -> str:
    return f"{root_rpc_url}/wallet/{quote(wallet_name, safe='')}"


def _ensure_wallet_client(
    root_client: BitcoinRpcClient,
    root_rpc_url: str,
    wallet_name: str,
) -> BitcoinRpcClient:
    loaded_wallets = set(root_client.list_wallets())
    if wallet_name not in loaded_wallets:
        try:
            root_client.load_wallet(wallet_name)
        except BitcoinRpcError as exc:
            error_message = str(exc)
            if (
                "Wallet file not found" in error_message
                or "Path does not exist" in error_message
            ):
                root_client.create_wallet(wallet_name)
            else:
                raise

    return BitcoinRpcClient(_wallet_rpc_url(root_rpc_url, wallet_name))


def _build_wallet_clients(
    *,
    wallet_name: str | None = None,
) -> tuple[BitcoinRpcClient, BitcoinRpcClient, BitcoinRpcClient]:
    root_rpc_url = _root_rpc_url()
    root_client = BitcoinRpcClient(root_rpc_url)
    root_wallet_client = _ensure_wallet_client(
        root_client,
        root_rpc_url,
        _root_wallet_name(),
    )
    target_wallet_client = _ensure_wallet_client(
        root_client,
        root_rpc_url,
        wallet_name or "stress-btc-payer",
    )
    return root_client, root_wallet_client, target_wallet_client


def _mine_blocks(
    wallet_client: BitcoinRpcClient,
    *,
    count: int,
    label: str = "stress-regtest-miner",
) -> str:
    miner_address = wallet_client.get_new_address(
        label=label,
        address_type="legacy",
    )
    wallet_client.generate_to_address(count, miner_address)
    return miner_address


def _fund_payer_wallet(
    root_wallet_client: BitcoinRpcClient,
    payer_address: str,
    funding_amount: Decimal,
) -> None:
    try:
        root_wallet_client.send_to_address(payer_address, funding_amount)
    except BitcoinRpcError as exc:
        if "Insufficient funds" not in str(exc):
            raise
        miner_address = _mine_blocks(root_wallet_client, count=101)
        logger.info("stress.btc.mined_initial_blocks", miner=miner_address)
        root_wallet_client.send_to_address(payer_address, funding_amount)

    _mine_blocks(root_wallet_client, count=1)


def send_btc(
    to: str,
    amount: Decimal | str,
    *,
    wallet_name: str | None = None,
) -> dict[str, str]:
    """使用独立 regtest wallet 支付 BTC，并返回交易哈希与付款地址。"""
    amount_decimal = Decimal(str(amount))
    _, root_wallet_client, payer_wallet_client = _build_wallet_clients(
        wallet_name=wallet_name or "stress-btc-payer",
    )

    payer_address = payer_wallet_client.get_new_address(
        label=wallet_name or "stress-btc-payer",
        address_type="bech32",
    )
    funding_amount = amount_decimal + _BTC_FUNDING_BUFFER
    _fund_payer_wallet(root_wallet_client, payer_address, funding_amount)

    tx_hash = payer_wallet_client.send_to_address(to, amount_decimal)
    _mine_blocks(root_wallet_client, count=1)

    logger.info(
        "stress.btc.sent",
        tx_hash=tx_hash,
        to=to,
        amount=str(amount_decimal),
        payer_address=payer_address,
        wallet_name=wallet_name or "stress-btc-payer",
    )
    return {
        "tx_hash": tx_hash,
        "payer_address": payer_address,
    }
