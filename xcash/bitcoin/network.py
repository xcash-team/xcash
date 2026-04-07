from __future__ import annotations

from dataclasses import dataclass

import environ
from bip_utils import Bip44Coins
from bip_utils import Bip84Coins

env = environ.Env()


@dataclass(frozen=True)
class BitcoinNetworkConfig:
    """描述当前部署所使用的 Bitcoin 网络参数。"""

    name: str
    p2pkh_version: bytes
    p2sh_version: bytes
    bech32_hrp: str
    wif_prefix: bytes
    bip44_coin: Bip44Coins
    bip84_coin: Bip84Coins
    bit_private_key_class_name: str


BITCOIN_NETWORKS: dict[str, BitcoinNetworkConfig] = {
    "mainnet": BitcoinNetworkConfig(
        name="mainnet",
        p2pkh_version=b"\x00",
        p2sh_version=b"\x05",
        bech32_hrp="bc",
        wif_prefix=b"\x80",
        bip44_coin=Bip44Coins.BITCOIN,
        bip84_coin=Bip84Coins.BITCOIN,
        bit_private_key_class_name="PrivateKeyMainnet",
    ),
    # regtest / signet 沿用 testnet 的 base58/WIF 版本；bech32 HRP 则分别使用 bcrt / tb。
    "testnet": BitcoinNetworkConfig(
        name="testnet",
        p2pkh_version=b"\x6f",
        p2sh_version=b"\xc4",
        bech32_hrp="tb",
        wif_prefix=b"\xef",
        bip44_coin=Bip44Coins.BITCOIN_TESTNET,
        bip84_coin=Bip84Coins.BITCOIN_TESTNET,
        bit_private_key_class_name="PrivateKeyTestnet",
    ),
    "signet": BitcoinNetworkConfig(
        name="signet",
        p2pkh_version=b"\x6f",
        p2sh_version=b"\xc4",
        bech32_hrp="tb",
        wif_prefix=b"\xef",
        bip44_coin=Bip44Coins.BITCOIN_TESTNET,
        bip84_coin=Bip84Coins.BITCOIN_TESTNET,
        bit_private_key_class_name="PrivateKeyTestnet",
    ),
    "regtest": BitcoinNetworkConfig(
        name="regtest",
        p2pkh_version=b"\x6f",
        p2sh_version=b"\xc4",
        bech32_hrp="bcrt",
        wif_prefix=b"\xef",
        bip44_coin=Bip44Coins.BITCOIN_TESTNET,
        bip84_coin=Bip84Coins.BITCOIN_REGTEST,
        bit_private_key_class_name="PrivateKeyTestnet",
    ),
}


def get_active_bitcoin_network() -> BitcoinNetworkConfig:
    """返回当前部署使用的 Bitcoin 网络配置。

    当前项目对 Bitcoin 地址派生仍采用“单网络部署”假设：
    - 生产默认 mainnet
    - 本地联调可显式切到 regtest
    """

    network_name = env.str("BITCOIN_NETWORK", default="mainnet").strip().lower()
    try:
        return BITCOIN_NETWORKS[network_name]
    except KeyError as exc:
        supported = ", ".join(sorted(BITCOIN_NETWORKS))
        msg = f"Unsupported BITCOIN_NETWORK={network_name}. Supported: {supported}"
        raise ValueError(msg) from exc
