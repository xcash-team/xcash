from bip_utils import P2PKHAddrDecoder
from bip_utils import P2SHAddrDecoder
from bip_utils import SegwitBech32Decoder

from bitcoin.network import get_active_bitcoin_network


def is_valid_bitcoin_address(address: str) -> bool:
    """对当前 Bitcoin 网络地址执行真实 checksum 校验，而不是只做正则匹配。"""
    network = get_active_bitcoin_network()

    # Base58 地址只可能是传统 P2PKH / P2SH；解码器内部会同时校验网络前缀与 checksum。
    try:
        P2PKHAddrDecoder.DecodeAddr(address, net_ver=network.p2pkh_version)
    except Exception:  # noqa: BLE001, S110
        pass
    else:
        return True

    try:
        P2SHAddrDecoder.DecodeAddr(address, net_ver=network.p2sh_version)
    except Exception:  # noqa: BLE001, S110
        pass
    else:
        return True

    # SegWit / Taproot 统一通过 bech32/bech32m 解码校验。
    try:
        SegwitBech32Decoder.Decode(network.bech32_hrp, address)
    except Exception:  # noqa: BLE001
        return False
    else:
        return True


def classify_bitcoin_address(address: str) -> str:
    """识别 Bitcoin 地址的脚本类型，返回 'p2pkh' / 'p2sh' / 'p2wpkh' / 'unknown'。

    用于估费时选择正确的输出体积。仅做前缀识别，
    完整 checksum 校验由 is_valid_bitcoin_address 负责。
    """
    if not address:
        return "unknown"
    lower = address.lower()
    if lower.startswith(("bc1q", "tb1q", "bcrt1q")):
        return "p2wpkh"
    if address[0] in ("3", "2"):
        return "p2sh"
    if address[0] in ("1", "m", "n"):
        return "p2pkh"
    return "unknown"
