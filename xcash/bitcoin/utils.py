from __future__ import annotations

import hashlib
from decimal import ROUND_DOWN
from decimal import Decimal
from typing import TYPE_CHECKING

from bip_utils import Base58Encoder  # type: ignore[import]

from bitcoin.constants import BTC_DEFAULT_FEE_RATE_SAT_PER_BYTE
from bitcoin.constants import BTC_P2PKH_INPUT_VBYTES
from bitcoin.constants import BTC_P2PKH_OUTPUT_VBYTES
from bitcoin.constants import BTC_P2PKH_TX_OVERHEAD_VBYTES
from bitcoin.constants import BTC_P2SH_OUTPUT_VBYTES
from bitcoin.constants import BTC_P2WPKH_INPUT_VBYTES
from bitcoin.constants import BTC_P2WPKH_OUTPUT_VBYTES
from bitcoin.constants import BTC_SEGWIT_TX_OVERHEAD_VBYTES
from bitcoin.constants import SATOSHI_PER_BTC
from bitcoin.network import get_active_bitcoin_network

if TYPE_CHECKING:
    from collections.abc import Sequence

    from bitcoin.rpc import BitcoinUtxo
    from chains.models import Chain
    from currencies.models import Crypto


def ensure_bitcoin_native_currency(*, chain: Chain, crypto: Crypto) -> None:
    """强约束 Bitcoin 链只能处理该链的原生 BTC。"""
    if chain.type != "btc":
        msg = f"链类型不是 Bitcoin: {chain.code}"
        raise ValueError(msg)

    if crypto.pk != chain.native_coin_id:
        msg = (
            f"Bitcoin 暂仅支持链原生币 {chain.native_coin.symbol}，"
            f"当前收到 {crypto.symbol}"
        )
        raise NotImplementedError(msg)


def btc_to_satoshi(amount: Decimal | float | str) -> int:
    normalized = Decimal(str(amount))
    return int(
        (normalized * SATOSHI_PER_BTC).quantize(Decimal("1"), rounding=ROUND_DOWN)
    )


def sat_per_byte_from_btc_per_kb(fee_rate_btc_per_kb: Decimal) -> int:
    return max(
        int(fee_rate_btc_per_kb * SATOSHI_PER_BTC / 1000),
        BTC_DEFAULT_FEE_RATE_SAT_PER_BYTE,
    )


def estimate_p2pkh_tx_vbytes(*, input_count: int, output_count: int = 2) -> int:
    """估算 legacy P2PKH 交易大小。

    采用保守估算：
    - 10 bytes 固定开销（version/locktime/varint 等）
    - 每个输入约 148 bytes
    - 每个输出约 34 bytes
    当前项目钱包派生的是 P2PKH（1...）地址，此估算成立。
    """
    return (
        BTC_P2PKH_TX_OVERHEAD_VBYTES
        + input_count * BTC_P2PKH_INPUT_VBYTES
        + output_count * BTC_P2PKH_OUTPUT_VBYTES
    )


def _output_vbytes_for_address_type(address_type: str) -> int:
    """返回指定地址类型的输出体积（vbytes）。"""
    if address_type == "p2wpkh":
        return BTC_P2WPKH_OUTPUT_VBYTES
    if address_type == "p2sh":
        return BTC_P2SH_OUTPUT_VBYTES
    return BTC_P2PKH_OUTPUT_VBYTES


def estimate_segwit_tx_vbytes(
    *,
    input_count: int,
    target_address_type: str = "p2wpkh",
    include_change: bool = True,
) -> int:
    """估算 P2WPKH 输入的 SegWit 交易 vbytes。

    内部输入统一按 P2WPKH 估算。
    找零输出固定按 P2WPKH 估算（内部 Native SegWit 地址）。
    目标输出按实际目标地址脚本类型估算。
    """
    vbytes = BTC_SEGWIT_TX_OVERHEAD_VBYTES + input_count * BTC_P2WPKH_INPUT_VBYTES
    vbytes += _output_vbytes_for_address_type(target_address_type)
    if include_change:
        vbytes += BTC_P2WPKH_OUTPUT_VBYTES
    return vbytes


def select_utxos_for_amount(
    *,
    utxos: Sequence[BitcoinUtxo],
    amount_satoshi: int,
    fee_rate_sat_per_byte: int,
    target_address_type: str = "p2wpkh",
) -> tuple[list[BitcoinUtxo], int]:
    """为支付金额选择一组 UTXO，并返回 SegWit 估算的矿工费。"""
    selected: list[BitcoinUtxo] = []
    total_satoshi = 0

    for utxo in sorted(
        utxos, key=lambda item: btc_to_satoshi(item["amount"]), reverse=True
    ):
        selected.append(utxo)
        total_satoshi += btc_to_satoshi(utxo["amount"])

        fee_satoshi = (
            estimate_segwit_tx_vbytes(
                input_count=len(selected),
                target_address_type=target_address_type,
                include_change=True,
            )
            * fee_rate_sat_per_byte
        )

        if total_satoshi >= amount_satoshi + fee_satoshi:
            return selected, fee_satoshi

    msg = "Bitcoin UTXO 余额不足以覆盖转账金额与矿工费"
    raise ValueError(msg)


def select_utxos_for_sweep(
    *,
    utxos: Sequence[BitcoinUtxo],
    fee_rate_sat_per_byte: int,
    target_address_type: str = "p2wpkh",
) -> tuple[list[BitcoinUtxo], int, int]:
    """选择全部 UTXO 执行 sweep，返回可转出净额与矿工费。"""
    selected = list(utxos)
    if not selected:
        raise ValueError("Bitcoin sweep 缺少可用 UTXO")

    total_satoshi = sum(btc_to_satoshi(utxo["amount"]) for utxo in selected)
    fee_satoshi = (
        estimate_segwit_tx_vbytes(
            input_count=len(selected),
            target_address_type=target_address_type,
            include_change=False,
        )
        * fee_rate_sat_per_byte
    )
    amount_satoshi = total_satoshi - fee_satoshi
    if amount_satoshi <= 0:
        raise ValueError("Bitcoin UTXO 余额不足以覆盖 sweep 矿工费")
    return selected, amount_satoshi, fee_satoshi


def privkey_bytes_to_wif(privkey_bytes: bytes) -> str:
    """将原始 32 字节 secp256k1 私钥转换为当前网络 WIF（压缩格式）。"""
    network = get_active_bitcoin_network()
    payload = network.wif_prefix + privkey_bytes + b"\x01"
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    return Base58Encoder.Encode(payload + checksum)


def compute_txid(signed_payload_hex: str) -> str:
    """从已签名原始交易 hex 计算 txid。

    SegWit 交易的 txid 基于去除 witness 数据后的序列化，
    使用 bit.transaction.calc_txid 正确处理 legacy 和 SegWit 两种格式。
    """
    from bit.transaction import calc_txid

    return calc_txid(signed_payload_hex)


def _read_bitcoin_varint(raw: bytes, offset: int) -> tuple[int, int]:
    if offset >= len(raw):
        raise ValueError("Bitcoin 原始交易缺少 varint")

    prefix = raw[offset]
    if prefix < 0xFD:
        return prefix, offset + 1
    if prefix == 0xFD:
        end = offset + 3
        if end > len(raw):
            raise ValueError("Bitcoin 原始交易 varint(uint16) 不完整")
        return int.from_bytes(raw[offset + 1 : end], "little"), end
    if prefix == 0xFE:
        end = offset + 5
        if end > len(raw):
            raise ValueError("Bitcoin 原始交易 varint(uint32) 不完整")
        return int.from_bytes(raw[offset + 1 : end], "little"), end

    end = offset + 9
    if end > len(raw):
        raise ValueError("Bitcoin 原始交易 varint(uint64) 不完整")
    return int.from_bytes(raw[offset + 1 : end], "little"), end


def extract_input_sequences_from_raw_transaction(
    signed_payload_hex: str,
) -> list[int]:
    """从原始交易 hex 中提取每个输入的 nSequence，用于判断是否 opt-in RBF。"""
    raw = bytes.fromhex(signed_payload_hex)
    if len(raw) < 5:
        raise ValueError("Bitcoin 原始交易长度不足")

    offset = 4
    if len(raw) > offset + 1 and raw[offset] == 0 and raw[offset + 1] == 1:
        # segwit 交易在 version 后插入 marker/flag；当前项目主用 P2PKH，
        # 这里仍保留解析兼容，避免后续地址类型扩展时重复造轮子。
        offset += 2

    input_count, offset = _read_bitcoin_varint(raw, offset)
    sequences: list[int] = []
    for _ in range(input_count):
        if offset + 36 > len(raw):
            raise ValueError("Bitcoin 原始交易缺少完整输入前缀")
        offset += 36  # prevout txid(32) + vout(4)
        script_length, offset = _read_bitcoin_varint(raw, offset)
        if offset + script_length + 4 > len(raw):
            raise ValueError("Bitcoin 原始交易缺少完整 scriptSig 或 sequence")
        offset += script_length
        sequences.append(int.from_bytes(raw[offset : offset + 4], "little"))
        offset += 4
    return sequences


def is_replaceable_signed_transaction(signed_payload_hex: str) -> bool:
    """检查原始交易是否显式 opt-in RBF。"""
    try:
        sequences = extract_input_sequences_from_raw_transaction(signed_payload_hex)
    except ValueError:
        return False
    return any(sequence < 0xFFFFFFFE for sequence in sequences)
