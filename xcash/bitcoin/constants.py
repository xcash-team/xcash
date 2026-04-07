# Bitcoin 系统常量

# Bitcoin 原生精度：1 BTC = 10^8 satoshi
BTC_DECIMALS = 8
SATOSHI_PER_BTC = 10**BTC_DECIMALS

# 默认目标确认块数（用于 estimatesmartfee）：6 块约 60 分钟
BTC_FEE_TARGET_BLOCKS = 6

# 默认最低矿工费（satoshi/vbyte），当节点 estimatesmartfee 失败时使用
BTC_DEFAULT_FEE_RATE_SAT_PER_BYTE = 10

# ── SegWit (P2WPKH) 交易体积估算参数 ──
# 系统内部地址统一为 Native SegWit，因此内部输入和找零输出始终按 P2WPKH 估算。
BTC_SEGWIT_TX_OVERHEAD_VBYTES = 11  # version(4) + marker(0.25) + flag(0.25) + locktime(4) + vin_count(~1) + vout_count(~1)
BTC_P2WPKH_INPUT_VBYTES = 68       # outpoint(36) + scriptSig_len(1) + sequence(4) + witness(~27 vbytes)
BTC_P2WPKH_OUTPUT_VBYTES = 31      # value(8) + scriptPubKey_len(1) + scriptPubKey(22)

# ── 外部目标地址输出体积 ──
# 输出体积按目标地址脚本类型区分，确保混合输出估费准确。
BTC_P2PKH_OUTPUT_VBYTES = 34       # value(8) + scriptPubKey_len(1) + scriptPubKey(25)
BTC_P2SH_OUTPUT_VBYTES = 32        # value(8) + scriptPubKey_len(1) + scriptPubKey(23)

# ── 便捷常量：标准 1 输入 2 输出 P2WPKH 交易 ──
BTC_P2WPKH_TX_VBYTES = (
    BTC_SEGWIT_TX_OVERHEAD_VBYTES + BTC_P2WPKH_INPUT_VBYTES + BTC_P2WPKH_OUTPUT_VBYTES * 2
)

# ── Dust 阈值 ──
BTC_P2WPKH_DUST_LIMIT = 294   # P2WPKH 输出的 dust 阈值 (Bitcoin Core 默认 dust_relay_fee=3000 sat/kvB)
BTC_P2PKH_DUST_LIMIT = 546    # P2PKH 输出的 dust 阈值

# ── 旧常量保留（供向后兼容参考） ──
BTC_P2PKH_TX_OVERHEAD_VBYTES = 10
BTC_P2PKH_INPUT_VBYTES = 148
BTC_P2PKH_TX_BYTES = (
    BTC_P2PKH_TX_OVERHEAD_VBYTES + BTC_P2PKH_INPUT_VBYTES + BTC_P2PKH_OUTPUT_VBYTES * 2
)

# Bitcoin 主网 WIF 前缀（mainnet compressed private key）
BTC_WIF_PREFIX = b"\x80"
