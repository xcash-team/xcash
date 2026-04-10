from __future__ import annotations

from decimal import Decimal

from chains.models import Chain
from chains.models import ChainType
from currencies.models import Crypto

from .bitcoin import send_btc
from .evm import send_erc20
from .evm import send_native


def simulate_payment(
    *,
    to_address: str,
    chain_code: str,
    crypto_symbol: str,
    amount: Decimal | str,
    payment_ref: str | None = None,
) -> dict[str, str]:
    """统一的 stress 链上转币入口，内部按链类型和资产类型分发。

    当前用于账单支付测试，但设计上不绑定“支付”语义：
    只要测试目标是“为某个地址模拟一笔链上入账”，无论是支付测试、
    后续可能新增的充币测试，还是其他转币类压测，都应该优先复用这里，
    避免在不同场景各自维护一套 payer 生成、注资、铸币和转账逻辑。
    """
    chain_obj = Chain.objects.get(code=chain_code)
    crypto_obj = Crypto.objects.get(symbol=crypto_symbol)

    if chain_obj.type == ChainType.BITCOIN:
        wallet_name = f"stress-{payment_ref}" if payment_ref else "stress-btc-payer"
        return send_btc(
            to=to_address,
            amount=amount,
            wallet_name=wallet_name,
        )

    if chain_obj.type != ChainType.EVM:
        raise ValueError(f"不支持的链类型: {chain_obj.type}")

    if crypto_obj.is_native or crypto_obj == chain_obj.native_coin:
        return send_native(
            to=to_address,
            amount=amount,
            decimals=crypto_obj.get_decimals(chain_obj),
        )

    token_address = crypto_obj.address(chain_obj)
    if not token_address:
        raise ValueError(f"ERC20 代币 {crypto_symbol} 在链 {chain_code} 上没有合约地址")

    return send_erc20(
        token_address=token_address,
        to=to_address,
        amount=amount,
        decimals=crypto_obj.get_decimals(chain_obj),
    )
