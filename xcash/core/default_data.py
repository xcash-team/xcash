from __future__ import annotations

from urllib.parse import quote

import environ
from django.conf import settings
from django.db import transaction
from web3 import Web3

from chains.models import Chain
from chains.models import ChainType
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.models import Fiat
from evm.local_erc20 import LOCAL_EVM_ERC20_ABI
from evm.local_erc20 import LOCAL_EVM_ERC20_BYTECODE
from evm.local_erc20 import has_standard_erc20_interface
from tron.codec import TronAddressCodec

env = environ.Env()

LOCAL_EVM_USDT_DECIMALS = 6

LOCAL_EVM_TOKEN_MAPPINGS = (
    {
        "crypto_symbol": "USDT",
        "env": "LOCAL_EVM_USDT_ADDRESS",
    },
    {
        "crypto_symbol": "USDC",
        "env": "LOCAL_EVM_USDC_ADDRESS",
    },
    {
        "crypto_symbol": "DAI",
        "env": "LOCAL_EVM_DAI_ADDRESS",
    },
)

PRODUCTION_MAINNET_CHAINS = (
    {
        "code": "ethereum-mainnet",
        "name": "Ethereum",
        "type": ChainType.EVM,
        "native_symbol": "ETH",
        "chain_id": 1,
        "is_poa": False,
    },
    {
        "code": "bsc-mainnet",
        "name": "BSC",
        "type": ChainType.EVM,
        "native_symbol": "BNB",
        "chain_id": 56,
        "is_poa": True,
    },
    {
        "code": "polygon-mainnet",
        "name": "Polygon",
        "type": ChainType.EVM,
        "native_symbol": "POL",
        "chain_id": 137,
        "is_poa": True,
    },
    {
        "code": "base-mainnet",
        "name": "Base",
        "type": ChainType.EVM,
        "native_symbol": "ETH",
        "chain_id": 8453,
        "is_poa": False,
    },
    {
        "code": "bitcoin-mainnet",
        "name": "Bitcoin",
        "type": ChainType.BITCOIN,
        "native_symbol": "BTC",
        "chain_id": None,
        "is_poa": None,
    },
    {
        "code": "tron-mainnet",
        "name": "Tron",
        "type": ChainType.TRON,
        "native_symbol": "TRX",
        "chain_id": None,
        "is_poa": None,
    },
)

PRODUCTION_MAINNET_TOKEN_MAPPINGS = (
    {
        "chain_code": "ethereum-mainnet",
        "crypto_symbol": "USDC",
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6,
    },
    {
        "chain_code": "ethereum-mainnet",
        "crypto_symbol": "USDT",
        "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "decimals": 6,
    },
    {
        "chain_code": "ethereum-mainnet",
        "crypto_symbol": "DAI",
        "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "decimals": 18,
    },
    {
        "chain_code": "bsc-mainnet",
        "crypto_symbol": "USDC",
        "address": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
        "decimals": 18,
    },
    {
        "chain_code": "bsc-mainnet",
        "crypto_symbol": "USDT",
        "address": "0x55d398326f99059fF775485246999027B3197955",
        "decimals": 18,
    },
    {
        "chain_code": "bsc-mainnet",
        "crypto_symbol": "DAI",
        "address": "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",
        "decimals": 18,
    },
    {
        "chain_code": "polygon-mainnet",
        "crypto_symbol": "USDC",
        "address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "decimals": 6,
    },
    {
        "chain_code": "polygon-mainnet",
        "crypto_symbol": "USDT",
        "address": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "decimals": 6,
    },
    {
        "chain_code": "polygon-mainnet",
        "crypto_symbol": "DAI",
        "address": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "decimals": 18,
    },
    {
        "chain_code": "base-mainnet",
        "crypto_symbol": "USDC",
        "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "decimals": 6,
    },
    {
        "chain_code": "base-mainnet",
        "crypto_symbol": "USDT",
        "address": "0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2",
        "decimals": 6,
    },
    {
        "chain_code": "tron-mainnet",
        "crypto_symbol": "USDT",
        "address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
        "decimals": 6,
    },
)


def ensure_base_currencies(*, using: str = "default", stdout=None) -> None:
    """初始化系统级法币与基础加密货币。"""
    fiat_manager = Fiat.objects.using(using)
    crypto_manager = Crypto.objects.using(using)

    for fiat_code in ("USD", "CNY", "EUR", "JPY", "HKD"):
        fiat_manager.get_or_create(code=fiat_code)

    crypto_manager.get_or_create(
        # Coingecko 正确 ID 为 ethereum，错误 ID 会导致价格刷新永远取不到数据。
        name="Ethereum",
        symbol="ETH",
        coingecko_id="ethereum",
        decimals=18,
    )
    crypto_manager.get_or_create(
        name="Bitcoin",
        symbol="BTC",
        coingecko_id="bitcoin",
        decimals=8,
    )
    crypto_manager.get_or_create(
        name="TRON",
        symbol="TRX",
        coingecko_id="tron",
        decimals=6,
    )
    crypto_manager.get_or_create(
        name="Tether",
        symbol="USDT",
        coingecko_id="tether",
        decimals=6,
    )
    crypto_manager.get_or_create(
        name="USDC",
        symbol="USDC",
        coingecko_id="usd-coin",
        decimals=6,
    )
    crypto_manager.get_or_create(
        name="Dai",
        symbol="DAI",
        coingecko_id="dai",
        decimals=18,
    )
    if stdout is not None:
        stdout.write("✅ 货币初始化完成")


def ensure_production_currencies(*, using: str = "default", stdout=None) -> None:
    """补齐生产主网专用的原生币主数据。"""
    crypto_manager = Crypto.objects.using(using)

    crypto_manager.get_or_create(
        name="BNB",
        symbol="BNB",
        coingecko_id="binancecoin",
        decimals=18,
    )
    # Polygon PoS 主网当前 gas token 为 POL；这里显式建模，避免把 Polygon 误绑到 ETH/BNB。
    crypto_manager.get_or_create(
        name="Polygon Ecosystem Token",
        symbol="POL",
        coingecko_id="polygon",
        decimals=18,
    )
    if stdout is not None:
        stdout.write("✅ 生产主网原生币初始化完成")


def ensure_chain_native_mapping(
    *, using: str = "default", chain_code: str, crypto_symbol: str
) -> None:
    """为链原生币补齐 ChainToken 映射，保持余额与支持判断可用。"""
    chain_obj = Chain.objects.using(using).get(code=chain_code)
    crypto_obj = Crypto.objects.using(using).get(symbol=crypto_symbol)
    ChainToken.objects.using(using).get_or_create(
        crypto=crypto_obj,
        chain=chain_obj,
        defaults={"address": ""},
    )


def ensure_chain_token_mapping(
    *,
    using: str = "default",
    chain_code: str,
    crypto_symbol: str,
    address: str,
    decimals: int | None = None,
) -> None:
    """为链上 ERC20/同类合约资产补齐 ChainToken 映射。"""
    chain_obj = Chain.objects.using(using).get(code=chain_code)
    normalized_address = address.strip()
    if not normalized_address:
        return
    if chain_obj.type == ChainType.TRON:
        normalized_address = TronAddressCodec.normalize_base58(normalized_address)
    else:
        if not Web3.is_address(normalized_address):
            raise ValueError(
                f"{chain_code} 的 {crypto_symbol} 合约地址非法: {normalized_address}"
            )
        normalized_address = Web3.to_checksum_address(normalized_address)

    crypto_obj = Crypto.objects.using(using).get(symbol=crypto_symbol)
    ChainToken.objects.using(using).update_or_create(
        crypto=crypto_obj,
        chain=chain_obj,
        defaults={
            "address": normalized_address,
            "decimals": decimals,
        },
    )


def ensure_default_evm_token_mappings(
    *,
    using: str = "default",
    chain_code: str,
    skip_symbols: set[str] | None = None,
    stdout=None,
) -> None:
    """按环境变量补齐本地开发 EVM 稳定币映射。"""
    created_symbols: list[str] = []
    skipped = skip_symbols or set()

    for token_config in LOCAL_EVM_TOKEN_MAPPINGS:
        if token_config["crypto_symbol"] in skipped:
            continue
        address = env.str(token_config["env"], default="").strip()
        if not address:
            continue
        ensure_chain_token_mapping(
            using=using,
            chain_code=chain_code,
            crypto_symbol=token_config["crypto_symbol"],
            address=address,
        )
        created_symbols.append(token_config["crypto_symbol"])

    if stdout is not None and created_symbols:
        joined_symbols = ", ".join(created_symbols)
        stdout.write(f"✅ {chain_code} ERC20 映射初始化完成: {joined_symbols}")


def _build_local_evm_web3(*, rpc: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 5}))


def _normalize_evm_address(*, address: str, label: str) -> str:
    normalized_address = address.strip()
    if not Web3.is_address(normalized_address):
        raise ValueError(f"{label} 非法: {normalized_address}")
    return Web3.to_checksum_address(normalized_address)


def _deploy_local_evm_erc20_contract(*, w3: Web3) -> str:
    try:
        deployer = w3.eth.accounts[0]
    except IndexError as exc:
        raise RuntimeError("本地 anvil 未提供可用部署账户") from exc

    try:
        contract = w3.eth.contract(
            abi=LOCAL_EVM_ERC20_ABI,
            bytecode=LOCAL_EVM_ERC20_BYTECODE,
        )
        tx_hash = contract.constructor().transact({"from": deployer})
        receipt = w3.eth.wait_for_transaction_receipt(
            tx_hash,
            timeout=30,
            poll_latency=0.5,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("部署本地 USDT 模拟合约失败") from exc

    contract_address = receipt.get("contractAddress")
    if not contract_address:
        raise RuntimeError("部署本地 USDT 模拟合约失败：回执缺少合约地址")
    return Web3.to_checksum_address(contract_address)


def ensure_local_evm_usdt_contract_address(
    *,
    using: str = "default",
    chain_code: str,
    rpc: str,
) -> str:
    """确保本地 EVM 链存在可用的 USDT 合约地址。

    优先级：
    1. 显式环境变量 `LOCAL_EVM_USDT_ADDRESS`
    2. 数据库中已登记且链上仍有代码的现有地址
    3. 连接 anvil 自动部署一个标准 ERC20 mock 合约
    """
    configured_address = env.str("LOCAL_EVM_USDT_ADDRESS", default="").strip()
    w3 = _build_local_evm_web3(rpc=rpc)
    if not w3.is_connected():
        raise RuntimeError(f"无法连接本地 EVM RPC: {rpc}")

    if configured_address:
        checksum_address = _normalize_evm_address(
            address=configured_address,
            label="LOCAL_EVM_USDT_ADDRESS",
        )
        if not has_standard_erc20_interface(w3=w3, address=checksum_address):
            raise RuntimeError(
                "LOCAL_EVM_USDT_ADDRESS 不是标准 ERC20 合约，无法作为本地 USDT 使用"
            )
        return checksum_address

    existing_address = (
        ChainToken.objects.using(using)
        .filter(chain__code=chain_code, crypto__symbol="USDT")
        .values_list("address", flat=True)
        .first()
    )
    if existing_address and Web3.is_address(existing_address):
        checksum_address = Web3.to_checksum_address(existing_address)
        try:
            if has_standard_erc20_interface(w3=w3, address=checksum_address):
                return checksum_address
        except Exception:  # noqa: BLE001, S110
            # 旧地址探测失败时回退为重新部署，避免持久化链重建后卡在脏地址。
            pass

    return _deploy_local_evm_erc20_contract(w3=w3)


def ensure_public_chains(*, using: str = "default", stdout=None) -> None:
    """初始化生产环境默认主网配置。"""
    chain_manager = Chain.objects.using(using)

    for chain_config in PRODUCTION_MAINNET_CHAINS:
        native_coin = Crypto.objects.using(using).get(
            symbol=chain_config["native_symbol"]
        )
        chain_manager.update_or_create(
            code=chain_config["code"],
            defaults={
                "name": chain_config["name"],
                "type": chain_config["type"],
                "native_coin": native_coin,
                "chain_id": chain_config["chain_id"],
                "is_poa": chain_config["is_poa"],
                # 生产默认主数据只创建骨架配置；待管理员补完 RPC 后再手动启用。
                "rpc": "",
                "active": False,
            },
        )
        ensure_chain_native_mapping(
            using=using,
            chain_code=chain_config["code"],
            crypto_symbol=chain_config["native_symbol"],
        )
    for token_mapping in PRODUCTION_MAINNET_TOKEN_MAPPINGS:
        ensure_chain_token_mapping(using=using, **token_mapping)

    if stdout is not None:
        stdout.write("✅ 生产主网初始化完成")


def _build_local_bitcoin_wallet_rpc() -> str:
    """构造钱包级 Bitcoin RPC 地址，保证 watch-only / listunspent 使用同一钱包上下文。"""
    host = env.str("LOCAL_BTC_RPC_HOST", default="127.0.0.1")
    port = env.int("LOCAL_BTC_RPC_PORT", default=18443)
    user = quote(env.str("LOCAL_BTC_RPC_USER", default="xcash"), safe="")
    password = quote(env.str("LOCAL_BTC_RPC_PASSWORD", default="xcash"), safe="")
    wallet_name = quote(env.str("LOCAL_BTC_WALLET_NAME", default="xcash"), safe="")
    return f"http://{user}:{password}@{host}:{port}/wallet/{wallet_name}"


def build_local_bitcoin_root_rpc() -> str:
    """构造无钱包路径的 Bitcoin 根 RPC 地址，供创建/加载钱包使用。"""
    host = env.str("LOCAL_BTC_RPC_HOST", default="127.0.0.1")
    port = env.int("LOCAL_BTC_RPC_PORT", default=18443)
    user = quote(env.str("LOCAL_BTC_RPC_USER", default="xcash"), safe="")
    password = quote(env.str("LOCAL_BTC_RPC_PASSWORD", default="xcash"), safe="")
    return f"http://{user}:{password}@{host}:{port}"


def ensure_local_chains(*, using: str = "default", stdout=None) -> None:
    """初始化本地联调链配置，供本地 Ethereum / Bitcoin 端到端验证使用。"""
    chain_manager = Chain.objects.using(using)
    eth = Crypto.objects.using(using).get(symbol="ETH")
    btc = Crypto.objects.using(using).get(symbol="BTC")
    local_evm_chain_code = env.str("LOCAL_EVM_CHAIN_CODE", default="ethereum-local")
    local_btc_chain_code = env.str("LOCAL_BTC_CHAIN_CODE", default="bitcoin-local")
    local_evm_rpc = env.str("LOCAL_EVM_RPC", default="http://127.0.0.1:8545")
    local_usdt_address = ensure_local_evm_usdt_contract_address(
        using=using,
        chain_code=local_evm_chain_code,
        rpc=local_evm_rpc,
    )

    with transaction.atomic(using=using):
        chain_manager.update_or_create(
            code=local_evm_chain_code,
            defaults={
                "name": env.str("LOCAL_EVM_CHAIN_NAME", default="Ethereum Local"),
                "type": ChainType.EVM,
                "native_coin": eth,
                "chain_id": env.int("LOCAL_EVM_CHAIN_ID", default=31337),
                "is_poa": env.bool("LOCAL_EVM_IS_POA", default=False),
                "rpc": local_evm_rpc,
                # 本地联调要尽快推进状态，默认只要求 1 个确认块。
                "confirm_block_count": env.int("LOCAL_EVM_CONFIRM_BLOCKS", default=1),
                "active": True,
            },
        )
        chain_manager.update_or_create(
            code=local_btc_chain_code,
            defaults={
                "name": env.str("LOCAL_BTC_CHAIN_NAME", default="Bitcoin Local"),
                "type": ChainType.BITCOIN,
                "native_coin": btc,
                "rpc": env.str(
                    "LOCAL_BTC_RPC", default=_build_local_bitcoin_wallet_rpc()
                ),
                "confirm_block_count": env.int("LOCAL_BTC_CONFIRM_BLOCKS", default=1),
                "active": True,
            },
        )
        for chain_code, crypto_symbol in (
            (local_evm_chain_code, "ETH"),
            (local_btc_chain_code, "BTC"),
        ):
            ensure_chain_native_mapping(
                using=using,
                chain_code=chain_code,
                crypto_symbol=crypto_symbol,
            )
        ensure_chain_token_mapping(
            using=using,
            chain_code=local_evm_chain_code,
            crypto_symbol="USDT",
            address=local_usdt_address,
            decimals=LOCAL_EVM_USDT_DECIMALS,
        )
        ensure_default_evm_token_mappings(
            using=using,
            chain_code=local_evm_chain_code,
            skip_symbols={"USDT"},
            stdout=stdout,
        )

    if stdout is not None:
        stdout.write("✅ 本地联调链初始化完成")


def resolve_chain_bootstrap_profile() -> str:
    """解析默认链初始化方案。

    auto 策略遵循当前项目约定：
    - 开发/联调环境默认走本地 anvil + regtest
    - 其他环境默认走生产主网骨架配置
    """
    profile = env.str("DEFAULT_CHAIN_BOOTSTRAP_PROFILE", default="auto").strip().lower()
    if profile in {"off", "none"}:
        return "off"
    if profile in {"public", "local"}:
        return profile
    if profile != "auto":
        raise ValueError("DEFAULT_CHAIN_BOOTSTRAP_PROFILE 仅支持 auto/public/local/off")
    if settings.DEBUG or env.str("BITCOIN_NETWORK", default="mainnet") == "regtest":
        return "local"
    return "public"


def ensure_default_reference_data(*, using: str = "default", stdout=None) -> None:
    """自动补齐系统默认主数据。"""
    ensure_base_currencies(using=using, stdout=stdout)

    profile = resolve_chain_bootstrap_profile()
    if profile == "off":
        if stdout is not None:
            stdout.write("ℹ️ 默认链初始化已关闭")
        return
    if profile == "local":
        ensure_local_chains(using=using, stdout=stdout)
        return
    ensure_production_currencies(using=using, stdout=stdout)
    ensure_public_chains(using=using, stdout=stdout)
