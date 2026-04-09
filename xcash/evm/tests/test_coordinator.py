"""
协调器 _observe_confirmed_transaction 和 _parse_erc20_transfer_log 的单元测试。

覆盖：
- 原生币路径：get_block + get_transaction → 构建正确的 ObservedTransferPayload
- ERC-20 路径：从 receipt.logs 解析 Transfer 事件 → 构建正确的 ObservedTransferPayload
- _parse_erc20_transfer_log 独立测试：正常解析、空 logs、非 Transfer topic
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock
from unittest.mock import patch

from django.test import TestCase
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import TransferType
from chains.models import Wallet
from currencies.models import ChainToken
from currencies.models import Crypto
from evm.coordinator import InternalEvmTaskCoordinator
from evm.coordinator import _parse_erc20_transfer_log
from evm.models import EvmBroadcastTask
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0


# ---------------------------------------------------------------------------
# 公共测试地址（已通过 Web3.to_checksum_address 转换，满足 EIP-55 checksum 要求）
# ---------------------------------------------------------------------------
_SENDER_HEX = Web3.to_checksum_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
_RECEIVER_HEX = Web3.to_checksum_address("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
_VAULT_HEX = Web3.to_checksum_address("0xcccccccccccccccccccccccccccccccccccccccc")
_CONTRACT_HEX = Web3.to_checksum_address("0xdddddddddddddddddddddddddddddddddddddddd")


def _make_erc20_transfer_log(
    *,
    from_hex: str = _SENDER_HEX,
    to_hex: str = _RECEIVER_HEX,
    value_int: int = 100_000_000,
    log_index: int = 5,
) -> dict:
    """构造一条符合 ERC-20 Transfer 规范的 receipt log 字典。

    topics[0] = Transfer(address,address,uint256) keccak
    topics[1] = from（左填充 32 字节）
    topics[2] = to（左填充 32 字节）
    data       = value（32 字节大端整数，hex）
    """
    topic0_bytes = bytes.fromhex(ERC20_TRANSFER_TOPIC0.removeprefix("0x"))
    # 地址左填充到 32 字节（去掉 0x 前缀后补 0 到 64 位）
    from_padded = bytes.fromhex(from_hex.removeprefix("0x").zfill(64))
    to_padded = bytes.fromhex(to_hex.removeprefix("0x").zfill(64))
    value_hex = "0x" + hex(value_int)[2:].zfill(64)

    return {
        "topics": [topic0_bytes, from_padded, to_padded],
        "data": value_hex,
        "logIndex": log_index,
    }


# ---------------------------------------------------------------------------
# Task 5a：_parse_erc20_transfer_log 独立测试
# ---------------------------------------------------------------------------
class ParseErc20TransferLogTest(TestCase):
    """_parse_erc20_transfer_log 的纯逻辑单元测试，不依赖 DB。"""

    def test_parses_valid_transfer_log(self):
        """正常 ERC-20 Transfer 事件可被正确解析，字段值与原始数据一致。"""
        log = _make_erc20_transfer_log(
            from_hex=_SENDER_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=100_000_000,
            log_index=5,
        )
        receipt = {"logs": [log]}

        result = _parse_erc20_transfer_log(receipt=receipt)

        self.assertIsNotNone(result)
        self.assertEqual(
            result["from_address"],
            Web3.to_checksum_address(_SENDER_HEX),
        )
        self.assertEqual(
            result["to_address"],
            Web3.to_checksum_address(_RECEIVER_HEX),
        )
        self.assertEqual(result["value"], Decimal(100_000_000))
        self.assertEqual(result["event_id"], "erc20:5")

    def test_returns_none_when_no_transfer_log(self):
        """logs 为空时返回 None。"""
        result = _parse_erc20_transfer_log(receipt={"logs": []})
        self.assertIsNone(result)

    def test_skips_non_transfer_topics(self):
        """topic0 不匹配 Transfer 签名的日志（如 Approval）应被跳过，返回 None。"""
        # 构造一条 Approval 日志：topic0 使用 Approval keccak
        approval_topic0 = Web3.keccak(text="Approval(address,address,uint256)")
        from_padded = bytes.fromhex(_SENDER_HEX.removeprefix("0x").zfill(64))
        to_padded = bytes.fromhex(_RECEIVER_HEX.removeprefix("0x").zfill(64))
        approval_log = {
            "topics": [approval_topic0, from_padded, to_padded],
            "data": "0x" + hex(1000)[2:].zfill(64),
            "logIndex": 0,
        }
        receipt = {"logs": [approval_log]}

        result = _parse_erc20_transfer_log(receipt=receipt)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# Task 4：_observe_confirmed_transaction — 原生币路径
# ---------------------------------------------------------------------------
class ObserveConfirmedNativeTest(TestCase):
    """原生币路径：从 get_transaction 取 from/to/value，喂回扫描器管线。"""

    def setUp(self):
        self.eth = Crypto.objects.create(
            name="Ethereum Coordinator Native",
            symbol="ETHCN",
            decimals=18,
            coingecko_id="ethereum-coordinator-native",
        )
        self.chain = Chain.objects.create(
            code="eth-coord-native",
            name="Ethereum Coordinator Native",
            type=ChainType.EVM,
            chain_id=90_001,
            # rpc 设为空字符串以跳过 save() 内的自动 RPC 检测
            rpc="",
            native_coin=self.eth,
            active=True,
        )
        # currencies.signals 在 Chain 创建后自动触发 ensure_native_crypto_mapping_for_chain，
        # 已经建好 (eth, chain) 的 ChainToken；这里只需补齐精度覆盖字段即可。
        ChainToken.objects.filter(crypto=self.eth, chain=self.chain).update(decimals=18)
        self.wallet = Wallet.objects.create()
        self.address = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=0,
            address_index=0,
            address=_VAULT_HEX,
        )
        self.base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.address,
            transfer_type=TransferType.Withdrawal,
            crypto=self.eth,
            recipient=_RECEIVER_HEX,
            amount=Decimal("1.5"),
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        self.evm_task = EvmBroadcastTask.objects.create(
            base_task=self.base_task,
            address=self.address,
            chain=self.chain,
            to=Web3.to_checksum_address(_RECEIVER_HEX),
            value=Decimal("1500000000000000000"),
            nonce=0,
            gas=21000,
        )

    def test_native_confirmed_feeds_to_scanner_pipeline(self):
        """原生币已确认时，_observe_confirmed_transaction 用正确载荷调用 TransferService。"""
        tx_hash = "0x" + "ab" * 32
        receipt = {
            "blockNumber": 100,
            "status": 1,
            "logs": [],
        }

        mock_block = {"timestamp": 1700000000}
        mock_tx = {
            "from": _SENDER_HEX,
            "to": _RECEIVER_HEX,
            "value": 1500000000000000000,
        }

        mock_w3 = MagicMock()
        mock_w3.eth.get_block.return_value = mock_block
        mock_w3.eth.get_transaction.return_value = mock_tx

        with (
            patch.object(type(self.chain), "w3", new_callable=lambda: property(lambda self: mock_w3)),
            patch("chains.service.TransferService.create_observed_transfer") as mock_create,
        ):
            InternalEvmTaskCoordinator._observe_confirmed_transaction(
                evm_task=self.evm_task,
                tx_hash=tx_hash,
                receipt=receipt,
            )

        mock_create.assert_called_once()
        payload = mock_create.call_args.kwargs["observed"]

        self.assertEqual(payload.chain, self.chain)
        self.assertEqual(payload.block, 100)
        self.assertEqual(payload.tx_hash, tx_hash)
        self.assertEqual(payload.event_id, "native:tx")
        self.assertEqual(
            payload.from_address, Web3.to_checksum_address(_SENDER_HEX)
        )
        self.assertEqual(
            payload.to_address, Web3.to_checksum_address(_RECEIVER_HEX)
        )
        self.assertEqual(payload.crypto, self.eth)
        self.assertEqual(payload.value, Decimal("1500000000000000000"))
        self.assertEqual(payload.amount, Decimal("1.5"))
        self.assertEqual(payload.source, "evm-coordinator")

        # 原生币路径必须调用 get_transaction 来获取 from/to/value
        mock_w3.eth.get_transaction.assert_called_once_with(tx_hash)


# ---------------------------------------------------------------------------
# Task 5b：_observe_confirmed_transaction — ERC-20 路径
# ---------------------------------------------------------------------------
class ObserveConfirmedErc20Test(TestCase):
    """ERC-20 路径：从 receipt.logs 解析 Transfer，无需调用 get_transaction。"""

    def setUp(self):
        self.eth = Crypto.objects.create(
            name="Ethereum Coordinator ERC20",
            symbol="ETHCE",
            decimals=18,
            coingecko_id="ethereum-coordinator-erc20",
        )
        self.usdt = Crypto.objects.create(
            name="Tether Coordinator",
            symbol="USDTC",
            decimals=6,
            coingecko_id="tether-coordinator",
        )
        self.chain = Chain.objects.create(
            code="eth-coord-erc20",
            name="Ethereum Coordinator ERC20",
            type=ChainType.EVM,
            chain_id=90_002,
            rpc="",
            native_coin=self.eth,
            active=True,
        )
        # currencies.signals 在 Chain 创建后自动触发 ensure_native_crypto_mapping_for_chain，
        # 已经建好 (eth, chain) 的 ChainToken；仅补齐精度覆盖和补建 USDT ChainToken。
        ChainToken.objects.filter(crypto=self.eth, chain=self.chain).update(decimals=18)
        ChainToken.objects.create(
            crypto=self.usdt,
            chain=self.chain,
            address=_CONTRACT_HEX,
            decimals=6,
        )
        self.wallet = Wallet.objects.create()
        self.address = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=0,
            address_index=0,
            address=_VAULT_HEX,
        )
        self.base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.address,
            transfer_type=TransferType.Withdrawal,
            crypto=self.usdt,
            recipient=_RECEIVER_HEX,
            amount=Decimal("100"),
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        # ERC-20 发送时 value=0，data 包含 transfer calldata
        self.evm_task = EvmBroadcastTask.objects.create(
            base_task=self.base_task,
            address=self.address,
            chain=self.chain,
            to=Web3.to_checksum_address(_CONTRACT_HEX),
            value=Decimal("0"),
            nonce=0,
            gas=100000,
            data="0xa9059cbb",
        )

    def test_erc20_confirmed_feeds_to_scanner_pipeline(self):
        """ERC-20 已确认时，从 receipt.logs 解析 Transfer，不调用 get_transaction。"""
        tx_hash = "0x" + "cd" * 32
        transfer_log = _make_erc20_transfer_log(
            from_hex=_SENDER_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=100_000_000,  # 100 USDT（精度 6）
            log_index=5,
        )
        receipt = {
            "blockNumber": 200,
            "status": 1,
            "logs": [transfer_log],
        }

        mock_block = {"timestamp": 1700001000}
        mock_w3 = MagicMock()
        mock_w3.eth.get_block.return_value = mock_block

        with (
            patch.object(type(self.chain), "w3", new_callable=lambda: property(lambda self: mock_w3)),
            patch("chains.service.TransferService.create_observed_transfer") as mock_create,
        ):
            InternalEvmTaskCoordinator._observe_confirmed_transaction(
                evm_task=self.evm_task,
                tx_hash=tx_hash,
                receipt=receipt,
            )

        mock_create.assert_called_once()
        payload = mock_create.call_args.kwargs["observed"]

        self.assertEqual(payload.chain, self.chain)
        self.assertEqual(payload.block, 200)
        self.assertEqual(payload.tx_hash, tx_hash)
        self.assertEqual(payload.event_id, "erc20:5")
        self.assertEqual(
            payload.from_address, Web3.to_checksum_address(_SENDER_HEX)
        )
        self.assertEqual(
            payload.to_address, Web3.to_checksum_address(_RECEIVER_HEX)
        )
        self.assertEqual(payload.crypto, self.usdt)
        self.assertEqual(payload.value, Decimal(100_000_000))
        self.assertEqual(payload.amount, Decimal("100"))
        self.assertEqual(payload.source, "evm-coordinator")

        # ERC-20 路径不应调用 get_transaction，from/to/value 来自 receipt.logs
        mock_w3.eth.get_transaction.assert_not_called()
