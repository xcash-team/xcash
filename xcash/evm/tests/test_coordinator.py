"""
协调器 _observe_confirmed_transaction 和 _parse_erc20_transfer_log 的单元测试。

覆盖：
- 原生币路径：get_block + get_transaction → 构建正确的 ObservedTransferPayload
- ERC-20 路径：从 receipt.logs 解析 Transfer 事件 → 构建正确的 ObservedTransferPayload
- _parse_erc20_transfer_log 独立测试：正常解析、空 logs、非 Transfer topic
- 集成测试：reconcile_chain → create_observed_transfer → process() 完整管线
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskFailureReason
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
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
    contract_address: str = _CONTRACT_HEX,
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
        "address": contract_address,
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

    def test_filters_by_contract_sender_recipient_and_value(self):
        """内部任务解析 receipt 时必须按任务预期筛选唯一 Transfer 日志。"""
        wrong_contract = _make_erc20_transfer_log(
            contract_address=Web3.to_checksum_address(
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            from_hex=_SENDER_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=100_000_000,
            log_index=1,
        )
        wrong_value = _make_erc20_transfer_log(
            contract_address=_CONTRACT_HEX,
            from_hex=_SENDER_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=1,
            log_index=2,
        )
        expected = _make_erc20_transfer_log(
            contract_address=_CONTRACT_HEX,
            from_hex=_SENDER_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=100_000_000,
            log_index=3,
        )

        result = _parse_erc20_transfer_log(
            receipt={"logs": [wrong_contract, wrong_value, expected]},
            token_address=_CONTRACT_HEX,
            from_address=_SENDER_HEX,
            to_address=_RECEIVER_HEX,
            value=Decimal(100_000_000),
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["event_id"], "erc20:3")


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
            contract_address=_CONTRACT_HEX,
            from_hex=_VAULT_HEX,
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
            payload.from_address, Web3.to_checksum_address(_VAULT_HEX)
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

    def test_erc20_confirmed_ignores_mismatched_transfer_log(self):
        """receipt 内没有符合任务参数的 Transfer 日志时，不应创建观察转账。"""
        tx_hash = "0x" + "ce" * 32
        wrong_transfer_log = _make_erc20_transfer_log(
            contract_address=_CONTRACT_HEX,
            from_hex=_SENDER_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=100_000_000,
            log_index=5,
        )
        receipt = {
            "blockNumber": 201,
            "status": 1,
            "logs": [wrong_transfer_log],
        }

        mock_w3 = MagicMock()
        mock_w3.eth.get_block.return_value = {"timestamp": 1700001000}

        with (
            patch.object(type(self.chain), "w3", new_callable=lambda: property(lambda self: mock_w3)),
            patch("chains.service.TransferService.create_observed_transfer") as mock_create,
        ):
            InternalEvmTaskCoordinator._observe_confirmed_transaction(
                evm_task=self.evm_task,
                tx_hash=tx_hash,
                receipt=receipt,
            )

        mock_create.assert_not_called()


# ---------------------------------------------------------------------------
# 集成测试：reconcile_chain → TransferService → process() 完整管线
# ---------------------------------------------------------------------------
class CoordinatorIntegrationTest(TestCase):
    """协调器兜底路径集成测试：scanner 漏扫时，协调器作为兜底创建 OnchainTransfer 并完成匹配。"""

    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.native = Crypto.objects.create(
            name="Ethereum Coordinator Integration",
            symbol="ETHCI",
            coingecko_id="ethereum-coordinator-integration",
            decimals=18,
        )
        self.token = Crypto.objects.create(
            name="USDC Coordinator Integration",
            symbol="USDCCI",
            coingecko_id="usdc-coordinator-integration",
            decimals=6,
        )
        self.chain = Chain.objects.create(
            code="eth-coord-integ",
            name="Ethereum Coordinator Integration",
            type=ChainType.EVM,
            chain_id=90_100,
            rpc="",
            native_coin=self.native,
            active=True,
        )
        # currencies.signals 自动创建 native ChainToken；只需创建 token ChainToken
        ChainToken.objects.create(
            crypto=self.token,
            chain=self.chain,
            address=_CONTRACT_HEX,
            decimals=6,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=0,
            address_index=0,
            address=_VAULT_HEX,
        )

    # ---- helpers ----

    def _create_erc20_withdrawal(self, *, tx_hash):
        """创建一个 ERC-20 提币场景的完整 fixture。"""
        from chains.models import TxHash
        from projects.models import Project
        from withdrawals.models import Withdrawal
        from withdrawals.models import WithdrawalStatus

        project = Project.objects.create(
            name=f"proj-{tx_hash[-6:]}",
            wallet=self.wallet,
            webhook="https://example.com/wh",
        )
        base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.token,
            recipient=_RECEIVER_HEX,
            amount=Decimal("100"),
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        TxHash.objects.create(
            broadcast_task=base_task, chain=self.chain, hash=tx_hash, version=0,
        )
        evm_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=self.addr,
            chain=self.chain,
            nonce=0,
            to=_CONTRACT_HEX,
            value=0,
            data="0xa9059cbb",
            gas=60000,
            gas_price=1,
            signed_payload="0x01",
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            chain=self.chain,
            crypto=self.token,
            amount=Decimal("100"),
            worth=Decimal("100"),
            out_no=f"out-{tx_hash[-6:]}",
            to=_RECEIVER_HEX,
            broadcast_task=base_task,
            status=WithdrawalStatus.PENDING,
            hash=tx_hash,
        )
        return withdrawal, base_task, evm_task

    def _create_native_withdrawal(self, *, tx_hash):
        """创建一个 Native 提币场景的完整 fixture。"""
        from chains.models import TxHash
        from projects.models import Project
        from withdrawals.models import Withdrawal
        from withdrawals.models import WithdrawalStatus

        project = Project.objects.create(
            name=f"proj-native-{tx_hash[-6:]}",
            wallet=self.wallet,
            webhook="https://example.com/wh",
        )
        base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.native,
            recipient=_RECEIVER_HEX,
            amount=Decimal("1.5"),
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        TxHash.objects.create(
            broadcast_task=base_task, chain=self.chain, hash=tx_hash, version=0,
        )
        evm_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=self.addr,
            chain=self.chain,
            nonce=0,
            to=_RECEIVER_HEX,
            value=Decimal("1500000000000000000"),
            data="",
            gas=21000,
            gas_price=1,
            signed_payload="0x01",
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            chain=self.chain,
            crypto=self.native,
            amount=Decimal("1.5"),
            worth=Decimal("1.5"),
            out_no=f"out-native-{tx_hash[-6:]}",
            to=_RECEIVER_HEX,
            broadcast_task=base_task,
            status=WithdrawalStatus.PENDING,
            hash=tx_hash,
        )
        return withdrawal, base_task, evm_task

    def _make_overdue(self, evm_task):
        from datetime import timedelta

        from evm.constants import EVM_PENDING_REBROADCAST_TIMEOUT

        evm_task.last_attempt_at = timezone.now() - timedelta(
            seconds=EVM_PENDING_REBROADCAST_TIMEOUT + 60
        )
        evm_task.save(update_fields=["last_attempt_at"])

    def _mock_erc20_rpc(self, *, tx_hash, block_number=100, timestamp=1700000000):
        """返回一个配好 ERC-20 场景 RPC mock 的 SimpleNamespace。"""
        transfer_log = _make_erc20_transfer_log(
            from_hex=_VAULT_HEX,
            to_hex=_RECEIVER_HEX,
            value_int=100_000_000,  # 100 USDC (6 decimals)
            log_index=3,
        )
        receipt = {"status": 1, "blockNumber": block_number, "logs": [transfer_log]}
        return SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value=receipt),
                get_block=Mock(return_value={"timestamp": timestamp}),
            ),
        )

    def _mock_native_rpc(self, *, tx_hash, block_number=100, timestamp=1700000000):
        """返回一个配好 native 场景 RPC mock 的 SimpleNamespace。"""
        receipt = {"status": 1, "blockNumber": block_number, "logs": []}
        return SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value=receipt),
                get_block=Mock(return_value={"timestamp": timestamp}),
                get_transaction=Mock(return_value={
                    "from": _VAULT_HEX,
                    "to": _RECEIVER_HEX,
                    "value": 1500000000000000000,
                }),
            ),
        )

    # ---- Test 1 ----

    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_erc20_coordinator_creates_onchain_transfer_and_dispatches_process(
        self,
        chain_w3_mock,
        process_mock,
        webhook_mock,
    ):
        """ERC-20 提币超时后，协调器创建 OnchainTransfer 并派发 process 任务。"""
        tx_hash = "0x" + "a1" * 32
        withdrawal, base_task, evm_task = self._create_erc20_withdrawal(tx_hash=tx_hash)
        self._make_overdue(evm_task)
        chain_w3_mock.return_value = self._mock_erc20_rpc(tx_hash=tx_hash)

        with self.captureOnCommitCallbacks(execute=True):
            InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        # 验证 OnchainTransfer 被创建且字段正确
        self.assertEqual(
            OnchainTransfer.objects.filter(chain=self.chain, hash=tx_hash).count(), 1,
        )
        transfer = OnchainTransfer.objects.get(chain=self.chain, hash=tx_hash)
        self.assertEqual(transfer.event_id, "erc20:3")
        self.assertEqual(transfer.from_address, _VAULT_HEX)
        self.assertEqual(transfer.to_address, _RECEIVER_HEX)
        self.assertEqual(transfer.value, Decimal("100000000"))
        self.assertEqual(transfer.amount, Decimal("100"))

        # process_transfer.apply_async 应被调用一次（on_commit 回调）
        process_mock.assert_called_once()

        # BroadcastTask 应已被推进到 PENDING_CONFIRM
        base_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CONFIRM)

    # ---- Test 2 ----

    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_erc20_coordinator_observe_then_process_matches_withdrawal(
        self,
        chain_w3_mock,
        process_mock,
        webhook_mock,
    ):
        """ERC-20 完整管线：reconcile 创建 Transfer → process() 匹配提币。"""
        from withdrawals.models import WithdrawalStatus

        tx_hash = "0x" + "a2" * 32
        withdrawal, base_task, evm_task = self._create_erc20_withdrawal(tx_hash=tx_hash)
        self._make_overdue(evm_task)
        chain_w3_mock.return_value = self._mock_erc20_rpc(tx_hash=tx_hash)

        with self.captureOnCommitCallbacks(execute=True):
            InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        # 手动调用 process()（模拟 Celery worker 执行）
        transfer = OnchainTransfer.objects.get(chain=self.chain, hash=tx_hash)
        transfer.process()

        withdrawal.refresh_from_db()
        transfer.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)
        self.assertEqual(withdrawal.transfer, transfer)
        self.assertEqual(transfer.type, TransferType.Withdrawal)
        self.assertIsNotNone(transfer.processed_at)

    def test_process_ignores_internal_withdrawal_when_transfer_value_mismatches(self):
        """同 tx_hash 的异常事件不应仅凭 hash 绑定为内部提币。"""
        from withdrawals.models import WithdrawalStatus

        tx_hash = "0x" + "a6" * 32
        withdrawal, base_task, _evm_task = self._create_erc20_withdrawal(
            tx_hash=tx_hash
        )
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=100,
            hash=tx_hash,
            event_id="erc20:bad",
            crypto=self.token,
            from_address=self.addr.address,
            to_address=_RECEIVER_HEX,
            value=Decimal("1"),
            amount=Decimal("0.000001"),
            timestamp=1700000000,
            datetime=timezone.now(),
        )

        transfer.process()

        withdrawal.refresh_from_db()
        base_task.refresh_from_db()
        transfer.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer_id)
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(transfer.type, "")
        self.assertIsNotNone(transfer.processed_at)

    # ---- Test 3 ----

    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_native_coordinator_observe_then_process_matches_withdrawal(
        self,
        chain_w3_mock,
        process_mock,
        webhook_mock,
    ):
        """Native 完整管线：reconcile 创建 Transfer → process() 匹配提币。"""
        from withdrawals.models import WithdrawalStatus

        tx_hash = "0x" + "a3" * 32
        withdrawal, base_task, evm_task = self._create_native_withdrawal(tx_hash=tx_hash)
        self._make_overdue(evm_task)
        chain_w3_mock.return_value = self._mock_native_rpc(tx_hash=tx_hash)

        with self.captureOnCommitCallbacks(execute=True):
            InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        transfer = OnchainTransfer.objects.get(chain=self.chain, hash=tx_hash)
        self.assertEqual(transfer.event_id, "native:tx")

        # 手动调用 process()
        transfer.process()

        withdrawal.refresh_from_db()
        transfer.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)
        self.assertEqual(withdrawal.transfer, transfer)
        self.assertEqual(transfer.type, TransferType.Withdrawal)
        self.assertIsNotNone(transfer.processed_at)

    # ---- Test 4 ----

    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_coordinator_idempotent_when_scanner_already_created_transfer(
        self,
        chain_w3_mock,
        process_mock,
        webhook_mock,
    ):
        """Scanner 已创建同一 Transfer 时，协调器不重复创建也不重复派发 process。"""
        tx_hash = "0x" + "a4" * 32
        withdrawal, base_task, evm_task = self._create_erc20_withdrawal(tx_hash=tx_hash)
        self._make_overdue(evm_task)

        # 预先创建 OnchainTransfer，模拟 scanner 已处理
        OnchainTransfer.objects.create(
            chain=self.chain,
            block=100,
            hash=tx_hash,
            event_id="erc20:3",
            crypto=self.token,
            from_address=_VAULT_HEX,
            to_address=_RECEIVER_HEX,
            value=Decimal("100000000"),
            amount=Decimal("100"),
            timestamp=1700000000,
            datetime=timezone.now(),
        )

        chain_w3_mock.return_value = self._mock_erc20_rpc(tx_hash=tx_hash)

        with self.captureOnCommitCallbacks(execute=True):
            InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        # 不应产生重复记录
        self.assertEqual(
            OnchainTransfer.objects.filter(chain=self.chain, hash=tx_hash).count(), 1,
        )
        # BroadcastTask 仍被推进到 PENDING_CONFIRM（幂等路径也会调用 mark_pending_confirm）
        base_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CONFIRM)
        # 幂等路径不应派发 process（created=False）
        process_mock.assert_not_called()

    # ---- Test 5 ----

    @patch("withdrawals.service.WebhookService.create_event")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_coordinator_marks_failed_when_receipt_status_zero(
        self,
        chain_w3_mock,
        webhook_mock,
    ):
        """链上 receipt status=0 时，协调器标记 BroadcastTask 失败、提币失败。"""
        from withdrawals.models import WithdrawalStatus

        tx_hash = "0x" + "a5" * 32
        withdrawal, base_task, evm_task = self._create_erc20_withdrawal(tx_hash=tx_hash)
        self._make_overdue(evm_task)

        chain_w3_mock.return_value = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"status": 0}),
            ),
        )

        with self.captureOnCommitCallbacks(execute=True):
            InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        base_task.refresh_from_db()
        withdrawal.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(base_task.result, BroadcastTaskResult.FAILED)
        self.assertEqual(
            base_task.failure_reason,
            BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )
        self.assertEqual(withdrawal.status, WithdrawalStatus.FAILED)
        # 失败交易不应创建 OnchainTransfer
        self.assertEqual(
            OnchainTransfer.objects.filter(hash=tx_hash).count(), 0,
        )
