import threading
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.contrib.admin.sites import AdminSite
from django.core.cache import cache
from django.db import connections
from django.db import close_old_connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.test import override_settings
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
from chains.models import TxHash
from chains.models import Wallet
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from common.consts import ERC20_TRANSFER_GAS
from currencies.models import ChainToken
from currencies.models import Crypto
from evm.admin import EvmScanCursorAdmin
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.erc20 import EvmErc20TransferScanner
from evm.scanner.native import EvmNativeDirectScanner
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcError
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage


class EvmBroadcastTaskTests(TestCase):
    def test_next_nonce_returns_count_of_existing_tasks(self):
        # nonce 基于已有任务数量推算，事务回滚时自动复用，不会产生空洞。
        from chains.models import AddressChainState

        native = Crypto.objects.create(
            name="Ethereum Nonce",
            symbol="ETHN",
            coingecko_id="ethereum-nonce",
        )
        chain = Chain.objects.create(
            code="eth-nonce",
            name="Ethereum Nonce",
            type=ChainType.EVM,
            chain_id=999,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        wallet = Wallet.objects.create()
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000F01",
        )

        state = AddressChainState.acquire_for_update(address=addr, chain=chain)

        # 无任何任务时 nonce 应从 0 开始
        self.assertEqual(EvmBroadcastTask._next_nonce(addr, chain, state=state), 0)

        # 创建一个任务后 nonce 应为 1
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            tx_hash="0x" + "a1" * 32,
            stage=BroadcastTaskStage.QUEUED,
        )
        EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            to="0x0000000000000000000000000000000000000002",
            value=0,
            nonce=0,
            gas=21000,
            gas_price=1,
            signed_payload="0x00",
        )
        state.refresh_from_db()
        self.assertEqual(EvmBroadcastTask._next_nonce(addr, chain, state=state), 1)

    def test_broadcast_records_last_attempt_without_marking_completion(self):
        # EVM 主执行对象只记录发送尝试；是否上链由统一父任务状态推进。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                # 余额覆盖 2 * erc20_transfer_gas 阈值即可通过主动检查
                get_balance=Mock(return_value=10**18),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=Mock(),
            ),
        )
        addr = Address(
            wallet=Wallet(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000001",
        )
        broadcast_task = EvmBroadcastTask(
            address=addr,
            chain=chain,
            nonce=1,
            to="0x0000000000000000000000000000000000000002",
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )
        broadcast_task.save = Mock()

        broadcast_task.broadcast()

        self.assertIsNotNone(broadcast_task.last_attempt_at)

    def test_broadcast_preflight_threshold_recharges_gas_for_collection(self):
        # 归集场景 native 余额低于阈值：pre-flight 主动补 gas，保持 QUEUED，
        # 不调用 estimate_gas / send_raw_transaction，不更新 last_attempt_at。
        from deposits.models import DepositAddress
        from projects.models import Project
        from users.models import Customer

        native = Crypto.objects.create(
            name="Ethereum Broadcast Failure",
            symbol="ETHBF",
            coingecko_id="ethereum-broadcast-failure",
        )
        chain = Chain.objects.create(
            code="eth-broadcast-failure",
            name="Ethereum Broadcast Failure",
            type=ChainType.EVM,
            chain_id=20101,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
            base_transfer_gas=21_000,
            erc20_transfer_gas=60_000,
        )
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="broadcast-failure-project",
            wallet=wallet,
            webhook="https://example.com/webhook",
        )
        customer = Customer.objects.create(project=project, uid="c-ebf")
        # Vault 地址必须存在，GasRechargeService 需要从钱包派生
        Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=100_000_000,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000100"
            ),
        )
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000101"
            ),
        )
        DepositAddress.objects.create(
            customer=customer,
            chain_type=ChainType.EVM,
            address=addr,
        )
        # 阈值 = value(10^18) + 2 * 1 * 60_000 = 10^18 + 120_000
        # 余额 10^17 远低于阈值 → 触发主动补给
        estimate_gas_mock = Mock()
        send_raw_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**17),
                estimate_gas=estimate_gas_mock,
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.DepositCollection,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000102"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "1" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with patch(
            "deposits.service.GasRechargeService.request_recharge",
            return_value=True,
        ) as recharge_mock:
            broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")
        # last_attempt_at 未推进，等下一轮 dispatch 再试
        self.assertIsNone(broadcast_task.last_attempt_at)
        estimate_gas_mock.assert_not_called()
        send_raw_mock.assert_not_called()
        # 核心证据：GasRechargeService.request_recharge 被触发，参数为 erc20_gas_cost
        recharge_mock.assert_called_once()
        _, kwargs = recharge_mock.call_args
        self.assertEqual(kwargs["chain"], chain)
        self.assertEqual(kwargs["deposit_address"].address_id, addr.pk)
        self.assertEqual(kwargs["erc20_gas_cost"], 1 * 60_000)

    def test_broadcast_preflight_threshold_delegates_to_idempotent_recharge_service(self):
        # 反复广播不应重复创建补给记录：broadcast 只负责"检测到余额不足 → 委派给
        # GasRechargeService"，真正的幂等由 GasRechargeService.request_recharge 负责
        # （见 GasRechargeServiceTests）。这里断言 broadcast 端每次都如实调用同一入口、
        # 参数一致，让 service 层的幂等判定成为唯一真理。
        from deposits.models import DepositAddress
        from projects.models import Project
        from users.models import Customer

        native = Crypto.objects.create(
            name="Ethereum Preflight Idempotent",
            symbol="ETHPFID",
            coingecko_id="ethereum-preflight-idempotent",
        )
        chain = Chain.objects.create(
            code="eth-preflight-idempotent",
            name="Ethereum Preflight Idempotent",
            type=ChainType.EVM,
            chain_id=20109,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
            base_transfer_gas=21_000,
            erc20_transfer_gas=60_000,
        )
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="preflight-idempotent-project",
            wallet=wallet,
            webhook="https://example.com/webhook",
        )
        customer = Customer.objects.create(project=project, uid="c-pfid")
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000111"
            ),
        )
        DepositAddress.objects.create(
            customer=customer,
            chain_type=ChainType.EVM,
            address=addr,
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**17),
                estimate_gas=Mock(),
                send_raw_transaction=Mock(),
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.DepositCollection,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000112"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with patch(
            "deposits.service.GasRechargeService.request_recharge",
            return_value=True,
        ) as recharge_mock:
            broadcast_task.broadcast()
            broadcast_task.broadcast()

        # 两次广播都委派给同一幂等入口，参数一致；真正的去重由 service 层保障。
        self.assertEqual(recharge_mock.call_count, 2)
        for _, kwargs in recharge_mock.call_args_list:
            self.assertEqual(kwargs["chain"], chain)
            self.assertEqual(kwargs["deposit_address"].address_id, addr.pk)
            self.assertEqual(kwargs["erc20_gas_cost"], 1 * 60_000)
        base_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)

    def test_broadcast_preflight_threshold_skips_gas_recharge_for_withdrawal(self):
        # Withdrawal 从 Vault 发起，余额不足时不应补 gas（补也是 vault→vault 死循环），
        # 仅保持 QUEUED 静默返回，等运营向 Vault 注资即可。
        from deposits.models import GasRecharge

        native = Crypto.objects.create(
            name="Ethereum Vault Reraise",
            symbol="ETHVR",
            coingecko_id="ethereum-vault-reraise",
        )
        chain = Chain.objects.create(
            code="eth-vault-reraise",
            name="Ethereum Vault Reraise",
            type=ChainType.EVM,
            chain_id=20199,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
            base_transfer_gas=21_000,
            erc20_transfer_gas=60_000,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000199"
            ),
        )
        estimate_gas_mock = Mock()
        send_raw_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**17),
                estimate_gas=estimate_gas_mock,
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000200"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "d" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")
        # Withdrawal 不补 gas：GasRecharge 记录为空
        self.assertFalse(GasRecharge.objects.exists())
        estimate_gas_mock.assert_not_called()
        send_raw_mock.assert_not_called()
        self.assertIsNone(broadcast_task.last_attempt_at)

    def test_broadcast_keeps_queued_on_preflight_revert_to_preserve_nonce(self):
        # pre-flight 命中合约 revert 时不能直接终局失败；该 nonce 尚未上链消费，
        # 必须保持 QUEUED 来阻断后续更高 nonce，避免制造链上 nonce 空洞。
        from deposits.models import Deposit
        from deposits.models import DepositAddress
        from deposits.models import DepositCollection
        from deposits.models import DepositStatus
        from chains.models import OnchainTransfer
        from chains.models import TransferStatus
        from projects.models import Project
        from users.models import Customer
        from web3.exceptions import ContractLogicError

        native = Crypto.objects.create(
            name="Ethereum Preflight Revert Native",
            symbol="ETHPR",
            coingecko_id="ethereum-preflight-revert-native",
        )
        crypto = Crypto.objects.create(
            name="Tether Preflight Revert",
            symbol="USDTPR",
            coingecko_id="tether-preflight-revert",
            decimals=6,
        )
        chain = Chain.objects.create(
            code="eth-preflight-revert",
            name="Ethereum Preflight Revert",
            type=ChainType.EVM,
            chain_id=20301,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="preflight-revert-project", wallet=wallet,
        )
        customer = Customer.objects.create(project=project, uid="c-pr")
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000301"
            ),
        )
        DepositAddress.objects.create(
            customer=customer, chain_type=ChainType.EVM, address=addr,
        )
        send_raw_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                # 余额远超 value + 2 * erc20_gas_cost，主动阈值通过
                get_balance=Mock(return_value=10**18),
                estimate_gas=Mock(
                    side_effect=ContractLogicError("execution reverted")
                ),
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.DepositCollection,
            crypto=crypto,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000302"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        collection = DepositCollection.objects.create(
            collection_hash=None,
            broadcast_task=base_task,
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "e1" * 32,
            event_id="erc20:pr1",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000311",
            to_address=addr.address,
            value="1",
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        deposit = Deposit.objects.create(
            customer=customer,
            transfer=transfer,
            status=DepositStatus.COMPLETED,
            collection=collection,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**6,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        deposit.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")
        send_raw_mock.assert_not_called()
        self.assertIsNotNone(broadcast_task.last_attempt_at)
        # 业务关系仍保留，等待重试、补救或人工处理；不能提前释放并跳过 nonce。
        self.assertEqual(deposit.collection_id, collection.pk)
        self.assertTrue(DepositCollection.objects.filter(pk=collection.pk).exists())

    def test_broadcast_preflight_success_proceeds_to_send(self):
        # pre-flight 通过时继续进入 send_raw_transaction 流程，base_task 进入 PENDING_CHAIN。
        native = Crypto.objects.create(
            name="Ethereum Preflight Ok",
            symbol="ETHPOK",
            coingecko_id="ethereum-preflight-ok",
        )
        chain = Chain.objects.create(
            code="eth-preflight-ok",
            name="Ethereum Preflight Ok",
            type=ChainType.EVM,
            chain_id=20401,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000401"
            ),
        )
        estimate_gas_mock = Mock(return_value=21_000)
        send_raw_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                # 余额充足：主动阈值通过
                get_balance=Mock(return_value=10**19),
                estimate_gas=estimate_gas_mock,
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000402"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        estimate_gas_mock.assert_called_once()
        send_raw_mock.assert_called_once()
        self.assertIsNotNone(broadcast_task.last_attempt_at)

        # 回归保护：estimate_gas 必须不带 nonce，否则同地址前序 tx 未 confirm 时
        # 节点会把本 preflight 直接判为 "Nonce too high"(-32003)，使后续任务被当成
        # 假失败反复重试。nonce 顺序由 has_lower_queued_nonce + pipeline 保证，与
        # estimate_gas 校验的"交易语义是否可执行"属于两件事，务必解耦。
        preflight_arg = estimate_gas_mock.call_args.args[0]
        self.assertNotIn("nonce", preflight_arg)

    @patch.object(EvmBroadcastTask, "is_pipeline_full", return_value=True)
    def test_pending_chain_rebroadcast_ignores_pipeline_full(self, _pipeline_full_mock):
        # 低 nonce 的 PENDING_CHAIN 任务超时重播是为了释放同地址 pipeline；
        # 如果它也被 pipeline_full 阻断，满 pipeline 会无法自愈。
        native = Crypto.objects.create(
            name="Ethereum Rebroadcast Pipeline",
            symbol="ETHRBP",
            coingecko_id="ethereum-rebroadcast-pipeline",
        )
        chain = Chain.objects.create(
            code="eth-rebroadcast-pipeline",
            name="Ethereum Rebroadcast Pipeline",
            type=ChainType.EVM,
            chain_id=20403,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000404"
            ),
        )
        send_raw_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**19),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000405"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        broadcast_task.broadcast(allow_pending_chain_rebroadcast=True)

        send_raw_mock.assert_called_once()

    def test_broadcast_preflight_rpc_error_reraises(self):
        # pre-flight 遇到通用 RPC 错误（非 insufficient funds / revert）应上抛给 Celery 重试，
        # base_task 保持 QUEUED，不创建 GasRecharge。
        from deposits.models import GasRecharge

        native = Crypto.objects.create(
            name="Ethereum Preflight Timeout",
            symbol="ETHPTO",
            coingecko_id="ethereum-preflight-timeout",
        )
        chain = Chain.objects.create(
            code="eth-preflight-timeout",
            name="Ethereum Preflight Timeout",
            type=ChainType.EVM,
            chain_id=20402,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000402"
            ),
        )
        send_raw_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                # 余额充足，主动阈值通过，才能进入 estimate_gas 分支
                get_balance=Mock(return_value=10**19),
                estimate_gas=Mock(
                    side_effect=RuntimeError("connection timeout")
                ),
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000403"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with self.assertRaisesMessage(RuntimeError, "connection timeout"):
            broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        send_raw_mock.assert_not_called()
        # 没有任何 GasRecharge 记录被创建
        self.assertFalse(GasRecharge.objects.exists())

    def test_broadcast_keeps_fee_too_low_error_retryable_without_finalizing(self):
        native = Crypto.objects.create(
            name="Ethereum Fee Too Low",
            symbol="ETHFTL",
            coingecko_id="ethereum-fee-too-low",
        )
        chain = Chain.objects.create(
            code="eth-fee-too-low",
            name="Ethereum Fee Too Low",
            type=ChainType.EVM,
            chain_id=20102,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000103"
            ),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**18),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=Mock(
                    side_effect=RuntimeError("replacement transaction underpriced")
                )
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000104"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "2" * 64,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with self.assertRaisesMessage(
            RuntimeError,
            "replacement transaction underpriced",
        ):
            broadcast_task.broadcast(allow_pending_chain_rebroadcast=True)

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")

    def test_broadcast_reraises_nonce_too_low_without_marking_pending(self):
        native = Crypto.objects.create(
            name="Ethereum Nonce Too Low",
            symbol="ETHNTL",
            coingecko_id="ethereum-nonce-too-low",
        )
        chain = Chain.objects.create(
            code="eth-nonce-too-low",
            name="Ethereum Nonce Too Low",
            type=ChainType.EVM,
            chain_id=20103,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000105"
            ),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**18),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=Mock(side_effect=RuntimeError("nonce too low"))
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000106"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "3" * 64,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with self.assertRaisesMessage(RuntimeError, "nonce too low"):
            broadcast_task.broadcast(allow_pending_chain_rebroadcast=True)

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")

    def test_broadcast_blocks_higher_nonce_until_lower_nonce_settles(self):
        native = Crypto.objects.create(
            name="Ethereum Nonce Block",
            symbol="ETHNB",
            coingecko_id="ethereum-nonce-block",
        )
        chain = Chain.objects.create(
            code="eth-nonce-block",
            name="Ethereum Nonce Block",
            type=ChainType.EVM,
            chain_id=20104,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000107"
            ),
        )
        send_raw_transaction_mock = Mock()
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                send_raw_transaction=send_raw_transaction_mock,
            )
        )
        lower_base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000108"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        EvmBroadcastTask.objects.create(
            base_task=lower_base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=lower_base_task.recipient,
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000109"
            ),
            amount=Decimal("1"),
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=1,
            to=base_task.recipient,
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        broadcast_task.broadcast()

        send_raw_transaction_mock.assert_not_called()
        base_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertIsNone(broadcast_task.last_attempt_at)

    def test_broadcast_treats_already_known_as_idempotent_success(self):
        native = Crypto.objects.create(
            name="Ethereum Already Known",
            symbol="ETHAK",
            coingecko_id="ethereum-already-known",
        )
        chain = Chain.objects.create(
            code="eth-already-known",
            name="Ethereum Already Known",
            type=ChainType.EVM,
            chain_id=20104,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000107"
            ),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_balance=Mock(return_value=10**18),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=Mock(side_effect=RuntimeError("already known"))
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000108"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "4" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")

    def test_queued_task_with_existing_hash_recovers_from_confirmed_receipt(self):
        """首播已被节点接受但阶段仍是 QUEUED 时，应先查 receipt 自愈而不是重发。"""
        native = Crypto.objects.create(
            name="Ethereum Queued Receipt Recovery",
            symbol="ETHQRR",
            coingecko_id="ethereum-queued-receipt-recovery",
        )
        chain = Chain.objects.create(
            code="eth-queued-receipt-recovery",
            name="Ethereum Queued Receipt Recovery",
            type=ChainType.EVM,
            chain_id=20105,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000109"
            ),
        )
        tx_hash = "0x" + "5" * 64
        send_raw_mock = Mock()
        receipt = {"status": 1, "blockNumber": 100, "logs": []}
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_transaction_receipt=Mock(return_value=receipt),
                get_balance=Mock(return_value=10**18),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000110"
            ),
            amount=Decimal("1"),
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        TxHash.objects.create(
            broadcast_task=base_task,
            chain=chain,
            hash=tx_hash,
            version=0,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with patch(
            "evm.coordinator.InternalEvmTaskCoordinator._observe_confirmed_transaction"
        ) as observe_mock:
            broadcast_task.broadcast()

        send_raw_mock.assert_not_called()
        observe_mock.assert_called_once()
        base_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)

    def test_nonce_too_low_checks_existing_hash_before_reraising(self):
        """nonce too low 时若历史 hash 已有 receipt，应自动恢复而不是继续卡 QUEUED。"""
        from web3.exceptions import TransactionNotFound

        native = Crypto.objects.create(
            name="Ethereum Nonce Too Low Recovery",
            symbol="ETHNTLR",
            coingecko_id="ethereum-nonce-too-low-recovery",
        )
        chain = Chain.objects.create(
            code="eth-nonce-too-low-recovery",
            name="Ethereum Nonce Too Low Recovery",
            type=ChainType.EVM,
            chain_id=20106,
            rpc="http://localhost:8545",
            native_coin=native,
            active=True,
        )
        addr = Address.objects.create(
            wallet=Wallet.objects.create(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000111"
            ),
        )
        tx_hash = "0x" + "6" * 64
        receipt = {"status": 1, "blockNumber": 100, "logs": []}
        get_receipt_mock = Mock(
            side_effect=[TransactionNotFound(tx_hash), receipt],
        )
        send_raw_mock = Mock(side_effect=RuntimeError("nonce too low"))
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                get_transaction_receipt=get_receipt_mock,
                get_balance=Mock(return_value=10**19),
                estimate_gas=Mock(return_value=21_000),
                send_raw_transaction=send_raw_mock,
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000112"
            ),
            amount=Decimal("1"),
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        TxHash.objects.create(
            broadcast_task=base_task,
            chain=chain,
            hash=tx_hash,
            version=0,
        )
        broadcast_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=base_task.recipient,
            value=10**18,
            gas=21_000,
            gas_price=1,
            signed_payload="0x7261772d6279746573",
        )

        with patch(
            "evm.coordinator.InternalEvmTaskCoordinator._observe_confirmed_transaction"
        ) as observe_mock:
            broadcast_task.broadcast()

        send_raw_mock.assert_called_once()
        observe_mock.assert_called_once()
        base_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)

