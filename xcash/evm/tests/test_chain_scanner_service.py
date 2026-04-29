from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.test import TestCase
from django.test import override_settings
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import TransferType
from chains.models import TxHash
from chains.models import Wallet
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from currencies.models import Crypto
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcError
from evm.scanner.service import EvmChainScannerService


class EvmChainScannerServiceTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="Ethereum Scanner Service",
            symbol="ETHSS",
            coingecko_id="ethereum-scanner-service",
        )
        self.chain = Chain.objects.create(
            code="eth-scanner-service",
            name="Ethereum Scanner Service",
            type=ChainType.EVM,
            chain_id=20001,
            rpc="http://localhost:8545",
            native_coin=self.native,
            active=True,
            latest_block_number=88,
        )

    @patch("evm.scanner.service.EvmErc20TransferScanner.scan_chain")
    @patch("evm.scanner.service.EvmNativeDirectScanner.scan_chain")
    def test_scan_chain_skips_disabled_native_cursor(
        self,
        native_scan_mock,
        erc20_scan_mock,
    ):
        EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            enabled=False,
        )
        erc20_scan_mock.return_value = EvmErc20ScanResult(
            from_block=1,
            to_block=2,
            latest_block=88,
            observed_logs=3,
            created_transfers=1,
        )

        result = EvmChainScannerService.scan_chain(chain=self.chain)

        native_scan_mock.assert_not_called()
        erc20_scan_mock.assert_called_once_with(chain=self.chain)
        self.assertEqual(result.native.created_transfers, 0)
        self.assertEqual(result.native.latest_block, 88)
        self.assertEqual(result.erc20.created_transfers, 1)

    @patch("evm.scanner.service.EvmErc20TransferScanner.scan_chain")
    @patch("evm.scanner.service.EvmNativeDirectScanner.scan_chain")
    def test_scan_chain_skips_disabled_erc20_cursor(
        self,
        native_scan_mock,
        erc20_scan_mock,
    ):
        EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            enabled=False,
        )
        native_scan_mock.return_value = EvmNativeScanResult(
            from_block=5,
            to_block=8,
            latest_block=88,
            observed_transfers=2,
            created_transfers=1,
        )

        result = EvmChainScannerService.scan_chain(chain=self.chain)

        native_scan_mock.assert_called_once_with(chain=self.chain)
        erc20_scan_mock.assert_not_called()
        self.assertEqual(result.erc20.created_transfers, 0)
        self.assertEqual(result.erc20.latest_block, 88)
        self.assertEqual(result.native.created_transfers, 1)

    @patch("evm.scanner.service.EvmErc20TransferScanner.scan_chain")
    @patch("evm.scanner.service.EvmNativeDirectScanner.scan_chain")
    def test_scan_chain_defaults_to_enabled_when_cursor_missing(
        self,
        native_scan_mock,
        erc20_scan_mock,
    ):
        native_scan_mock.return_value = EvmNativeScanResult(
            from_block=1,
            to_block=1,
            latest_block=88,
            observed_transfers=1,
            created_transfers=1,
        )
        erc20_scan_mock.return_value = EvmErc20ScanResult(
            from_block=1,
            to_block=1,
            latest_block=88,
            observed_logs=1,
            created_transfers=1,
        )

        result = EvmChainScannerService.scan_chain(chain=self.chain)

        native_scan_mock.assert_called_once_with(chain=self.chain)
        erc20_scan_mock.assert_called_once_with(chain=self.chain)
        self.assertEqual(result.native.created_transfers, 1)
        self.assertEqual(result.erc20.created_transfers, 1)

    @patch("evm.scanner.service.EvmErc20TransferScanner.scan_chain")
    @patch("evm.scanner.service.EvmNativeDirectScanner.scan_chain")
    def test_native_rpc_failure_does_not_block_erc20_scan(
        self,
        native_scan_mock,
        erc20_scan_mock,
    ):
        native_scan_mock.side_effect = EvmScannerRpcError("archive plan denied")
        erc20_scan_mock.return_value = EvmErc20ScanResult(
            from_block=10,
            to_block=12,
            latest_block=88,
            observed_logs=4,
            created_transfers=2,
        )

        result = EvmChainScannerService.scan_chain(chain=self.chain)

        native_scan_mock.assert_called_once_with(chain=self.chain)
        erc20_scan_mock.assert_called_once_with(chain=self.chain)
        self.assertEqual(result.native.observed_transfers, 0)
        self.assertEqual(result.native.created_transfers, 0)
        self.assertEqual(result.erc20.observed_logs, 4)
        self.assertEqual(result.erc20.created_transfers, 2)

    @override_settings(SIGNER_BACKEND="remote")
    def test_broadcast_rejects_local_fallback_when_remote_signer_enabled(self):
        # remote signer 模式下，广播阶段不允许再用本地私钥补签，避免应用进程重新持钥。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(gas_price=2, send_raw_transaction=Mock(), account=Mock()),
        )
        addr = Address(
            wallet=Wallet(),
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000001"
            ),
        )
        broadcast_task = EvmBroadcastTask(
            address=addr,
            chain=chain,
            nonce=1,
            to=Web3.to_checksum_address("0x0000000000000000000000000000000000000002"),
            value=0,
            gas=21_000,
            gas_price=1,
            signed_payload="",
        )
        broadcast_task.save = Mock()

        with self.assertRaisesMessage(Exception, "远端 signer 请求失败"):
            broadcast_task.broadcast()

        chain.w3.eth.account.sign_transaction.assert_not_called()

    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=0)
    def test_create_broadcast_task_defers_signing_until_first_broadcast(
        self,
        _next_nonce_mock,
    ):
        # 新 EVM 任务创建时只分配 nonce，不应提前签名或生成 tx_hash。
        native = Crypto.objects.create(
            name="Ethereum Deferred Signing",
            symbol="ETHDS",
            coingecko_id="ethereum-deferred-signing",
        )
        chain = Chain.objects.create(
            code="eth-deferred-sign",
            name="Ethereum Deferred Signing",
            type=ChainType.EVM,
            chain_id=1,
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
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f1"
            ),
        )
        task = EvmBroadcastTask.schedule_transfer(
            address=addr,
            chain=chain,
            crypto=native,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f2"),
            value_raw=123,
            transfer_type=TransferType.Withdrawal,
        )

        self.assertEqual(task.signed_payload, "")
        self.assertIsNone(task.gas_price)
        self.assertIsNone(task.base_task.tx_hash)
        self.assertFalse(
            TxHash.objects.filter(broadcast_task=task.base_task).exists()
        )

    @patch("evm.models.get_signer_backend")
    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=0)
    def test_first_broadcast_creates_initial_tx_hash_history(
        self,
        _next_nonce_mock,
        get_signer_backend_mock,
    ):
        native = Crypto.objects.create(
            name="Ethereum TxHash History",
            symbol="ETHTXH",
            coingecko_id="ethereum-txhash-history-evm",
        )
        chain = Chain.objects.create(
            code="eth-txhash-history",
            name="Ethereum TxHash History",
            type=ChainType.EVM,
            chain_id=101,
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
                "0x00000000000000000000000000000000000000fa"
            ),
        )
        chain.__dict__["w3"] = SimpleNamespace(eth=SimpleNamespace(gas_price=9))
        signer_backend = Mock()
        signer_backend.sign_evm_transaction.return_value = SimpleNamespace(
            tx_hash="0x" + "ac" * 32,
            raw_transaction="0xdeadbeef",
        )
        get_signer_backend_mock.return_value = signer_backend

        task = EvmBroadcastTask.schedule_transfer(
            address=addr,
            chain=chain,
            crypto=native,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000fb"),
            value_raw=123,
            transfer_type=TransferType.Withdrawal,
        )

        self.assertIsNone(task.base_task.tx_hash)
        self.assertFalse(TxHash.objects.filter(broadcast_task=task.base_task).exists())

        chain.__dict__["w3"].eth.send_raw_transaction = Mock()
        chain.__dict__["w3"].eth.estimate_gas = Mock(return_value=21_000)
        # 提供 get_balance，让主动阈值通过 pre-flight 进入 estimate_gas
        chain.__dict__["w3"].eth.get_balance = Mock(return_value=10**18)
        task.broadcast()

        task.refresh_from_db()
        task.base_task.refresh_from_db()
        history = TxHash.objects.get(broadcast_task=task.base_task, version=1)
        self.assertEqual(history.hash, task.base_task.tx_hash)
        self.assertEqual(history.chain_id, chain.pk)
        self.assertEqual(task.signed_payload, "0xdeadbeef")
        self.assertEqual(task.gas_price, 9)

    @patch("evm.models.get_signer_backend")
    def test_schedule_transfer_uses_next_nonce_after_highest_existing_nonce(
        self,
        get_signer_backend_mock,
    ):
        native = Crypto.objects.create(
            name="Ethereum Nonce State",
            symbol="ETHNS",
            coingecko_id="ethereum-nonce-state",
        )
        chain = Chain.objects.create(
            code="eth-nonce-state",
            name="Ethereum Nonce State",
            type=ChainType.EVM,
            chain_id=3,
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
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f5"
            ),
        )
        # 填充 nonce 0-4，满足触发器连续性约束
        for n in range(5):
            filler_base = BroadcastTask.objects.create(
                chain=chain,
                address=addr,
                transfer_type=TransferType.Withdrawal,
                stage=BroadcastTaskStage.FINALIZED,
                result=BroadcastTaskResult.SUCCESS,
            )
            EvmBroadcastTask.objects.create(
                base_task=filler_base,
                address=addr,
                chain=chain,
                to=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000f6"
                ),
                value=0,
                nonce=n,
                gas=21_000,
                gas_price=1,
            )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f6"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "ef" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=addr,
            chain=chain,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f6"),
            value=0,
            nonce=5,
            gas=21_000,
            gas_price=1,
            signed_payload="0x01",
        )
        chain.__dict__["w3"] = SimpleNamespace(eth=SimpleNamespace(gas_price=9))
        signer_backend = Mock()
        signer_backend.sign_evm_transaction.return_value = SimpleNamespace(
            tx_hash="0x" + "aa" * 32,
            raw_transaction="0xdeadbeef",
        )
        get_signer_backend_mock.return_value = signer_backend

        task = EvmBroadcastTask.schedule_transfer(
            address=addr,
            chain=chain,
            crypto=native,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f7"),
            value_raw=123,
            transfer_type=TransferType.Withdrawal,
        )

        self.assertEqual(task.nonce, 6)

    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=0)
    @patch("evm.models.AddressChainState.acquire_for_update")
    def test_schedule_transfer_no_longer_reads_gas_price_before_acquiring_account_chain_state_lock(
        self,
        acquire_state_mock,
        _next_nonce_mock,
    ):
        native = Crypto.objects.create(
            name="Ethereum Gas Price Prefetch",
            symbol="ETHGP",
            coingecko_id="ethereum-gas-price-prefetch",
        )
        chain = Chain.objects.create(
            code="eth-gas-prefetch",
            name="Ethereum Gas Price Prefetch",
            type=ChainType.EVM,
            chain_id=4,
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
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f8"
            ),
        )
        order: list[str] = []

        class EthClient:
            @property
            def gas_price(self):
                order.append("gas_price")
                return 9

        chain.__dict__["w3"] = SimpleNamespace(eth=EthClient())
        acquire_state_mock.side_effect = lambda **kwargs: (
            order.append("lock"),
            SimpleNamespace(next_nonce=0, save=Mock()),
        )[1]

        EvmBroadcastTask.schedule_transfer(
            address=addr,
            chain=chain,
            crypto=native,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f9"),
            value_raw=123,
            transfer_type=TransferType.Withdrawal,
        )

        self.assertEqual(order[:1], ["lock"])

    @patch("chains.service.OnchainTransfer.objects.create")
    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    def test_create_observed_transfer_marks_matching_broadcast_task_pending_confirm(
        self,
        mark_pending_confirm_mock,
        transfer_create_mock,
    ):
        # 只要链上已经观察到该 EVM hash，就应推进统一父任务进入待确认。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        crypto = chain.native_coin
        transfer_create_mock.return_value = Mock()
        observed = ObservedTransferPayload(
            chain=chain,
            block=1,
            tx_hash="0x" + "2" * 64,
            event_id="native:0",
            from_address="0x0000000000000000000000000000000000000001",
            to_address="0x0000000000000000000000000000000000000002",
            crypto=crypto,
            value=1,
            amount=1,
            timestamp=1,
            occurred_at=SimpleNamespace(),
        )

        TransferService.create_observed_transfer(observed=observed)

        mark_pending_confirm_mock.assert_called_once_with(
            chain=chain,
            tx_hash="0x" + "2" * 64,
        )
