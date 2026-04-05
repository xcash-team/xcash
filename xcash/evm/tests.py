import importlib
import os
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.contrib.admin.sites import AdminSite
from django.core.cache import cache
from django.test import TestCase
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


class EvmScanCursorAdminTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="Ethereum Admin Test",
            symbol="ETHA",
            coingecko_id="ethereum-admin-test",
        )
        self.chain = Chain.objects.create(
            code="eth-admin-test",
            name="Ethereum Admin Test",
            type=ChainType.EVM,
            chain_id=30_001,
            rpc="http://localhost:8545",
            native_coin=self.native,
            active=True,
            confirm_block_count=8,
            latest_block_number=88,
        )
        self.native_cursor = EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            last_scanned_block=11,
            last_safe_block=7,
            last_error="rpc timeout",
            last_error_at=timezone.now(),
        )
        self.erc20_cursor = EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            last_scanned_block=12,
            last_safe_block=8,
            last_error="old error",
            last_error_at=timezone.now(),
        )
        self.admin = EvmScanCursorAdmin(EvmScanCursor, AdminSite())
        self.admin.message_user = Mock()

    @patch.object(Chain, "get_latest_block_number", new_callable=PropertyMock)
    def test_sync_selected_to_latest_updates_only_selected_cursors(
        self, get_latest_block_number_mock
    ):
        get_latest_block_number_mock.return_value = 120

        self.admin.sync_selected_to_latest(
            request=Mock(),
            queryset=EvmScanCursor.objects.filter(pk=self.native_cursor.pk),
        )

        self.native_cursor.refresh_from_db()
        self.erc20_cursor.refresh_from_db()
        self.chain.refresh_from_db()

        self.assertEqual(self.native_cursor.last_scanned_block, 120)
        self.assertEqual(self.native_cursor.last_safe_block, 112)
        self.assertEqual(self.native_cursor.last_error, "")
        self.assertIsNone(self.native_cursor.last_error_at)
        self.assertEqual(self.erc20_cursor.last_scanned_block, 12)
        self.assertEqual(self.chain.latest_block_number, 120)
        self.admin.message_user.assert_called_once()
        self.assertEqual(get_latest_block_number_mock.call_count, 1)

    @patch.object(Chain, "get_latest_block_number", new_callable=PropertyMock)
    def test_sync_selected_to_latest_reports_rpc_error_without_mutation(
        self, get_latest_block_number_mock
    ):
        get_latest_block_number_mock.side_effect = RuntimeError("rpc timeout")

        self.admin.sync_selected_to_latest(
            request=Mock(),
            queryset=EvmScanCursor.objects.filter(pk=self.native_cursor.pk),
        )

        self.native_cursor.refresh_from_db()
        self.chain.refresh_from_db()

        self.assertEqual(self.native_cursor.last_scanned_block, 11)
        self.assertEqual(self.chain.latest_block_number, 88)
        self.admin.message_user.assert_called_once()
        self.assertIn("rpc timeout", self.admin.message_user.call_args.args[1])


class EvmScannerDefaultsTests(TestCase):
    def test_native_scan_uses_expected_default_batch_size(self):
        from evm.scanner.constants import DEFAULT_NATIVE_SCAN_BATCH_SIZE

        self.assertEqual(DEFAULT_NATIVE_SCAN_BATCH_SIZE, 16)

    def test_evm_scan_schedule_defaults_to_five_seconds(self):
        original_value = os.environ.get("EVM_SCAN_SCHEDULE_SECONDS")
        try:
            os.environ.pop("EVM_SCAN_SCHEDULE_SECONDS", None)
            from config import celery as celery_config

            reloaded = importlib.reload(celery_config)
            self.assertEqual(reloaded.EVM_SCAN_SCHEDULE_SECONDS, 5)
            self.assertEqual(
                reloaded.app.conf.beat_schedule["scan_active_evm_chains"]["schedule"],
                5,
            )
        finally:
            if original_value is None:
                os.environ.pop("EVM_SCAN_SCHEDULE_SECONDS", None)
            else:
                os.environ["EVM_SCAN_SCHEDULE_SECONDS"] = original_value
            importlib.reload(celery_config)


class EvmBroadcastTaskTests(TestCase):
    def test_next_nonce_returns_count_of_existing_tasks(self):
        # nonce 基于已有任务数量推算，事务回滚时自动复用，不会产生空洞。
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

        # 无任何任务时 nonce 应从 0 开始
        self.assertEqual(EvmBroadcastTask._next_nonce(addr, chain), 0)

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
        self.assertEqual(EvmBroadcastTask._next_nonce(addr, chain), 1)

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
            eth=SimpleNamespace(gas_price=1, send_raw_transaction=Mock()),
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
        self.assertFalse(broadcast_task.completed)

    @patch("withdrawals.service.WebhookService.create_event")
    def test_broadcast_keeps_insufficient_funds_retryable_without_finalizing(
        self, webhook_mock
    ):
        from projects.models import Project
        from withdrawals.models import Withdrawal
        from withdrawals.models import WithdrawalStatus

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
        )
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="broadcast-failure-project",
            wallet=wallet,
            webhook="https://example.com/webhook",
        )
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000101"
            ),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                gas_price=1,
                send_raw_transaction=Mock(
                    side_effect=RuntimeError(
                        "insufficient funds for gas * price + value"
                    )
                )
            )
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000102"
            ),
            amount=Decimal("1"),
            tx_hash="0x" + "1" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            chain=chain,
            crypto=native,
            amount=Decimal("1"),
            worth=Decimal("1"),
            out_no="withdrawal-broadcast-failure",
            to=base_task.recipient,
            broadcast_task=base_task,
            status=WithdrawalStatus.PENDING,
            hash=base_task.tx_hash,
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

        with self.assertRaisesMessage(
            RuntimeError,
            "insufficient funds for gas * price + value",
        ):
            with self.captureOnCommitCallbacks(execute=True):
                broadcast_task.broadcast()

        withdrawal.refresh_from_db()
        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertEqual(base_task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")
        self.assertFalse(broadcast_task.completed)
        webhook_mock.assert_not_called()

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
            nonce=2,
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
            broadcast_task.broadcast()

        base_task.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(base_task.failure_reason, "")
        self.assertFalse(broadcast_task.completed)

    def test_broadcast_keeps_nonce_too_low_for_followup_reconciliation(self):
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
            nonce=3,
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
        self.assertFalse(broadcast_task.completed)

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
            nonce=4,
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
            nonce=5,
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
            nonce=4,
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
        self.assertFalse(broadcast_task.completed)


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
        from evm.scanner.service import EvmChainScannerService

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
        from evm.scanner.service import EvmChainScannerService

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
        from evm.scanner.service import EvmChainScannerService

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

    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=3)
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
        addr.get_lock = Mock(return_value=True)
        addr.release_lock = Mock()

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
    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=3)
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
        task.broadcast()

        task.refresh_from_db()
        task.base_task.refresh_from_db()
        history = TxHash.objects.get(broadcast_task=task.base_task, version=1)
        self.assertEqual(history.hash, task.base_task.tx_hash)
        self.assertEqual(history.chain_id, chain.pk)
        self.assertEqual(task.signed_payload, "0xdeadbeef")
        self.assertEqual(task.gas_price, 9)

    @patch("evm.models.get_signer_backend")
    @patch("chains.models.Address.get_lock", return_value=True)
    @patch("chains.models.Address.release_lock")
    def test_schedule_transfer_uses_next_nonce_after_highest_existing_nonce(
        self,
        _release_lock_mock,
        _get_lock_mock,
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
    @patch("chains.models.Address.release_lock")
    @patch(
        "chains.models.Address.get_lock",
        side_effect=AssertionError("redis address lock should not be used"),
    )
    def test_schedule_transfer_no_longer_depends_on_redis_address_lock(
        self,
        _get_lock_mock,
        _release_lock_mock,
        _next_nonce_mock,
    ):
        native = Crypto.objects.create(
            name="Ethereum DB Lock",
            symbol="ETHDBL",
            coingecko_id="ethereum-db-lock",
        )
        chain = Chain.objects.create(
            code="eth-dblock",
            name="Ethereum DB Lock",
            type=ChainType.EVM,
            chain_id=2,
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
                "0x00000000000000000000000000000000000000f3"
            ),
        )
        chain.__dict__["w3"] = SimpleNamespace(eth=SimpleNamespace(gas_price=9))

        task = EvmBroadcastTask.schedule_transfer(
            address=addr,
            chain=chain,
            crypto=native,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f4"),
            value_raw=123,
            transfer_type=TransferType.Withdrawal,
        )

        self.assertEqual(task.nonce, 0)
        self.assertEqual(task.signed_payload, "")
        self.assertIsNone(task.base_task.tx_hash)

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


class EvmTaskQueueTests(TestCase):
    queue_lock_key = "dispatch_due_evm_broadcast_tasks-locked"

    def setUp(self):
        self._clear_singleton_locks()
        self.wallet = Wallet.objects.create()
        self.native = Crypto.objects.create(
            name="Ethereum Queue",
            symbol="ETHQ",
            coingecko_id="ethereum-queue",
        )
        self.chain = Chain.objects.create(
            code="ethq",
            name="Ethereum Queue",
            type=ChainType.EVM,
            chain_id=1,
            rpc="http://ethq.local",
            native_coin=self.native,
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f1"
            ),
        )

    def _clear_singleton_locks(self):
        cache.delete(self.queue_lock_key)

    def _create_evm_task(
        self,
        *,
        tx_hash: str,
        stage: str,
        result: str,
        nonce: int | None = None,
        address: Address | None = None,
    ) -> EvmBroadcastTask:
        # 任务级测试直接手工落库，聚焦“队列如何挑任务”和“终局任务是否被错误重播”。
        task_address = address or self.addr
        base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=task_address,
            transfer_type=TransferType.Withdrawal,
            crypto=self.native,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f2"
            ),
            amount=Decimal("1"),
            tx_hash=tx_hash,
            stage=stage,
            result=result,
        )
        return EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=task_address,
            chain=self.chain,
            nonce=base_task.pk if nonce is None else nonce,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f2"),
            value=0,
            gas=21_000,
            gas_price=1,
        )

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_skips_finalized_broadcast_task(self, broadcast_mock):
        # 已终局的链上任务不应再次广播，否则会把成功/失败终态重新拉回执行面。
        from evm.tasks import broadcast_evm_task

        broadcast_task = self._create_evm_task(
            tx_hash="0x" + "a" * 64,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.SUCCESS,
        )

        broadcast_evm_task.run(broadcast_task.pk)

        broadcast_mock.assert_not_called()

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_dispatches_only_due_unknown_tasks(
        self, delay_mock
    ):
        # 广播队列只能挑“到期且仍未知”的任务，避免 recent / finalized 任务被误重试。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=2,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f4"
            ),
        )

        due_queued = self._create_evm_task(
            tx_hash="0x" + "b" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        due_pending_chain = self._create_evm_task(
            tx_hash="0x" + "c" * 64,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
            address=other_addr,
        )
        recent_task = self._create_evm_task(
            tx_hash="0x" + "d" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        finalized_task = self._create_evm_task(
            tx_hash="0x" + "e" * 64,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.SUCCESS,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        stale_attempt_at = timezone.now() - timedelta(minutes=5)
        fresh_created_at = timezone.now()
        EvmBroadcastTask.objects.filter(pk=due_queued.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(pk=due_pending_chain.pk).update(
            created_at=stale_created_at,
            last_attempt_at=stale_attempt_at,
        )
        EvmBroadcastTask.objects.filter(pk=recent_task.pk).update(
            created_at=fresh_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(pk=finalized_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=stale_attempt_at,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            {call.args[0] for call in delay_mock.call_args_list},
            {due_queued.pk, due_pending_chain.pk},
        )

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_skips_when_lower_unsettled_nonce_exists(
        self,
        broadcast_mock,
    ):
        # 同账户更高 nonce 在更低 nonce 未收口前不应越过广播，否则会把真实阻塞点扩散成整串噪音。
        from evm.tasks import broadcast_evm_task

        self._create_evm_task(
            tx_hash="0x" + "1" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "2" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        broadcast_evm_task.run(higher_task.pk)

        broadcast_mock.assert_not_called()

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_allows_higher_nonce_after_lower_task_enters_pending_confirm(
        self,
        broadcast_mock,
    ):
        # 一旦更低 nonce 已被链上观察到并进入 PENDING_CONFIRM，说明该 nonce 已消费，不应继续阻断后续 nonce。
        from evm.tasks import broadcast_evm_task

        self._create_evm_task(
            tx_hash="0x" + "11" * 32,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "12" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        broadcast_evm_task.run(higher_task.pk)

        broadcast_mock.assert_called_once()

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_dispatches_only_lowest_unsettled_nonce_per_account(
        self, delay_mock
    ):
        # 队列层只应放行每个账户当前最小未收口 nonce，避免高 nonce 在前序缺口存在时被反复重试。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=1,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f3"
            ),
        )
        lower_task = self._create_evm_task(
            tx_hash="0x" + "3" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=5,
        )
        blocked_higher_task = self._create_evm_task(
            tx_hash="0x" + "4" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=6,
        )
        other_account_task = self._create_evm_task(
            tx_hash="0x" + "5" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
            address=other_addr,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(
            pk__in=[lower_task.pk, blocked_higher_task.pk, other_account_task.pk]
        ).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            {call.args[0] for call in delay_mock.call_args_list},
            {lower_task.pk, other_account_task.pk},
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_treats_pending_confirm_as_nonce_consumed(
        self,
        delay_mock,
    ):
        # SQL 选取最小阻塞 nonce 时，不应把已进入 PENDING_CONFIRM 的前序任务继续当作缺口。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        lower_confirming_task = self._create_evm_task(
            tx_hash="0x" + "13" * 32,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "14" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        stale_attempt_at = timezone.now() - timedelta(minutes=5)
        EvmBroadcastTask.objects.filter(pk=lower_confirming_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=stale_attempt_at,
        )
        EvmBroadcastTask.objects.filter(pk=higher_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            [call.args[0] for call in delay_mock.call_args_list],
            [higher_task.pk],
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_avoids_slice_starvation_from_blocked_high_nonces(
        self, delay_mock
    ):
        # SQL 层应直接挑每账户最小未收口 nonce，避免更高 nonce 候选占满 slice 后被 Python 层全部跳过。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=3,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f5"
            ),
        )
        lower_task = self._create_evm_task(
            tx_hash="0x" + "6" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        blocked_tasks = [
            self._create_evm_task(
                tx_hash=f"0x{i:064x}",
                stage=BroadcastTaskStage.QUEUED,
                result=BroadcastTaskResult.UNKNOWN,
                nonce=i,
            )
            for i in range(2, 10)
        ]
        other_account_task = self._create_evm_task(
            tx_hash="0x" + "7" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
            address=other_addr,
        )

        older_created_at = timezone.now() - timedelta(seconds=12)
        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(pk=lower_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(
            pk__in=[task.pk for task in blocked_tasks]
        ).update(
            created_at=older_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(pk=other_account_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            {call.args[0] for call in delay_mock.call_args_list},
            {lower_task.pk, other_account_task.pk},
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_clear_singleton_locks_allows_queue_dispatch_after_stale_lock(
        self,
        delay_mock,
    ):
        # singleton 锁残留会让队列任务直接返回；测试夹具必须主动清理，避免用例依赖外部缓存状态。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        cache.set(self.queue_lock_key, "true", 60)
        due_task = self._create_evm_task(
            tx_hash="0x" + "f" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        EvmBroadcastTask.objects.filter(pk=due_task.pk).update(
            created_at=timezone.now() - timedelta(seconds=8),
            last_attempt_at=None,
        )

        self._clear_singleton_locks()
        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            [call.args[0] for call in delay_mock.call_args_list],
            [due_task.pk],
        )


class EvmInternalTaskConfirmationTests(TestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.native = Crypto.objects.create(
            name="Ethereum Internal Confirm",
            symbol="ETHIC",
            coingecko_id="ethereum-internal-confirm",
        )
        self.token = Crypto.objects.create(
            name="USD Coin Internal Confirm",
            symbol="USDCIC",
            coingecko_id="usd-coin-internal-confirm",
            decimals=6,
        )
        self.chain = Chain.objects.create(
            code="eth-internal-confirm",
            name="Ethereum Internal Confirm",
            type=ChainType.EVM,
            chain_id=20002,
            rpc="http://localhost:8545",
            native_coin=self.native,
            active=True,
        )
        ChainToken.objects.create(
            crypto=self.token,
            chain=self.chain,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000c1"
            ),
            decimals=6,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000c2"
            ),
        )

    def _create_withdrawal_with_pending_evm_task(
        self,
        *,
        tx_hash: str,
    ):
        from projects.models import Project
        from withdrawals.models import Withdrawal
        from withdrawals.models import WithdrawalStatus

        project = Project.objects.create(
            name=f"project-{tx_hash[-6:]}",
            wallet=self.wallet,
            webhook="https://example.com/webhook",
        )
        base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.token,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000c3"
            ),
            amount=Decimal("12.34"),
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )
        evm_task = EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=self.addr,
            chain=self.chain,
            nonce=1,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000c1"),
            value=0,
            data="0xa9059cbb",
            gas=ERC20_TRANSFER_GAS,
            gas_price=1,
            signed_payload="0x01",
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            chain=self.chain,
            crypto=self.token,
            amount=Decimal("12.34"),
            worth=Decimal("12.34"),
            out_no=f"out-{tx_hash[-6:]}",
            to=base_task.recipient,
            broadcast_task=base_task,
            status=WithdrawalStatus.PENDING,
            hash=tx_hash,
        )
        return withdrawal, base_task, evm_task

    @patch("withdrawals.service.WebhookService.create_event")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_coordinator_fails_internal_withdrawal_when_receipt_status_zero(
        self,
        chain_w3_mock,
        webhook_mock,
    ):
        from evm.coordinator import InternalEvmTaskCoordinator

        withdrawal, base_task, evm_task = self._create_withdrawal_with_pending_evm_task(
            tx_hash="0x" + "7" * 64
        )
        chain_w3_mock.return_value = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"status": 0}),
            )
        )

        with self.captureOnCommitCallbacks(execute=True):
            InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        withdrawal.refresh_from_db()
        base_task.refresh_from_db()
        evm_task.refresh_from_db()
        self.assertEqual(withdrawal.status, "failed")
        self.assertEqual(base_task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(base_task.result, BroadcastTaskResult.FAILED)
        self.assertEqual(
            base_task.failure_reason,
            BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )
        self.assertTrue(evm_task.completed)
        self.assertEqual(OnchainTransfer.objects.count(), 0)
        webhook_mock.assert_called_once()

    @patch("withdrawals.service.WebhookService.create_event")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_coordinator_keeps_internal_withdrawal_pending_when_receipt_missing(
        self,
        chain_w3_mock,
        webhook_mock,
    ):
        from web3.exceptions import TransactionNotFound

        from evm.coordinator import InternalEvmTaskCoordinator
        from withdrawals.models import WithdrawalStatus

        withdrawal, base_task, evm_task = self._create_withdrawal_with_pending_evm_task(
            tx_hash="0x" + "8" * 64
        )
        chain_w3_mock.return_value = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(
                    side_effect=TransactionNotFound("missing")
                ),
            )
        )
        InternalEvmTaskCoordinator.reconcile_chain(chain=self.chain)

        withdrawal.refresh_from_db()
        base_task.refresh_from_db()
        evm_task.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertEqual(base_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(base_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertFalse(evm_task.completed)
        webhook_mock.assert_not_called()


@override_settings(DEBUG=False)
class EvmErc20ScannerTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="BNB",
            symbol="BNB",
            coingecko_id="binancecoin",
        )
        self.chain = Chain.objects.create(
            code="bsc",
            name="BSC",
            type=ChainType.EVM,
            chain_id=56,
            rpc="http://bsc.local",
            native_coin=self.native,
            confirm_block_count=6,
            active=True,
        )
        self.token = Crypto.objects.create(
            name="Tether USD",
            symbol="USDT",
            coingecko_id="tether",
            decimals=18,
        )
        self.token_deployment = ChainToken.objects.create(
            crypto=self.token,
            chain=self.chain,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000aa"
            ),
            decimals=18,
        )
        self.wallet = Wallet.objects.create()
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000bb"
            ),
        )

    @staticmethod
    def _address_topic(address: str) -> str:
        normalized = Web3.to_checksum_address(address)
        return "0x" + "0" * 24 + normalized[2:].lower()

    def _build_transfer_log(
        self,
        *,
        from_address: str,
        to_address: str,
        log_index: int = 5,
        value: int = 10**18,
        block_number: int = 100,
    ) -> dict:
        return {
            "address": self.token_deployment.address,
            "topics": [
                Web3.keccak(text="Transfer(address,address,uint256)"),
                self._address_topic(from_address),
                self._address_topic(to_address),
            ],
            "data": hex(value),
            "blockNumber": block_number,
            "logIndex": log_index,
            "transactionHash": bytes.fromhex("ab" * 32),
        }

    def _build_native_block(
        self,
        *,
        txs: list[dict],
        timestamp: int = 1_700_000_123,
    ) -> dict:
        return {
            "number": 20,
            "timestamp": timestamp,
            "transactions": txs,
        }

    @staticmethod
    def _build_native_tx(
        *,
        from_address: str,
        to_address: str,
        value: int,
        tx_hash_hex: str,
        input_data: str = "0x",
    ) -> dict:
        return {
            "hash": bytes.fromhex(tx_hash_hex * 32),
            "from": from_address,
            "to": to_address,
            "value": value,
            "input": input_data,
        }

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_erc20_first_scan_without_cursor_starts_from_latest_tail_window(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        _get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 首次创建游标时不应从创世块补扫；应直接对齐到链头附近，仅覆盖近端重扫窗口。
        get_latest_block_number_mock.return_value = 100
        get_transfer_logs_mock.return_value = []

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        # reorg_lookback = max(6, 6) = 6, from_block = 100 + 1 - 6 = 95
        self.assertEqual(result.from_block, 95)
        self.assertEqual(result.to_block, 100)
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_creates_transfer_and_advances_cursor(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 命中的 ERC20 OnchainTransfer 应落到统一 OnchainTransfer 表；首扫会直接对齐链头附近窗口。
        get_latest_block_number_mock.return_value = 100
        get_block_timestamp_mock.return_value = 1_700_000_000
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=self.addr.address,
            )
        ]

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        transfer = OnchainTransfer.objects.get()
        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )

        self.assertEqual(result.created_transfers, 1)
        self.assertEqual(result.observed_logs, 1)
        self.assertEqual(transfer.event_id, "erc20:5")
        self.assertEqual(transfer.hash, "0x" + "ab" * 32)
        self.assertEqual(
            transfer.to_address, Web3.to_checksum_address(self.addr.address)
        )
        self.assertEqual(transfer.amount, Decimal("1"))
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_rewind_window_keeps_transfer_idempotent(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 近端重扫会重复看到同一日志，但统一唯一键必须保证不会重复落库。
        get_latest_block_number_mock.return_value = 100
        get_block_timestamp_mock.return_value = 1_700_000_000
        repeated_log = self._build_transfer_log(
            from_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000cc"
            ),
            to_address=self.addr.address,
            block_number=100,
        )
        get_transfer_logs_mock.return_value = [repeated_log]

        first = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)
        second = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(first.created_transfers, 1)
        self.assertEqual(second.created_transfers, 0)
        self.assertEqual(OnchainTransfer.objects.count(), 1)
        self.assertEqual(cursor.last_scanned_block, 100)

    @override_settings(DEBUG=True)
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_debug_mode_bootstraps_cursor_once_from_latest_block_per_process(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
    ):
        # 本地 DEBUG 开发模式下，首次扫描应直接把历史游标提升到当前链头；
        # 但同一进程后续轮询不能重复执行这次“启动对齐”，否则会不断抹平正常增量进度。
        EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            last_scanned_block=12,
            last_safe_block=6,
            enabled=True,
        )
        get_latest_block_number_mock.side_effect = [100, 110]
        get_transfer_logs_mock.return_value = []

        first = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)
        second = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=100)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        # reorg_lookback = 6, 第一轮 bootstrap 到 100: from = 100+1-6 = 95
        self.assertEqual(first.from_block, 95)
        self.assertEqual(first.to_block, 100)
        # 第二轮: last_scanned=100, from = 100+1-6 = 95
        self.assertEqual(second.from_block, 95)
        self.assertEqual(second.to_block, 110)
        self.assertEqual(cursor.last_scanned_block, 110)

    @patch(
        "currencies.models.Crypto.get_decimals",
        side_effect=AssertionError("scanner should use prefetched token decimals"),
    )
    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_block_timestamp")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_uses_chain_token_decimals_without_extra_lookup(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        get_block_timestamp_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
        _crypto_get_decimals_mock,
    ):
        # ERC20 扫描已持有 ChainToken 行数据，应直接复用链特定精度，避免逐条日志额外查库。
        self.token_deployment.decimals = 6
        self.token_deployment.save(update_fields=["decimals"])
        get_latest_block_number_mock.return_value = 100
        get_block_timestamp_mock.return_value = 1_700_000_000
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=self.addr.address,
                value=10**6,
            )
        ]

        EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        transfer = OnchainTransfer.objects.get()
        self.assertEqual(transfer.amount, Decimal("1"))

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_ignores_logs_outside_watch_set(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        create_observed_transfer_mock,
    ):
        # 非系统地址相关的日志必须在扫描层被过滤，避免把全链事件都送进业务入口。
        get_latest_block_number_mock.return_value = 40
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000dd"
                ),
                block_number=40,
            )
        ]

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=40)

        self.assertEqual(result.created_transfers, 0)
        self.assertEqual(result.observed_logs, 1)
        create_observed_transfer_mock.assert_not_called()
        self.assertEqual(OnchainTransfer.objects.count(), 0)

    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_uses_prefixed_transfer_topic_for_rpc_logs(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
    ):
        # 部分 RPC（如 NodeReal）要求日志 topic 必须是 0x 前缀 hex；少前缀会直接报 -32602。
        get_latest_block_number_mock.return_value = 100
        get_transfer_logs_mock.return_value = []

        EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        _, kwargs = get_transfer_logs_mock.call_args
        self.assertEqual(
            kwargs["topic0"],
            Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)")),
        )

    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_scan_chain_advances_cursor_when_no_tokens_configured(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
    ):
        # 当链上尚未配置任何 ERC20 合约时，不应长期显示积压；游标可直接追到当前链头。
        self.token_deployment.delete()
        get_latest_block_number_mock.return_value = 100

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(result.created_transfers, 0)
        self.assertEqual(result.observed_logs, 0)
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)
        get_transfer_logs_mock.assert_not_called()

    @patch("evm.tasks.InternalEvmTaskCoordinator.reconcile_chain")
    @patch("evm.tasks.EvmChainScannerService.scan_chain")
    def test_scan_evm_chain_task_dispatches_chain_scanner(
        self,
        scan_chain_mock,
        reconcile_chain_mock,
    ):
        # Celery 入口应只负责链级调度，不再混入具体日志解析逻辑。
        from evm.tasks import scan_evm_chain

        scan_evm_chain(self.chain.pk)

        scan_chain_mock.assert_called_once()
        reconcile_chain_mock.assert_called_once()

    def test_watch_set_includes_recipient_addresses(self):
        # 收币地址同样属于系统观察集，后续 ERC20 扫描需要能命中这些地址。
        RecipientAddress.objects.create(
            name="project-recipient",
            project_id=self._create_project_id(),
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000dd"
            ),
        )

        from evm.scanner.watchers import load_watch_set

        watch_set = load_watch_set(chain=self.chain)

        self.assertIn(
            Web3.to_checksum_address("0x00000000000000000000000000000000000000dD"),
            watch_set.watched_addresses,
        )

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_first_scan_without_cursor_starts_from_latest_tail_window(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        _get_receipt_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 原生币首扫若系统中没有游标，也应只覆盖链头附近窗口，不能从 1 开始全量爬。
        get_latest_block_number_mock.return_value = 20
        get_full_block_mock.side_effect = (
            lambda *, block_number: self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        # reorg_lookback = max(6, 6) = 6, from_block = 20 + 1 - 6 = 15
        self.assertEqual(result.from_block, 15)
        self.assertEqual(result.to_block, 20)
        self.assertEqual(cursor.last_scanned_block, 20)
        self.assertEqual(cursor.last_safe_block, 14)

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_creates_transfer_for_direct_value_transfer(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 顶层 input=0x 的 value transfer 若命中系统地址，应按 native:tx 统一落库。
        # 首扫窗口直接对齐链头附近，因此命中交易也应位于最新尾部区间内。
        get_latest_block_number_mock.return_value = 20
        get_receipt_status_mock.return_value = 1
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=self.addr.address,
                        value=10**18,
                        tx_hash_hex="cd",
                    )
                ]
            )
            if block_number == 20
            else self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        transfer = OnchainTransfer.objects.get(event_id="native:tx")
        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )

        self.assertEqual(result.observed_transfers, 1)
        self.assertEqual(result.created_transfers, 1)
        self.assertEqual(transfer.hash, "0x" + "cd" * 32)
        self.assertEqual(transfer.amount, Decimal("1"))
        self.assertEqual(cursor.last_scanned_block, 20)
        self.assertEqual(cursor.last_safe_block, 14)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_ignores_failed_transaction_without_creating_transfer(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        create_observed_transfer_mock,
    ):
        # status=0 的原生交易不应落成 OnchainTransfer；失败语义只属于内部任务协调器。
        get_latest_block_number_mock.return_value = 12
        get_receipt_status_mock.return_value = 0
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=self.addr.address,
                        to_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        value=10**18,
                        tx_hash_hex="de",
                    )
                ]
            )
            if block_number == 1
            else self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=3)

        self.assertEqual(result.observed_transfers, 0)
        self.assertEqual(result.created_transfers, 0)
        create_observed_transfer_mock.assert_not_called()
        self.assertEqual(OnchainTransfer.objects.count(), 0)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_ignores_contract_calls_with_calldata(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        create_observed_transfer_mock,
    ):
        # 原生币扫描首版只认直转；带 calldata 的合约调用即使 value>0 也必须跳过。
        get_latest_block_number_mock.return_value = 12
        get_full_block_mock.side_effect = lambda *, block_number: (
            self._build_native_block(
                txs=[
                    self._build_native_tx(
                        from_address=Web3.to_checksum_address(
                            "0x00000000000000000000000000000000000000cc"
                        ),
                        to_address=self.addr.address,
                        value=10**18,
                        tx_hash_hex="ef",
                        input_data="0xa9059cbb",
                    )
                ]
            )
            if block_number == 1
            else self._build_native_block(txs=[])
        )

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=3)

        self.assertEqual(result.observed_transfers, 0)
        self.assertEqual(result.created_transfers, 0)
        create_observed_transfer_mock.assert_not_called()

    @patch("chains.service.TransferService._mark_broadcast_task_pending_confirm")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_transaction_receipt_status")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_rewind_window_is_idempotent(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
        get_receipt_status_mock,
        _enqueue_processing_mock,
        _mark_pending_confirm_mock,
    ):
        # 原生币尾部重扫会重复看到同一笔交易，但 OnchainTransfer 唯一键必须保证不重复落库。
        get_latest_block_number_mock.return_value = 12
        get_receipt_status_mock.return_value = 1
        repeated_block = self._build_native_block(
            txs=[
                self._build_native_tx(
                    from_address=Web3.to_checksum_address(
                        "0x00000000000000000000000000000000000000cc"
                    ),
                    to_address=self.addr.address,
                    value=10**18,
                    tx_hash_hex="fa",
                )
            ]
        )
        get_full_block_mock.side_effect = lambda *, block_number: (
            repeated_block if block_number == 8 else self._build_native_block(txs=[])
        )

        first = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=12)
        second = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=12)

        self.assertEqual(first.created_transfers, 1)
        self.assertEqual(second.created_transfers, 0)
        self.assertEqual(
            OnchainTransfer.objects.filter(event_id="native:tx").count(), 1
        )

    @patch(
        "evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number",
        side_effect=EvmScannerRpcError("rpc timeout"),
    )
    def test_erc20_scan_records_cursor_error_when_rpc_fails(
        self, _get_latest_block_number_mock
    ):
        # RPC 失败后必须把错误留在游标上，方便后台与运维定位扫描停滞原因。
        with self.assertRaises(EvmScannerRpcError):
            EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        self.assertEqual(cursor.last_scanned_block, 0)
        self.assertEqual(cursor.last_error, "rpc timeout")
        self.assertIsNotNone(cursor.last_error_at)

    @patch("chains.service.TransferService.create_observed_transfer")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_transfer_logs")
    @patch("evm.scanner.erc20.EvmScannerRpcClient.get_latest_block_number")
    def test_erc20_scan_ignores_zero_value_transfer(
        self,
        get_latest_block_number_mock,
        get_transfer_logs_mock,
        create_observed_transfer_mock,
    ):
        # ERC20 OnchainTransfer 事件 value=0 无业务意义（如某些代币的 approve 触发），应在扫描层过滤。
        get_latest_block_number_mock.return_value = 40
        get_transfer_logs_mock.return_value = [
            self._build_transfer_log(
                from_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000cc"
                ),
                to_address=self.addr.address,
                value=0,
                block_number=40,
            )
        ]

        result = EvmErc20TransferScanner.scan_chain(chain=self.chain, batch_size=40)

        self.assertEqual(result.created_transfers, 0)
        create_observed_transfer_mock.assert_not_called()

    @patch(
        "evm.scanner.native.EvmScannerRpcClient.get_latest_block_number",
        side_effect=EvmScannerRpcError("node unreachable"),
    )
    def test_native_scan_records_cursor_error_when_rpc_fails(
        self, _get_latest_block_number_mock
    ):
        # 原生币扫描 RPC 失败后必须把错误留在游标上，与 ERC20 扫描行为一致。
        with self.assertRaises(EvmScannerRpcError):
            EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=4)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        self.assertEqual(cursor.last_scanned_block, 0)
        self.assertEqual(cursor.last_error, "node unreachable")
        self.assertIsNotNone(cursor.last_error_at)

    def test_compute_scan_window_returns_empty_when_latest_block_is_zero(self):
        # latest_block=0 表示链尚未出块或 RPC 返回异常值，扫描窗口应为空。
        cursor = EvmScanCursor(last_scanned_block=0)
        from_block, to_block = EvmErc20TransferScanner._compute_scan_window(
            cursor=cursor,
            latest_block=0,
            confirm_block_count=6,
            batch_size=100,
        )
        self.assertGreater(from_block, to_block)

    def test_compute_scan_window_returns_empty_when_fully_caught_up(self):
        # 游标已追平链头时，窗口仅覆盖未确认区域（safe_height 以上），不重扫已确认块。
        cursor = EvmScanCursor(last_scanned_block=100)
        from_block, to_block = EvmErc20TransferScanner._compute_scan_window(
            cursor=cursor,
            latest_block=100,
            confirm_block_count=6,
            batch_size=100,
        )
        # reorg_lookback = max(6, 6) = 6, from_block = 100 + 1 - 6 = 95
        self.assertEqual(from_block, 95)
        self.assertEqual(to_block, 100)

    def test_native_compute_scan_window_must_still_progress_when_far_behind(self):
        # 当原生币游标明显落后于链头时，窗口可以回退重扫，但本轮必须有净推进。
        # 否则会反复扫描同一段 [last_scanned - lookback + 1, last_scanned] 区间，游标永远卡住。
        cursor = EvmScanCursor(last_scanned_block=10_516_050)
        from_block, to_block = EvmNativeDirectScanner._compute_scan_window(
            cursor=cursor,
            latest_block=10_516_343,
            confirm_block_count=10,
            batch_size=12,
        )
        # reorg_lookback = max(10, 6) = 10, from_block = 10_516_050 + 1 - 10 = 10_516_041
        self.assertEqual(from_block, 10_516_041)
        self.assertGreater(to_block, cursor.last_scanned_block)

    def _create_project_id(self) -> int:

        from projects.models import Project

        project = Project.objects.create(
            name="scanner-project",
            wallet=Wallet.objects.create(),
            webhook="https://example.com/webhook",
        )
        return project.pk


@override_settings(DEBUG=False)
class EvmNativeScannerNoWatchSetTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="Ethereum Native No Watch",
            symbol="ETHNW",
            coingecko_id="ethereum-native-no-watch",
        )
        self.chain = Chain.objects.create(
            code="eth-no-watch",
            name="Ethereum No Watch",
            type=ChainType.EVM,
            chain_id=30_101,
            rpc="http://eth-no-watch.local",
            native_coin=self.native,
            confirm_block_count=6,
            active=True,
        )

    @patch("evm.scanner.native.EvmScannerRpcClient.get_full_block")
    @patch("evm.scanner.native.EvmScannerRpcClient.get_latest_block_number")
    def test_native_scan_advances_cursor_when_no_watched_addresses(
        self,
        get_latest_block_number_mock,
        get_full_block_mock,
    ):
        # 当系统尚未配置任何 EVM 监听地址时，原生币扫描也不应长期显示历史积压。
        EvmScanCursor.objects.create(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            last_scanned_block=39,
            last_safe_block=33,
            enabled=True,
        )
        get_latest_block_number_mock.return_value = 100

        result = EvmNativeDirectScanner.scan_chain(chain=self.chain, batch_size=32)

        cursor = EvmScanCursor.objects.get(
            chain=self.chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        self.assertEqual(result.created_transfers, 0)
        self.assertEqual(result.observed_transfers, 0)
        self.assertEqual(cursor.last_scanned_block, 100)
        self.assertEqual(cursor.last_safe_block, 94)
        get_full_block_mock.assert_not_called()


class EvmAdapterTests(TestCase):
    def test_tx_result_returns_confirmed_when_status_is_one(self):
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"status": 1}),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.CONFIRMED)

    def test_tx_result_returns_failed_when_status_is_zero(self):
        # 链上执行失败（revert）应返回 FAILED，而不是和 pending / not found 混为一类。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"status": 0}),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.FAILED)

    def test_tx_result_returns_dropped_when_transaction_not_found(self):
        from web3.exceptions import TransactionNotFound

        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(
                    side_effect=TransactionNotFound("0x" + "ab" * 32),
                ),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.DROPPED)

    def test_tx_result_returns_dropped_when_receipt_is_none(self):
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value=None),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.DROPPED)

    def test_tx_result_returns_exception_when_receipt_missing_status(self):
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"transactionHash": "0x01"}),
            ),
        )

        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertIsInstance(result, RuntimeError)

    def test_tx_result_returns_exception_on_rpc_error(self):
        # RPC 调用异常（网络问题等）应返回异常对象，由上层决定是否重试。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        rpc_error = ConnectionError("node unreachable")
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(side_effect=rpc_error),
            ),
        )

        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertIsInstance(result, ConnectionError)
