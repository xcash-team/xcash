import hashlib
import hmac
import json
from io import StringIO
from unittest.mock import Mock
from unittest.mock import patch

from django.core import checks
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError
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
from chains.models import ConfirmMode
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.models import TransferType
from chains.models import TxHash
from chains.models import Wallet
from chains.signer import RemoteSignerBackend
from chains.signer import SignerAdminSummary
from chains.signer import SignerServiceError
from chains.signer import build_signer_signature_payload
from chains.signer import get_signer_backend
from currencies.models import Crypto


class BroadcastTaskValidationTests(TestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.crypto = Crypto.objects.create(
            name="Ethereum Onchain Task Validation",
            symbol="ETH-OTV",
            coingecko_id="ethereum-onchain-task-validation",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Onchain Task Validation",
            code="eth-otv",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=10001,
            rpc="http://localhost:8545",
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000001",
        )

    def test_failed_result_must_be_finalized_with_failure_reason(self):
        # 失败是终局结果，必须落在已结束阶段，并给出可统计的失败原因。
        task = BroadcastTask(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient="0x0000000000000000000000000000000000000002",
            amount="1",
            tx_hash="0x" + "1" * 64,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.FAILED,
            failure_reason=BroadcastTaskFailureReason.RPC_REJECTED,
        )

        with self.assertRaises(ValidationError):
            task.full_clean()

    def test_non_failed_task_cannot_keep_failure_reason(self):
        # 非失败任务如果残留失败原因，会让后台和统计系统误判终局。
        task = BroadcastTask(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient="0x0000000000000000000000000000000000000002",
            amount="1",
            tx_hash="0x" + "2" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            failure_reason=BroadcastTaskFailureReason.RPC_REJECTED,
        )

        with self.assertRaises(ValidationError):
            task.full_clean()


class WalletBip44AccountMapTests(TestCase):
    def test_deposit_maps_to_bip44_account_1(self):
        self.assertEqual(Wallet.get_bip44_account(AddressUsage.Deposit), 1)

    def test_vault_maps_to_bip44_account_0(self):
        self.assertEqual(Wallet.get_bip44_account(AddressUsage.Vault), 0)

    def test_unknown_usage_raises_value_error(self):
        with self.assertRaises(ValueError):
            Wallet.get_bip44_account("nonexistent")

    def test_wallet_str_for_non_project_wallet_is_stable_identifier(self):
        # 非项目钱包也必须输出稳定可区分的标识，避免被误显示成 Core。
        wallet = Wallet.objects.create()
        self.assertEqual(str(wallet), f"Wallet-{wallet.pk}")


class TxHashModelTests(TestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.crypto = Crypto.objects.create(
            name="Ethereum TxHash",
            symbol="ETH-TXH",
            coingecko_id="ethereum-txhash",
        )
        self.chain = Chain.objects.create(
            name="Ethereum TxHash",
            code="eth-txh",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=10002,
            rpc="http://localhost:8545",
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000011",
        )
        self.task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient="0x0000000000000000000000000000000000000012",
            amount="1",
            tx_hash="0x" + "a1" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )

    def test_tx_hash_unique_per_chain_hash(self):
        TxHash.objects.create(
            broadcast_task=self.task,
            chain=self.chain,
            hash="0x" + "b1" * 32,
            version=1,
        )

        with self.assertRaises(IntegrityError):
            TxHash.objects.create(
                broadcast_task=self.task,
                chain=self.chain,
                hash="0x" + "b1" * 32,
                version=2,
            )

    def test_tx_hash_unique_version_per_broadcast_task(self):
        TxHash.objects.create(
            broadcast_task=self.task,
            chain=self.chain,
            hash="0x" + "c1" * 32,
            version=1,
        )

        with self.assertRaises(IntegrityError):
            TxHash.objects.create(
                broadcast_task=self.task,
                chain=self.chain,
                hash="0x" + "c2" * 32,
                version=1,
            )

    def test_tx_hash_chain_must_match_broadcast_task_chain(self):
        other_crypto = Crypto.objects.create(
            name="Ethereum TxHash Other",
            symbol="ETH-TXHO",
            coingecko_id="ethereum-txhash-other",
        )
        other_chain = Chain.objects.create(
            name="Ethereum TxHash Other",
            code="eth-txho",
            type=ChainType.EVM,
            native_coin=other_crypto,
            chain_id=10003,
            rpc="http://localhost:8545",
            active=True,
        )

        tx_hash = TxHash(
            broadcast_task=self.task,
            chain=other_chain,
            hash="0x" + "d1" * 32,
            version=1,
        )

        with self.assertRaises(ValidationError):
            tx_hash.full_clean()


class BroadcastTaskTxHashHistoryTests(TestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.crypto = Crypto.objects.create(
            name="Ethereum TxHash History",
            symbol="ETH-TXHH",
            coingecko_id="ethereum-txhash-history",
        )
        self.chain = Chain.objects.create(
            name="Ethereum TxHash History",
            code="eth-txhh",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=10004,
            rpc="http://localhost:8545",
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000021",
        )
        self.task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient="0x0000000000000000000000000000000000000022",
            amount="1",
            tx_hash="0x" + "e1" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )

    def test_append_tx_hash_updates_current_tx_hash_and_keeps_history(self):
        self.task.append_tx_hash(self.task.tx_hash)

        appended = self.task.append_tx_hash("0x" + "e2" * 32)

        self.task.refresh_from_db()
        history = list(self.task.tx_hashes.order_by("version"))
        self.assertEqual(self.task.tx_hash, "0x" + "e2" * 32)
        self.assertEqual(appended.version, 2)
        self.assertEqual(
            [item.hash for item in history], ["0x" + "e1" * 32, "0x" + "e2" * 32]
        )

    def test_resolve_broadcast_task_by_old_hash(self):
        self.task.append_tx_hash(self.task.tx_hash)
        self.task.append_tx_hash("0x" + "e2" * 32)

        resolved = BroadcastTask.resolve_by_hash(
            chain=self.chain,
            tx_hash="0x" + "e1" * 32,
        )

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.pk, self.task.pk)

    def test_resolve_broadcast_task_falls_back_to_current_tx_hash(self):
        resolved = BroadcastTask.resolve_by_hash(
            chain=self.chain,
            tx_hash=self.task.tx_hash,
        )

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.pk, self.task.pk)


class AddressIdentityTests(TestCase):
    def test_address_identity_tuple_must_be_unique(self):
        # 同一钱包在同链同 usage + address_index 上只能有一个 Address。
        wallet = Wallet.objects.create()
        Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x0000000000000000000000000000000000000001",
        )

        with self.assertRaises(IntegrityError):
            Address.objects.create(
                wallet=wallet,
                chain_type=ChainType.EVM,
                usage=AddressUsage.Deposit,
                bip44_account=0,
                address_index=0,
                address="0x0000000000000000000000000000000000000002",
            )

    @patch("chains.signer.get_signer_backend")
    def test_get_address_rejects_corrupted_existing_identity(
        self, get_signer_backend_mock
    ):
        # 历史脏数据若把同一 HD 身份写成错误地址，运行时必须立即报错而不是继续使用。
        signer_backend = Mock(spec=RemoteSignerBackend)
        signer_backend.derive_address.return_value = Web3.to_checksum_address(
            "0x00000000000000000000000000000000000000aa"
        )
        get_signer_backend_mock.return_value = signer_backend
        wallet = Wallet.objects.create()
        Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x0000000000000000000000000000000000000001",
        )

        with self.assertRaises(RuntimeError):
            wallet.get_address(
                chain_type=ChainType.EVM,
                usage=AddressUsage.Deposit,
                address_index=0,
            )

    @patch("chains.signer.get_signer_backend")
    def test_get_address_preserves_non_identity_integrity_error(
        self, get_signer_backend_mock
    ):
        # 若冲突的是别的唯一约束（如地址被其他地址记录占用），不能误判成 tuple 并发创建成功。
        expected_address = Web3.to_checksum_address(
            "0x00000000000000000000000000000000000000ab"
        )
        signer_backend = Mock(spec=RemoteSignerBackend)
        signer_backend.derive_address.return_value = expected_address
        get_signer_backend_mock.return_value = signer_backend
        wallet = Wallet.objects.create()
        occupied_wallet = Wallet.objects.create()
        Address.objects.create(
            wallet=occupied_wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=9_999,
            address=expected_address,
        )

        with self.assertRaises(IntegrityError):
            wallet.get_address(
                chain_type=ChainType.EVM,
                usage=AddressUsage.Deposit,
                address_index=0,
            )


class TransferConfirmDispatchTests(TestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.crypto = Crypto.objects.create(
            name="Ethereum Confirm Dispatch",
            symbol="ETHCD",
            coingecko_id="ethereum-confirm-dispatch",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Confirm Dispatch",
            code="eth-confirm-dispatch",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=101,
            rpc="http://localhost:8545",
            active=True,
            confirm_block_count=12,
            latest_block_number=100,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000c1"
            ),
        )

    def _create_withdrawal_transfer_fixture(self, *, tx_hash: str):
        from projects.models import Project
        from withdrawals.models import Withdrawal
        from withdrawals.models import WithdrawalStatus

        project = Project.objects.create(
            name=f"project-{tx_hash[-6:]}",
            wallet=self.wallet,
            webhook="https://example.com/webhook",
        )
        broadcast_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000c2"
            ),
            amount="1",
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            chain=self.chain,
            crypto=self.crypto,
            amount="1",
            worth="1",
            out_no=f"out-{tx_hash[-6:]}",
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000c3"),
            broadcast_task=broadcast_task,
            status=WithdrawalStatus.CONFIRMING,
        )
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=100,
            hash=tx_hash,
            event_id="withdrawal:tx",
            crypto=self.crypto,
            from_address=self.addr.address,
            to_address=withdrawal.to,
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
            type=TransferType.Withdrawal,
        )
        withdrawal.transfer = transfer
        withdrawal.save(update_fields=["transfer", "updated_at"])
        return transfer, withdrawal, broadcast_task

    @patch("chains.tasks.confirm_transfer.delay")
    def test_block_number_updated_dispatches_quick_transfer_without_waiting_depth(
        self,
        confirm_transfer_delay_mock,
    ):
        # QUICK 模式只要已进入 confirming 且完成业务归类，就应立即进入确认任务，不等区块深度。
        from chains.tasks import block_number_updated

        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=100,
            hash="0x" + "7" * 64,
            event_id="native:tx",
            crypto=self.crypto,
            from_address=self.addr.address,
            to_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000c2"
            ),
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
            confirm_mode=ConfirmMode.QUICK,
            type=TransferType.Withdrawal,
            processed_at=timezone.now(),
        )

        block_number_updated.run(self.chain.pk)

        confirm_transfer_delay_mock.assert_called_once_with(transfer.pk)

    @patch("common.decorators.cache.delete", return_value=True)
    @patch("common.decorators.cache.add", return_value=True)
    @patch("withdrawals.service.WithdrawalService.notify_status_changed")
    @patch("chains.tasks.AdapterFactory.get_adapter")
    def test_confirm_transfer_raises_when_failed_result_appears_on_existing_transfer(
        self,
        get_adapter_mock,
        _notify_mock,
        _cache_add_mock,
        _cache_delete_mock,
    ):
        from chains.adapters import TxCheckStatus
        from chains.tasks import confirm_transfer
        from withdrawals.models import WithdrawalStatus

        transfer, withdrawal, broadcast_task = self._create_withdrawal_transfer_fixture(
            tx_hash="0x" + "f" * 64
        )
        adapter = Mock()
        adapter.tx_result.return_value = TxCheckStatus.FAILED
        get_adapter_mock.return_value = adapter

        with self.assertRaisesMessage(
            RuntimeError, "失败交易不应存在 OnchainTransfer 记录"
        ):
            confirm_transfer.run(transfer.pk)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)
        self.assertEqual(withdrawal.transfer_id, transfer.pk)
        broadcast_task.refresh_from_db()
        self.assertEqual(broadcast_task.stage, BroadcastTaskStage.PENDING_CONFIRM)
        self.assertEqual(broadcast_task.result, BroadcastTaskResult.UNKNOWN)

    @patch("common.decorators.cache.delete", return_value=True)
    @patch("common.decorators.cache.add", return_value=True)
    @patch("chains.tasks.AdapterFactory.get_adapter")
    def test_confirm_transfer_handles_dropped_result_by_reverting_pending_chain(
        self, get_adapter_mock, _cache_add_mock, _cache_delete_mock
    ):
        from chains.adapters import TxCheckStatus
        from chains.tasks import confirm_transfer
        from withdrawals.models import WithdrawalStatus

        transfer, withdrawal, broadcast_task = self._create_withdrawal_transfer_fixture(
            tx_hash="0x" + "e" * 64
        )
        adapter = Mock()
        adapter.tx_result.return_value = TxCheckStatus.DROPPED
        get_adapter_mock.return_value = adapter

        confirm_transfer.run(transfer.pk)

        self.assertFalse(OnchainTransfer.objects.filter(pk=transfer.pk).exists())
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer)
        broadcast_task.refresh_from_db()
        self.assertEqual(broadcast_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(broadcast_task.result, BroadcastTaskResult.UNKNOWN)


class SignerBackendTests(TestCase):
    @override_settings(
        SIGNER_BACKEND="remote",
        SIGNER_BASE_URL="http://signer.internal",
        SIGNER_TIMEOUT=3.5,
        SIGNER_SHARED_SECRET="secret",
    )
    @patch("chains.signer.httpx.post")
    def test_remote_signer_backend_posts_wallet_chain_and_bip44_params(
        self, httpx_post_mock
    ):
        # 远端 signer 接受 wallet_id / chain_type / bip44_account / address_index。
        addr = Mock(
            wallet_id=12, chain_type=ChainType.EVM, bip44_account=1, address_index=0
        )
        chain = Mock()
        response = Mock()
        response.json.return_value = {
            "tx_hash": "0x" + "11" * 32,
            "raw_transaction": "0xdeadbeef",
        }
        httpx_post_mock.return_value = response

        payload = get_signer_backend().sign_evm_transaction(
            address=addr,
            chain=chain,
            tx_dict={"nonce": 7, "data": "0x"},
        )

        self.assertEqual(payload.tx_hash, "0x" + "11" * 32)
        self.assertEqual(payload.raw_transaction, "0xdeadbeef")
        _, kwargs = httpx_post_mock.call_args
        body = kwargs["content"].decode("utf-8")
        self.assertIn('"wallet_id":12', body)
        self.assertIn(f'"chain_type":"{ChainType.EVM}"', body)
        self.assertIn('"bip44_account":1', body)
        self.assertIn('"address_index":0', body)

    @override_settings(
        SIGNER_BACKEND="remote",
        SIGNER_BASE_URL="http://signer.internal",
        SIGNER_TIMEOUT=3.5,
        SIGNER_SHARED_SECRET="secret",
    )
    @patch("chains.signer.httpx.post")
    def test_remote_signer_backend_create_wallet_returns_wallet_id(
        self, httpx_post_mock
    ):
        response = Mock()
        response.json.return_value = {
            "wallet_id": 99,
            "created": True,
        }
        httpx_post_mock.return_value = response

        wallet_id = get_signer_backend().create_wallet(wallet_id=99)

        self.assertEqual(wallet_id, 99)
        _, kwargs = httpx_post_mock.call_args
        self.assertEqual(
            json.loads(kwargs["content"].decode("utf-8")),
            {
                "wallet_id": 99,
                "request_id": json.loads(kwargs["content"].decode("utf-8"))[
                    "request_id"
                ],
            },
        )
        request_payload = json.loads(kwargs["content"].decode("utf-8"))
        self.assertEqual(
            kwargs["headers"]["X-Signer-Signature"],
            hmac.new(
                b"secret",
                build_signer_signature_payload(
                    method="POST",
                    path="/v1/wallets/create",
                    request_id=request_payload["request_id"],
                    request_body=kwargs["content"],
                ),
                hashlib.sha256,
            ).hexdigest(),
        )

    @override_settings(
        SIGNER_BACKEND="remote",
        SIGNER_BASE_URL="http://signer.internal",
        SIGNER_TIMEOUT=3.5,
        SIGNER_SHARED_SECRET="secret",
    )
    @patch("chains.signer.httpx.post")
    def test_remote_signer_backend_create_wallet_rejects_mismatched_wallet_id(
        self, httpx_post_mock
    ):
        response = Mock()
        response.json.return_value = {
            "wallet_id": 100,
            "created": True,
        }
        httpx_post_mock.return_value = response

        with self.assertRaisesMessage(SignerServiceError, "wallet_id 不匹配"):
            get_signer_backend().create_wallet(wallet_id=99)

    @override_settings(
        SIGNER_BACKEND="remote",
        SIGNER_BASE_URL="http://signer.internal",
        SIGNER_TIMEOUT=3.5,
        SIGNER_SHARED_SECRET="secret",
    )
    @patch("chains.signer.httpx.post")
    def test_remote_signer_backend_derive_address_posts_bip44_params(
        self, httpx_post_mock
    ):
        wallet = Mock(pk=12)
        response = Mock()
        response.json.return_value = {
            "address": "0x00000000000000000000000000000000000000f1",
        }
        httpx_post_mock.return_value = response

        address = get_signer_backend().derive_address(
            wallet=wallet,
            chain_type=ChainType.EVM,
            bip44_account=1,
            address_index=0,
        )

        self.assertEqual(address, "0x00000000000000000000000000000000000000f1")
        _, kwargs = httpx_post_mock.call_args
        body = kwargs["content"].decode("utf-8")
        self.assertIn('"wallet_id":12', body)
        self.assertIn(f'"chain_type":"{ChainType.EVM}"', body)
        self.assertIn('"bip44_account":1', body)
        self.assertIn('"address_index":0', body)

    @override_settings(
        SIGNER_BACKEND="remote",
        SIGNER_BASE_URL="http://signer.internal",
        SIGNER_TIMEOUT=3.5,
        SIGNER_SHARED_SECRET="secret",
    )
    @patch("chains.signer.httpx.get")
    def test_remote_signer_backend_fetches_admin_summary(self, httpx_get_mock):
        # 主应用后台只通过内部只读 API 拉取 signer 摘要，不直接读取 signer 数据库。
        response = Mock()
        response.json.return_value = {
            "health": {
                "database": True,
                "cache": True,
                "auth_configured": True,
                "healthy": True,
            },
            "wallets": {"total": 3, "active": 2, "frozen": 1},
            "requests_last_hour": {
                "total": 10,
                "succeeded": 8,
                "failed": 1,
                "rate_limited": 1,
            },
            "recent_anomalies": [
                {
                    "request_id": "req-1",
                    "endpoint": "/v1/sign/evm",
                    "wallet_id": 12,
                    "chain_type": ChainType.EVM,
                    "bip44_account": 0,
                    "address_index": 0,
                    "status": "failed",
                    "error_code": "1005",
                    "detail": "wallet 已冻结",
                    "created_at": "2026-03-14T10:00:00+00:00",
                }
            ],
        }
        httpx_get_mock.return_value = response

        summary = get_signer_backend().fetch_admin_summary()

        self.assertIsInstance(summary, SignerAdminSummary)
        self.assertEqual(summary.wallets["frozen"], 1)
        self.assertEqual(summary.requests_last_hour["failed"], 1)
        self.assertEqual(summary.recent_anomalies[0]["wallet_id"], 12)
        _, kwargs = httpx_get_mock.call_args
        self.assertEqual(
            kwargs["headers"]["X-Signer-Signature"],
            hmac.new(
                b"secret",
                build_signer_signature_payload(
                    method="GET",
                    path="/internal/admin-summary",
                    request_id=kwargs["headers"]["X-Signer-Request-Id"],
                    request_body=b"",
                ),
                hashlib.sha256,
            ).hexdigest(),
        )


@override_settings(
    SIGNER_BACKEND="remote",
    SIGNER_BASE_URL="http://signer.internal",
    SIGNER_TIMEOUT=3.5,
    SIGNER_SHARED_SECRET="secret",
)
class WalletRemoteGenerationTests(TestCase):
    @patch("chains.signer.httpx.post")
    def test_generate_remote_wallet_uses_signer_create_and_derive_address(
        self, httpx_post_mock
    ):
        # remote 模式下新钱包不再本地生成助记词，但后续 get_address 仍应能通过 signer 派生地址。
        def side_effect(url, **kwargs):
            response = Mock()
            body = json.loads(kwargs["content"].decode("utf-8"))
            if url.endswith("/v1/wallets/create"):
                self.assertIsInstance(body["wallet_id"], int)
                response.json.return_value = {
                    "wallet_id": body["wallet_id"],
                    "created": True,
                }
                return response
            if url.endswith("/v1/wallets/derive-address"):
                self.assertEqual(body["wallet_id"], created_wallet_id)
                response.json.return_value = {
                    "address": Web3.to_checksum_address(
                        "0x000000000000000000000000000000000000abcd"
                    ),
                }
                return response
            raise AssertionError(f"unexpected url: {url}")

        created_wallet_id = None

        def capturing_side_effect(url, **kwargs):
            nonlocal created_wallet_id
            response = side_effect(url, **kwargs)
            body = json.loads(kwargs["content"].decode("utf-8"))
            if url.endswith("/v1/wallets/create"):
                created_wallet_id = body["wallet_id"]
            return response

        httpx_post_mock.side_effect = capturing_side_effect

        wallet = Wallet.generate()
        addr = wallet.get_address(
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
        )

        self.assertEqual(wallet.pk, created_wallet_id)
        self.assertEqual(
            addr.address,
            Web3.to_checksum_address("0x000000000000000000000000000000000000abcd"),
        )

    @patch("chains.signer.get_signer_backend")
    def test_generate_remote_wallet_raises_readable_error_when_signer_unavailable(
        self,
        get_signer_backend_mock,
    ):
        signer_backend = Mock(spec=RemoteSignerBackend)
        signer_backend.create_wallet.side_effect = SignerServiceError("signer down")
        get_signer_backend_mock.return_value = signer_backend

        with self.assertRaisesMessage(
            RuntimeError, "signer 服务不可用，无法创建新钱包"
        ):
            Wallet.generate()

    @patch("chains.signer.get_signer_backend")
    def test_get_address_raises_readable_error_when_signer_unavailable(
        self,
        get_signer_backend_mock,
    ):
        signer_backend = Mock(spec=RemoteSignerBackend)
        signer_backend.derive_address.side_effect = SignerServiceError("signer down")
        get_signer_backend_mock.return_value = signer_backend
        wallet = Wallet.objects.create()

        with self.assertRaisesMessage(RuntimeError, "signer 服务不可用，无法为钱包"):
            wallet.get_address(
                chain_type=ChainType.EVM,
                usage=AddressUsage.Vault,
            )


@override_settings(
    SIGNER_BACKEND="remote",
    SIGNER_BASE_URL="",
    SIGNER_SHARED_SECRET="",
)
class SignerSystemCheckTests(TestCase):
    def test_remote_signer_requires_base_url_and_shared_secret(self):
        errors = checks.run_checks()
        error_ids = {error.id for error in errors}

        self.assertIn("chains.E002", error_ids)
        self.assertIn("chains.E003", error_ids)


@override_settings(
    SIGNER_BACKEND="remote",
    SIGNER_BASE_URL="http://signer.internal",
    SIGNER_TIMEOUT=3.5,
)
class CheckSignerServiceCommandTests(TestCase):
    @patch("chains.management.commands.check_signer_service.httpx.get")
    def test_command_accepts_healthz_ok_payload(self, httpx_get_mock):
        response = Mock()
        response.json.return_value = {"ok": True}
        httpx_get_mock.return_value = response
        stdout = StringIO()

        call_command("check_signer_service", stdout=stdout)

        self.assertIn("signer 服务检查通过", stdout.getvalue())

    @patch("chains.management.commands.check_signer_service.httpx.get")
    def test_command_reports_healthy_signer_service(self, httpx_get_mock):
        response = Mock()
        response.json.return_value = {
            "database": True,
            "cache": True,
            "signer_shared_secret": True,
            "healthy": True,
        }
        httpx_get_mock.return_value = response
        stdout = StringIO()

        call_command("check_signer_service", stdout=stdout)

        self.assertIn("signer 服务检查通过", stdout.getvalue())

    @patch("chains.management.commands.check_signer_service.httpx.get")
    def test_command_fails_when_signer_service_not_ready(self, httpx_get_mock):
        response = Mock()
        response.json.return_value = {
            "database": True,
            "cache": False,
            "signer_shared_secret": True,
            "healthy": False,
        }
        httpx_get_mock.return_value = response

        with self.assertRaises(CommandError):
            call_command("check_signer_service")


class UpdateLatestBlockTaskConfigTests(TestCase):
    def test_update_the_latest_block_time_limit_exceeds_bitcoin_rpc_timeout(self):
        from chains.tasks import update_the_latest_block

        # Bitcoin RPC 当前客户端超时是 30s，任务硬超时必须更长，避免 worker 先杀死任务。
        self.assertGreater(update_the_latest_block.time_limit, 30)

    @patch("chains.tasks.block_number_updated.delay")
    def test_update_the_latest_block_keeps_tron_height_without_rpc_polling(
        self,
        block_number_updated_delay_mock,
    ):
        from chains.tasks import update_the_latest_block

        trx = Crypto.objects.create(
            name="Tron Native Height Guard",
            symbol="TRXH",
            coingecko_id="tron-native-height-guard",
        )
        chain = Chain.objects.create(
            name="Tron Height Guard",
            code="tron-height-guard",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="http://tron.invalid",
            active=True,
            latest_block_number=456,
        )

        update_the_latest_block.run(chain.pk)

        chain.refresh_from_db()
        self.assertEqual(chain.latest_block_number, 456)
        block_number_updated_delay_mock.assert_not_called()


class TransferServiceCreateObservedTests(TestCase):
    """覆盖 TransferService.create_observed_transfer 的幂等与冲突场景。"""

    def setUp(self):
        from chains.service import ObservedTransferPayload

        self.crypto = Crypto.objects.create(
            name="Ether OT",
            symbol="ETH-OT",
            coingecko_id="ether-ot",
        )
        self.chain = Chain.objects.create(
            name="Ethereum OT",
            code="eth-ot",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=201,
            rpc="http://localhost:8545",
            active=True,
        )
        self.payload = ObservedTransferPayload(
            chain=self.chain,
            block=100,
            tx_hash="0x" + "ab" * 32,
            event_id="native:tx",
            from_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000a1"
            ),
            to_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000a2"
            ),
            crypto=self.crypto,
            value=1000,
            amount=1,
            timestamp=1700000000,
            occurred_at=timezone.now(),
            source="test",
        )

    @patch("chains.service.TransferService.enqueue_processing")
    def test_first_create_returns_created_true(self, enqueue_mock):
        from chains.service import TransferService

        result = TransferService.create_observed_transfer(observed=self.payload)

        self.assertTrue(result.created)
        self.assertFalse(result.conflict)
        self.assertIsNotNone(result.transfer)
        enqueue_mock.assert_called_once()

    @patch("chains.service.TransferService.enqueue_processing")
    def test_idempotent_replay_returns_created_false_no_conflict(self, enqueue_mock):
        from chains.service import TransferService

        first = TransferService.create_observed_transfer(observed=self.payload)
        second = TransferService.create_observed_transfer(observed=self.payload)

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertFalse(second.conflict)
        self.assertEqual(first.transfer.pk, second.transfer.pk)
        # 只有首次创建才触发 enqueue
        enqueue_mock.assert_called_once()

    @patch("chains.service.TransferService.enqueue_processing")
    def test_conflicting_content_returns_conflict_true(self, enqueue_mock):
        from chains.service import ObservedTransferPayload
        from chains.service import TransferService

        TransferService.create_observed_transfer(observed=self.payload)

        # 相同唯一键但不同 amount
        conflicting = ObservedTransferPayload(
            chain=self.payload.chain,
            block=self.payload.block,
            tx_hash=self.payload.tx_hash,
            event_id=self.payload.event_id,
            from_address=self.payload.from_address,
            to_address=self.payload.to_address,
            crypto=self.payload.crypto,
            value=self.payload.value,
            amount=999,  # 不同金额
            timestamp=self.payload.timestamp,
            occurred_at=self.payload.occurred_at,
            source="test-conflict",
        )
        result = TransferService.create_observed_transfer(observed=conflicting)

        self.assertFalse(result.created)
        self.assertTrue(result.conflict)


class BroadcastTaskTransitionTests(TestCase):
    """验证 BroadcastTask 封装的状态转换方法。"""

    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.crypto = Crypto.objects.create(
            name="Ether Trans",
            symbol="ETH-TR",
            coingecko_id="ether-trans",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Trans",
            code="eth-trans",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=301,
            rpc="http://localhost:8545",
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=0,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000d1"
            ),
        )
        self.task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000d2"
            ),
            amount="1",
            tx_hash="0x" + "dd" * 32,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
        )

    def test_mark_finalized_success_transitions_correctly(self):
        updated = BroadcastTask.mark_finalized_success(
            chain=self.chain, tx_hash=self.task.tx_hash
        )
        self.assertEqual(updated, 1)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(self.task.result, BroadcastTaskResult.SUCCESS)
        self.assertEqual(self.task.failure_reason, "")

    def test_reset_to_pending_chain_transitions_correctly(self):
        updated = BroadcastTask.reset_to_pending_chain(
            chain=self.chain, tx_hash=self.task.tx_hash
        )
        self.assertEqual(updated, 1)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(self.task.result, BroadcastTaskResult.UNKNOWN)

    def test_mark_finalized_success_can_resolve_old_hash(self):
        old_hash = self.task.tx_hash
        self.task.append_tx_hash(old_hash)
        self.task.append_tx_hash("0x" + "ee" * 32)

        updated = BroadcastTask.mark_finalized_success(
            chain=self.chain,
            tx_hash=old_hash,
        )

        self.assertEqual(updated, 1)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(self.task.result, BroadcastTaskResult.SUCCESS)
        self.assertEqual(self.task.tx_hash, old_hash)

    def test_reset_to_pending_chain_can_resolve_old_hash(self):
        old_hash = self.task.tx_hash
        self.task.append_tx_hash(old_hash)
        self.task.append_tx_hash("0x" + "ef" * 32)

        updated = BroadcastTask.reset_to_pending_chain(
            chain=self.chain,
            tx_hash=old_hash,
        )

        self.assertEqual(updated, 1)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(self.task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(self.task.tx_hash, old_hash)

    def test_mark_finalized_failed_transitions_correctly(self):
        updated = BroadcastTask.mark_finalized_failed(
            task_id=self.task.pk,
            reason=BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )
        self.assertEqual(updated, 1)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(self.task.result, BroadcastTaskResult.FAILED)
        self.assertEqual(
            self.task.failure_reason, BroadcastTaskFailureReason.EXECUTION_REVERTED
        )

    def test_mark_finalized_success_does_not_override_failed_final_state(self):
        BroadcastTask.mark_finalized_failed(
            task_id=self.task.pk,
            reason=BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )

        updated = BroadcastTask.mark_finalized_success(
            chain=self.chain,
            tx_hash=self.task.tx_hash,
        )

        self.assertEqual(updated, 0)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(self.task.result, BroadcastTaskResult.FAILED)
        self.assertEqual(
            self.task.failure_reason, BroadcastTaskFailureReason.EXECUTION_REVERTED
        )

    def test_mark_finalized_failed_does_not_override_success_final_state(self):
        BroadcastTask.mark_finalized_success(
            chain=self.chain,
            tx_hash=self.task.tx_hash,
        )

        updated = BroadcastTask.mark_finalized_failed(
            task_id=self.task.pk,
            reason=BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )

        self.assertEqual(updated, 0)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.FINALIZED)
        self.assertEqual(self.task.result, BroadcastTaskResult.SUCCESS)
        self.assertEqual(self.task.failure_reason, "")

    def test_mark_pending_confirm_skips_finalized_tasks(self):
        # 先将任务标记为已终结
        BroadcastTask.mark_finalized_success(
            chain=self.chain, tx_hash=self.task.tx_hash
        )
        # mark_pending_confirm 不应回退已终结的任务
        updated = BroadcastTask.mark_pending_confirm(
            chain=self.chain, tx_hash=self.task.tx_hash
        )
        self.assertEqual(updated, 0)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.FINALIZED)

    def test_mark_pending_confirm_with_empty_hash_is_noop(self):
        updated = BroadcastTask.mark_pending_confirm(chain=self.chain, tx_hash="")
        self.assertEqual(updated, 0)

    def test_mark_pending_confirm_can_resolve_old_hash(self):
        old_hash = self.task.tx_hash
        self.task.append_tx_hash(old_hash)
        self.task.append_tx_hash("0x" + "f0" * 32)

        updated = BroadcastTask.mark_pending_confirm(
            chain=self.chain,
            tx_hash=old_hash,
        )

        self.assertEqual(updated, 1)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.PENDING_CONFIRM)
        self.assertEqual(self.task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(self.task.tx_hash, old_hash)

    def test_reset_to_pending_chain_skips_non_pending_confirm_tasks(self):
        BroadcastTask.objects.filter(pk=self.task.pk).update(
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        updated = BroadcastTask.reset_to_pending_chain(
            chain=self.chain,
            tx_hash=self.task.tx_hash,
        )

        self.assertEqual(updated, 0)
        self.task.refresh_from_db()
        self.assertEqual(self.task.stage, BroadcastTaskStage.QUEUED)
        self.assertEqual(self.task.result, BroadcastTaskResult.UNKNOWN)


class BlockNumberUpdatedCompensationTests(TestCase):
    """验证 block_number_updated 在满批时自调度补偿。"""

    def setUp(self):
        self.crypto = Crypto.objects.create(
            name="Ether BN",
            symbol="ETH-BN",
            coingecko_id="ether-bn",
        )
        self.chain = Chain.objects.create(
            name="Ethereum BN",
            code="eth-bn",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=401,
            rpc="http://localhost:8545",
            active=True,
            confirm_block_count=6,
            latest_block_number=200,
        )
        self.wallet = Wallet.objects.create()
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=0,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000e1"
            ),
        )

    @patch("chains.tasks.block_number_updated.apply_async")
    @patch("chains.tasks.confirm_transfer.delay")
    def test_reschedules_when_quick_batch_is_full(
        self, confirm_delay_mock, reschedule_mock
    ):
        from chains.tasks import block_number_updated

        # 创建 17 个 QUICK 模式的 confirming 转账（超过 BATCH_SIZE=16）
        for i in range(17):
            OnchainTransfer.objects.create(
                chain=self.chain,
                block=190,
                hash="0x" + f"{i:064x}",
                event_id="native:tx",
                crypto=self.crypto,
                from_address=self.addr.address,
                to_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000e2"
                ),
                value="1",
                amount="1",
                timestamp=1700000000 + i,
                datetime=timezone.now(),
                status=TransferStatus.CONFIRMING,
                confirm_mode=ConfirmMode.QUICK,
                type=TransferType.Deposit,
                processed_at=timezone.now(),
            )

        block_number_updated.run(self.chain.pk)

        # 应派发 16 个确认任务
        self.assertEqual(confirm_delay_mock.call_count, 16)
        # 应自调度一次补偿
        reschedule_mock.assert_called_once_with(args=(self.chain.pk,), countdown=2)

    @patch("chains.tasks.block_number_updated.apply_async")
    @patch("chains.tasks.confirm_transfer.delay")
    def test_no_reschedule_when_batch_not_full(
        self, confirm_delay_mock, reschedule_mock
    ):
        from chains.tasks import block_number_updated

        # 只创建 3 个转账，不满批
        for i in range(3):
            OnchainTransfer.objects.create(
                chain=self.chain,
                block=190,
                hash="0x" + f"{i+100:064x}",
                event_id="native:tx",
                crypto=self.crypto,
                from_address=self.addr.address,
                to_address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000e2"
                ),
                value="1",
                amount="1",
                timestamp=1700000000 + i,
                datetime=timezone.now(),
                status=TransferStatus.CONFIRMING,
                confirm_mode=ConfirmMode.QUICK,
                type=TransferType.Deposit,
                processed_at=timezone.now(),
            )

        block_number_updated.run(self.chain.pk)

        self.assertEqual(confirm_delay_mock.call_count, 3)
        reschedule_mock.assert_not_called()
