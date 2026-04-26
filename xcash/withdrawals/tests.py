from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth.models import Permission
from django.db import IntegrityError
from django.test import TestCase
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from django_otp.oath import TOTP
from django_otp.plugins.otp_totp.models import TOTPDevice
from rest_framework.test import APIRequestFactory
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
from chains.models import TransferStatus
from chains.models import TransferType
from chains.models import Wallet
from common.error_codes import ErrorCode
from common.exceptions import APIError
from currencies.models import Crypto
from evm.models import EvmBroadcastTask
from projects.models import Project
from users.models import User
from users.otp import ADMIN_OTP_VERIFIED_AT_SESSION_KEY
from users.otp import build_admin_approval_context
from withdrawals.models import Withdrawal
from withdrawals.models import WithdrawalReviewLog
from withdrawals.models import WithdrawalStatus
from withdrawals.service import WithdrawalService
from withdrawals.viewsets import WithdrawalViewSet
from withdrawals.serializers import CreateWithdrawalSerializer


class WithdrawalBroadcastTaskTests(TestCase):
    def test_build_webhook_payload_omits_uid_when_customer_missing(self):
        project = Project.objects.create(
            name="DemoPayload",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Payload",
            symbol="ETHP",
            coingecko_id="ethereum-withdrawal-payload",
        )
        chain = Chain.objects.create(
            name="Ethereum Payload",
            code="eth-payload",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=21,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="order-payload",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000021",
            status=WithdrawalStatus.CONFIRMING,
        )

        payload = WithdrawalService.build_webhook_payload(withdrawal)

        self.assertEqual(payload["type"], "withdrawal")
        self.assertEqual(payload["data"]["sys_no"], withdrawal.sys_no)
        self.assertEqual(payload["data"]["out_no"], withdrawal.out_no)
        self.assertEqual(payload["data"]["chain"], chain.code)
        self.assertFalse(payload["data"]["confirmed"])
        self.assertNotIn("uid", payload["data"])

    @patch("chains.tasks.process_transfer.apply_async")
    def test_try_match_withdrawal_requires_broadcast_task_anchor(
        self,
        _process_transfer_mock,
    ):
        # 提币单必须通过 broadcast_task 关联链上任务，不再允许按旧 hash 字段兜底匹配。
        # User 的 post_save 会触发项目初始化；测试里用 bulk_create 绕过副作用，专注验证提币匹配逻辑。
        User.objects.bulk_create([User(username="merchant")])
        wallet = Wallet.objects.create()
        project = Project.objects.create(name="Demo", wallet=wallet)
        crypto = Crypto.objects.create(
            name="Ethereum",
            symbol="ETH",
            coingecko_id="ethereum",
        )
        chain = Chain.objects.create(
            name="Ethereum",
            code="eth",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=1,
            rpc="http://localhost:8545",
            active=True,
        )
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000001",
        )
        broadcast_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=crypto,
            recipient="0x0000000000000000000000000000000000000002",
            amount="1",
            tx_hash="0x" + "2" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        EvmBroadcastTask.objects.create(
            base_task=broadcast_task,
            address=addr,
            chain=chain,
            nonce=0,
            to=broadcast_task.recipient,
            value=1,
            gas=21_000,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="order-1",
            chain=chain,
            crypto=crypto,
            amount="1",
            to="0x0000000000000000000000000000000000000002",
            broadcast_task=broadcast_task,
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "2" * 64,
            event_id="native:0",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000001",
            to_address="0x0000000000000000000000000000000000000002",
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
        )

        matched = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)

        withdrawal.refresh_from_db()
        transfer.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertTrue(matched)
        self.assertEqual(withdrawal.transfer_id, transfer.id)
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)
        self.assertEqual(transfer.type, TransferType.Withdrawal)
        self.assertEqual(broadcast_task.stage, BroadcastTaskStage.PENDING_CONFIRM)
        self.assertEqual(broadcast_task.result, BroadcastTaskResult.UNKNOWN)

    @patch("withdrawals.service.WebhookService.create_event")
    def test_confirm_withdrawal_emits_completed_webhook(self, create_event_mock):
        # 提币确认完成后必须显式通知商户，不再依赖 post_save signal。
        project = Project.objects.create(
            name="DemoComplete",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Complete",
            symbol="ETHW",
            coingecko_id="ethereum-withdrawal-complete",
        )
        chain = Chain.objects.create(
            name="Ethereum W",
            code="eth-w",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=11,
            rpc="http://localhost:8545",
            active=True,
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "3" * 64,
            event_id="native:0",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000011",
            to_address="0x0000000000000000000000000000000000000012",
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Withdrawal,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="order-complete",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000012",
            hash=transfer.hash,
            transfer=transfer,
            status=WithdrawalStatus.CONFIRMING,
        )

        with self.captureOnCommitCallbacks(execute=True):
            WithdrawalService.confirm_withdrawal(transfer)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)
        create_event_mock.assert_called_once()

    def test_drop_withdrawal_reverts_pending_to_pending(self):
        # OnchainTransfer 被 drop 仅意味着链上观测消失，提币应回退到 PENDING 等待重新匹配，不发 webhook。
        project = Project.objects.create(
            name="DemoDrop",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Drop",
            symbol="ETHD",
            coingecko_id="ethereum-withdrawal-drop",
        )
        chain = Chain.objects.create(
            name="Ethereum D",
            code="eth-d",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=12,
            rpc="http://localhost:8545",
            active=True,
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "5" * 64,
            event_id="native:5",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000021",
            to_address="0x0000000000000000000000000000000000000022",
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
            type=TransferType.Withdrawal,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="order-drop",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000022",
            hash=transfer.hash,
            transfer=transfer,
            status=WithdrawalStatus.PENDING,
        )

        WithdrawalService.drop_withdrawal(transfer)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer_id)


class WithdrawalBalanceReservationTests(TestCase):
    def test_native_withdrawal_requires_extra_gas_budget(self):
        # 原生币提币必须把当前单子的 gas 一起计入可用余额，不能把全部余额都当作可转出金额。
        chain = type(
            "ChainStub",
            (),
            {
                "type": ChainType.EVM,
                "code": "eth-native-balance",
                "native_coin": None,
            },
        )()
        crypto = type(
            "CryptoStub",
            (),
            {
                "is_native": True,
                "get_decimals": staticmethod(lambda _chain: 0),
            },
        )()
        chain.native_coin = crypto
        adapter = type(
            "AdapterStub",
            (),
            {
                "get_balance": staticmethod(
                    lambda address, current_chain, current_crypto: 100
                ),
            },
        )()

        with (
            patch.object(WithdrawalService, "pending_amount_raw", return_value=0),
            patch.object(
                WithdrawalService,
                "pending_gas_reserved_raw",
                return_value=0,
            ),
            patch.object(
                WithdrawalService,
                "estimate_current_network_fee_raw",
                return_value=10,
            ),
        ):
            enough = WithdrawalService.has_sufficient_balance(
                project=object(),
                chain=chain,
                crypto=crypto,
                address="0x00000000000000000000000000000000000000F1",
                amount=Decimal("95"),
                adapter=adapter,
            )

        self.assertFalse(enough)


class CreateWithdrawalSerializerCapabilityTests(TestCase):
    def test_validate_rejects_tron_usdt_before_balance_check(self):
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="Tron Withdrawal Guard Project",
            wallet=wallet,
        )
        trx = Crypto.objects.create(
            name="Tron Native Withdrawal",
            symbol="TRXW",
            coingecko_id="tron-native-withdrawal",
        )
        usdt = Crypto.objects.create(
            name="Tether Withdrawal",
            symbol="USDT",
            coingecko_id="tether-withdrawal",
            decimals=6,
        )
        chain = Chain.objects.create(
            name="Tron Withdrawal",
            code="tron-withdrawal",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="http://tron.invalid",
            active=True,
        )
        request = APIRequestFactory().post(
            "/v1/withdrawal",
            {},
            format="json",
            HTTP_XC_APPID=project.appid,
        )
        serializer = CreateWithdrawalSerializer(
            context={"request": request},
        )

        with (
            patch("withdrawals.serializers.Project.retrieve", return_value=project),
            patch(
                "withdrawals.serializers.CryptoService.is_supported_on_chain",
                return_value=True,
            ),
            patch(
                "withdrawals.serializers.ChainProductCapabilityService.supports_withdrawal",
                return_value=False,
            ) as supports_withdrawal_mock,
            patch(
                "withdrawals.serializers.AdapterFactory.get_adapter",
                return_value=SimpleNamespace(validate_address=Mock(return_value=True)),
            ),
            patch(
                "withdrawals.serializers.AddressService.find_by_address",
                return_value=None,
            ),
            patch.object(
                CreateWithdrawalSerializer,
                "_is_valid_address",
                return_value=True,
            ),
            patch(
                "withdrawals.serializers.WithdrawalService.has_sufficient_balance",
                side_effect=AssertionError("余额检查不应执行"),
            ) as has_balance_mock,
        ):
            with self.assertRaises(APIError) as ctx:
                serializer.validate(
                    {
                        "out_no": "tron-order",
                        "to": "TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT",
                        "uid": None,
                        "crypto": usdt.symbol,
                        "chain": chain.code,
                        "amount": Decimal("1"),
                    }
                )

        self.assertEqual(
            ctx.exception.error_code.code,
            ErrorCode.INVALID_CHAIN.code,
        )
        supports_withdrawal_mock.assert_called_once_with(chain=chain, crypto=usdt)
        has_balance_mock.assert_not_called()


class WithdrawalPolicyTests(TestCase):
    def setUp(self):
        self.user = User.objects.create(username="merchant-policy")
        self.wallet = Wallet.objects.create()
        self.project = Project.objects.create(
            name="PolicyProject",
            wallet=self.wallet,
        )
        self.crypto = Crypto.objects.create(
            name="Ethereum Policy",
            symbol="ETHP",
            coingecko_id="ethereum-policy",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Policy",
            code="eth-policy",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=302,
            rpc="http://localhost:8545",
            active=True,
        )

    @patch.object(
        WithdrawalService, "estimate_withdrawal_worth", return_value=Decimal("120")
    )
    def test_policy_rejects_single_limit(self, _estimate_worth_mock):
        # 单笔限额按 USD 校验，超过阈值时不能让请求进入审核队列。
        self.project.withdrawal_single_limit = Decimal("100")
        self.project.save(update_fields=["withdrawal_single_limit"])

        with self.assertRaises(APIError) as ctx:
            WithdrawalService.assert_project_policy(
                project=self.project,
                chain=self.chain,
                crypto=self.crypto,
                to="0x0000000000000000000000000000000000000011",
                amount=Decimal("1"),
            )
        self.assertEqual(
            ctx.exception.detail["code"],
            ErrorCode.WITHDRAWAL_SINGLE_LIMIT_EXCEEDED.code,
        )

    @patch.object(
        WithdrawalService, "estimate_withdrawal_worth", return_value=Decimal("30")
    )
    def test_policy_rejects_daily_limit(self, _estimate_worth_mock):
        # 当日限额要把当天已创建的提币请求一并算上，避免拆单绕过额度。
        self.project.withdrawal_daily_limit = Decimal("100")
        self.project.save(update_fields=["withdrawal_daily_limit"])
        Withdrawal.objects.create(
            project=self.project,
            out_no="daily-existing",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            worth=Decimal("80"),
            to="0x0000000000000000000000000000000000000022",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.assertRaises(APIError) as ctx:
            WithdrawalService.assert_project_policy(
                project=self.project,
                chain=self.chain,
                crypto=self.crypto,
                to="0x0000000000000000000000000000000000000011",
                amount=Decimal("1"),
            )
        self.assertEqual(
            ctx.exception.detail["code"], ErrorCode.WITHDRAWAL_DAILY_LIMIT_EXCEEDED.code
        )

    @patch.object(
        WithdrawalService, "estimate_withdrawal_worth", return_value=Decimal("20")
    )
    def test_policy_returns_worth_when_review_exempt_limit_enabled(
        self, _estimate_worth_mock
    ):
        # 免审核门槛同样依赖 USD 价值判断，因此即便未配置限额也必须先计算 worth。
        self.project.withdrawal_review_exempt_limit = Decimal("50")
        self.project.save(update_fields=["withdrawal_review_exempt_limit"])

        worth = WithdrawalService.assert_project_policy(
            project=self.project,
            chain=self.chain,
            crypto=self.crypto,
            to="0x0000000000000000000000000000000000000011",
            amount=Decimal("1"),
        )

        self.assertEqual(worth, Decimal("20"))


class WithdrawalViewSetTests(TestCase):
    def setUp(self):
        # 屏蔽 SaaS 权限回调，避免单测触发真实 HTTP 请求
        patcher = patch("withdrawals.viewsets.check_saas_permission")
        self.mock_check_saas = patcher.start()
        self.addCleanup(patcher.stop)

    def test_viewset_create_translates_unique_conflict_to_api_error(self):
        # 提币创建命中数据库唯一约束时必须返回业务错误，而不是数据库异常或 500。
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="DuplicateWithdrawProject",
            wallet=wallet,
        )
        crypto = Crypto.objects.create(
            name="Ethereum Duplicate Withdraw",
            symbol="ETHDW",
            coingecko_id="ethereum-duplicate-withdraw",
        )
        chain = Chain.objects.create(
            name="Ethereum Duplicate Withdraw",
            code="eth-duplicate-withdraw",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=301,
            rpc="http://localhost:8545",
            active=True,
        )
        request = APIRequestFactory().post(
            "/v1/withdrawal",
            {},
            format="json",
            HTTP_XC_APPID=project.appid,
        )
        serializer = SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "dup-withdraw-order",
                "to": "0x0000000000000000000000000000000000000011",
                "uid": None,
                "crypto": crypto.symbol,
                "chain": chain.code,
                "amount": Decimal("1"),
            },
            errors={},
        )
        select_for_update_manager = Mock()
        select_for_update_manager.get.return_value = project

        with (
            patch("withdrawals.viewsets.Project.retrieve", return_value=project),
            patch(
                "withdrawals.viewsets.Project.objects.select_for_update",
                return_value=select_for_update_manager,
            ),
            patch(
                "withdrawals.viewsets.CreateWithdrawalSerializer",
                return_value=serializer,
            ),
            patch(
                "withdrawals.viewsets.Withdrawal.objects.create",
                side_effect=IntegrityError,
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.assert_project_policy",
                return_value=Decimal("0"),
            ),
        ):
            response = WithdrawalViewSet.as_view({"post": "create"})(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["code"], ErrorCode.DUPLICATE_OUT_NO.code)

    def test_viewset_create_returns_reviewing_when_project_requires_review(self):
        # 审核开关开启时，创建接口只落审核单，不应提前调度链上签名。
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="ReviewingProject",
            wallet=wallet,
            withdrawal_review_required=True,
        )
        crypto = Crypto.objects.create(
            name="Ethereum Reviewing",
            symbol="ETHR",
            coingecko_id="ethereum-reviewing",
        )
        chain = Chain.objects.create(
            name="Ethereum Reviewing",
            code="eth-reviewing",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=303,
            rpc="http://localhost:8545",
            active=True,
        )
        request = APIRequestFactory().post(
            "/v1/withdrawal",
            {},
            format="json",
            HTTP_XC_APPID=project.appid,
        )
        serializer = SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "review-order",
                "to": "0x0000000000000000000000000000000000000011",
                "uid": None,
                "crypto": crypto.symbol,
                "chain": chain.code,
                "amount": Decimal("1"),
            },
            errors={},
        )
        select_for_update_manager = Mock()
        select_for_update_manager.get.return_value = project

        with (
            patch("withdrawals.viewsets.Project.retrieve", return_value=project),
            patch(
                "withdrawals.viewsets.Project.objects.select_for_update",
                return_value=select_for_update_manager,
            ),
            patch(
                "withdrawals.viewsets.CreateWithdrawalSerializer",
                return_value=serializer,
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.assert_project_policy",
                return_value=Decimal("0"),
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.submit_withdrawal",
            ) as submit_mock,
        ):
            response = WithdrawalViewSet.as_view({"post": "create"})(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], WithdrawalStatus.REVIEWING)
        self.assertEqual(response.data["hash"], "")
        self.assertTrue(
            Withdrawal.objects.filter(
                project=project,
                out_no="review-order",
                status=WithdrawalStatus.REVIEWING,
            ).exists()
        )
        submit_mock.assert_not_called()

    def test_viewset_create_skips_review_when_worth_below_exempt_limit(self):
        # 开启审核但命中免审核门槛时，提币应直接进入发送队列，不再停留在 REVIEWING。
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="ExemptReviewProject",
            wallet=wallet,
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("50"),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Exempt Review",
            symbol="ETHE",
            coingecko_id="ethereum-exempt-review",
        )
        chain = Chain.objects.create(
            name="Ethereum Exempt Review",
            code="eth-exempt-review",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=308,
            rpc="http://localhost:8545",
            active=True,
        )
        request = APIRequestFactory().post(
            "/v1/withdrawal",
            {},
            format="json",
            HTTP_XC_APPID=project.appid,
        )
        serializer = SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "exempt-review-order",
                "to": "0x0000000000000000000000000000000000000011",
                "uid": None,
                "crypto": crypto.symbol,
                "chain": chain.code,
                "amount": Decimal("1"),
            },
            errors={},
        )
        select_for_update_manager = Mock()
        select_for_update_manager.get.return_value = project

        with (
            patch("withdrawals.viewsets.Project.retrieve", return_value=project),
            patch(
                "withdrawals.viewsets.Project.objects.select_for_update",
                return_value=select_for_update_manager,
            ),
            patch(
                "withdrawals.viewsets.CreateWithdrawalSerializer",
                return_value=serializer,
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.assert_project_policy",
                return_value=Decimal("20"),
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.submit_withdrawal",
                side_effect=lambda *, withdrawal: withdrawal,
            ) as submit_mock,
        ):
            response = WithdrawalViewSet.as_view({"post": "create"})(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], WithdrawalStatus.PENDING)
        self.assertTrue(
            Withdrawal.objects.filter(
                project=project,
                out_no="exempt-review-order",
                status=WithdrawalStatus.PENDING,
            ).exists()
        )
        submit_mock.assert_called_once()

    def test_viewset_create_keeps_review_when_worth_reaches_exempt_limit(self):
        # 免审核门槛采用严格“小于”判断；达到门槛的提币仍需要人工审核。
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="EqualReviewProject",
            wallet=wallet,
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("50"),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Equal Review",
            symbol="ETHQ",
            coingecko_id="ethereum-equal-review",
        )
        chain = Chain.objects.create(
            name="Ethereum Equal Review",
            code="eth-equal-review",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=309,
            rpc="http://localhost:8545",
            active=True,
        )
        request = APIRequestFactory().post(
            "/v1/withdrawal",
            {},
            format="json",
            HTTP_XC_APPID=project.appid,
        )
        serializer = SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "equal-review-order",
                "to": "0x0000000000000000000000000000000000000011",
                "uid": None,
                "crypto": crypto.symbol,
                "chain": chain.code,
                "amount": Decimal("1"),
            },
            errors={},
        )
        select_for_update_manager = Mock()
        select_for_update_manager.get.return_value = project

        with (
            patch("withdrawals.viewsets.Project.retrieve", return_value=project),
            patch(
                "withdrawals.viewsets.Project.objects.select_for_update",
                return_value=select_for_update_manager,
            ),
            patch(
                "withdrawals.viewsets.CreateWithdrawalSerializer",
                return_value=serializer,
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.assert_project_policy",
                return_value=Decimal("50"),
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.submit_withdrawal",
            ) as submit_mock,
        ):
            response = WithdrawalViewSet.as_view({"post": "create"})(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], WithdrawalStatus.REVIEWING)
        self.assertTrue(
            Withdrawal.objects.filter(
                project=project,
                out_no="equal-review-order",
                status=WithdrawalStatus.REVIEWING,
            ).exists()
        )
        submit_mock.assert_not_called()

    def test_token_withdrawal_subtracts_pending_gas_from_native_balance(self):
        # ERC20 提币虽然不消耗代币余额作为手续费，但原生币余额仍要扣除在途单子的 gas 预留。
        native = type(
            "NativeStub",
            (),
            {
                "is_native": True,
                "get_decimals": staticmethod(lambda _chain: 0),
            },
        )()
        crypto = type(
            "TokenStub",
            (),
            {
                "is_native": False,
                "get_decimals": staticmethod(lambda _chain: 0),
            },
        )()
        chain = type(
            "ChainStub",
            (),
            {
                "type": ChainType.EVM,
                "code": "eth-token-balance",
                "native_coin": native,
            },
        )()

        def get_balance(_address, _chain, current_crypto):
            return 100 if current_crypto is crypto else 10

        adapter = type(
            "AdapterStub",
            (),
            {
                "get_balance": staticmethod(get_balance),
            },
        )()

        with (
            patch.object(WithdrawalService, "pending_amount_raw", return_value=0),
            patch.object(
                WithdrawalService,
                "pending_gas_reserved_raw",
                return_value=7,
            ),
            patch.object(
                WithdrawalService,
                "estimate_current_network_fee_raw",
                return_value=5,
            ),
        ):
            enough = WithdrawalService.has_sufficient_balance(
                project=object(),
                chain=chain,
                crypto=crypto,
                address="0x00000000000000000000000000000000000000F2",
                amount=Decimal("50"),
                adapter=adapter,
            )

        self.assertFalse(enough)


class WithdrawalReviewTests(TestCase):
    def _current_token(self, device: TOTPDevice) -> str:
        return str(
            TOTP(
                device.bin_key, device.step, device.t0, device.digits, device.drift
            ).token()
        ).zfill(device.digits)

    def _build_approval_context(
        self, *, verified_at=None, source="test_review"
    ) -> dict[str, object]:
        # 提币审批测试统一构造一份通过 OTP 新鲜度校验的上下文，避免每个用例重复拼字典。
        return build_admin_approval_context(verified_at=verified_at, source=source)

    def _login_reviewer_with_expired_otp(self, reviewer: User) -> TOTPDevice:
        device = TOTPDevice.objects.create(
            user=reviewer, name="Withdrawal Reviewer OTP"
        )
        self.client.force_login(reviewer)
        session = self.client.session
        session["otp_device_id"] = device.persistent_id
        session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = (
            timezone.now() - timedelta(minutes=16)
        ).isoformat()
        session.save()
        return device

    @patch.object(WithdrawalService, "submit_withdrawal")
    def test_approve_withdrawal_sets_reviewer_and_moves_into_queue(self, submit_mock):
        # 提币仅超管可审核，批准成功应补链上任务并标记超管为审核人。
        owner = User.objects.create_superuser(
            username="merchant-approved", password="secret"
        )
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="ApprovedProject",
            wallet=wallet,
        )
        crypto = Crypto.objects.create(
            name="Ethereum Approved",
            symbol="ETHA",
            coingecko_id="ethereum-approved",
        )
        chain = Chain.objects.create(
            name="Ethereum Approved",
            code="eth-approved",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=304,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="approve-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        def mark_pending(*, withdrawal):
            withdrawal.status = WithdrawalStatus.PENDING
            withdrawal.hash = "0x" + "b" * 64
            withdrawal.save(update_fields=["status", "hash", "updated_at"])
            return withdrawal

        submit_mock.side_effect = mark_pending

        WithdrawalService.approve_withdrawal(
            withdrawal_id=withdrawal.pk,
            reviewer=owner,
            approval_context=self._build_approval_context(),
        )

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertEqual(withdrawal.reviewed_by, owner)
        self.assertIsNotNone(withdrawal.reviewed_at)
        submit_mock.assert_called_once()

    def test_non_owner_cannot_approve_other_project_withdrawal(self):
        # 提币改为商户自审后，非超管且非项目 owner 的后台用户不能审核他人项目提币。
        reviewer = User.objects.create(username="reviewer-outsider")
        project = Project.objects.create(
            name="OwnedProject",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Outsider Review",
            symbol="ETHOR",
            coingecko_id="ethereum-outsider-review",
        )
        chain = Chain.objects.create(
            name="Ethereum Outsider Review",
            code="eth-outsider-review",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=305,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="outsider-review-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.assertRaises(PermissionError):
            WithdrawalService.approve_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=reviewer,
                approval_context=self._build_approval_context(),
            )

    def test_superuser_can_review_withdrawal(self):
        # 提币仅超管可审核，超管应可直接批准任意项目的提币。
        owner = User.objects.create_superuser(
            username="merchant-self-review", password="secret"
        )
        project = Project.objects.create(
            name="SelfReviewProject",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Self Review",
            symbol="ETHSR",
            coingecko_id="ethereum-self-review",
        )
        chain = Chain.objects.create(
            name="Ethereum Self Review",
            code="eth-self-review",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=306,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="self-review-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        with patch.object(WithdrawalService, "submit_withdrawal") as submit_mock:

            def mark_pending(*, withdrawal):
                withdrawal.status = WithdrawalStatus.PENDING
                withdrawal.hash = "0x" + "d" * 64
                withdrawal.save(update_fields=["status", "hash", "updated_at"])
                return withdrawal

            submit_mock.side_effect = mark_pending
            WithdrawalService.approve_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=owner,
                approval_context=self._build_approval_context(),
            )

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertEqual(withdrawal.reviewed_by, owner)

    @patch.object(WithdrawalService, "submit_withdrawal")
    def test_approve_withdrawal_writes_review_log(self, submit_mock):
        # 每次批准都必须留下审核日志，记录操作者、状态变化和备注。
        owner = User.objects.create_superuser(
            username="merchant-audit", password="secret"
        )
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="AuditProject",
            wallet=wallet,
        )
        crypto = Crypto.objects.create(
            name="Ethereum Audit",
            symbol="ETHAU",
            coingecko_id="ethereum-audit",
        )
        chain = Chain.objects.create(
            name="Ethereum Audit",
            code="eth-audit",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=307,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="audit-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1.5"),
            worth=Decimal("33.25"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        def mark_pending(*, withdrawal):
            withdrawal.status = WithdrawalStatus.PENDING
            withdrawal.hash = "0x" + "c" * 64
            withdrawal.save(update_fields=["status", "hash", "updated_at"])
            return withdrawal

        submit_mock.side_effect = mark_pending

        WithdrawalService.approve_withdrawal(
            withdrawal_id=withdrawal.pk,
            reviewer=owner,
            note="人工复核通过",
            approval_context=self._build_approval_context(source="unit_test_approve"),
        )

        log = WithdrawalReviewLog.objects.get(withdrawal=withdrawal)
        self.assertEqual(log.actor, owner)
        self.assertEqual(log.action, WithdrawalReviewLog.Action.APPROVED)
        self.assertEqual(log.from_status, WithdrawalStatus.REVIEWING)
        self.assertEqual(log.to_status, WithdrawalStatus.PENDING)
        self.assertEqual(log.note, "人工复核通过")
        self.assertEqual(log.snapshot["out_no"], withdrawal.out_no)
        self.assertEqual(
            log.snapshot["approval_context"]["source"], "unit_test_approve"
        )
        self.assertTrue(log.snapshot["approval_context"]["otp_verified"])

    def test_approve_withdrawal_requires_admin_otp_context(self):
        # 即使是超管，审核提币时也必须拿到近期 OTP 验证上下文才能放行。
        owner = User.objects.create_superuser(
            username="merchant-no-otp", password="secret"
        )
        project = Project.objects.create(
            name="NoOTPProject",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum No OTP",
            symbol="ETHNO",
            coingecko_id="ethereum-no-otp",
        )
        chain = Chain.objects.create(
            name="Ethereum No OTP",
            code="eth-no-otp",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=308,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="no-otp-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.assertRaises(PermissionError):
            WithdrawalService.approve_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=owner,
            )

    def test_approve_withdrawal_rejects_expired_admin_otp_context(self):
        # 审批必须依赖”近期”OTP，即使是超管也不能使用过期的 OTP 上下文。
        owner = User.objects.create_superuser(
            username="merchant-expired-otp", password="secret"
        )
        project = Project.objects.create(
            name="ExpiredOTPProject",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Expired OTP",
            symbol="ETHEX",
            coingecko_id="ethereum-expired-otp",
        )
        chain = Chain.objects.create(
            name="Ethereum Expired OTP",
            code="eth-expired-otp",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=309,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="expired-otp-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.assertRaises(PermissionError):
            WithdrawalService.approve_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=owner,
                approval_context=self._build_approval_context(
                    verified_at=timezone.now() - timedelta(minutes=16),
                    source="expired_context",
                ),
            )

    @patch("withdrawals.admin.WithdrawalService.approve_withdrawal")
    def test_withdrawal_admin_action_renders_otp_modal_when_session_expired(
        self, approve_mock
    ):
        owner = User.objects.create(username="merchant-admin-modal", is_staff=True)
        owner.user_permissions.add(Permission.objects.get(codename="view_withdrawal"))
        project = Project.objects.create(
            name="AdminModalProject",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Admin Modal",
            symbol="ETHAM",
            coingecko_id="ethereum-admin-modal",
        )
        chain = Chain.objects.create(
            name="Ethereum Admin Modal",
            code="eth-admin-modal",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=310,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="admin-modal-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )
        self._login_reviewer_with_expired_otp(owner)

        response = self.client.post(
            reverse("admin:withdrawals_withdrawal_changelist"),
            {
                "action": "approve_selected_withdrawals",
                "_selected_action": [withdrawal.pk],
                "index": 0,
            },
        )

        self.assertEqual(response.status_code, 200)
        approve_mock.assert_not_called()

    @patch("withdrawals.admin.WithdrawalService.approve_withdrawal")
    def test_withdrawal_admin_action_continues_after_modal_otp_verification(
        self, approve_mock
    ):
        owner = User.objects.create(
            username="merchant-admin-modal-verify", is_staff=True
        )
        owner.user_permissions.add(Permission.objects.get(codename="view_withdrawal"))
        project = Project.objects.create(
            name="AdminModalVerifyProject",
            wallet=Wallet.objects.create(),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Admin Modal Verify",
            symbol="ETHMV",
            coingecko_id="ethereum-admin-modal-verify",
        )
        chain = Chain.objects.create(
            name="Ethereum Admin Modal Verify",
            code="eth-admin-modal-verify",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=311,
            rpc="http://localhost:8545",
            active=True,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="admin-modal-verify-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )
        device = self._login_reviewer_with_expired_otp(owner)

        # 第一次提交只负责把 OTP modal 渲染出来并在 session 中写入 pending 上下文。
        self.client.post(
            reverse("admin:withdrawals_withdrawal_changelist"),
            {
                "action": "approve_selected_withdrawals",
                "_selected_action": [withdrawal.pk],
                "index": 0,
            },
        )

        response = self.client.post(
            reverse("admin:withdrawals_withdrawal_changelist"),
            {
                "action": "approve_selected_withdrawals",
                "_selected_action": [withdrawal.pk],
                "index": 0,
                "_otp_modal_submit": 1,
                "token": self._current_token(device),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        approve_mock.assert_called_once()


@override_settings(
    SIGNER_BACKEND="remote",
    SIGNER_BASE_URL="http://signer.internal",
    SIGNER_SHARED_SECRET="secret",
)
class WithdrawalRemoteSignerFlowTests(TestCase):
    @patch("evm.models.get_signer_backend")
    @patch("chains.signer.get_signer_backend")
    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=0)
    @patch.object(
        WithdrawalService, "_make_balance_verify_fn", return_value=lambda: None
    )
    def test_submit_withdrawal_uses_remote_signer_without_local_mnemonic(
        self,
        _make_balance_verify_fn_mock,
        _next_nonce_mock,
        get_wallet_signer_backend_mock,
        get_evm_signer_backend_mock,
    ):
        # remote signer 模式下，提币提交必须走远端派生和远端签名，主应用不能再读取本地助记词。
        signer_backend = Mock()
        signer_backend.derive_address.return_value = Web3.to_checksum_address(
            "0x000000000000000000000000000000000000f001"
        )
        get_wallet_signer_backend_mock.return_value = signer_backend
        get_evm_signer_backend_mock.return_value = signer_backend
        wallet = Wallet.objects.create()
        with patch("projects.signals.Wallet.generate", return_value=wallet):
            project = Project.objects.create(
                name="RemoteWithdrawSubmitProject",
                wallet=wallet,
            )
        crypto = Crypto.objects.create(
            name="Ethereum Remote Withdrawal",
            symbol="ETHRW",
            prices={"USD": "1"},
            coingecko_id="ethereum-remote-withdrawal",
        )
        chain = Chain.objects.create(
            name="Ethereum Remote Withdrawal",
            code="eth-remote-withdrawal",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=403,
            rpc="http://localhost:8545",
            active=True,
        )
        chain.__dict__["w3"] = SimpleNamespace(eth=SimpleNamespace(gas_price=9))
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="remote-withdraw-submit",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            to=Web3.to_checksum_address("0x000000000000000000000000000000000000f002"),
            status=WithdrawalStatus.REVIEWING,
        )

        submitted = WithdrawalService.submit_withdrawal(withdrawal=withdrawal)

        submitted.refresh_from_db()
        self.assertEqual(submitted.status, WithdrawalStatus.PENDING)
        self.assertIsNone(submitted.hash)
        self.assertIsNotNone(submitted.broadcast_task_id)
        signer_backend.derive_address.assert_called_once()
        signer_backend.sign_evm_transaction.assert_not_called()


# ---------------------------------------------------------------------------
# 拒绝提币完整流程测试
# ---------------------------------------------------------------------------


class WithdrawalRejectTests(TestCase):
    """reject_withdrawal 的完整测试覆盖：状态流转、权限校验、日志记录。"""

    def setUp(self):
        self.owner = User.objects.create_superuser(
            username="reject-owner", password="secret"
        )
        wallet = Wallet.objects.create()
        self.project = Project.objects.create(name="RejectProject", wallet=wallet)
        self.crypto = Crypto.objects.create(
            name="Ethereum Reject",
            symbol="ETHRJ",
            coingecko_id="ethereum-reject",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Reject",
            code="eth-reject",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=401,
            rpc="http://localhost:8545",
            active=True,
        )

    def _build_approval_context(self):
        from users.otp import build_admin_approval_context

        return build_admin_approval_context(source="test_reject")

    @patch("withdrawals.service.WebhookService.create_event")
    def test_reject_withdrawal_sets_status_without_webhook(self, webhook_mock):
        """拒绝审核中提币后：状态改为 REJECTED，记录审核人，但不触发 Webhook。"""
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="reject-order-1",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.captureOnCommitCallbacks(execute=True):
            result = WithdrawalService.reject_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=self.owner,
                note="风控拒绝",
                approval_context=self._build_approval_context(),
            )

        result.refresh_from_db()
        self.assertEqual(result.status, WithdrawalStatus.REJECTED)
        self.assertEqual(result.reviewed_by, self.owner)
        self.assertIsNotNone(result.reviewed_at)
        webhook_mock.assert_not_called()

    @patch("withdrawals.service.WebhookService.create_event")
    def test_reject_withdrawal_writes_review_log(self, _webhook_mock):
        """拒绝操作必须留下完整审核日志。"""
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="reject-order-log",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("2.5"),
            worth=Decimal("50"),
            to="0x0000000000000000000000000000000000000022",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.captureOnCommitCallbacks(execute=True):
            WithdrawalService.reject_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=self.owner,
                note="金额异常",
                approval_context=self._build_approval_context(),
            )

        log = WithdrawalReviewLog.objects.get(withdrawal=withdrawal)
        self.assertEqual(log.actor, self.owner)
        self.assertEqual(log.action, WithdrawalReviewLog.Action.REJECTED)
        self.assertEqual(log.from_status, WithdrawalStatus.REVIEWING)
        self.assertEqual(log.to_status, WithdrawalStatus.REJECTED)
        self.assertEqual(log.note, "金额异常")
        # amount 通过 format_decimal_stripped 格式化，断言字符串包含数值即可
        self.assertIn("2.5", log.snapshot["amount"])

    def test_non_owner_cannot_reject_other_project_withdrawal(self):
        """提币改为商户自审后，非超管且非项目 owner 的后台用户不能拒绝他人项目提币。"""
        outsider = User.objects.create(username="reject-outsider")
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="reject-order-outsider",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000033",
            status=WithdrawalStatus.REVIEWING,
        )

        with self.assertRaises(PermissionError):
            WithdrawalService.reject_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=outsider,
                approval_context=self._build_approval_context(),
            )

    def test_reject_non_reviewing_raises_value_error(self):
        """只有审核中的提币才能被拒绝，其他状态应抛 ValueError。"""
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="reject-order-pending",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000044",
            status=WithdrawalStatus.PENDING,
        )

        with self.assertRaises(ValueError):
            WithdrawalService.reject_withdrawal(
                withdrawal_id=withdrawal.pk,
                reviewer=self.owner,
                approval_context=self._build_approval_context(),
            )

    def test_superuser_can_reject_withdrawal(self):
        """提币仅超管可审核，超管应可直接拒绝任意项目的提币。"""
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="reject-order-owner",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000055",
            status=WithdrawalStatus.REVIEWING,
        )

        result = WithdrawalService.reject_withdrawal(
            withdrawal_id=withdrawal.pk,
            reviewer=self.owner,
            approval_context=self._build_approval_context(),
        )

        result.refresh_from_db()
        self.assertEqual(result.status, WithdrawalStatus.REJECTED)
        self.assertEqual(result.reviewed_by, self.owner)


# ---------------------------------------------------------------------------
# 状态转换异常路径测试
# ---------------------------------------------------------------------------


class WithdrawalStateTransitionTests(TestCase):
    """覆盖 confirm_withdrawal / drop_withdrawal 对非法状态的保护。"""

    # 测试用 hash 计数器，确保每个 OnchainTransfer 拥有唯一且合法的 hash
    _hash_counter = 0

    def setUp(self):
        User.objects.bulk_create([User(username="state-merchant")])
        wallet = Wallet.objects.create()
        self.project = Project.objects.create(name="StateProject", wallet=wallet)
        self.crypto = Crypto.objects.create(
            name="Ethereum State",
            symbol="ETHS",
            coingecko_id="ethereum-state",
        )
        self.chain = Chain.objects.create(
            name="Ethereum State",
            code="eth-state",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=402,
            rpc="http://localhost:8545",
            active=True,
        )
        self.wallet = wallet

    def _next_hash(self):
        """生成唯一的合法 0x 前缀 64 位十六进制哈希。"""
        WithdrawalStateTransitionTests._hash_counter += 1
        return "0x" + hex(self._hash_counter)[2:].zfill(64)

    def _make_withdrawal_with_transfer(self, *, status, out_no):
        """创建带 transfer 的提币单，用于 confirm/drop 测试。"""
        tx_hash = self._next_hash()
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=1,
            hash=tx_hash,
            event_id=f"native:{out_no}",
            crypto=self.crypto,
            from_address="0x0000000000000000000000000000000000000001",
            to_address="0x0000000000000000000000000000000000000002",
            value=1,
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
        )
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no=out_no,
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=status,
            transfer=transfer,
        )
        return withdrawal, transfer

    # --- confirm_withdrawal 异常路径 ---

    def test_confirm_already_completed_is_idempotent(self):
        """已完成的提币收到重复确认应幂等跳过，不抛异常。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.COMPLETED, out_no="confirm-completed"
        )
        # 不应抛异常，静默跳过
        WithdrawalService.confirm_withdrawal(transfer)

    def test_confirm_pending_raises_value_error(self):
        """PENDING 状态的提币不能直接确认，必须先经过 CONFIRMING。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.PENDING, out_no="confirm-pending"
        )
        with self.assertRaises(ValueError):
            WithdrawalService.confirm_withdrawal(transfer)

    def test_confirm_rejected_raises_value_error(self):
        """已拒绝的提币不能确认。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.REJECTED, out_no="confirm-rejected"
        )
        with self.assertRaises(ValueError):
            WithdrawalService.confirm_withdrawal(transfer)

    @patch("withdrawals.service.WebhookService.create_event")
    def test_confirm_confirming_succeeds(self, webhook_mock):
        """CONFIRMING → COMPLETED 是正常路径，确认后触发 Webhook。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.CONFIRMING, out_no="confirm-ok"
        )
        with self.captureOnCommitCallbacks(execute=True):
            WithdrawalService.confirm_withdrawal(transfer)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)
        webhook_mock.assert_called_once()

    # --- drop_withdrawal 异常路径 ---

    def test_drop_already_rejected_is_idempotent(self):
        """已拒绝的提币收到重复 drop 应幂等跳过。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.REJECTED, out_no="drop-rejected"
        )
        # 不应抛异常
        WithdrawalService.drop_withdrawal(transfer)

    def test_drop_already_failed_is_idempotent(self):
        """已失败的提币收到 drop 应幂等跳过。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.FAILED, out_no="drop-failed"
        )
        WithdrawalService.drop_withdrawal(transfer)
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.FAILED)

    def test_drop_completed_is_idempotent(self):
        """已完成的提币收到 drop（链 reorg 场景）应幂等跳过，不抛异常。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.COMPLETED, out_no="drop-completed"
        )
        # 不应抛异常，应静默跳过
        WithdrawalService.drop_withdrawal(transfer)
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)

    def test_drop_pending_reverts_to_pending(self):
        """PENDING 状态的提币被 drop 后应保持 PENDING 并清除 transfer 关联。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.PENDING, out_no="drop-pending"
        )
        WithdrawalService.drop_withdrawal(transfer)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer_id)

    def test_drop_confirming_reverts_to_pending(self):
        """CONFIRMING 状态的提币被 drop 后应回退到 PENDING 并清除 transfer 关联。"""
        withdrawal, transfer = self._make_withdrawal_with_transfer(
            status=WithdrawalStatus.CONFIRMING, out_no="drop-confirming"
        )
        WithdrawalService.drop_withdrawal(transfer)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer_id)

    def test_drop_withdrawal_does_not_finalize_broadcast_task(self):
        """drop_withdrawal 不应修改 BroadcastTask 状态, 由 OnchainTransfer.drop() 统一管理。"""
        tx_hash = self._next_hash()
        addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x0000000000000000000000000000000000000099",
        )
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=1,
            hash=tx_hash,
            event_id="native:drop-task",
            crypto=self.crypto,
            from_address="0x0000000000000000000000000000000000000001",
            to_address="0x0000000000000000000000000000000000000002",
            value=1,
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
        )
        broadcast_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
        )
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="drop-with-task",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=WithdrawalStatus.PENDING,
            transfer=transfer,
            broadcast_task=broadcast_task,
        )

        WithdrawalService.drop_withdrawal(transfer)

        # Withdrawal 回退到 PENDING，清除 transfer 关联
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer_id)

        # BroadcastTask 状态不应被 drop_withdrawal 修改, 保持原状
        broadcast_task.refresh_from_db()
        self.assertEqual(broadcast_task.stage, BroadcastTaskStage.PENDING_CONFIRM)
        self.assertEqual(broadcast_task.result, BroadcastTaskResult.UNKNOWN)
        self.assertEqual(broadcast_task.failure_reason, "")

    # --- fail_withdrawal 测试 ---

    def _make_withdrawal_with_broadcast_task(self, *, status, out_no):
        """创建带 broadcast_task 的提币单，用于 fail_withdrawal 测试。"""
        tx_hash = self._next_hash()
        addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address="0x00000000000000000000000000000000000000A1",
        )
        broadcast_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.FAILED,
            failure_reason=BroadcastTaskFailureReason.RPC_REJECTED,
        )
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no=out_no,
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=status,
            broadcast_task=broadcast_task,
        )
        return withdrawal, broadcast_task

    @patch("withdrawals.service.WebhookService.create_event")
    def test_fail_pending_sets_failed(self, webhook_mock):
        """PENDING 状态的提币在 BroadcastTask 确认失败后应终局为 FAILED，且不发 Webhook。"""
        withdrawal, broadcast_task = self._make_withdrawal_with_broadcast_task(
            status=WithdrawalStatus.PENDING, out_no="fail-pending"
        )
        with self.captureOnCommitCallbacks(execute=True):
            WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.FAILED)
        self.assertIsNone(withdrawal.transfer_id)
        webhook_mock.assert_not_called()

    @patch("withdrawals.service.WebhookService.create_event")
    def test_fail_confirming_sets_failed(self, webhook_mock):
        """CONFIRMING 状态的提币在 BroadcastTask 确认失败后应终局为 FAILED，且不发 Webhook。"""
        withdrawal, broadcast_task = self._make_withdrawal_with_broadcast_task(
            status=WithdrawalStatus.CONFIRMING, out_no="fail-confirming"
        )
        with self.captureOnCommitCallbacks(execute=True):
            WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)

        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.FAILED)
        webhook_mock.assert_not_called()

    def test_fail_completed_is_idempotent(self):
        """已完成的提币收到 fail 应幂等跳过。"""
        withdrawal, broadcast_task = self._make_withdrawal_with_broadcast_task(
            status=WithdrawalStatus.COMPLETED, out_no="fail-completed"
        )
        WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)

    def test_fail_already_failed_is_idempotent(self):
        """已失败的提币收到重复 fail 应幂等跳过。"""
        withdrawal, broadcast_task = self._make_withdrawal_with_broadcast_task(
            status=WithdrawalStatus.FAILED, out_no="fail-already-failed"
        )
        WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.FAILED)

    def test_fail_rejected_is_idempotent(self):
        """已审核拒绝的提币收到 fail 应幂等跳过。"""
        withdrawal, broadcast_task = self._make_withdrawal_with_broadcast_task(
            status=WithdrawalStatus.REJECTED, out_no="fail-rejected"
        )
        WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.REJECTED)

    def test_fail_no_matching_withdrawal_is_noop(self):
        """BroadcastTask 无对应提币时 fail_withdrawal 应静默跳过。"""
        tx_hash = self._next_hash()
        addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=2,
            address_index=0,
            address="0x00000000000000000000000000000000000000A2",
        )
        broadcast_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.FAILED,
            failure_reason=BroadcastTaskFailureReason.RPC_REJECTED,
        )
        # 不应抛异常
        WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)

    def test_fail_reviewing_raises_value_error(self):
        """REVIEWING 状态的提币不应通过 fail_withdrawal 处理。"""
        withdrawal, broadcast_task = self._make_withdrawal_with_broadcast_task(
            status=WithdrawalStatus.REVIEWING, out_no="fail-reviewing"
        )
        with self.assertRaises(ValueError):
            WithdrawalService.fail_withdrawal(broadcast_task=broadcast_task)


# ---------------------------------------------------------------------------
# try_match_withdrawal 异常分支测试
# ---------------------------------------------------------------------------


class WithdrawalTryMatchTests(TestCase):
    """覆盖 try_match_withdrawal 的异常分支：链不匹配、非 PENDING 状态、无匹配提币。"""

    _hash_counter = 1000

    def setUp(self):
        User.objects.bulk_create([User(username="match-merchant")])
        self.wallet = Wallet.objects.create()
        self.project = Project.objects.create(name="MatchProject", wallet=self.wallet)
        self.crypto = Crypto.objects.create(
            name="Ethereum Match",
            symbol="ETHM",
            coingecko_id="ethereum-match",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Match",
            code="eth-match",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=403,
            rpc="http://localhost:8545",
            active=True,
        )
        self.other_chain = Chain.objects.create(
            name="BSC Match",
            code="bsc-match",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=56,
            rpc="http://localhost:8545",
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=1,
            address="0x0000000000000000000000000000000000000098",
        )

    def _next_hash(self):
        WithdrawalTryMatchTests._hash_counter += 1
        return "0x" + hex(self._hash_counter)[2:].zfill(64)

    def _make_transfer(self, *, chain=None, tx_hash=None, event_id="native:0"):
        """创建完整的 OnchainTransfer 对象。"""
        return OnchainTransfer.objects.create(
            chain=chain or self.chain,
            block=1,
            hash=tx_hash or self._next_hash(),
            event_id=event_id,
            crypto=self.crypto,
            from_address=self.addr.address,
            to_address="0x0000000000000000000000000000000000000002",
            value=1,
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
        )

    def _make_broadcast_task(self, *, tx_hash, stage=BroadcastTaskStage.PENDING_CHAIN):
        """创建完整的 BroadcastTask 对象。"""
        recipient = "0x0000000000000000000000000000000000000002"
        broadcast_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=self.addr,
            transfer_type=TransferType.Withdrawal,
            crypto=self.crypto,
            recipient=recipient,
            amount=Decimal("1"),
            tx_hash=tx_hash,
            stage=stage,
            result=BroadcastTaskResult.UNKNOWN,
        )
        EvmBroadcastTask.objects.create(
            base_task=broadcast_task,
            address=self.addr,
            chain=self.chain,
            nonce=0,
            to=recipient,
            value=1,
            gas=21_000,
        )
        return broadcast_task

    def test_match_returns_false_when_no_withdrawal_found(self):
        """链上转账没有对应提币单时应返回 False。"""
        broadcast_task = self._make_broadcast_task(tx_hash=self._next_hash())
        transfer = self._make_transfer(event_id="native:no-match")
        result = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)
        self.assertFalse(result)

    def test_match_returns_false_when_chain_mismatch(self):
        """链上转账的链与提币单的链不一致时应返回 False，记录 warning。"""
        tx_hash = self._next_hash()
        broadcast_task = self._make_broadcast_task(tx_hash=tx_hash)
        Withdrawal.objects.create(
            project=self.project,
            out_no="match-chain-mismatch",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=WithdrawalStatus.PENDING,
            broadcast_task=broadcast_task,
            hash=tx_hash,
        )
        # 链上转账来自另一条链
        transfer = self._make_transfer(
            chain=self.other_chain, tx_hash=tx_hash, event_id="native:mismatch"
        )

        result = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)
        self.assertFalse(result)

    def test_match_returns_false_when_not_pending(self):
        """非 PENDING 状态的提币收到重复匹配事件应静默跳过。"""
        tx_hash = self._next_hash()
        broadcast_task = self._make_broadcast_task(
            tx_hash=tx_hash, stage=BroadcastTaskStage.PENDING_CONFIRM
        )
        transfer = self._make_transfer(tx_hash=tx_hash, event_id="native:not-pending")
        Withdrawal.objects.create(
            project=self.project,
            out_no="match-not-pending",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=WithdrawalStatus.CONFIRMING,
            broadcast_task=broadcast_task,
            hash=tx_hash,
            transfer=transfer,
        )

        result = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)
        self.assertFalse(result)

    @patch("chains.tasks.process_transfer.apply_async")
    def test_match_success_sets_confirming_and_updates_task(self, _process_mock):
        """正常匹配成功时：PENDING → CONFIRMING，BroadcastTask 更新为 PENDING_CONFIRM。"""
        tx_hash = self._next_hash()
        broadcast_task = self._make_broadcast_task(tx_hash=tx_hash)
        Withdrawal.objects.create(
            project=self.project,
            out_no="match-success",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=WithdrawalStatus.PENDING,
            broadcast_task=broadcast_task,
            hash=tx_hash,
        )
        transfer = self._make_transfer(tx_hash=tx_hash, event_id="native:success")

        result = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)
        self.assertTrue(result)

        withdrawal = Withdrawal.objects.get(out_no="match-success")
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)
        self.assertEqual(withdrawal.transfer, transfer)

        broadcast_task.refresh_from_db()
        self.assertEqual(broadcast_task.stage, BroadcastTaskStage.PENDING_CONFIRM)

        transfer.refresh_from_db()
        self.assertEqual(transfer.type, TransferType.Withdrawal)

    @patch("chains.tasks.process_transfer.apply_async")
    def test_match_backfills_chain_when_null(self, _process_mock):
        """提币单 chain 为 None 时，匹配成功后应从 transfer 回填链信息。"""
        tx_hash = self._next_hash()
        broadcast_task = self._make_broadcast_task(tx_hash=tx_hash)
        Withdrawal.objects.create(
            project=self.project,
            out_no="match-backfill-chain",
            chain=None,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=WithdrawalStatus.PENDING,
            broadcast_task=broadcast_task,
            hash=tx_hash,
        )
        transfer = self._make_transfer(tx_hash=tx_hash, event_id="native:backfill")

        result = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)
        self.assertTrue(result)

        withdrawal = Withdrawal.objects.get(out_no="match-backfill-chain")
        self.assertEqual(withdrawal.chain, self.chain)

    @patch("chains.tasks.process_transfer.apply_async")
    def test_match_success_with_old_tx_hash_history(self, _process_mock):
        old_hash = self._next_hash()
        broadcast_task = self._make_broadcast_task(tx_hash=old_hash)
        broadcast_task.append_tx_hash(old_hash)
        broadcast_task.append_tx_hash(self._next_hash())
        Withdrawal.objects.create(
            project=self.project,
            out_no="match-old-hash",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000002",
            status=WithdrawalStatus.PENDING,
            broadcast_task=broadcast_task,
            hash=old_hash,
        )
        transfer = self._make_transfer(tx_hash=old_hash, event_id="native:old-hash")

        result = WithdrawalService.try_match_withdrawal(transfer, broadcast_task)

        self.assertTrue(result)
        withdrawal = Withdrawal.objects.get(out_no="match-old-hash")
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)
        broadcast_task.refresh_from_db()
        self.assertEqual(broadcast_task.tx_hash, old_hash)


# ---------------------------------------------------------------------------
# 余额计算与额度边界测试
# ---------------------------------------------------------------------------


class WithdrawalBalanceAndPolicyEdgeCaseTests(TestCase):
    """覆盖 has_sufficient_balance 原生币路径和日额度边界值。"""

    def test_native_coin_balance_deducts_amount_plus_gas(self):
        """原生币提币：可用余额 = 链上余额 - 在途金额 - 在途gas，需同时覆盖转出金额和当前fee。"""
        native = type(
            "NativeStub",
            (),
            {"is_native": True, "get_decimals": staticmethod(lambda _chain: 0)},
        )()
        chain = type(
            "ChainStub",
            (),
            {"type": ChainType.EVM, "code": "eth-native-bal", "native_coin": native},
        )()
        adapter = type(
            "AdapterStub",
            (),
            # 链上余额 100
            {"get_balance": staticmethod(lambda _addr, _chain, _crypto: 100)},
        )()

        with (
            # 在途金额 30
            patch.object(WithdrawalService, "pending_amount_raw", return_value=30),
            # 在途 gas 20
            patch.object(
                WithdrawalService, "pending_gas_reserved_raw", return_value=20
            ),
            # 当前 fee 10
            patch.object(
                WithdrawalService, "estimate_current_network_fee_raw", return_value=10
            ),
        ):
            # 可用 = 100 - 30 - 20 = 50，需要 amount(40) + fee(10) = 50，刚好够
            enough = WithdrawalService.has_sufficient_balance(
                project=object(),
                chain=chain,
                crypto=native,
                address="0x00",
                amount=Decimal("40"),
                adapter=adapter,
            )
            self.assertTrue(enough)

            # 多一个 wei 就不够
            enough2 = WithdrawalService.has_sufficient_balance(
                project=object(),
                chain=chain,
                crypto=native,
                address="0x00",
                amount=Decimal("41"),
                adapter=adapter,
            )
            self.assertFalse(enough2)

    def test_zero_amount_returns_false(self):
        """金额为 0 时直接返回 False，无需访问链上余额。"""
        native = type(
            "NativeStub",
            (),
            {"is_native": True, "get_decimals": staticmethod(lambda _chain: 0)},
        )()
        chain = type(
            "ChainStub",
            (),
            {"type": ChainType.EVM, "code": "eth-zero", "native_coin": native},
        )()
        adapter = type(
            "AdapterStub",
            (),
            {"get_balance": staticmethod(lambda _addr, _chain, _crypto: 1000)},
        )()

        result = WithdrawalService.has_sufficient_balance(
            project=object(),
            chain=chain,
            crypto=native,
            address="0x00",
            amount=Decimal("0"),
            adapter=adapter,
        )
        self.assertFalse(result)

    def test_daily_limit_exactly_at_boundary(self):
        """当日已用额度 + 本笔恰好等于日限额时，应通过校验。"""
        User.objects.bulk_create([User(username="policy-edge-user")])
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="PolicyEdge",
            wallet=wallet,
            withdrawal_daily_limit=Decimal("100"),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Edge",
            symbol="ETHED",
            coingecko_id="ethereum-edge",
        )
        chain = Chain.objects.create(
            name="Ethereum Edge",
            code="eth-edge",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=404,
            rpc="http://localhost:8545",
            active=True,
        )
        # 已有 70 USD 的提币
        Withdrawal.objects.create(
            project=project,
            out_no="edge-existing",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            worth=Decimal("70"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.PENDING,
        )

        # 本笔 30 USD → 总计 100 = 刚好等于限额，应通过
        with patch.object(
            WithdrawalService, "estimate_withdrawal_worth", return_value=Decimal("30")
        ):
            worth = WithdrawalService.assert_project_policy(
                project=project,
                chain=chain,
                crypto=crypto,
                to="0x0000000000000000000000000000000000000022",
                amount=Decimal("1"),
            )
            self.assertEqual(worth, Decimal("30"))

    def test_daily_limit_one_cent_over_rejected(self):
        """当日已用额度 + 本笔超过日限额 1 美分时，应被拒绝。"""
        User.objects.bulk_create([User(username="policy-over-user")])
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="PolicyOver",
            wallet=wallet,
            withdrawal_daily_limit=Decimal("100"),
        )
        crypto = Crypto.objects.create(
            name="Ethereum Over",
            symbol="ETHO",
            coingecko_id="ethereum-over",
        )
        chain = Chain.objects.create(
            name="Ethereum Over",
            code="eth-over",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=405,
            rpc="http://localhost:8545",
            active=True,
        )
        Withdrawal.objects.create(
            project=project,
            out_no="over-existing",
            chain=chain,
            crypto=crypto,
            amount=Decimal("1"),
            worth=Decimal("70"),
            to="0x0000000000000000000000000000000000000011",
            status=WithdrawalStatus.REVIEWING,
        )

        # 本笔 30.01 → 总计 100.01 > 100
        with patch.object(
            WithdrawalService,
            "estimate_withdrawal_worth",
            return_value=Decimal("30.01"),
        ):
            with self.assertRaises(APIError) as ctx:
                WithdrawalService.assert_project_policy(
                    project=project,
                    chain=chain,
                    crypto=crypto,
                    to="0x0000000000000000000000000000000000000022",
                    amount=Decimal("1"),
                )
            self.assertEqual(
                ctx.exception.detail["code"],
                ErrorCode.WITHDRAWAL_DAILY_LIMIT_EXCEEDED.code,
            )

    def test_single_limit_exactly_at_boundary_passes(self):
        """单笔限额恰好等于 worth 时不超限（worth > limit 才拒绝）。"""
        User.objects.bulk_create([User(username="single-edge-user")])
        wallet = Wallet.objects.create()
        project = Project.objects.create(
            name="SingleEdge",
            wallet=wallet,
            withdrawal_single_limit=Decimal("100"),
        )
        crypto = Crypto.objects.create(
            name="Ethereum SingleEdge",
            symbol="ETHSE",
            coingecko_id="ethereum-single-edge",
        )
        chain = Chain.objects.create(
            name="Ethereum SingleEdge",
            code="eth-single-edge",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=406,
            rpc="http://localhost:8545",
            active=True,
        )

        with patch.object(
            WithdrawalService, "estimate_withdrawal_worth", return_value=Decimal("100")
        ):
            # worth == limit → 不应被拒绝（代码判断 worth > limit 才拒绝）
            worth = WithdrawalService.assert_project_policy(
                project=project,
                chain=chain,
                crypto=crypto,
                to="0x0000000000000000000000000000000000000022",
                amount=Decimal("1"),
            )
            self.assertEqual(worth, Decimal("100"))


# ---------------------------------------------------------------------------
# should_require_review 各分支测试
# ---------------------------------------------------------------------------


class WithdrawalShouldRequireReviewTests(TestCase):
    """覆盖 should_require_review 的所有条件分支。"""

    def _make_project(self, **kwargs):
        return type("ProjectStub", (), kwargs)()

    def test_review_not_required_always_returns_false(self):
        """审核开关关闭时，无论 worth 多大都不需要审核。"""
        project = self._make_project(withdrawal_review_required=False)
        self.assertFalse(
            WithdrawalService.should_require_review(
                project=project, worth=Decimal("999999")
            )
        )

    def test_review_required_no_exempt_limit(self):
        """审核开关开启，无免审核门槛时，所有提币都需要审核。"""
        project = self._make_project(
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=None,
        )
        self.assertTrue(
            WithdrawalService.should_require_review(project=project, worth=Decimal("1"))
        )

    def test_review_required_exempt_limit_zero(self):
        """免审核门槛为 0 时等同于未配置，所有提币都需要审核。"""
        project = self._make_project(
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("0"),
        )
        self.assertTrue(
            WithdrawalService.should_require_review(project=project, worth=Decimal("1"))
        )

    def test_worth_below_exempt_skips_review(self):
        """worth 严格小于免审核门槛时，跳过审核。"""
        project = self._make_project(
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("50"),
        )
        self.assertFalse(
            WithdrawalService.should_require_review(
                project=project, worth=Decimal("49.99")
            )
        )

    def test_worth_equals_exempt_requires_review(self):
        """worth 恰好等于免审核门槛时，仍需审核（严格小于才免审核）。"""
        project = self._make_project(
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("50"),
        )
        self.assertTrue(
            WithdrawalService.should_require_review(
                project=project, worth=Decimal("50")
            )
        )

    def test_worth_above_exempt_requires_review(self):
        """worth 超过免审核门槛时，需要审核。"""
        project = self._make_project(
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("50"),
        )
        self.assertTrue(
            WithdrawalService.should_require_review(
                project=project, worth=Decimal("50.01")
            )
        )

    def test_worth_zero_below_exempt_skips_review(self):
        """worth 为 0 但门槛 > 0 时，不需要审核。"""
        project = self._make_project(
            withdrawal_review_required=True,
            withdrawal_review_exempt_limit=Decimal("50"),
        )
        self.assertFalse(
            WithdrawalService.should_require_review(project=project, worth=Decimal("0"))
        )


class WithdrawalCreatePermissionCheckTests(TestCase):
    """v2 SaaS 模式：提币创建入口调用 check_saas_permission。"""

    def setUp(self):
        self.wallet = Wallet.objects.create()
        self.project = Project.objects.create(
            name="PermissionCheckProject",
            wallet=self.wallet,
        )
        self.crypto = Crypto.objects.create(
            name="Ethereum PermCheck",
            symbol="ETHPC",
            coingecko_id="ethereum-permcheck",
        )
        self.chain = Chain.objects.create(
            name="Ethereum PermCheck",
            code="eth-permcheck",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=9901,
            rpc="http://localhost:8545",
            active=True,
        )

    def _make_request(self):
        return APIRequestFactory().post(
            "/v1/withdrawal",
            {},
            format="json",
            HTTP_XC_APPID=self.project.appid,
        )

    def _make_serializer_stub(self):
        return SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "perm-order",
                "to": "0x0000000000000000000000000000000000000099",
                "uid": None,
                "crypto": self.crypto.symbol,
                "chain": self.chain.code,
                "amount": Decimal("1"),
            },
            errors={},
        )

    @patch("withdrawals.viewsets.check_saas_permission")
    def test_create_calls_permission_check_with_correct_args(self, mock_check):
        """提币创建时必须以正确的 appid 和 action='withdrawal' 调用权限校验。"""
        serializer_stub = self._make_serializer_stub()
        select_for_update_manager = Mock()
        select_for_update_manager.get.return_value = self.project

        with (
            patch("withdrawals.viewsets.Project.retrieve", return_value=self.project),
            patch(
                "withdrawals.viewsets.Project.objects.select_for_update",
                return_value=select_for_update_manager,
            ),
            patch(
                "withdrawals.viewsets.CreateWithdrawalSerializer",
                return_value=serializer_stub,
            ),
            patch(
                "withdrawals.viewsets.WithdrawalService.assert_project_policy",
                return_value=Decimal("0"),
            ),
            patch("withdrawals.viewsets.WithdrawalService.submit_withdrawal"),
        ):
            WithdrawalViewSet.as_view({"post": "create"})(self._make_request())

        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="withdrawal",
        )

    @patch("withdrawals.viewsets.check_saas_permission")
    def test_create_blocked_when_feature_not_enabled(self, mock_check):
        """check_saas_permission 抛出 APIError 时，提币创建应返回 403。"""
        from common.error_codes import ErrorCode
        from common.exceptions import APIError

        mock_check.side_effect = APIError(ErrorCode.FEATURE_NOT_ENABLED, detail="withdrawal")

        response = WithdrawalViewSet.as_view({"post": "create"})(self._make_request())

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["code"], ErrorCode.FEATURE_NOT_ENABLED.code)

    @patch("withdrawals.viewsets.check_saas_permission")
    def test_create_blocked_when_account_frozen(self, mock_check):
        """账户冻结时，提币创建应返回 403。"""
        from common.error_codes import ErrorCode
        from common.exceptions import APIError

        mock_check.side_effect = APIError(ErrorCode.ACCOUNT_FROZEN)

        response = WithdrawalViewSet.as_view({"post": "create"})(self._make_request())

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["code"], ErrorCode.ACCOUNT_FROZEN.code)
