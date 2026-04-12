from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.contrib.sessions.middleware import SessionMiddleware
from django.core.cache import cache
from django.core.exceptions import PermissionDenied
from django.test import RequestFactory
from django.test import TestCase
from django.utils import timezone
from django_otp.oath import TOTP
from django_otp.plugins.otp_totp.models import TOTPDevice

from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.models import Wallet
from chains.signer import SignerAdminSummary
from chains.signer import SignerServiceError
from core.dashboard import signer_overview_view
from core.dashboard_metrics import build_dashboard_metrics
from core.models import PLATFORM_SETTINGS_CACHE_KEY
from core.models import PlatformSettings
from core.monitoring import OperationalRiskService
from core.tasks import scan_operational_risks
from currencies.models import Crypto
from deposits.models import Deposit
from deposits.models import DepositCollection
from deposits.models import DepositStatus
from users.models import Customer
from users.models import User
from users.otp import ADMIN_OTP_VERIFIED_AT_SESSION_KEY
from webhooks.models import WebhookEvent
from withdrawals.models import Withdrawal
from withdrawals.models import WithdrawalStatus


class OperationalRiskServiceTests(TestCase):
    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        super().tearDown()

    def setUp(self):
        from projects.models import Project

        self.project = Project.objects.create(
            name="Monitor Project",
            wallet=Wallet.objects.create(),
        )
        self.customer = Customer.objects.create(
            project=self.project, uid="monitor-customer"
        )
        self.crypto = Crypto.objects.create(
            name="Ethereum Monitor",
            symbol="ETHM",
            coingecko_id="ethereum-monitor",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Monitor",
            code="eth-monitor",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=401,
            rpc="http://localhost:8545",
            active=True,
        )

    def test_build_summary_collects_stalled_withdrawal_deposit_and_webhook(self):
        # 巡检需要把提币卡单、归集卡单和回调超时统一收口成一份运营概览。
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=1,
            hash="0x" + "1" * 64,
            event_id="monitor:0",
            crypto=self.crypto,
            from_address="0x0000000000000000000000000000000000000011",
            to_address="0x0000000000000000000000000000000000000012",
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
        )
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="monitor-withdrawal",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000012",
            status=WithdrawalStatus.REVIEWING,
        )
        collection = DepositCollection.objects.create(collection_hash="0x" + "2" * 64)
        Deposit.objects.create(
            customer=self.customer,
            transfer=transfer,
            status=DepositStatus.COMPLETED,
            collection=collection,
        )
        WebhookEvent.objects.create(
            project=self.project,
            payload={"type": "withdrawal", "data": {"sys_no": "x"}},
            status=WebhookEvent.Status.PENDING,
        )

        old_time = timezone.now() - timedelta(hours=1)
        Withdrawal.objects.filter(pk=withdrawal.pk).update(updated_at=old_time)
        # stalled 判断基于 DepositCollection.updated_at，需更新归集记录的时间戳。
        DepositCollection.objects.filter(pk=collection.pk).update(updated_at=old_time)
        WebhookEvent.objects.filter(project=self.project).update(created_at=old_time)

        summary = OperationalRiskService.build_summary()

        self.assertEqual(summary["reviewing_withdrawal_count"], 1)
        self.assertEqual(summary["stalled_withdrawal_count"], 1)
        self.assertEqual(summary["stalled_deposit_collection_count"], 1)
        self.assertEqual(summary["stalled_webhook_event_count"], 1)
        self.assertEqual(len(summary["recent_stalled_withdrawals"]), 1)
        self.assertEqual(len(summary["recent_stalled_deposit_collections"]), 1)
        self.assertEqual(len(summary["recent_stalled_webhook_events"]), 1)

    def test_build_summary_uses_platform_timeout_override(self):
        # 平台参数中心调低巡检阈值后，后台摘要必须立即采用新阈值，而不是继续使用硬编码分钟数。
        PlatformSettings.objects.create(
            reviewing_withdrawal_timeout_minutes=5,
            pending_withdrawal_timeout_minutes=5,
            confirming_withdrawal_timeout_minutes=5,
            deposit_collection_timeout_minutes=5,
            webhook_event_timeout_minutes=5,
        )
        withdrawal = Withdrawal.objects.create(
            project=self.project,
            out_no="monitor-withdrawal-override",
            chain=self.chain,
            crypto=self.crypto,
            amount=Decimal("1"),
            to="0x0000000000000000000000000000000000000013",
            status=WithdrawalStatus.REVIEWING,
        )
        Withdrawal.objects.filter(pk=withdrawal.pk).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        summary = OperationalRiskService.build_summary()

        self.assertEqual(summary["reviewing_withdrawal_count"], 1)


class OperationalRiskTaskTests(TestCase):
    @patch("core.tasks.logger.warning")
    @patch("core.tasks.TelegramAlertService.sync_operational_alerts")
    @patch("core.tasks.OperationalRiskService.build_summary")
    def test_scan_operational_risks_logs_warning_when_any_risk_exists(
        self,
        build_summary_mock,
        sync_operational_alerts_mock,
        warning_mock,
    ):
        # 巡检任务发现卡单后必须输出结构化告警，便于后续接入外部通知通道。
        build_summary_mock.return_value = {
            "reviewing_withdrawal_count": 1,
            "pending_withdrawal_count": 1,
            "confirming_withdrawal_count": 0,
            "stalled_withdrawal_count": 2,
            "stalled_deposit_collection_count": 1,
            "stalled_webhook_event_count": 1,
            "recent_stalled_withdrawals": [],
            "recent_stalled_deposit_collections": [],
            "recent_stalled_webhook_events": [],
        }

        scan_operational_risks()

        sync_operational_alerts_mock.assert_called_once()
        warning_mock.assert_called_once()


class DashboardSignerSummaryTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.superuser = User.objects.create_superuser(
            username="dashboard-admin",
            password="secret",
        )

    def _current_token(self, device: TOTPDevice) -> str:
        # Signer OTP 弹窗沿用同一套 TOTP 校验，测试里直接按设备当前参数生成一次有效验证码。
        return str(
            TOTP(
                device.bin_key, device.step, device.t0, device.digits, device.drift
            ).token()
        ).zfill(device.digits)

    def _attach_verified_admin_session(self, request, *, user, verified_at=None):
        device = TOTPDevice.objects.create(user=user, name=f"{user.username}-totp")
        SessionMiddleware(lambda req: None).process_request(request)
        request.session["otp_device_id"] = device.persistent_id
        request.session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = (
            verified_at or timezone.now()
        ).isoformat()
        request.session.save()
        request.user = user
        request.user.otp_device = device
        return request

    def _inspection_metrics(self, signer_summary=None):
        # 改动原因：独立巡检页测试只需覆盖页面装配，不需要重复构造整套业务数据。
        return {
            "snapshot": {
                "confirming_count": 2,
                "expiring_soon_count": 1,
                "reviewing_withdrawal_count": 3,
                "stalled_withdrawal_count": 1,
                "stalled_deposit_collection_count": 1,
                "pending_events_count": 4,
                "failed_events_count": 2,
                "stalled_webhook_event_count": 1,
            },
            "recent_failed_attempts": [],
            "recent_stalled_invoices": [],
            "recent_stalled_withdrawals": [],
            "recent_stalled_deposit_collections": [],
            "recent_stalled_webhook_events": [],
            "signer_summary": signer_summary,
        }

    @patch("core.dashboard_metrics.get_signer_backend")
    def test_superuser_dashboard_includes_signer_summary(self, get_signer_backend_mock):
        # signer 摘要属于系统级观测信息，只应在超管首页中显示。
        signer_backend = get_signer_backend_mock.return_value
        signer_backend.fetch_admin_summary.return_value = SignerAdminSummary(
            health={
                "database": True,
                "cache": True,
                "signer_shared_secret": True,
                "healthy": True,
            },
            wallets={"total": 2, "active": 1, "frozen": 1},
            requests_last_hour={
                "total": 10,
                "succeeded": 8,
                "failed": 1,
                "rate_limited": 1,
            },
            recent_anomalies=[],
        )

        metrics = build_dashboard_metrics()

        self.assertTrue(metrics["signer_summary"]["available"])
        self.assertEqual(metrics["signer_summary"]["wallets"]["frozen"], 1)

    @patch("core.dashboard_metrics.get_signer_backend")
    def test_superuser_dashboard_downgrades_signer_failure(
        self, get_signer_backend_mock
    ):
        signer_backend = get_signer_backend_mock.return_value
        signer_backend.fetch_admin_summary.side_effect = SignerServiceError(
            "signer down"
        )

        metrics = build_dashboard_metrics()

        self.assertFalse(metrics["signer_summary"]["available"])
        self.assertEqual(metrics["signer_summary"]["detail"], "signer down")

    def test_signer_overview_view_rejects_non_superuser(self):
        staff_user = User.objects.create_user(
            username="staff-only",
            password="secret",
            is_staff=True,
        )
        request = self.factory.get("/signer/overview/")
        request.user = staff_user

        with self.assertRaises(PermissionDenied):
            signer_overview_view(request)

    def test_signer_overview_view_renders_otp_modal_when_otp_expired(self):
        request = self.factory.get("/signer/overview/")
        request = self._attach_verified_admin_session(
            request,
            user=self.superuser,
            verified_at=timezone.now() - timedelta(minutes=16),
        )

        response = signer_overview_view(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            request.session["admin_otp_pending_user_id"], self.superuser.pk
        )
        self.assertEqual(request.session["admin_otp_next_path"], "/signer/overview/")

    def test_signer_overview_view_accepts_modal_otp_submission(self):
        device = TOTPDevice.objects.create(
            user=self.superuser, name="dashboard-admin-modal"
        )
        request = self.factory.post(
            "/signer/overview/",
            {"token": self._current_token(device)},
        )
        SessionMiddleware(lambda req: None).process_request(request)
        request.user = self.superuser
        request.user.otp_device = device
        request.session["admin_otp_pending_user_id"] = self.superuser.pk
        request.session["admin_otp_next_path"] = "/signer/overview/"
        request.session.save()

        response = signer_overview_view(request)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/signer/overview/")
        self.assertTrue(request.session.get("otp_device_id"))
        self.assertTrue(request.session.get(ADMIN_OTP_VERIFIED_AT_SESSION_KEY))
