from __future__ import annotations

import time
from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

import httpx
from django.core.cache import cache
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone
from risk.clients import MistTrackRiskResult
from risk.clients import QuicknodeMistTrackClient
from risk.models import RiskAssessment
from risk.models import RiskAssessmentStatus
from risk.models import RiskLevel
from risk.models import RiskSource
from risk.service import RiskMarkingService

from chains.models import Address
from chains.models import AddressUsage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from chains.models import Wallet
from core.models import PlatformSettings
from currencies.models import Crypto
from currencies.models import Fiat
from deposits.models import Deposit
from deposits.models import DepositAddress
from deposits.service import DepositService
from invoices.models import Invoice
from invoices.models import InvoicePaySlot
from invoices.models import InvoiceStatus
from invoices.service import InvoiceService
from projects.models import Project
from users.models import Customer


class RiskTestMixin:
    def setUp(self):
        cache.clear()
        Fiat.objects.get_or_create(code="USD")
        self.native = Crypto.objects.create(
            name="Ethereum",
            symbol="ETH",
            prices={"USD": "2000"},
            coingecko_id="risk-eth",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Mainnet",
            code="ethereum-mainnet",
            type=ChainType.EVM,
            native_coin=self.native,
            chain_id=1,
            rpc="http://eth.local",
            active=True,
        )
        self.wallet = Wallet.objects.create()
        self.project = Project.objects.create(name="Risk Project", wallet=self.wallet)
        self.customer = Customer.objects.create(project=self.project, uid="u-1")
        self.transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=100,
            block_hash="0x" + "ab" * 32,
            hash="0x" + "cd" * 32,
            crypto=self.native,
            from_address="0x1111111111111111111111111111111111111111",
            to_address="0x2222222222222222222222222222222222222222",
            value=10**18,
            amount=Decimal("1"),
            type=TransferType.Invoice,
            timestamp=1_700_000_000,
            datetime=timezone.now(),
        )
        self.platform_settings = PlatformSettings.objects.create(
            risk_marking_enabled=True,
            risk_marking_threshold_usd=Decimal("100"),
            risk_marking_cache_seconds=300,
            risk_marking_force_refresh_threshold_usd=Decimal("10000"),
            quicknode_misttrack_endpoint_url="https://quicknode.example",
        )

    def make_invoice(self, *, worth: Decimal = Decimal("500")) -> Invoice:
        return Invoice.objects.create(
            project=self.project,
            out_no=f"INV-{worth}",
            title="Risk invoice",
            currency="USD",
            amount=worth,
            methods={"ETH": ["ethereum-mainnet"]},
            crypto=self.native,
            chain=self.chain,
            pay_amount=Decimal("1"),
            pay_address=self.transfer.to_address,
            worth=worth,
            transfer=self.transfer,
            status=InvoiceStatus.CONFIRMING,
            expires_at=timezone.now() + timedelta(minutes=10),
        )

    def make_deposit(self, *, worth: Decimal = Decimal("50")) -> Deposit:
        self.transfer.type = TransferType.Deposit
        self.transfer.save(update_fields=["type"])
        return Deposit.objects.create(
            customer=self.customer,
            transfer=self.transfer,
            worth=worth,
        )


class QuicknodeMistTrackClientTests(SimpleTestCase):
    @patch("risk.clients.httpx.post")
    def test_address_risk_score_posts_json_rpc_payload(self, httpx_post):
        response = httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "risk_level": "High",
                    "score": 88,
                    "detail_list": ["Sanctioned entity"],
                    "risk_detail": {"sanction": 1},
                    "risk_report_url": "https://report.example",
                },
            },
            request=httpx.Request("POST", "https://quicknode.example"),
        )
        httpx_post.return_value = response

        result = QuicknodeMistTrackClient(
            endpoint_url="https://quicknode.example"
        ).address_risk_score(chain="ETH", address="0xabc")

        httpx_post.assert_called_once_with(
            "https://quicknode.example",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "mt_addressRiskScore",
                "params": [{"chain": "ETH", "address": "0xabc"}],
            },
            timeout=5,
        )
        self.assertEqual(result.risk_level, RiskLevel.HIGH)
        self.assertEqual(result.risk_score, Decimal("88"))
        self.assertEqual(result.detail_list, ["Sanctioned entity"])
        self.assertEqual(result.risk_detail, {"sanction": 1})
        self.assertEqual(result.risk_report_url, "https://report.example")

    @patch("risk.clients.httpx.post")
    def test_json_rpc_error_raises_client_error(self, httpx_post):
        httpx_post.return_value = httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "error": {"message": "bad request"}},
            request=httpx.Request("POST", "https://quicknode.example"),
        )

        with self.assertRaisesMessage(RuntimeError, "bad request"):
            QuicknodeMistTrackClient(
                endpoint_url="https://quicknode.example"
            ).address_risk_score(chain="ETH", address="0xabc")


class RiskChainMappingTests(SimpleTestCase):
    def test_common_evm_mainnets_map_to_misttrack_chain_codes(self):
        cases = {
            1: "ETH",
            10: "OPTIMISM",
            56: "BNB",
            137: "POLYGON",
            324: "ZKSYNC",
            4200: "MERLIN",
            4689: "IOTX",
            8453: "BASE",
            42161: "ARBITRUM",
            43114: "AVAX",
        }

        for chain_id, expected in cases.items():
            with self.subTest(chain_id=chain_id):
                chain = Chain(type=ChainType.EVM, chain_id=chain_id)
                self.assertEqual(RiskMarkingService._misttrack_chain(chain), expected)


@override_settings(IS_SAAS=False)
class RiskMarkingServiceTests(RiskTestMixin, TestCase):
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_below_threshold_is_skipped_without_external_query(self, score):
        invoice = self.make_invoice(worth=Decimal("99.99"))

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)
        invoice.refresh_from_db()
        self.assertIsNone(invoice.risk_level)
        self.assertIsNone(invoice.risk_score)

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_below_threshold_is_skipped_without_external_query(self, score):
        deposit = self.make_deposit(worth=Decimal("99.99"))

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)
        deposit.refresh_from_db()
        self.assertIsNone(deposit.risk_level)
        self.assertIsNone(deposit.risk_score)

    # ===== SaaS gate（spec §5） =====
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_self_hosted_mode_marks_normally(self, score):
        """自托管模式（class-level IS_SAAS=False），gate 直接放行。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=[],
            risk_detail={},
            risk_report_url="",
            raw_response={},
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_saas_permission_granted_marks(self, score):
        """SaaS 模式 + 缓存命中 + enable_risk_marking=True → 正常标记。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        cache.set(
            f"saas:permission:{invoice.project.appid}",
            {"enable_risk_marking": True, "_fetched_at": time.time()},
            None,
        )
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=[],
            risk_detail={},
            risk_report_url="",
            raw_response={},
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_saas_permission_denied_skips(self, score):
        """SaaS 模式 + 缓存命中 + enable_risk_marking=False → skip，不调 MistTrack。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        cache.set(
            f"saas:permission:{invoice.project.appid}",
            {"enable_risk_marking": False, "_fetched_at": time.time()},
            None,
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)
        invoice.refresh_from_db()
        self.assertIsNone(invoice.risk_level)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_saas_cold_cache_fails_closed(self, score):
        """SaaS 模式 + 冷缓存 → fail-closed → skip，不调 MistTrack。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        # 不预写缓存，cache.clear() 已在 setUp 跑过

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_self_hosted_mode_marks_normally(self, score):
        """自托管模式（class-level IS_SAAS=False），gate 直接放行。"""
        deposit = self.make_deposit(worth=Decimal("500"))
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=[],
            risk_detail={},
            risk_report_url="",
            raw_response={},
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_saas_permission_granted_marks(self, score):
        deposit = self.make_deposit(worth=Decimal("500"))
        cache.set(
            f"saas:permission:{deposit.customer.project.appid}",
            {"enable_risk_marking": True, "_fetched_at": time.time()},
            None,
        )
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=[],
            risk_detail={},
            risk_report_url="",
            raw_response={},
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_saas_permission_denied_skips(self, score):
        deposit = self.make_deposit(worth=Decimal("500"))
        cache.set(
            f"saas:permission:{deposit.customer.project.appid}",
            {"enable_risk_marking": False, "_fetched_at": time.time()},
            None,
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_saas_cold_cache_fails_closed(self, score):
        deposit = self.make_deposit(worth=Decimal("500"))

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)


@override_settings(IS_SAAS=False)
class RiskBusinessDispatchTests(RiskTestMixin, TestCase):
    @patch("risk.tasks.mark_invoice_risk.delay")
    def test_invoice_match_enqueues_risk_after_transaction_commit(self, delay):
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="risk-match",
            title="Risk match",
            currency="USD",
            amount=Decimal("500"),
            methods={"ETH": ["ethereum-mainnet"]},
            worth=Decimal("500"),
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        InvoicePaySlot.objects.create(
            invoice=invoice,
            version=1,
            crypto=self.native,
            chain=self.chain,
            pay_address=self.transfer.to_address,
            pay_amount=self.transfer.amount,
        )
        self.transfer.datetime = timezone.now()
        self.transfer.save(update_fields=["datetime"])

        with self.captureOnCommitCallbacks(execute=True):
            matched = InvoiceService.try_match_invoice(self.transfer)

        self.assertTrue(matched)
        invoice.refresh_from_db()
        delay.assert_called_once_with(invoice.pk)

    @patch("risk.tasks.mark_deposit_risk.delay")
    def test_deposit_creation_enqueues_risk_after_transaction_commit(self, delay):
        address = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=1,
            address_index=0,
            address=self.transfer.to_address,
        )
        DepositAddress.objects.create(
            customer=self.customer,
            chain_type=ChainType.EVM,
            address=address,
        )
        self.transfer.type = ""
        self.transfer.save(update_fields=["type"])

        with self.captureOnCommitCallbacks(execute=True):
            created = DepositService.try_create_deposit(self.transfer)

        self.assertTrue(created)
        deposit = Deposit.objects.get(transfer=self.transfer)
        delay.assert_called_once_with(deposit.pk)

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_success_updates_assessment_snapshot_and_cache(self, score):
        invoice = self.make_invoice(worth=Decimal("500"))
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=["Mixer"],
            risk_detail={"mixer": 1},
            risk_report_url="https://report.example/1",
            raw_response={"risk_level": "Severe", "score": 95},
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.source, RiskSource.QUICKNODE_MISTTRACK)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)
        self.assertEqual(assessment.risk_level, RiskLevel.SEVERE)
        self.assertEqual(assessment.risk_score, Decimal("95"))
        invoice.refresh_from_db()
        self.assertEqual(invoice.risk_level, RiskLevel.SEVERE)
        self.assertEqual(invoice.risk_score, Decimal("95"))

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_uses_cached_address_result_without_external_query(self, score):
        deposit = self.make_deposit(worth=Decimal("500"))
        RiskMarkingService.write_cache(
            source=RiskSource.QUICKNODE_MISTTRACK,
            address=self.transfer.from_address,
            result={
                "risk_level": RiskLevel.MODERATE,
                "risk_score": "61",
                "detail_list": ["Phishing"],
                "risk_detail": {"phishing": 1},
                "risk_report_url": "https://report.example/cached",
                "raw_response": {"risk_level": "Moderate", "score": 61},
            },
            timeout=300,
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)
        self.assertEqual(assessment.risk_level, RiskLevel.MODERATE)
        self.assertEqual(assessment.risk_score, Decimal("61"))
        deposit.refresh_from_db()
        self.assertEqual(deposit.risk_level, RiskLevel.MODERATE)
        self.assertEqual(deposit.risk_score, Decimal("61"))

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_force_refresh_threshold_bypasses_cache(self, score):
        deposit = self.make_deposit(worth=Decimal("10000.01"))
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.LOW,
            risk_score=Decimal("10"),
            detail_list=[],
            risk_detail={},
            risk_report_url="",
            raw_response={"risk_level": "Low", "score": 10},
        )
        RiskMarkingService.write_cache(
            source=RiskSource.QUICKNODE_MISTTRACK,
            address=self.transfer.from_address,
            result={
                "risk_level": RiskLevel.SEVERE,
                "risk_score": "99",
                "detail_list": [],
                "risk_detail": {},
                "risk_report_url": "",
                "raw_response": {},
            },
            timeout=300,
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_called_once()
        deposit.refresh_from_db()
        self.assertEqual(deposit.risk_level, RiskLevel.LOW)
        self.assertEqual(deposit.risk_score, Decimal("10"))

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_external_failure_records_failed_and_clears_snapshot(self, score):
        invoice = self.make_invoice(worth=Decimal("500"))
        invoice.risk_level = RiskLevel.HIGH
        invoice.risk_score = Decimal("80")
        invoice.save(update_fields=["risk_level", "risk_score", "updated_at"])
        score.side_effect = RuntimeError("quicknode down")

        RiskMarkingService.mark_invoice(invoice.pk)

        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.FAILED)
        self.assertIn("quicknode down", assessment.error_message)
        invoice.refresh_from_db()
        self.assertIsNone(invoice.risk_level)
        self.assertIsNone(invoice.risk_score)
