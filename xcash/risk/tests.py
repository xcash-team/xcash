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
from risk.clients import MistTrackOpenApiClient
from risk.clients import MistTrackRiskResult
from risk.clients import QuicknodeMistTrackClient
from risk.models import RiskAssessment
from risk.models import RiskAssessmentStatus
from risk.models import RiskLevel
from risk.models import RiskSkipReason
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
    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_address_risk_score_posts_json_rpc_payload(self, httpx_request, _sleep):
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
        httpx_request.return_value = response

        result = QuicknodeMistTrackClient(
            endpoint_url="https://quicknode.example"
        ).address_risk_score(chain="ETH", address="0xabc")

        httpx_request.assert_called_once_with(
            "POST",
            "https://quicknode.example",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "mt_addressRiskScore",
                "params": [{"chain": "ETH", "address": "0xabc"}],
            },
            timeout=5.0,
        )
        self.assertEqual(result.risk_level, RiskLevel.HIGH)
        self.assertEqual(result.risk_score, Decimal("88"))
        self.assertEqual(result.detail_list, ["Sanctioned entity"])
        # QuickNode 历史 dict → list[dict] 适配后单元素 list
        self.assertEqual(result.risk_detail, [{"sanction": 1}])
        self.assertEqual(result.risk_report_url, "https://report.example")
        # QuickNode add-on 不返回 address_label
        self.assertIsNone(result.address_label)

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_json_rpc_error_raises_client_error(self, httpx_request, _sleep):
        httpx_request.return_value = httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "error": {"message": "bad request"}},
            request=httpx.Request("POST", "https://quicknode.example"),
        )

        with self.assertRaisesMessage(RuntimeError, "bad request"):
            QuicknodeMistTrackClient(
                endpoint_url="https://quicknode.example"
            ).address_risk_score(chain="ETH", address="0xabc")


class MistTrackOpenApiClientTests(SimpleTestCase):
    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_address_risk_score_calls_v3_endpoint_with_api_key(
        self, httpx_request, _sleep
    ):
        response = httpx.Response(
            200,
            json={
                "success": True,
                "msg": "",
                "data": {
                    "risk_level": "High",
                    "score": 75,
                    "detail_list": ["Interact With High-risk Tag Address"],
                    "risk_detail": [
                        {
                            "entity": "huionepay",
                            "risk_type": "sanctioned_entity",
                            "hop_dic": {"1": ["huionepay"]},
                        }
                    ],
                    "address_label": "Binance",
                    "risk_report_url": "https://report.example/v3",
                },
            },
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )
        httpx_request.return_value = response

        result = MistTrackOpenApiClient(api_key="secret").address_risk_score(
            coin="ETH", address="0xabc"
        )

        httpx_request.assert_called_once_with(
            "GET",
            "https://openapi.misttrack.io/v3/risk_score",
            params={"coin": "ETH", "address": "0xabc", "api_key": "secret"},
            timeout=5.0,
        )
        self.assertEqual(result.risk_level, RiskLevel.HIGH)
        self.assertEqual(result.risk_score, Decimal("75"))
        self.assertEqual(result.detail_list, ["Interact With High-risk Tag Address"])
        self.assertEqual(result.risk_detail[0]["hop_dic"], {"1": ["huionepay"]})
        self.assertEqual(result.address_label, "Binance")
        self.assertEqual(result.raw_response["address_label"], "Binance")
        self.assertEqual(result.risk_report_url, "https://report.example/v3")

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_api_error_raises_client_error(self, httpx_request, _sleep):
        httpx_request.return_value = httpx.Response(
            200,
            json={"success": False, "msg": "InvalidApiKey"},
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )

        with self.assertRaisesMessage(RuntimeError, "InvalidApiKey"):
            MistTrackOpenApiClient(api_key="bad").address_risk_score(
                coin="ETH", address="0xabc"
            )

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_4xx_response_does_not_leak_api_key(self, httpx_request, _sleep):
        """HTTP 4xx 抛出的异常消息不得包含明文 api_key（防日志泄露）。"""
        httpx_request.return_value = httpx.Response(
            401,
            text="api_key=super-secret-leaked is invalid",
            request=httpx.Request(
                "GET",
                "https://openapi.misttrack.io/v3/risk_score?api_key=super-secret-leaked",
            ),
        )

        with self.assertRaises(RuntimeError) as ctx:
            MistTrackOpenApiClient(api_key="super-secret-leaked").address_risk_score(
                coin="ETH", address="0xabc"
            )

        self.assertNotIn("super-secret-leaked", str(ctx.exception))
        # 4xx 不重试
        self.assertEqual(httpx_request.call_count, 1)

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_5xx_retries_then_succeeds(self, httpx_request, _sleep):
        ok = httpx.Response(
            200,
            json={
                "success": True,
                "msg": "",
                "data": {
                    "risk_level": "Low",
                    "score": 10,
                    "detail_list": [],
                    "risk_detail": [],
                    "risk_report_url": "",
                },
            },
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )
        fail = httpx.Response(
            502,
            text="Bad Gateway",
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )
        httpx_request.side_effect = [fail, fail, ok]

        result = MistTrackOpenApiClient(api_key="k").address_risk_score(
            coin="ETH", address="0xabc"
        )

        self.assertEqual(result.risk_level, RiskLevel.LOW)
        self.assertEqual(httpx_request.call_count, 3)

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_429_with_retry_after_retries_then_succeeds(self, httpx_request, _sleep):
        """429 响应体携带 retry_after 时应按该值休眠后重试。"""
        ok = httpx.Response(
            200,
            json={
                "success": True,
                "msg": "",
                "data": {
                    "risk_level": "Low",
                    "score": 10,
                    "detail_list": [],
                    "risk_detail": [],
                    "risk_report_url": "",
                },
            },
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )
        rate_limited = httpx.Response(
            429,
            json={"success": False, "msg": "ExceededRateLimit", "retry_after": 2},
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )
        httpx_request.side_effect = [rate_limited, ok]

        result = MistTrackOpenApiClient(api_key="k").address_risk_score(
            coin="ETH", address="0xabc"
        )

        self.assertEqual(result.risk_level, RiskLevel.LOW)
        self.assertEqual(httpx_request.call_count, 2)

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_429_exhausted_retries_raises_error(self, httpx_request, _sleep):
        """429 连续重试耗尽后应抛出异常。"""
        rate_limited = httpx.Response(
            429,
            json={"success": False, "msg": "ExceededRateLimit", "retry_after": 1},
            request=httpx.Request(
                "GET", "https://openapi.misttrack.io/v3/risk_score"
            ),
        )
        httpx_request.side_effect = [rate_limited, rate_limited, rate_limited]

        with self.assertRaises(RuntimeError):
            MistTrackOpenApiClient(api_key="k").address_risk_score(
                coin="ETH", address="0xabc"
            )

        self.assertEqual(httpx_request.call_count, 3)

    @patch("risk.clients.time.sleep", return_value=None)
    @patch("risk.clients.httpx.request")
    def test_network_error_message_does_not_leak_api_key(
        self, httpx_request, _sleep
    ):
        """网络异常重试耗尽后抛出的消息不得包含 api_key。"""
        httpx_request.side_effect = httpx.ConnectError(
            "connection refused for https://openapi.misttrack.io/v3/risk_score?api_key=leak-me"
        )

        with self.assertRaises(RuntimeError) as ctx:
            MistTrackOpenApiClient(api_key="leak-me").address_risk_score(
                coin="ETH", address="0xabc"
            )

        self.assertNotIn("leak-me", str(ctx.exception))
        self.assertEqual(httpx_request.call_count, 3)


class RiskChainMappingTests(SimpleTestCase):
    def test_quicknode_maps_only_addon_supported_networks(self):
        cases = {
            ChainType.BITCOIN: "BTC",
            ChainType.TRON: "TRX",
            1: "ETH",
            56: "BNB",
            42161: "ARBITRUM",
        }

        for chain_key, expected in cases.items():
            with self.subTest(chain_key=chain_key):
                if isinstance(chain_key, int):
                    chain = Chain(type=ChainType.EVM, chain_id=chain_key)
                else:
                    chain = Chain(type=chain_key)
                self.assertEqual(
                    RiskMarkingService._quicknode_misttrack_chain(chain), expected
                )

    def test_common_evm_mainnets_map_to_misttrack_openapi_coin_codes(self):
        cases = {
            (1, "ETH"): "ETH",
            (1, "USDT"): "USDT-ERC20",
            (10, "ETH"): "ETH-Optimism",
            (10, "USDT"): "USDT-Optimism",
            (10, "USDC"): "USDC-Optimism",
            (56, "BNB"): "BNB",
            (56, "USDT"): "USDT-BEP20",
            (56, "BUSD"): "BUSD-BEP20",
            (137, "POL"): "POL-Polygon",
            (137, "USDT"): "USDT-Polygon",
            (137, "USDC.E"): "USDC.e-Polygon",
            (324, "ETH"): "ETH-zkSync",
            (324, "ZK"): "ZK-zkSync",
            (4200, "BTC"): "BTC-Merlin",
            (4689, "IOTX"): "IOTX",
            (8453, "ETH"): "ETH-Base",
            (8453, "USDC"): "USDC-Base",
            (8453, "USDT"): "USDT-Base",
            (8453, "CBBTC"): "cbBTC-Base",
            (42161, "ETH"): "ETH-Arbitrum",
            (42161, "USDT"): "USDT-Arbitrum",
            (42161, "ARB"): "ARB-Arbitrum",
            (43114, "AVAX"): "AVAX-Avalanche",
            (43114, "USDT"): "USDT-Avalanche",
            (43114, "BTC.B"): "BTC.b-Avalanche",
        }

        for (chain_id, symbol), expected in cases.items():
            with self.subTest(chain_id=chain_id, symbol=symbol):
                chain = Chain(type=ChainType.EVM, chain_id=chain_id)
                crypto = Crypto(symbol=symbol)
                self.assertEqual(
                    RiskMarkingService._misttrack_openapi_coin(
                        chain=chain, crypto=crypto
                    ),
                    expected,
                )

    def test_tron_usdt_maps_to_trc20_coin_code(self):
        chain = Chain(type=ChainType.TRON)
        crypto = Crypto(symbol="USDT")

        self.assertEqual(
            RiskMarkingService._misttrack_openapi_coin(chain=chain, crypto=crypto),
            "USDT-TRC20",
        )


@override_settings(IS_SAAS=False)
class RiskMarkingServiceTests(RiskTestMixin, TestCase):
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_below_threshold_does_not_create_assessment(self, score):
        invoice = self.make_invoice(worth=Decimal("99.99"))

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(invoice=invoice).exists())
        invoice.refresh_from_db()
        self.assertIsNone(invoice.risk_level)
        self.assertIsNone(invoice.risk_score)

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_below_threshold_does_not_create_assessment(self, score):
        deposit = self.make_deposit(worth=Decimal("99.99"))

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(deposit=deposit).exists())
        deposit.refresh_from_db()
        self.assertIsNone(deposit.risk_level)
        self.assertIsNone(deposit.risk_score)

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_marking_disabled_does_not_create_assessment(self, score):
        self.platform_settings.risk_marking_enabled = False
        self.platform_settings.save(update_fields=["risk_marking_enabled"])
        invoice = self.make_invoice(worth=Decimal("500"))

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(invoice=invoice).exists())

    # ===== SaaS gate（spec §5） =====
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_self_hosted_mode_marks_normally(self, score):
        """自托管模式（class-level IS_SAAS=False），gate 直接放行。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=[],
            risk_detail=[],
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
            risk_detail=[],
            risk_report_url="",
            raw_response={},
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_saas_permission_denied_does_not_create_assessment(self, score):
        """SaaS 模式 + 缓存命中 + enable_risk_marking=False → 直接 return，不写记录。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        cache.set(
            f"saas:permission:{invoice.project.appid}",
            {"enable_risk_marking": False, "_fetched_at": time.time()},
            None,
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(invoice=invoice).exists())
        invoice.refresh_from_db()
        self.assertIsNone(invoice.risk_level)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_invoice_saas_cold_cache_fails_closed(self, score):
        """SaaS 模式 + 冷缓存 → fail-closed → 直接 return，不调 MistTrack 也不写记录。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        # 不预写缓存，cache.clear() 已在 setUp 跑过

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(invoice=invoice).exists())

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_self_hosted_mode_marks_normally(self, score):
        """自托管模式（class-level IS_SAAS=False），gate 直接放行。"""
        deposit = self.make_deposit(worth=Decimal("500"))
        score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.SEVERE,
            risk_score=Decimal("95"),
            detail_list=[],
            risk_detail=[],
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
            risk_detail=[],
            risk_report_url="",
            raw_response={},
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_called_once()
        assessment = RiskAssessment.objects.get(deposit=deposit)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_saas_permission_denied_does_not_create_assessment(self, score):
        deposit = self.make_deposit(worth=Decimal("500"))
        cache.set(
            f"saas:permission:{deposit.customer.project.appid}",
            {"enable_risk_marking": False, "_fetched_at": time.time()},
            None,
        )

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(deposit=deposit).exists())

    @override_settings(IS_SAAS=True, INTERNAL_API_TOKEN="t")
    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_deposit_saas_cold_cache_fails_closed(self, score):
        deposit = self.make_deposit(worth=Decimal("500"))

        RiskMarkingService.mark_deposit(deposit.pk)

        score.assert_not_called()
        self.assertFalse(RiskAssessment.objects.filter(deposit=deposit).exists())

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    @patch("risk.service.MistTrackOpenApiClient.address_risk_score")
    def test_openapi_api_key_takes_precedence_over_quicknode_endpoint(
        self, openapi_score, quicknode_score
    ):
        invoice = self.make_invoice(worth=Decimal("500"))
        self.platform_settings.misttrack_openapi_api_key = "openapi-secret"
        self.platform_settings.save(update_fields=["misttrack_openapi_api_key"])
        openapi_score.return_value = MistTrackRiskResult(
            risk_level=RiskLevel.HIGH,
            risk_score=Decimal("75"),
            detail_list=[],
            risk_detail=[],
            risk_report_url="https://report.example/v3",
            raw_response={"risk_level": "High", "score": 75},
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        openapi_score.assert_called_once_with(
            coin="ETH", address=self.transfer.from_address
        )
        quicknode_score.assert_not_called()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.source, RiskSource.MISTTRACK_OPENAPI)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SUCCESS)
        invoice.refresh_from_db()
        self.assertEqual(invoice.risk_level, RiskLevel.HIGH)

    @patch("risk.service.QuicknodeMistTrackClient.address_risk_score")
    def test_quicknode_unsupported_chain_is_skipped_without_external_query(self, score):
        invoice = self.make_invoice(worth=Decimal("500"))
        self.chain.chain_id = 137
        self.chain.code = "polygon-mainnet"
        self.chain.save(update_fields=["chain_id", "code"])

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.source, RiskSource.QUICKNODE_MISTTRACK)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)
        self.assertEqual(assessment.skip_reason, RiskSkipReason.UNSUPPORTED_CHAIN)
        self.assertIn("unsupported QuickNode MistTrack chain", assessment.error_message)

    @patch("risk.service.MistTrackOpenApiClient.address_risk_score")
    def test_openapi_unsupported_chain_is_skipped_without_external_query(self, score):
        """OpenAPI 路径未覆盖的链/币种与 QuickNode 一致走 SKIPPED 而非 FAILED。"""
        invoice = self.make_invoice(worth=Decimal("500"))
        self.platform_settings.misttrack_openapi_api_key = "openapi-secret"
        self.platform_settings.save(update_fields=["misttrack_openapi_api_key"])
        # 切到 OpenAPI 也未映射的某条 EVM 链
        self.chain.chain_id = 999999
        self.chain.code = "exotic-chain"
        self.chain.save(update_fields=["chain_id", "code"])

        RiskMarkingService.mark_invoice(invoice.pk)

        score.assert_not_called()
        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.source, RiskSource.MISTTRACK_OPENAPI)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)
        self.assertEqual(assessment.skip_reason, RiskSkipReason.UNSUPPORTED_CHAIN)

    def test_provider_not_configured_records_skip_reason(self):
        invoice = self.make_invoice(worth=Decimal("500"))
        self.platform_settings.quicknode_misttrack_endpoint_url = ""
        self.platform_settings.misttrack_openapi_api_key = ""
        self.platform_settings.save(
            update_fields=[
                "quicknode_misttrack_endpoint_url",
                "misttrack_openapi_api_key",
            ]
        )

        RiskMarkingService.mark_invoice(invoice.pk)

        assessment = RiskAssessment.objects.get(invoice=invoice)
        self.assertEqual(assessment.status, RiskAssessmentStatus.SKIPPED)
        self.assertEqual(
            assessment.skip_reason, RiskSkipReason.PROVIDER_NOT_CONFIGURED
        )


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
            risk_detail=[{"mixer": 1}],
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
            chain=self.chain.code,
            address=self.transfer.from_address,
            result={
                "risk_level": RiskLevel.MODERATE,
                "risk_score": "61",
                "detail_list": ["Phishing"],
                "risk_detail": [{"phishing": 1}],
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
            risk_detail=[],
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
                "risk_detail": [],
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
