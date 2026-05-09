from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from django.test import TestCase
from django.utils import timezone
from web3 import Web3

from chains.models import Chain
from chains.models import ChainType
from chains.models import Wallet
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.models import Fiat
from invoices.epay import build_epay_v1_sign
from invoices.epay import epay_v1_signing_string
from invoices.epay import format_epay_money
from invoices.epay import verify_epay_v1_sign
from invoices.epay_serializers import EpaySubmitSerializer
from invoices.epay_service import EpaySubmitError
from invoices.epay_service import EpaySubmitService
from invoices.models import EpayMerchant
from invoices.models import EpayOrder
from invoices.models import Invoice
from invoices.models import InvoiceProtocol
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage


class EpaySignatureTests(TestCase):
    def test_epay_v1_signing_string_sorts_keys_and_skips_unsigned_values(self):
        params = {
            "pid": 1001,
            "type": "usdt",
            "out_trade_no": "ORDER1001",
            "notify_url": "https://merchant.example.com/notify",
            "name": "VIP",
            "money": Decimal("18.50"),
            "return_url": "",
            "param": None,
            "sign": "ignored",
            "sign_type": "MD5",
        }
        expected_signing_string = (
            "money=18.50&name=VIP"
            "&notify_url=https://merchant.example.com/notify"
            "&out_trade_no=ORDER1001&pid=1001&type=usdt"
        )

        self.assertEqual(epay_v1_signing_string(params), expected_signing_string)

    def test_build_epay_v1_sign_appends_key_and_returns_lowercase_md5(self):
        params = {
            "pid": "1001",
            "type": "usdt",
            "out_trade_no": "ORDER1001",
            "notify_url": "https://merchant.example.com/notify",
            "name": "VIP",
            "money": "18.50",
        }

        self.assertEqual(
            build_epay_v1_sign(params, "epay-secret"),
            "ebd914c3205469db3e7c755ea1e520d8",
        )

    def test_verify_epay_v1_sign_compares_supplied_sign(self):
        params = {
            "pid": "1001",
            "type": "usdt",
            "out_trade_no": "ORDER1001",
            "notify_url": "https://merchant.example.com/notify",
            "name": "VIP",
            "money": "18.50",
            "sign": "ebd914c3205469db3e7c755ea1e520d8",
            "sign_type": "MD5",
        }

        self.assertTrue(verify_epay_v1_sign(params, "epay-secret"))

        params["sign"] = "bad-sign"
        self.assertFalse(verify_epay_v1_sign(params, "epay-secret"))

    def test_format_epay_money_outputs_two_decimal_places(self):
        self.assertEqual(format_epay_money(Decimal("18")), "18.00")
        self.assertEqual(format_epay_money(Decimal("18.5")), "18.50")
        self.assertEqual(format_epay_money(Decimal("18.999")), "19.00")


class EpayModelTests(TestCase):
    def setUp(self):
        self.project = Project.objects.create(
            name="EPay Project",
            wallet=Wallet.objects.create(),
        )

    def test_invoice_defaults_to_native_protocol(self):
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="native-1",
            title="Native",
            currency="USD",
            amount=Decimal("10.00"),
            methods={},
            expires_at=timezone.now() + timedelta(minutes=10),
        )

        self.assertEqual(invoice.protocol, InvoiceProtocol.NATIVE)

    def test_epay_order_stores_protocol_metadata_without_polluting_invoice(self):
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1001,
            secret_key="epay-secret",
            default_currency="CNY",
        )
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="ORDER1001",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )

        epay_order = EpayOrder.objects.create(
            invoice=invoice,
            merchant=merchant,
            pid="1001",
            trade_no=invoice.sys_no,
            out_trade_no="ORDER1001",
            type="usdt",
            name="VIP",
            money=Decimal("18.50"),
            notify_url="https://merchant.example.com/notify",
            return_url="https://merchant.example.com/return",
            param="u=42",
            sign_type="MD5",
            raw_request={"pid": "1001", "out_trade_no": "ORDER1001"},
        )

        self.assertEqual(epay_order.invoice, invoice)
        self.assertEqual(invoice.protocol, InvoiceProtocol.EPAY_V1)
        self.assertEqual(epay_order.notify_url, "https://merchant.example.com/notify")

    def test_epay_merchant_signing_key_falls_back_to_project_hmac_key(self):
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1002,
            secret_key="",
        )

        self.assertEqual(merchant.signing_key, self.project.hmac_key)

    def test_epay_order_rejects_invoice_from_different_project(self):
        other_project = Project.objects.create(
            name="Other EPay Project",
            wallet=Wallet.objects.create(),
        )
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1003,
            secret_key="epay-secret",
        )
        invoice = Invoice.objects.create(
            project=other_project,
            out_no="ORDER1003",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )

        with self.assertRaises(ValidationError):
            EpayOrder.objects.create(
                invoice=invoice,
                merchant=merchant,
                pid="1003",
                trade_no=invoice.sys_no,
                out_trade_no="ORDER1003",
                type="usdt",
                name="VIP",
                money=Decimal("18.50"),
                notify_url="https://merchant.example.com/notify",
                raw_request={"pid": "1003", "out_trade_no": "ORDER1003"},
            )

    def test_epay_order_rejects_pid_that_does_not_match_merchant(self):
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1004,
            secret_key="epay-secret",
        )
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="ORDER1004",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )

        with self.assertRaises(ValidationError):
            EpayOrder.objects.create(
                invoice=invoice,
                merchant=merchant,
                pid="9999",
                trade_no=invoice.sys_no,
                out_trade_no="ORDER1004",
                type="usdt",
                name="VIP",
                money=Decimal("18.50"),
                notify_url="https://merchant.example.com/notify",
                raw_request={"pid": "9999", "out_trade_no": "ORDER1004"},
            )

    def test_epay_order_enforces_unique_out_trade_no_per_merchant(self):
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1005,
            secret_key="epay-secret",
        )
        first_invoice = Invoice.objects.create(
            project=self.project,
            out_no="ORDER1005",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        second_invoice = Invoice.objects.create(
            project=self.project,
            out_no="ORDER1005-DUP",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        EpayOrder.objects.create(
            invoice=first_invoice,
            merchant=merchant,
            pid="1005",
            trade_no=first_invoice.sys_no,
            out_trade_no="ORDER1005",
            type="usdt",
            name="VIP",
            money=Decimal("18.50"),
            notify_url="https://merchant.example.com/notify",
            raw_request={"pid": "1005", "out_trade_no": "ORDER1005"},
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            EpayOrder.objects.create(
                invoice=second_invoice,
                merchant=merchant,
                pid="1005",
                trade_no=second_invoice.sys_no,
                out_trade_no="ORDER1005",
                type="usdt",
                name="VIP",
                money=Decimal("18.50"),
                notify_url="https://merchant.example.com/notify",
                raw_request={"pid": "1005", "out_trade_no": "ORDER1005"},
            )

    def test_epay_order_enforces_unique_trade_no_per_merchant(self):
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1006,
            secret_key="epay-secret",
        )
        first_invoice = Invoice.objects.create(
            project=self.project,
            out_no="ORDER1006",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        second_invoice = Invoice.objects.create(
            project=self.project,
            out_no="ORDER1006-ALT",
            title="VIP",
            currency="CNY",
            amount=Decimal("18.50"),
            methods={},
            protocol=InvoiceProtocol.EPAY_V1,
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        EpayOrder.objects.create(
            invoice=first_invoice,
            merchant=merchant,
            pid="1006",
            trade_no=first_invoice.sys_no,
            out_trade_no="ORDER1006",
            type="usdt",
            name="VIP",
            money=Decimal("18.50"),
            notify_url="https://merchant.example.com/notify",
            raw_request={"pid": "1006", "out_trade_no": "ORDER1006"},
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            EpayOrder.objects.create(
                invoice=second_invoice,
                merchant=merchant,
                pid="1006",
                trade_no=first_invoice.sys_no,
                out_trade_no="ORDER1006-ALT",
                type="usdt",
                name="VIP",
                money=Decimal("18.50"),
                notify_url="https://merchant.example.com/notify",
                raw_request={"pid": "1006", "out_trade_no": "ORDER1006-ALT"},
            )


class EpaySubmitServiceTests(TestCase):
    def setUp(self):
        self.project = Project.objects.create(
            name="EPay Submit Project",
            wallet=Wallet.objects.create(),
        )
        self.merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=2001,
            secret_key="epay-submit-secret",
            default_currency="CNY",
        )
        self.crypto = Crypto.objects.create(
            name="EPay Submit USDT",
            symbol="EPAY-USDT",
            prices={"USD": "1"},
            coingecko_id="epay-submit-usdt",
        )
        self.native = Crypto.objects.create(
            name="EPay Submit ETH",
            symbol="EPAY-ETH",
            coingecko_id="epay-submit-eth",
        )
        self.chain = Chain.objects.create(
            name="EPay Submit Chain",
            code="epay-submit-chain",
            type=ChainType.EVM,
            native_coin=self.native,
            chain_id=92001,
            rpc="http://localhost:8545",
            active=True,
        )
        ChainToken.objects.create(
            crypto=self.crypto,
            chain=self.chain,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000B1"
            ),
        )
        Fiat.objects.get_or_create(code="CNY")
        RecipientAddress.objects.create(
            name="EPay Submit Recipient",
            project=self.project,
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000C1"
            ),
            usage=RecipientAddressUsage.INVOICE,
        )

    def _signed_params(self, **overrides):
        params = {
            "pid": str(self.merchant.pid),
            "type": "usdt",
            "out_trade_no": "EPAY-SUBMIT-1001",
            "notify_url": "https://merchant.example.com/notify",
            "return_url": "https://merchant.example.com/return",
            "name": "VIP Package",
            "money": "18.50",
            "param": "user=42",
            "sign_type": "MD5",
        }
        params.update(overrides)
        params["sign"] = build_epay_v1_sign(params, self.merchant.signing_key)
        return params

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_creates_invoice_and_epay_order(self, mock_check, mock_initialize):
        mock_initialize.side_effect = lambda invoice: invoice

        invoice = EpaySubmitService.submit(self._signed_params())

        invoice.refresh_from_db()
        epay_order = invoice.epay_order
        self.assertEqual(invoice.project, self.project)
        self.assertEqual(invoice.out_no, "EPAY-SUBMIT-1001")
        self.assertEqual(invoice.title, "VIP Package")
        self.assertEqual(invoice.currency, "CNY")
        self.assertEqual(invoice.amount, Decimal("18.50"))
        self.assertEqual(invoice.protocol, InvoiceProtocol.EPAY_V1)
        self.assertEqual(invoice.redirect_url, "https://merchant.example.com/return")
        self.assertEqual(invoice.methods, Invoice.available_methods(self.project))
        self.assertEqual(invoice.methods[self.crypto.symbol], [self.chain.code])
        self.assertEqual(epay_order.merchant, self.merchant)
        self.assertEqual(epay_order.trade_no, invoice.sys_no)
        self.assertEqual(epay_order.out_trade_no, invoice.out_no)
        self.assertEqual(epay_order.notify_url, "https://merchant.example.com/notify")
        self.assertEqual(epay_order.raw_request["out_trade_no"], "EPAY-SUBMIT-1001")
        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )
        mock_initialize.assert_called_once_with(invoice)

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_rejects_bad_sign(self, mock_check, mock_initialize):
        params = self._signed_params()
        params["sign"] = "bad-sign"

        with self.assertRaises(EpaySubmitError):
            EpaySubmitService.submit(params)

        self.assertFalse(Invoice.objects.filter(out_no="EPAY-SUBMIT-1001").exists())
        mock_check.assert_not_called()
        mock_initialize.assert_not_called()

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_verifies_sign_with_raw_parameter_shape(
        self,
        mock_check,
        mock_initialize,
    ):
        mock_initialize.side_effect = lambda invoice: invoice
        raw_pid_params = self._signed_params(
            pid=f"0{self.merchant.pid}",
            out_trade_no="EPAY-RAW-PID-1001",
        )

        invoice = EpaySubmitService.submit(raw_pid_params)

        self.assertEqual(invoice.out_no, "EPAY-RAW-PID-1001")
        self.assertEqual(invoice.epay_order.pid, str(self.merchant.pid))
        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_rejects_sign_built_from_normalized_parameter_shape(
        self,
        mock_check,
        mock_initialize,
    ):
        params = self._signed_params(
            pid=f"0{self.merchant.pid}",
            out_trade_no="EPAY-RAW-PID-1002",
        )
        normalized_sign_params = {**params, "pid": str(self.merchant.pid)}
        params["sign"] = build_epay_v1_sign(
            normalized_sign_params,
            self.merchant.signing_key,
        )

        with self.assertRaises(EpaySubmitError):
            EpaySubmitService.submit(params)

        self.assertFalse(Invoice.objects.filter(out_no="EPAY-RAW-PID-1002").exists())
        mock_check.assert_not_called()
        mock_initialize.assert_not_called()

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_reuses_existing_order_when_metadata_matches(
        self,
        mock_check,
        mock_initialize,
    ):
        mock_initialize.side_effect = lambda invoice: invoice
        params = self._signed_params()
        first_invoice = EpaySubmitService.submit(params)
        second_invoice = EpaySubmitService.submit(params)

        self.assertEqual(second_invoice.pk, first_invoice.pk)
        self.assertEqual(Invoice.objects.count(), 1)
        self.assertEqual(EpayOrder.objects.count(), 1)
        self.assertEqual(mock_check.call_count, 2)
        mock_initialize.assert_called_once_with(first_invoice)

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_rejects_existing_order_when_money_differs(
        self,
        mock_check,
        mock_initialize,
    ):
        EpaySubmitService.submit(self._signed_params())
        changed_params = self._signed_params(money="19.50")

        with self.assertRaises(EpaySubmitError):
            EpaySubmitService.submit(changed_params)

        self.assertEqual(Invoice.objects.count(), 1)
        self.assertEqual(EpayOrder.objects.count(), 1)
        self.assertEqual(mock_check.call_count, 2)
        mock_initialize.assert_called_once()

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_rejects_money_without_two_decimal_string(
        self,
        mock_check,
        mock_initialize,
    ):
        for money in ("18", "18.5"):
            with self.subTest(money=money), self.assertRaises(EpaySubmitError):
                EpaySubmitService.submit(
                    self._signed_params(
                        out_trade_no=f"EPAY-SUBMIT-{money}",
                        money=money,
                    )
                )

        self.assertFalse(Invoice.objects.exists())
        mock_check.assert_not_called()
        mock_initialize.assert_not_called()

    def test_submit_serializer_requires_money_two_decimal_string(self):
        valid_params = self._signed_params(money="18.50")
        serializer = EpaySubmitSerializer(data=valid_params)
        self.assertTrue(serializer.is_valid(), serializer.errors)

        for money in ("18", "18.5", Decimal("18.50")):
            params = self._signed_params(money=money)
            with self.subTest(money=money):
                serializer = EpaySubmitSerializer(data=params)
                self.assertFalse(serializer.is_valid())
                self.assertIn("money", serializer.errors)

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    def test_create_invoice_and_order_reuses_existing_order_after_integrity_error(
        self,
        mock_initialize,
    ):
        params = {
            "pid": self.merchant.pid,
            "type": "usdt",
            "out_trade_no": "EPAY-CONCURRENT-1001",
            "notify_url": "https://merchant.example.com/notify",
            "return_url": "https://merchant.example.com/return",
            "name": "VIP Package",
            "money": Decimal("18.50"),
            "param": "user=42",
            "sign_type": "MD5",
        }
        existing_invoice = Invoice.objects.create(
            project=self.project,
            out_no=params["out_trade_no"],
            title=params["name"],
            currency=self.merchant.default_currency,
            amount=params["money"],
            methods=Invoice.available_methods(self.project),
            redirect_url=params["return_url"],
            expires_at=timezone.now() + timedelta(minutes=10),
            protocol=InvoiceProtocol.EPAY_V1,
        )
        EpayOrder.objects.create(
            invoice=existing_invoice,
            merchant=self.merchant,
            pid=str(self.merchant.pid),
            trade_no=existing_invoice.sys_no,
            out_trade_no=params["out_trade_no"],
            type=params["type"],
            name=params["name"],
            money=params["money"],
            notify_url=params["notify_url"],
            return_url=params["return_url"],
            param=params["param"],
            sign_type=params["sign_type"],
            raw_request={"out_trade_no": params["out_trade_no"]},
        )

        with patch(
            "invoices.epay_service.Invoice.objects.create",
            side_effect=IntegrityError("duplicate out_trade_no"),
        ):
            invoice = EpaySubmitService._create_invoice_and_order(
                merchant=self.merchant,
                params=params,
                raw_request={"out_trade_no": params["out_trade_no"]},
            )

        self.assertEqual(invoice, existing_invoice)
        mock_initialize.assert_not_called()


class EpaySubmitRouteTests(TestCase):
    def setUp(self):
        EpaySubmitServiceTests.setUp(self)

    def _signed_params(self, **overrides):
        return EpaySubmitServiceTests._signed_params(self, **overrides)

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_post_submit_php_redirects_to_hosted_checkout(
        self,
        mock_check,
        mock_initialize,
    ):
        mock_initialize.side_effect = lambda invoice: invoice

        response = self.client.post("/submit.php", data=self._signed_params())

        invoice = Invoice.objects.get(out_no="EPAY-SUBMIT-1001")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response["Location"],
            f"http://testserver/pay/{invoice.sys_no}",
        )
        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_get_epay_submit_php_redirects_to_hosted_checkout(
        self,
        mock_check,
        mock_initialize,
    ):
        mock_initialize.side_effect = lambda invoice: invoice

        response = self.client.get(
            "/epay/submit.php",
            data=self._signed_params(out_trade_no="EPAY-SUBMIT-GET-1001"),
        )

        invoice = Invoice.objects.get(out_no="EPAY-SUBMIT-GET-1001")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response["Location"],
            f"http://testserver/pay/{invoice.sys_no}",
        )
        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_bad_sign_returns_fail_plain_text(
        self,
        mock_check,
        mock_initialize,
    ):
        params = self._signed_params()
        params["sign"] = "bad-sign"

        response = self.client.post("/submit.php", data=params)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response["Content-Type"], "text/plain; charset=utf-8")
        self.assertTrue(response.content.decode().startswith("fail"))
        mock_check.assert_not_called()
        mock_initialize.assert_not_called()

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_post_submit_php_passes_csrf_check(
        self,
        mock_check,
        mock_initialize,
    ):
        """外部商户 POST 不携带 CSRF token，必须正常建单而非 403。"""
        from django.test import Client

        mock_initialize.side_effect = lambda invoice: invoice
        csrf_client = Client(enforce_csrf_checks=True)

        response = csrf_client.post(
            "/submit.php",
            data=self._signed_params(out_trade_no="EPAY-CSRF-1001"),
        )

        self.assertEqual(response.status_code, 302)


class EpayNotifyTests(TestCase):
    def setUp(self):
        EpaySubmitServiceTests.setUp(self)

    def _signed_params(self, **overrides):
        return EpaySubmitServiceTests._signed_params(self, **overrides)

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_build_notify_payload_uses_epay_fields_and_signature(
        self, mock_check, mock_initialize
    ):
        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params(param="u=42"))

        payload = EpaySubmitService.build_notify_payload(invoice)

        self.assertEqual(payload["pid"], str(self.merchant.pid))
        self.assertEqual(payload["trade_no"], invoice.sys_no)
        self.assertEqual(payload["out_trade_no"], "EPAY-SUBMIT-1001")
        self.assertEqual(payload["type"], "usdt")
        self.assertEqual(payload["name"], "VIP Package")
        self.assertEqual(payload["money"], "18.50")
        self.assertEqual(payload["trade_status"], "TRADE_SUCCESS")
        self.assertEqual(payload["param"], "u=42")
        self.assertEqual(payload["sign_type"], "MD5")
        self.assertTrue(verify_epay_v1_sign(payload, self.merchant.signing_key))

    @patch("webhooks.service.WebhookService.enqueue_delivery")
    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_enqueue_paid_notify_creates_get_query_event(
        self, mock_check, mock_initialize, enqueue_mock
    ):
        from webhooks.models import WebhookEvent

        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params())

        event = EpaySubmitService.enqueue_paid_notify(invoice)

        self.assertEqual(event.delivery_url, "https://merchant.example.com/notify")
        self.assertEqual(event.delivery_method, WebhookEvent.DeliveryMethod.GET_QUERY)
        self.assertEqual(event.expected_response_body, "success")
        self.assertEqual(event.payload["trade_status"], "TRADE_SUCCESS")
        enqueue_mock.assert_called_once_with(event)
        invoice.epay_order.refresh_from_db()
        self.assertEqual(invoice.epay_order.notify_event_id, event.pk)

    @patch("invoices.service.send_internal_callback")
    @patch("invoices.epay_service.EpaySubmitService.enqueue_paid_notify")
    @patch("webhooks.service.WebhookService.create_event")
    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_confirm_epay_invoice_uses_epay_notify_not_native_webhook(
        self, mock_check, mock_initialize, native_webhook_mock, epay_notify_mock, _
    ):
        from invoices.models import InvoiceStatus
        from invoices.service import InvoiceService

        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params())
        Invoice.objects.filter(pk=invoice.pk).update(
            status=InvoiceStatus.CONFIRMING,
            crypto=self.crypto,
        )
        invoice.refresh_from_db()

        InvoiceService.confirm_invoice(invoice)

        epay_notify_mock.assert_called_once()
        native_webhook_mock.assert_not_called()

    @patch("invoices.service.send_internal_callback")
    @patch("invoices.epay_service.EpaySubmitService.enqueue_paid_notify")
    @patch("invoices.service.WebhookService.create_event")
    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_confirm_native_invoice_uses_native_webhook_not_epay(
        self, mock_check, mock_initialize, native_webhook_mock, epay_notify_mock, _
    ):
        from invoices.models import InvoiceStatus
        from invoices.service import InvoiceService

        native_invoice = Invoice.objects.create(
            project=self.project,
            out_no="NATIVE-1001",
            title="Native Order",
            currency="CNY",
            amount=Decimal("10.00"),
            methods={},
            protocol=InvoiceProtocol.NATIVE,
            expires_at=timezone.now() + timedelta(minutes=10),
            status=InvoiceStatus.CONFIRMING,
            crypto=self.crypto,
        )

        InvoiceService.confirm_invoice(native_invoice)

        native_webhook_mock.assert_called_once()
        epay_notify_mock.assert_not_called()
