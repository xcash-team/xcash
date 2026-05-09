from datetime import timedelta
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from django.test import TestCase
from django.utils import timezone

from chains.models import Wallet
from invoices.models import EpayMerchant
from invoices.models import EpayOrder
from invoices.models import Invoice
from invoices.models import InvoiceProtocol
from projects.models import Project


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
