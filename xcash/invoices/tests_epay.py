from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from django.test import TestCase
from django.test import override_settings
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
from invoices.models import InvoiceStatus
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

    def test_epay_merchant_signing_key_returns_secret_key(self):
        merchant = EpayMerchant.objects.create(
            project=self.project,
            pid=1002,
            secret_key="epay-test-secret",
        )

        self.assertEqual(merchant.signing_key, merchant.secret_key)
        self.assertNotEqual(merchant.signing_key, self.project.hmac_key)

    def test_epay_merchant_signing_key_raises_when_secret_key_empty(self):
        # 防御性 fail-fast：即便绕过 admin 表单/migration 写入空 secret_key，
        # 也必须在签名前抛错，杜绝以空 KEY 计算合法签名的攻击面。
        merchant = EpayMerchant(
            project=self.project,
            pid=9999,
            secret_key="",
        )

        with self.assertRaises(ValueError):
            _ = merchant.signing_key

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
            "currency": "CNY",
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
        self.assertEqual(invoice.return_url, "https://merchant.example.com/return")
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
    def test_submit_uses_currency_from_request(self, mock_check, mock_initialize):
        # 商户必须显式指定 currency，且会写到 Invoice.currency 上；
        # CNY 在 setUp 中已注入 Fiat 表。
        mock_initialize.side_effect = lambda invoice: invoice

        invoice = EpaySubmitService.submit(self._signed_params(currency="CNY"))

        invoice.refresh_from_db()
        self.assertEqual(invoice.currency, "CNY")
        mock_check.assert_called_once()

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_defaults_to_cny_when_currency_omitted(
        self, mock_check, mock_initialize,
    ):
        # EPay V1 标准协议没有 currency 字段（typecho/wordpress/discuz 插件都不会传），
        # 缺省时按协议事实默认 CNY 落库；签名也基于不含 currency 的原始参数计算。
        mock_initialize.side_effect = lambda invoice: invoice
        params = self._signed_params()
        del params["currency"]
        params["sign"] = build_epay_v1_sign(params, self.merchant.signing_key)

        invoice = EpaySubmitService.submit(params)

        invoice.refresh_from_db()
        self.assertEqual(invoice.currency, "CNY")

    def test_submit_rejects_currency_not_in_fiat_table(self):
        # JPY 不在 setUp 注入的 Fiat 中，应触发 Fiat 校验失败。
        params = self._signed_params(currency="JPY")
        params["sign"] = build_epay_v1_sign(params, self.merchant.signing_key)

        with self.assertRaises(EpaySubmitError):
            EpaySubmitService.submit(params)
        self.assertFalse(Invoice.objects.filter(out_no=params["out_trade_no"]).exists())

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_normalizes_currency_to_uppercase(self, mock_check, mock_initialize):
        # 商户大小写不一致是常见错配，落库统一为大写避免后续匹配失败。
        # 注意签名是按原始 "cny" 计算的：currency 进入签名前 raw_params 还是小写。
        mock_initialize.side_effect = lambda invoice: invoice
        params = self._signed_params(currency="cny")
        # 用 currency="cny" 重新签名才能通过签名校验
        params["sign"] = build_epay_v1_sign(params, self.merchant.signing_key)

        invoice = EpaySubmitService.submit(params)

        invoice.refresh_from_db()
        self.assertEqual(invoice.currency, "CNY")

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
    def test_submit_accepts_money_in_loose_format(
        self,
        mock_check,
        mock_initialize,
    ):
        # EPay V1 协议允许 "整数"、"一位小数"、"两位小数" 三种 money 写法。
        # 各 subTest 必须使用不同的 out_trade_no，否则会命中已有订单的幂等分支，
        # 绕过对实际格式接受的验证。
        mock_initialize.side_effect = lambda invoice: invoice

        cases = (
            ("18", Decimal("18.00")),
            ("18.5", Decimal("18.50")),
            ("18.50", Decimal("18.50")),
        )
        for money, expected in cases:
            out_trade_no = f"EPAY-SUBMIT-LOOSE-{money}"
            with self.subTest(money=money):
                invoice = EpaySubmitService.submit(
                    self._signed_params(
                        out_trade_no=out_trade_no,
                        money=money,
                    )
                )
                invoice.refresh_from_db()
                self.assertEqual(invoice.amount, expected)
                self.assertEqual(invoice.epay_order.money, expected)

        self.assertEqual(Invoice.objects.count(), len(cases))
        self.assertEqual(mock_check.call_count, len(cases))
        self.assertEqual(mock_initialize.call_count, len(cases))

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_accepts_missing_optional_type_param_and_return_url(
        self,
        mock_check,
        mock_initialize,
    ):
        # EPay V1 协议中 type / param / return_url 都是可选字段，typecho/wordpress/discuz 等
        # 真实商户插件常常完全不发送它们；入口必须能在两个键缺省时正常成单，并在重复提交时
        # 命中幂等分支（_validate_idempotent_order 会比较 params["param"]/["return_url"]
        # 与模型字段，两侧应统一为空字符串）。
        mock_initialize.side_effect = lambda invoice: invoice

        params = {
            "pid": str(self.merchant.pid),
            "out_trade_no": "EPAY-SUBMIT-NO-OPTIONALS",
            "notify_url": "https://merchant.example.com/notify",
            "name": "VIP Package",
            "money": "18.50",
            "currency": "CNY",
            "sign_type": "MD5",
        }
        params["sign"] = build_epay_v1_sign(params, self.merchant.signing_key)

        invoice = EpaySubmitService.submit(params)
        invoice.refresh_from_db()
        epay_order = invoice.epay_order
        self.assertEqual(invoice.return_url, "")
        self.assertEqual(epay_order.type, "")
        self.assertEqual(epay_order.return_url, "")
        self.assertEqual(epay_order.param, "")

        replay_invoice = EpaySubmitService.submit(params)
        self.assertEqual(replay_invoice.pk, invoice.pk)
        self.assertEqual(Invoice.objects.count(), 1)
        self.assertEqual(EpayOrder.objects.count(), 1)
        self.assertEqual(mock_check.call_count, 2)
        mock_initialize.assert_called_once_with(invoice)

    def test_submit_serializer_accepts_loose_money_format(self):
        # 接受：整数、一位小数、两位小数。
        for money in ("18", "18.5", "18.50"):
            params = self._signed_params(money=money)
            with self.subTest(valid=money):
                serializer = EpaySubmitSerializer(data=params)
                self.assertTrue(serializer.is_valid(), serializer.errors)

        # 拒绝：
        # - 非字符串、空串、纯字母、缺整数部分、3+ 位小数、负数；
        # - 0 / 0.0 / 0.00（< 0.01 拦下）；
        # - 整数部分超过 24 位（Invoice.amount 是 max_digits=32/decimal_places=8，
        #   只能容纳 24 位整数；必须在 serializer 阶段拦下，避免 DB 写入 500）。
        valid_large = self._signed_params(
            money=("9" * 24) + ".99",
            out_trade_no="EPAY-SUBMIT-MONEY-24",
        )
        serializer = EpaySubmitSerializer(data=valid_large)
        self.assertTrue(serializer.is_valid(), serializer.errors)

        invalid_cases = (
            Decimal("18.50"),
            "",
            "abc",
            ".50",
            "18.555",
            "-18",
            "0",
            "0.0",
            "0.00",
            "9" * 25,
            "9" * 30,
            "9" * 30 + ".99",
        )
        for money in invalid_cases:
            params = self._signed_params(money=money)
            with self.subTest(invalid=money):
                serializer = EpaySubmitSerializer(data=params)
                self.assertFalse(serializer.is_valid())
                self.assertIn("money", serializer.errors)

    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_rejects_when_project_has_no_payment_methods(self, mock_check):
        # EPay 建单若项目没有任何可用收款方式，必须拒绝而不是创建不可支付账单并占用商户单号。
        RecipientAddress.objects.filter(project=self.project).delete()

        params = self._signed_params(out_trade_no="EPAY-NO-METHODS-1001")

        with self.assertRaises(EpaySubmitError):
            EpaySubmitService.submit(params)

        self.assertFalse(Invoice.objects.filter(out_no="EPAY-NO-METHODS-1001").exists())
        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )

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
            "currency": "CNY",
            "param": "user=42",
            "sign_type": "MD5",
        }
        existing_invoice = Invoice.objects.create(
            project=self.project,
            out_no=params["out_trade_no"],
            title=params["name"],
            currency=params["currency"],
            amount=params["money"],
            methods=Invoice.available_methods(self.project),
            return_url=params["return_url"],
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

        response = self.client.post("/epay/submit.php", data=self._signed_params())

        invoice = Invoice.objects.get(out_no="EPAY-SUBMIT-1001")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"/pay/{invoice.sys_no}")
        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_get_submit_php_redirects_to_hosted_checkout(
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
        self.assertEqual(response["Location"], f"/pay/{invoice.sys_no}")
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

        response = self.client.post("/epay/submit.php", data=params)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response["Content-Type"], "text/plain; charset=utf-8")
        # 严格相等：响应体只能是 "fail"，不允许带冒号或任何错误细节，
        # 否则商户/攻击者可据此区分错误类型，造成 pid 枚举或验证规则泄漏。
        self.assertEqual(response.content, b"fail")
        mock_check.assert_not_called()
        mock_initialize.assert_not_called()

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_invalid_pid_and_invalid_sign_return_identical_response(
        self,
        mock_check,
        mock_initialize,
    ):
        # 无效 pid（商户不存在）与无效签名（商户存在但签错）必须对外不可区分，
        # 否则攻击者可遍历 pid 区间识别有效商户 ID。
        invalid_pid_params = self._signed_params(pid="999999")
        invalid_sign_params = self._signed_params()
        invalid_sign_params["sign"] = "bad-sign"

        responses = [
            self.client.post("/epay/submit.php", data=invalid_pid_params),
            self.client.post("/epay/submit.php", data=invalid_sign_params),
        ]

        for response in responses:
            self.assertEqual(response.status_code, 400)
            self.assertEqual(
                response["Content-Type"],
                "text/plain; charset=utf-8",
            )
            self.assertEqual(response.content, b"fail")

        mock_check.assert_not_called()
        mock_initialize.assert_not_called()

    @patch("invoices.epay_views.logger")
    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_error_logged_with_pid_and_client_ip(
        self,
        mock_check,
        mock_initialize,
        mock_logger,
    ):
        # 详情必须落到 structlog（pid / client_ip / error），
        # 避免对外吞错后服务端也无法定位问题。
        params = self._signed_params()
        params["sign"] = "bad-sign"

        response = self.client.post("/epay/submit.php", data=params)

        self.assertEqual(response.status_code, 400)
        self.assertTrue(mock_logger.warning.called)
        _, kwargs = mock_logger.warning.call_args
        self.assertEqual(kwargs["pid"], str(self.merchant.pid))
        self.assertTrue(kwargs["client_ip"])
        self.assertIsInstance(kwargs["error"], str)
        self.assertTrue(kwargs["error"])

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
            "/epay/submit.php",
            data=self._signed_params(out_trade_no="EPAY-CSRF-1001"),
        )

        self.assertEqual(response.status_code, 302)

    @override_settings(EPAY_SUBMIT_RATE_LIMIT="2/m")
    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_submit_php_rate_limited_after_threshold(
        self,
        mock_check,
        mock_initialize,
    ):
        # 防止有效 pid + 错误 sign 的查询型 DoS：同一 IP 每分钟超阈值后必须返回 429。
        # 测试用 override_settings 把阈值从 60/m 调到 2/m，避免真发 60 个请求。
        # 用独立 X-Forwarded-For，让计数 key 与其他测试隔离，避免 Redis 中遗留计数串扰。
        mock_initialize.side_effect = lambda invoice: invoice
        attacker_ip = "203.0.113.77"
        # 库会优先读 X-Forwarded-For 作为 IP 维度 key，本测试单独清一下
        # Django cache（即使 ratelimit 走的是独立 Redis backend，也保险）。
        cache.clear()

        # 前两个请求：合法签名，应当被放行。
        first = self.client.post(
            "/epay/submit.php",
            data=self._signed_params(out_trade_no="EPAY-RATELIMIT-1"),
            HTTP_X_FORWARDED_FOR=attacker_ip,
        )
        self.assertEqual(first.status_code, 302)

        second = self.client.post(
            "/epay/submit.php",
            data=self._signed_params(out_trade_no="EPAY-RATELIMIT-2"),
            HTTP_X_FORWARDED_FOR=attacker_ip,
        )
        self.assertEqual(second.status_code, 302)

        # 第三个请求：超过阈值，必须 429，且未触发到下游建单逻辑。
        before_initialize_calls = mock_initialize.call_count
        third = self.client.post(
            "/epay/submit.php",
            data=self._signed_params(out_trade_no="EPAY-RATELIMIT-3"),
            HTTP_X_FORWARDED_FOR=attacker_ip,
        )
        self.assertEqual(third.status_code, 429)
        # 限流必须在进入业务逻辑之前生效，不能空打一次 InvoiceService。
        self.assertEqual(mock_initialize.call_count, before_initialize_calls)


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

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_build_return_url_appends_signed_query_for_completed_invoice(
        self, mock_check, mock_initialize
    ):
        # 已完成的 EPay V1 订单：build_return_url 必须把 EPay 标准同步跳转
        # 参数 + 签名拼到商户 return_url 末尾，且签名能通过 verify_epay_v1_sign。
        from urllib.parse import parse_qs
        from urllib.parse import urlparse

        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params(param="u=42"))
        Invoice.objects.filter(pk=invoice.pk).update(status=InvoiceStatus.COMPLETED)
        invoice.refresh_from_db()

        return_url = EpaySubmitService.build_return_url(invoice)

        parsed = urlparse(return_url)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "merchant.example.com")
        self.assertEqual(parsed.path, "/return")
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        self.assertEqual(query["pid"], str(self.merchant.pid))
        self.assertEqual(query["trade_no"], invoice.sys_no)
        self.assertEqual(query["out_trade_no"], "EPAY-SUBMIT-1001")
        self.assertEqual(query["money"], "18.50")
        self.assertEqual(query["trade_status"], "TRADE_SUCCESS")
        self.assertEqual(query["sign_type"], "MD5")
        self.assertEqual(query["param"], "u=42")
        self.assertTrue(verify_epay_v1_sign(query, self.merchant.signing_key))

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_build_return_url_preserves_existing_query_on_return_url(
        self, mock_check, mock_initialize
    ):
        # 商户 return_url 自带 query（如 ?source=xcash）时，原 query 必须保留，
        # EPay 字段追加在后面而不能覆盖。
        from urllib.parse import parse_qs
        from urllib.parse import urlparse

        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(
            self._signed_params(return_url="https://merchant.example.com/return?source=xcash")
        )
        Invoice.objects.filter(pk=invoice.pk).update(status=InvoiceStatus.COMPLETED)
        invoice.refresh_from_db()

        return_url = EpaySubmitService.build_return_url(invoice)
        parsed = urlparse(return_url)
        query = parse_qs(parsed.query)
        self.assertEqual(query["source"], ["xcash"])
        self.assertIn("sign", query)
        self.assertEqual(query["trade_status"], ["TRADE_SUCCESS"])

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_build_return_url_returns_empty_when_not_completed(
        self, mock_check, mock_initialize
    ):
        # waiting / confirming 阶段不应把同步跳转 URL 暴露给前端：用户还没付完
        # 不能伪造 TRADE_SUCCESS 跳转回商户。
        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params())
        invoice.refresh_from_db()

        self.assertEqual(invoice.status, InvoiceStatus.WAITING)
        self.assertEqual(EpaySubmitService.build_return_url(invoice), "")

        Invoice.objects.filter(pk=invoice.pk).update(status=InvoiceStatus.CONFIRMING)
        invoice.refresh_from_db()
        self.assertEqual(EpaySubmitService.build_return_url(invoice), "")

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_build_return_url_returns_empty_when_return_url_not_configured(
        self, mock_check, mock_initialize
    ):
        # 商户没传 return_url（EPay V1 允许）时不应拼出无效 URL。
        mock_initialize.side_effect = lambda invoice: invoice
        params = {
            "pid": str(self.merchant.pid),
            "type": "usdt",
            "out_trade_no": "EPAY-RETURN-NO-URL",
            "notify_url": "https://merchant.example.com/notify",
            "name": "VIP Package",
            "money": "18.50",
            "currency": "CNY",
            "sign_type": "MD5",
        }
        params["sign"] = build_epay_v1_sign(params, self.merchant.signing_key)
        invoice = EpaySubmitService.submit(params)
        Invoice.objects.filter(pk=invoice.pk).update(status=InvoiceStatus.COMPLETED)
        invoice.refresh_from_db()

        self.assertEqual(EpaySubmitService.build_return_url(invoice), "")

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_public_serializer_returns_signed_return_url_when_completed(
        self, mock_check, mock_initialize
    ):
        # InvoicePublicSerializer 是 SPA 拉取的对外接口；完成态下必须把签名 query
        # 输出在 return_url 上，否则前端「返回商户」按钮跳过去会 error。
        from urllib.parse import parse_qs
        from urllib.parse import urlparse

        from invoices.serializers import InvoicePublicSerializer

        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params(param="u=42"))
        Invoice.objects.filter(pk=invoice.pk).update(status=InvoiceStatus.COMPLETED)
        invoice.refresh_from_db()

        data = InvoicePublicSerializer(invoice).data
        parsed = urlparse(data["return_url"])
        self.assertEqual(parsed.path, "/return")
        query = parse_qs(parsed.query)
        self.assertEqual(query["trade_status"], ["TRADE_SUCCESS"])
        self.assertEqual(query["trade_no"], [invoice.sys_no])
        self.assertIn("sign", query)

    @patch("invoices.epay_service.InvoiceService.initialize_invoice")
    @patch("invoices.epay_service.check_saas_permission")
    def test_public_serializer_returns_raw_return_url_when_not_completed(
        self, mock_check, mock_initialize
    ):
        # 未完成态不能让前端拿到签名 URL：用户能点这个按钮就等于无支付完成跳转回商户。
        from invoices.serializers import InvoicePublicSerializer

        mock_initialize.side_effect = lambda invoice: invoice
        invoice = EpaySubmitService.submit(self._signed_params())
        invoice.refresh_from_db()

        data = InvoicePublicSerializer(invoice).data
        self.assertEqual(data["return_url"], "https://merchant.example.com/return")

    def test_build_return_url_returns_empty_for_native_protocol(self):
        # native 协议没有 epay_order，build_return_url 必须早退而不报错。
        native_invoice = Invoice.objects.create(
            project=self.project,
            out_no="NATIVE-RETURN-1",
            title="Native",
            currency="CNY",
            amount=Decimal("10.00"),
            methods={},
            protocol=InvoiceProtocol.NATIVE,
            status=InvoiceStatus.COMPLETED,
            return_url="https://merchant.example.com/return",
            expires_at=timezone.now() + timedelta(minutes=10),
        )

        self.assertEqual(EpaySubmitService.build_return_url(native_invoice), "")

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
