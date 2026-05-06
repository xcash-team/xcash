import threading
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.core.cache import cache
from django.db import IntegrityError
from django.db import close_old_connections
from django.db import connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.test import override_settings
from django.utils import timezone
from rest_framework.test import APIRequestFactory
from web3 import Web3

from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from chains.models import Wallet
from common.error_codes import ErrorCode
from common.exceptions import APIError
from core.models import PlatformSettings
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.models import Fiat
from invoices.exceptions import InvoiceAllocationError
from invoices.exceptions import InvoiceStatusError
from invoices.models import Invoice
from invoices.models import InvoicePaySlot
from invoices.models import InvoicePaySlotDiscardReason
from invoices.models import InvoicePaySlotStatus
from invoices.models import InvoiceStatus
from invoices.service import InvoiceService
from invoices.tasks import check_expired
from invoices.tasks import fallback_invoice_expired
from invoices.viewsets import InvoiceViewSet
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage
from users.models import User


class InvoiceTestMixin:
    """共享的测试基础数据构造 mixin，避免各测试类重复创建 User/Project/Crypto/Chain 等。"""

    def setup_base_fixtures(
        self,
        *,
        username: str = "merchant",
        project_name: str = "TestProject",
        crypto_symbol: str = "USDT",
        chain_code: str = "eth-test",
        chain_id: int = 9999,
        with_recipient: bool = True,
    ):
        self.user = User.objects.create(username=username)
        self.project = Project.objects.create(
            name=project_name,
            wallet=Wallet.objects.create(),
        )
        self.crypto = Crypto.objects.create(
            name=f"{crypto_symbol} Token",
            symbol=crypto_symbol,
            prices={"USD": "1"},
            coingecko_id=f"{crypto_symbol.lower()}-test",
        )
        self.chain = Chain.objects.create(
            name=f"Chain {chain_code}",
            code=chain_code,
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name=f"ETH {chain_code}",
                symbol=f"ETH-{chain_code.upper()[:4]}",
                coingecko_id=f"eth-{chain_code}",
            ),
            chain_id=chain_id,
            rpc="http://localhost:8545",
            active=True,
        )
        Fiat.objects.get_or_create(code="USD")
        if with_recipient:
            self.recipient_address = Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000A1"
            )
            RecipientAddress.objects.create(
                name="收款地址-test",
                project=self.project,
                chain_type=ChainType.EVM,
                address=self.recipient_address,
                usage=RecipientAddressUsage.INVOICE,
            )

    def create_test_invoice(self, *, out_no: str = "test-order", **kwargs) -> Invoice:
        defaults = {
            "project": self.project,
            "out_no": out_no,
            "title": "Test invoice",
            "currency": self.crypto.symbol,
            "amount": Decimal("10"),
            "methods": {self.crypto.symbol: [self.chain.code]},
            "expires_at": timezone.now() + timedelta(minutes=10),
        }
        defaults.update(kwargs)
        return Invoice.objects.create(**defaults)


class InvoiceInitializationTests(TestCase):
    def setUp(self):
        self.user = User.objects.create(username="merchant")
        self.project = Project.objects.create(
            name="Demo",
            wallet=Wallet.objects.create(),
        )
        self.eth = Crypto.objects.create(
            name="Ethereum",
            symbol="ETH",
            coingecko_id="ethereum",
        )
        self.chain = Chain.objects.create(
            name="Ethereum",
            code="eth",
            type=ChainType.EVM,
            native_coin=self.eth,
            chain_id=1,
            rpc="http://localhost:8545",
            active=True,
        )

    def test_remote_signer_project_wallet_can_initialize_and_select_method_without_local_keys(
        self,
    ):
        # 支付链路本身不依赖项目钱包持钥；即使钱包助记词只在 signer 中，也应能正常创建账单和分配收款地址。
        remote_wallet = Wallet.objects.create()
        self.eth.prices = {"USD": "1"}
        self.eth.save(update_fields=["prices"])
        with patch("projects.signals.Wallet.generate", return_value=remote_wallet):
            project = Project.objects.create(
                name="RemoteSignerInvoice",
                wallet=remote_wallet,
            )
        RecipientAddress.objects.create(
            name="RemoteSigner 收款地址",
            project=project,
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000b1"
            ),
            usage=RecipientAddressUsage.INVOICE,
        )
        invoice = Invoice.objects.create(
            project=project,
            out_no="remote-signer-invoice",
            title="Remote invoice",
            currency="USD",
            amount=Decimal("15"),
            methods={"ETH": ["eth"]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )

        with (
            patch("invoices.tasks.check_expired.apply_async"),
            patch.object(
                Invoice,
                "select_method",
                wraps=invoice.select_method,
            ) as select_method_mock,
            patch(
                "invoices.service.CryptoService.get_by_symbol",
                return_value=self.eth,
            ),
            patch(
                "invoices.service.ChainService.get_by_code",
                return_value=self.chain,
            ),
            patch(
                "invoices.service.FiatService.get_by_code",
                side_effect=lambda code: SimpleNamespace(
                    code=code,
                    fiat_price=Mock(return_value=Decimal("1")),
                ),
            ),
            patch(
                "invoices.models.FiatService.to_crypto",
                return_value=Decimal("15"),
            ),
            patch(
                "invoices.models.FiatService.get_by_code",
                side_effect=lambda code: SimpleNamespace(
                    code=code,
                    fiat_price=Mock(return_value=Decimal("1")),
                ),
            ),
            self.captureOnCommitCallbacks(execute=True),
        ):
            InvoiceService.initialize_invoice(invoice)

        invoice.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.WAITING)
        self.assertEqual(
            invoice.pay_address,
            Web3.to_checksum_address("0x00000000000000000000000000000000000000b1"),
        )
        select_method_mock.assert_called_once_with(self.eth, self.chain)


class InvoicePaySlotTests(TestCase):
    def setUp(self):
        self.user = User.objects.create(username="merchant-slots")
        self.project = Project.objects.create(
            name="SlotProject",
            wallet=Wallet.objects.create(),
        )
        self.crypto = Crypto.objects.create(
            name="Tether USD",
            symbol="USDT",
            coingecko_id="tether-invoice-slots",
        )
        self.chain_a = Chain.objects.create(
            name="Ethereum Slot A",
            code="eth-slot-a",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="Ethereum Native Slot A",
                symbol="ETH-SLOTA",
                coingecko_id="ethereum-invoice-slot-a",
            ),
            chain_id=1888,
            rpc="http://localhost:8545",
            active=True,
        )
        self.chain_b = Chain.objects.create(
            name="Ethereum Slot B",
            code="eth-slot-b",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="Ethereum Native Slot B",
                symbol="ETH-SLOTB",
                coingecko_id="ethereum-invoice-slot-b",
            ),
            chain_id=2888,
            rpc="http://localhost:8545",
            active=True,
        )
        Fiat.objects.get_or_create(code="USD")
        RecipientAddress.objects.create(
            name="收款地址-1",
            project=self.project,
            chain_type=ChainType.EVM,
            address="0x00000000000000000000000000000000000000A1",
            usage=RecipientAddressUsage.INVOICE,
        )

    def create_invoice(self, *, out_no: str = "slot-order") -> Invoice:
        return Invoice.objects.create(
            project=self.project,
            out_no=out_no,
            title="Slot invoice",
            currency="USDT",
            amount=Decimal("10"),
            methods={"USDT": ["eth-slot-a", "eth-slot-b"]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )

    def create_transfer(
        self, *, chain: Chain, pay_amount: Decimal, pay_address: str
    ) -> OnchainTransfer:
        now = timezone.now()
        return OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash=f"0x{chain.chain_id:08x}{int(now.timestamp() * 1000000):056x}",
            event_id=f"{chain.code}-{int(now.timestamp() * 1000)}",
            crypto=self.crypto,
            from_address="0x00000000000000000000000000000000000000B1",
            to_address=pay_address,
            value=Decimal(pay_amount * Decimal("100000000")),
            amount=pay_amount,
            timestamp=int(now.timestamp()),
            datetime=now,
        )

    def test_select_method_keeps_only_two_newest_slots(self):
        # 账单切换支付方式时最多保留两个活跃槽位，更老的槽位直接失效。
        invoice = self.create_invoice()

        invoice.select_method(self.crypto, self.chain_a)
        invoice.select_method(self.crypto, self.chain_b)
        invoice.select_method(self.crypto, self.chain_a)

        invoice.refresh_from_db()
        pay_slots = list(invoice.pay_slots.order_by("version"))
        self.assertEqual([slot.version for slot in pay_slots], [1, 2, 3])
        self.assertEqual(
            [slot.status for slot in pay_slots],
            [
                InvoicePaySlotStatus.DISCARDED,
                InvoicePaySlotStatus.ACTIVE,
                InvoicePaySlotStatus.ACTIVE,
            ],
        )
        self.assertEqual(
            pay_slots[0].discard_reason,
            InvoicePaySlotDiscardReason.OVERFLOW,
        )
        self.assertEqual(invoice.pay_address, pay_slots[2].pay_address)
        self.assertEqual(invoice.pay_amount, pay_slots[2].pay_amount)

    def test_try_match_invoice_supports_previous_active_slot(self):
        # 当前快照虽然指向最新槽位，但历史上仍 active 的上一槽位付款依然必须命中同一账单。
        invoice = self.create_invoice(out_no="slot-match")

        invoice.select_method(self.crypto, self.chain_a)
        first_slot = invoice.pay_slots.get(version=1)
        invoice.select_method(self.crypto, self.chain_b)
        second_slot = invoice.pay_slots.get(version=2)

        transfer = self.create_transfer(
            chain=self.chain_a,
            pay_amount=first_slot.pay_amount,
            pay_address=first_slot.pay_address,
        )

        matched = InvoiceService.try_match_invoice(transfer)

        self.assertTrue(matched)
        invoice.refresh_from_db()
        first_slot.refresh_from_db()
        second_slot.refresh_from_db()
        transfer.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.CONFIRMING)
        self.assertEqual(invoice.transfer_id, transfer.pk)
        self.assertEqual(invoice.pay_address, first_slot.pay_address)
        self.assertEqual(invoice.pay_amount, first_slot.pay_amount)
        self.assertEqual(first_slot.status, InvoicePaySlotStatus.MATCHED)
        self.assertEqual(second_slot.status, InvoicePaySlotStatus.DISCARDED)
        self.assertEqual(
            second_slot.discard_reason,
            InvoicePaySlotDiscardReason.SETTLED,
        )
        self.assertEqual(transfer.type, TransferType.Invoice)

    def test_drop_invoice_reactivates_matched_slot(self):
        # 若链上观测后来被回滚，命中过的槽位要恢复为可再次匹配，避免账单永久卡死。
        invoice = self.create_invoice(out_no="slot-drop")

        invoice.select_method(self.crypto, self.chain_a)
        first_slot = invoice.pay_slots.get(version=1)
        transfer = self.create_transfer(
            chain=self.chain_a,
            pay_amount=first_slot.pay_amount,
            pay_address=first_slot.pay_address,
        )
        InvoiceService.try_match_invoice(transfer)
        invoice.refresh_from_db()

        InvoiceService.drop_invoice(invoice)

        invoice.refresh_from_db()
        first_slot.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.WAITING)
        self.assertIsNone(invoice.transfer_id)
        self.assertEqual(first_slot.status, InvoicePaySlotStatus.ACTIVE)
        self.assertIsNone(first_slot.discard_reason)
        self.assertIsNone(first_slot.matched_at)

    def test_check_expired_discards_active_slots(self):
        # 账单过期后必须释放活跃槽位，否则新的账单永远拿不到这组地址/金额组合。
        invoice = self.create_invoice(out_no="slot-expire")
        invoice.select_method(self.crypto, self.chain_a)
        active_slot = invoice.pay_slots.get(version=1)

        # 将账单设为已过期（check_expired 会校验 expires_at <= now）
        Invoice.objects.filter(pk=invoice.pk).update(
            expires_at=timezone.now() - timedelta(seconds=1),
        )

        check_expired(invoice.pk)

        invoice.refresh_from_db()
        active_slot.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.EXPIRED)
        self.assertEqual(active_slot.status, InvoicePaySlotStatus.DISCARDED)
        self.assertEqual(
            active_slot.discard_reason,
            InvoicePaySlotDiscardReason.EXPIRED,
        )


class InvoicePaySlotConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create(username="merchant-concurrency")
        self.project = Project.objects.create(
            name="ConcurrencyProject",
            wallet=Wallet.objects.create(),
        )
        self.crypto = Crypto.objects.create(
            name="Tether USD Concurrency",
            symbol="USDTC",
            prices={"USD": "1"},
            coingecko_id="tether-invoice-concurrency",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Concurrency",
            code="eth-concurrency",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="Ethereum Native Concurrency",
                symbol="ETH-CON",
                coingecko_id="ethereum-invoice-concurrency",
            ),
            chain_id=3888,
            rpc="http://localhost:8545",
            active=True,
        )
        Fiat.objects.get_or_create(code="USD")
        RecipientAddress.objects.create(
            name="收款地址-1",
            project=self.project,
            chain_type=ChainType.EVM,
            address="0x00000000000000000000000000000000000000A1",
            usage=RecipientAddressUsage.INVOICE,
        )

    def test_select_method_allocates_distinct_slots_under_concurrency(self):
        # 两个并发账单抢同一条链/币种支付槽时，必须各自拿到不同 pay slot。
        invoice1 = Invoice.objects.create(
            project=self.project,
            out_no="con-1",
            title="Concurrent 1",
            currency="USD",
            amount=Decimal("10"),
            methods={"USDTC": ["eth-concurrency"]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        invoice2 = Invoice.objects.create(
            project=self.project,
            out_no="con-2",
            title="Concurrent 2",
            currency="USD",
            amount=Decimal("10"),
            methods={"USDTC": ["eth-concurrency"]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        barrier = threading.Barrier(2)
        results: list[tuple[int, str, str]] = []
        errors: list[Exception] = []

        def allocate(invoice_id: int) -> None:
            close_old_connections()
            try:
                invoice = Invoice.objects.get(pk=invoice_id)
                barrier.wait()
                invoice.select_method(self.crypto, self.chain)
                invoice.refresh_from_db()
                active_slot = invoice.pay_slots.get(status=InvoicePaySlotStatus.ACTIVE)
                results.append(
                    (
                        invoice.pk,
                        active_slot.pay_address,
                        str(active_slot.pay_amount),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                # 线程内新开的数据库连接必须显式关闭，否则 TransactionTestCase flush 易死锁。
                connections.close_all()

        threads = [
            threading.Thread(target=allocate, args=(invoice1.pk,)),
            threading.Thread(target=allocate, args=(invoice2.pk,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertFalse(errors)
        self.assertEqual(len(results), 2)
        self.assertEqual(len({(address, amount) for _, address, amount in results}), 2)


class InvoiceDuplicateOutNoTests(TestCase):
    def setUp(self):
        # 屏蔽 SaaS 权限回调，避免单测触发真实 HTTP 请求
        patcher = patch("invoices.viewsets.check_saas_permission")
        self.mock_check_saas = patcher.start()
        self.addCleanup(patcher.stop)

    def test_viewset_create_translates_unique_conflict_to_api_error(self):
        # 并发重复 out_no 命中数据库唯一约束时，接口必须返回业务错误而不是 500。
        project = Project.objects.create(
            name="DuplicateInvoiceProject",
            wallet=Wallet.objects.create(),
        )
        request = APIRequestFactory().post(
            "/v1/invoice",
            {},
            format="json",
            HTTP_XC_APPID=project.appid,
        )
        serializer = SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "dup-order",
                "title": "Duplicate",
                "currency": "USD",
                "amount": Decimal("1"),
                "methods": {"ETH": ["eth"]},
                "duration": 10,
            },
            errors={},
        )

        with (
            patch.object(InvoiceViewSet, "get_serializer", return_value=serializer),
            patch(
                "invoices.viewsets.Invoice.objects.create",
                side_effect=IntegrityError,
            ),
        ):
            response = InvoiceViewSet.as_view({"post": "create"})(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["code"], ErrorCode.DUPLICATE_OUT_NO.code)


class InvoiceAllowedMethodsCapabilityTests(TestCase):
    def setUp(self):
        cache.clear()

    def test_available_methods_only_exposes_usdt_for_tron_invoice(self):
        project = Project.objects.create(
            name="Invoice Capability Project",
            wallet=Wallet.objects.create(),
        )
        trx = Crypto.objects.create(
            name="Tron Native",
            symbol="TRX",
            coingecko_id="tron-native-invoice-capability",
        )
        tron_usdt = Crypto.objects.create(
            name="Tether USD",
            symbol="USDT",
            coingecko_id="tether-tron-invoice-capability",
            decimals=6,
        )
        tron_usdc = Crypto.objects.create(
            name="USD Coin",
            symbol="USDC",
            coingecko_id="usd-coin-tron-invoice-capability",
            decimals=6,
        )
        tron_chain = Chain.objects.create(
            name="Tron Invoice Capability",
            code="tron-invoice-capability",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="http://tron.invalid",
            active=True,
        )
        ChainToken.objects.create(
            crypto=tron_usdt,
            chain=tron_chain,
            address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=tron_usdc,
            chain=tron_chain,
            address="TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8",
            decimals=6,
        )
        RecipientAddress.objects.create(
            name="tron-pay",
            project=project,
            chain_type=ChainType.TRON,
            address="TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb",
            usage=RecipientAddressUsage.INVOICE,
        )

        methods = Invoice.available_methods(project)

        self.assertEqual(methods["USDT"], [tron_chain.code])
        self.assertNotIn("USDC", methods)

    def test_available_methods_hide_evm_native_when_global_native_scanner_closed(self):
        project = Project.objects.create(
            name="Invoice Native Scanner Closed Project",
            wallet=Wallet.objects.create(),
        )
        native = Crypto.objects.create(
            name="Ethereum Invoice Native Closed",
            symbol="ETHINVCLOSED",
            coingecko_id="ethereum-invoice-native-closed",
        )
        usdt = Crypto.objects.create(
            name="USDT Invoice Native Closed",
            symbol="USDTINVCLOSED",
            coingecko_id="usdt-invoice-native-closed",
            decimals=6,
        )
        chain = Chain.objects.create(
            name="Ethereum Invoice Native Closed",
            code="eth-invoice-native-closed",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=9931,
            rpc="http://eth.invalid",
            active=True,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=chain,
            address="0x0000000000000000000000000000000000009931",
            decimals=6,
        )
        RecipientAddress.objects.create(
            name="evm-pay-native-closed",
            project=project,
            chain_type=ChainType.EVM,
            address="0x0000000000000000000000000000000000009932",
            usage=RecipientAddressUsage.INVOICE,
        )

        methods = Invoice.available_methods(project)

        self.assertNotIn(native.symbol, methods)
        self.assertEqual(methods[usdt.symbol], [chain.code])

    def test_available_methods_expose_evm_native_when_global_native_scanner_open(self):
        PlatformSettings.objects.create(open_native_scanner=True)
        project = Project.objects.create(
            name="Invoice Native Scanner Open Project",
            wallet=Wallet.objects.create(),
        )
        native = Crypto.objects.create(
            name="Ethereum Invoice Native Open",
            symbol="ETHINVOPEN",
            coingecko_id="ethereum-invoice-native-open",
        )
        chain = Chain.objects.create(
            name="Ethereum Invoice Native Open",
            code="eth-invoice-native-open",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=9933,
            rpc="http://eth.invalid",
            active=True,
        )
        RecipientAddress.objects.create(
            name="evm-pay-native-open",
            project=project,
            chain_type=ChainType.EVM,
            address="0x0000000000000000000000000000000000009934",
            usage=RecipientAddressUsage.INVOICE,
        )

        methods = Invoice.available_methods(project)

        self.assertEqual(methods[native.symbol], [chain.code])

    @override_settings(INTERNAL_API_TOKEN="xcash-saas-token")
    def test_available_methods_filters_by_cached_saas_chain_crypto_whitelist(self):
        project = Project.objects.create(
            name="Invoice SaaS Allowed Methods Project",
            wallet=Wallet.objects.create(),
        )
        eth_native = Crypto.objects.create(
            name="Ethereum SaaS Allowed",
            symbol="ETHSAASAM",
            coingecko_id="ethereum-saas-allowed-methods",
        )
        bsc_native = Crypto.objects.create(
            name="BNB SaaS Allowed",
            symbol="BNBSAASAM",
            coingecko_id="bnb-saas-allowed-methods",
        )
        eth_chain = Chain.objects.create(
            name="Ethereum SaaS Allowed",
            code="eth-saas-allowed-methods",
            type=ChainType.EVM,
            native_coin=eth_native,
            chain_id=9911,
            rpc="http://eth.invalid",
            active=True,
        )
        bsc_chain = Chain.objects.create(
            name="BSC SaaS Allowed",
            code="bsc-saas-allowed-methods",
            type=ChainType.EVM,
            native_coin=bsc_native,
            chain_id=9912,
            rpc="http://bsc.invalid",
            active=True,
        )
        usdt = Crypto.objects.create(
            name="USDT SaaS Allowed",
            symbol="USDTSAASAM",
            coingecko_id="usdt-saas-allowed-methods",
            decimals=6,
        )
        usdc = Crypto.objects.create(
            name="USDC SaaS Denied",
            symbol="USDCSAASAM",
            coingecko_id="usdc-saas-allowed-methods",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=eth_chain,
            address="0x0000000000000000000000000000000000009911",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=bsc_chain,
            address="0x0000000000000000000000000000000000009912",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=usdc,
            chain=eth_chain,
            address="0x0000000000000000000000000000000000009913",
            decimals=6,
        )
        RecipientAddress.objects.create(
            name="evm-pay",
            project=project,
            chain_type=ChainType.EVM,
            address="0x0000000000000000000000000000000000009914",
            usage=RecipientAddressUsage.INVOICE,
        )
        cache.set(
            f"saas:permission:{project.appid}",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": [eth_chain.code],
                "allowed_crypto_symbols": [usdt.symbol],
            },
            None,
        )

        methods = Invoice.available_methods(project)

        self.assertEqual(set(methods), {usdt.symbol})
        self.assertEqual(methods[usdt.symbol], [eth_chain.code])

    @override_settings(INTERNAL_API_TOKEN="xcash-saas-token")
    def test_available_methods_empty_saas_whitelists_keep_all_methods(self):
        project = Project.objects.create(
            name="Invoice SaaS Empty Whitelist Project",
            wallet=Wallet.objects.create(),
        )
        eth_native = Crypto.objects.create(
            name="Ethereum SaaS Empty",
            symbol="ETHSAASEM",
            coingecko_id="ethereum-saas-empty-methods",
        )
        bsc_native = Crypto.objects.create(
            name="BNB SaaS Empty",
            symbol="BNBSAASEM",
            coingecko_id="bnb-saas-empty-methods",
        )
        eth_chain = Chain.objects.create(
            name="Ethereum SaaS Empty",
            code="eth-saas-empty-methods",
            type=ChainType.EVM,
            native_coin=eth_native,
            chain_id=9921,
            rpc="http://eth.invalid",
            active=True,
        )
        bsc_chain = Chain.objects.create(
            name="BSC SaaS Empty",
            code="bsc-saas-empty-methods",
            type=ChainType.EVM,
            native_coin=bsc_native,
            chain_id=9922,
            rpc="http://bsc.invalid",
            active=True,
        )
        usdt = Crypto.objects.create(
            name="USDT SaaS Empty",
            symbol="USDTSAASEM",
            coingecko_id="usdt-saas-empty-methods",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=eth_chain,
            address="0x0000000000000000000000000000000000009921",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=bsc_chain,
            address="0x0000000000000000000000000000000000009922",
            decimals=6,
        )
        RecipientAddress.objects.create(
            name="evm-pay",
            project=project,
            chain_type=ChainType.EVM,
            address="0x0000000000000000000000000000000000009923",
            usage=RecipientAddressUsage.INVOICE,
        )
        cache.set(
            f"saas:permission:{project.appid}",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": [],
                "allowed_crypto_symbols": [],
            },
            None,
        )

        methods = Invoice.available_methods(project)

        self.assertEqual(set(methods[usdt.symbol]), {eth_chain.code, bsc_chain.code})


class InvoiceConfirmDropStatusTests(TestCase):
    """confirm_invoice / drop_invoice 的状态前置校验测试。"""

    def setUp(self):
        self.user = User.objects.create(username="merchant-status")
        self.project = Project.objects.create(
            name="StatusProject",
            wallet=Wallet.objects.create(),
        )

    def _make_invoice(self, status):
        return Invoice.objects.create(
            project=self.project,
            out_no=f"status-{status}",
            title="Status test",
            currency="USD",
            amount=Decimal("10"),
            methods={},
            status=status,
            expires_at=timezone.now() + timedelta(minutes=10),
        )

    def test_confirm_invoice_rejects_non_confirming_status(self):
        # confirm_invoice 仅接受 CONFIRMING 状态，其余应抛出 InvoiceStatusError。
        for bad_status in [
            InvoiceStatus.WAITING,
            InvoiceStatus.COMPLETED,
            InvoiceStatus.EXPIRED,
        ]:
            invoice = self._make_invoice(bad_status)
            with self.assertRaises(InvoiceStatusError):
                InvoiceService.confirm_invoice(invoice)

    def test_drop_invoice_rejects_non_confirming_status(self):
        # drop_invoice 仅接受 CONFIRMING 状态，其余应抛出 InvoiceStatusError。
        for bad_status in [
            InvoiceStatus.WAITING,
            InvoiceStatus.COMPLETED,
            InvoiceStatus.EXPIRED,
        ]:
            invoice = self._make_invoice(bad_status)
            with self.assertRaises(InvoiceStatusError):
                InvoiceService.drop_invoice(invoice)


class InvoiceWebhookPayloadTests(TestCase):
    """build_webhook_payload 边界测试：crypto/pay_amount 为 None 时不应崩溃。"""

    def setUp(self):
        self.user = User.objects.create(username="merchant-content")
        self.project = Project.objects.create(
            name="ContentProject",
            wallet=Wallet.objects.create(),
        )

    def test_payload_with_crypto_none(self):
        # 未选支付方式的账单，payload 应安全返回 None 字段而非抛异常。
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="content-none",
            title="Content test",
            currency="USD",
            amount=Decimal("10"),
            methods={},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        payload = InvoiceService.build_webhook_payload(invoice)
        self.assertEqual(payload["type"], "invoice")
        self.assertIsNone(payload["data"]["crypto"])
        self.assertIsNone(payload["data"]["pay_amount"])
        self.assertIsNone(payload["data"]["chain"])
        self.assertIsNone(payload["data"]["hash"])
        self.assertIsNone(payload["data"]["block"])
        self.assertFalse(payload["data"]["confirmed"])
        self.assertNotIn("tx", payload)


class InvoiceExpiredMatchTests(TestCase):
    """过期 Invoice 仍可被链上付款命中的集成测试。"""

    def setUp(self):
        self.user = User.objects.create(username="merchant-expired-match")
        self.project = Project.objects.create(
            name="ExpiredMatchProject",
            wallet=Wallet.objects.create(),
        )
        self.crypto = Crypto.objects.create(
            name="Tether Expired",
            symbol="USDTE",
            prices={"USD": "1"},
            coingecko_id="tether-expired-match",
        )
        self.chain = Chain.objects.create(
            name="Ethereum ExpiredMatch",
            code="eth-expired-match",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="ETH ExpiredMatch Native",
                symbol="ETH-EXPM",
                coingecko_id="ethereum-expired-match",
            ),
            chain_id=5888,
            rpc="http://localhost:8545",
            active=True,
        )
        Fiat.objects.get_or_create(code="USD")
        self.recipient_address = Web3.to_checksum_address(
            "0x00000000000000000000000000000000000000E1"
        )
        RecipientAddress.objects.create(
            name="收款地址-expired",
            project=self.project,
            chain_type=ChainType.EVM,
            address=self.recipient_address,
            usage=RecipientAddressUsage.INVOICE,
        )

    def test_expired_invoice_can_still_be_matched_by_transfer(self):
        # 产品宽容逻辑：账单过期后，如果链上付款仍匹配，应该接受而非拒绝。
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="expired-match-order",
            title="Expired match",
            currency="USDTE",
            amount=Decimal("10"),
            methods={"USDTE": ["eth-expired-match"]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        invoice.select_method(self.crypto, self.chain)
        slot = invoice.pay_slots.get(version=1)

        # 模拟过期：直接用 update 把状态设为 EXPIRED + 槽位设为 DISCARDED，
        # 模拟 check_expired 正常执行后的结果（避免时间线依赖）。
        expired_at = timezone.now()
        Invoice.objects.filter(pk=invoice.pk).update(
            status=InvoiceStatus.EXPIRED,
            updated_at=expired_at,
        )

        InvoicePaySlot.objects.filter(
            invoice=invoice,
            status=InvoicePaySlotStatus.ACTIVE,
        ).update(
            status=InvoicePaySlotStatus.DISCARDED,
            discard_reason=InvoicePaySlotDiscardReason.EXPIRED,
            discarded_at=expired_at,
            updated_at=expired_at,
        )
        invoice.refresh_from_db()
        slot.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.EXPIRED)
        self.assertEqual(slot.status, InvoicePaySlotStatus.DISCARDED)
        self.assertEqual(slot.discard_reason, InvoicePaySlotDiscardReason.EXPIRED)

        # 链上付款在过期前发生（datetime 在 started_at 和 expires_at 之间）
        transfer_time = invoice.started_at + timedelta(seconds=30)
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=1,
            hash="0x" + "e1" * 32,
            event_id="expired-match-event",
            crypto=self.crypto,
            from_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000F1"
            ),
            to_address=slot.pay_address,
            value=Decimal(slot.pay_amount * Decimal("100000000")),
            amount=slot.pay_amount,
            timestamp=int(transfer_time.timestamp()),
            datetime=transfer_time,
        )

        matched = InvoiceService.try_match_invoice(transfer)

        self.assertTrue(matched)
        invoice.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.CONFIRMING)
        self.assertEqual(invoice.transfer_id, transfer.pk)


class FallbackInvoiceExpiredTests(TestCase):
    """fallback_invoice_expired 批量过期的逻辑测试。"""

    def setUp(self):
        self.user = User.objects.create(username="merchant-fallback")
        self.project = Project.objects.create(
            name="FallbackProject",
            wallet=Wallet.objects.create(),
        )
        self.crypto = Crypto.objects.create(
            name="Tether Fallback",
            symbol="USDTF",
            prices={"USD": "1"},
            coingecko_id="tether-fallback",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Fallback",
            code="eth-fallback",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="ETH Fallback Native",
                symbol="ETH-FB",
                coingecko_id="ethereum-fallback",
            ),
            chain_id=6888,
            rpc="http://localhost:8545",
            active=True,
        )
        Fiat.objects.get_or_create(code="USD")
        RecipientAddress.objects.create(
            name="收款地址-fallback",
            project=self.project,
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000F1"
            ),
            usage=RecipientAddressUsage.INVOICE,
        )

    def test_fallback_expires_waiting_invoices_and_discards_slots(self):
        # fallback 任务应批量将过期的 WAITING 账单标记为 EXPIRED，并释放活跃槽位。
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="fallback-order",
            title="Fallback test",
            currency="USDTF",
            amount=Decimal("10"),
            methods={"USDTF": ["eth-fallback"]},
            # 设置过去的过期时间
            expires_at=timezone.now() - timedelta(minutes=1),
        )
        invoice.select_method(self.crypto, self.chain)
        slot = invoice.pay_slots.get(version=1)

        fallback_invoice_expired()

        invoice.refresh_from_db()
        slot.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.EXPIRED)
        self.assertEqual(slot.status, InvoicePaySlotStatus.DISCARDED)
        self.assertEqual(slot.discard_reason, InvoicePaySlotDiscardReason.EXPIRED)

    def test_fallback_skips_confirming_invoice(self):
        # 已进入 CONFIRMING 的账单不应被 fallback 误过期。
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="fallback-confirming",
            title="Fallback confirming",
            currency="USDTF",
            amount=Decimal("10"),
            methods={"USDTF": ["eth-fallback"]},
            status=InvoiceStatus.CONFIRMING,
            expires_at=timezone.now() - timedelta(minutes=1),
        )

        fallback_invoice_expired()

        invoice.refresh_from_db()
        # 状态不变
        self.assertEqual(invoice.status, InvoiceStatus.CONFIRMING)


class CheckExpiredAtomicityTests(TransactionTestCase):
    """验证 check_expired 在并发场景下的原子性。"""

    def setUp(self):
        self.user = User.objects.create(username="merchant-atomic")
        self.project = Project.objects.create(
            name="AtomicProject",
            wallet=Wallet.objects.create(),
        )
        self.crypto = Crypto.objects.create(
            name="Tether Atomic",
            symbol="USDTA",
            prices={"USD": "1"},
            coingecko_id="tether-atomic",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Atomic",
            code="eth-atomic",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="ETH Atomic Native",
                symbol="ETH-AT",
                coingecko_id="ethereum-atomic",
            ),
            chain_id=7888,
            rpc="http://localhost:8545",
            active=True,
        )
        Fiat.objects.get_or_create(code="USD")
        RecipientAddress.objects.create(
            name="收款地址-atomic",
            project=self.project,
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000A7"
            ),
            usage=RecipientAddressUsage.INVOICE,
        )

    def test_check_expired_skips_already_matched_invoice(self):
        # 并发场景：check_expired 执行时如果账单已被 try_match 推进到 CONFIRMING，
        # select_for_update + status 条件应使其安全跳过，不会误过期。
        invoice = Invoice.objects.create(
            project=self.project,
            out_no="atomic-order",
            title="Atomic test",
            currency="USDTA",
            amount=Decimal("10"),
            methods={"USDTA": ["eth-atomic"]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        invoice.select_method(self.crypto, self.chain)
        slot = invoice.pay_slots.get(version=1)

        # 模拟在 check_expired 执行前，账单已被匹配
        now = timezone.now()
        transfer = OnchainTransfer.objects.create(
            chain=self.chain,
            block=1,
            hash="0x" + "a7" * 32,
            event_id="atomic-event",
            crypto=self.crypto,
            from_address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000B7"
            ),
            to_address=slot.pay_address,
            value=Decimal(slot.pay_amount * Decimal("100000000")),
            amount=slot.pay_amount,
            timestamp=int(now.timestamp()),
            datetime=now,
        )
        InvoiceService.try_match_invoice(transfer)

        # check_expired 应该安全跳过已 CONFIRMING 的账单
        check_expired(invoice.pk)

        invoice.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.CONFIRMING)
        self.assertEqual(invoice.transfer_id, transfer.pk)


class InvoiceAllocationRetryExhaustedTests(InvoiceTestMixin, TestCase):
    """MAX_ALLOCATION_RETRY 耗尽场景：所有地址/金额组合被占用时应抛出 InvoiceAllocationError。"""

    def setUp(self):
        self.setup_base_fixtures(
            username="merchant-retry",
            project_name="RetryProject",
            crypto_symbol="USDTR",
            chain_code="eth-retry",
            chain_id=8999,
        )

    def test_select_method_raises_when_all_slots_occupied(self):
        # 当所有地址/金额组合都被占用时，应抛出 InvoiceAllocationError。
        invoice = self.create_test_invoice(out_no="retry-order")

        with (
            patch.object(Invoice, "get_pay_differ", return_value=(None, None)),
            self.assertRaises(InvoiceAllocationError),
        ):
            invoice.select_method(self.crypto, self.chain)


class InvoiceCreatePermissionCheckTests(TestCase):
    """v2 SaaS 模式：账单收款入口调用 check_saas_permission。"""

    def setUp(self):
        self.project = Project.objects.create(
            name="InvoicePermCheckProject",
            wallet=Wallet.objects.create(),
        )

    def _make_request(self):
        return APIRequestFactory().post(
            "/v1/invoice",
            {},
            format="json",
            HTTP_XC_APPID=self.project.appid,
        )

    def _make_serializer_stub(self):
        return SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={
                "out_no": "perm-inv-order",
                "title": "PermCheck Invoice",
                "currency": "USD",
                "amount": Decimal("10"),
                "methods": {},
                "duration": 10,
                "email": "",
                "redirect_url": "",
            },
            errors={},
        )

    @patch("invoices.viewsets.check_saas_permission")
    def test_create_calls_permission_check_with_correct_args(self, mock_check):
        """账单创建时只校验 invoice 账号/白名单语义，不占用 deposit 功能锁。"""
        serializer_stub = self._make_serializer_stub()

        with (
            patch.object(InvoiceViewSet, "get_serializer", return_value=serializer_stub),
            patch(
                "invoices.viewsets.Invoice.objects.create",
                return_value=Mock(
                    sys_no="inv-0001",
                    out_no="perm-inv-order",
                    project=self.project,
                    status="waiting",
                ),
            ),
            patch("invoices.viewsets.InvoiceService.initialize_invoice"),
            patch(
                "invoices.viewsets.InvoiceDisplaySerializer",
                return_value=Mock(data={}),
            ),
        ):
            InvoiceViewSet.as_view({"post": "create"})(self._make_request())

        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
        )

    @patch("invoices.viewsets.check_saas_permission")
    def test_create_checks_each_requested_method(self, mock_check):
        """账单创建时，每个 methods 链币组合都必须经过 SaaS 白名单校验。"""
        serializer_stub = self._make_serializer_stub()
        serializer_stub.validated_data["methods"] = {
            "USDT": ["ethereum-mainnet", "bsc-mainnet"],
            "USDC": ["ethereum-mainnet"],
        }

        with (
            patch.object(InvoiceViewSet, "get_serializer", return_value=serializer_stub),
            patch(
                "invoices.viewsets.Invoice.objects.create",
                return_value=Mock(
                    sys_no="inv-0002",
                    out_no="perm-inv-order",
                    project=self.project,
                    status="waiting",
                ),
            ),
            patch("invoices.viewsets.InvoiceService.initialize_invoice"),
            patch(
                "invoices.viewsets.InvoiceDisplaySerializer",
                return_value=Mock(data={}),
            ),
        ):
            InvoiceViewSet.as_view({"post": "create"})(self._make_request())

        mock_check.assert_any_call(appid=self.project.appid, action="invoice")
        mock_check.assert_any_call(
            appid=self.project.appid,
            action="invoice",
            chain_code="ethereum-mainnet",
            crypto_symbol="USDT",
        )
        mock_check.assert_any_call(
            appid=self.project.appid,
            action="invoice",
            chain_code="bsc-mainnet",
            crypto_symbol="USDT",
        )
        mock_check.assert_any_call(
            appid=self.project.appid,
            action="invoice",
            chain_code="ethereum-mainnet",
            crypto_symbol="USDC",
        )
        self.assertEqual(mock_check.call_count, 4)

    @patch("invoices.viewsets.check_saas_permission")
    def test_select_method_checks_selected_chain_and_crypto(self, mock_check):
        """支付页选择支付方式时，最终选中的链币组合必须经过 SaaS 白名单校验。"""
        invoice = Mock(
            status=InvoiceStatus.WAITING,
            expires_at=timezone.now() + timedelta(minutes=10),
            methods={"USDT": ["ethereum-mainnet"]},
            project=Mock(appid=self.project.appid),
        )
        serializer_stub = SimpleNamespace(
            is_valid=Mock(return_value=True),
            validated_data={"crypto": "USDT", "chain": "ethereum-mainnet"},
            errors={},
        )
        crypto = Mock(symbol="USDT")
        chain = Mock(code="ethereum-mainnet")

        with (
            patch.object(InvoiceViewSet, "get_serializer", return_value=serializer_stub),
            patch.object(InvoiceViewSet, "get_object", return_value=invoice),
            patch("invoices.viewsets.CryptoService.get_by_symbol", return_value=crypto),
            patch("invoices.viewsets.ChainService.get_by_code", return_value=chain),
            patch(
                "invoices.viewsets.InvoiceDisplaySerializer",
                return_value=Mock(data={}),
            ),
        ):
            InvoiceViewSet.as_view({"post": "select_method"})(
                APIRequestFactory().post(
                    "/v1/invoice/inv-0002/select-method",
                    {"crypto": "USDT", "chain": "ethereum-mainnet"},
                    format="json",
                    HTTP_XC_APPID=self.project.appid,
                ),
                sys_no="inv-0002",
            )

        mock_check.assert_called_once_with(
            appid=self.project.appid,
            action="invoice",
            chain_code="ethereum-mainnet",
            crypto_symbol="USDT",
        )

    @patch("invoices.viewsets.check_saas_permission")
    def test_create_does_not_use_deposit_feature_gate(self, mock_check):
        """创建 Invoice 时不应触发 deposit 功能锁，否则低套餐会被错误拒绝。"""

        def reject_deposit_action(*, action, **kwargs):
            if action == "deposit":
                raise APIError(ErrorCode.FEATURE_NOT_ENABLED, detail="deposit")

        mock_check.side_effect = reject_deposit_action

        serializer_stub = self._make_serializer_stub()

        with (
            patch.object(InvoiceViewSet, "get_serializer", return_value=serializer_stub),
            patch(
                "invoices.viewsets.Invoice.objects.create",
                return_value=Mock(
                    sys_no="inv-0003",
                    out_no="perm-inv-order",
                    project=self.project,
                    status="waiting",
                ),
            ),
            patch("invoices.viewsets.InvoiceService.initialize_invoice"),
            patch(
                "invoices.viewsets.InvoiceDisplaySerializer",
                return_value=Mock(data={}),
            ),
        ):
            response = InvoiceViewSet.as_view({"post": "create"})(self._make_request())

        self.assertEqual(response.status_code, 201)
        self.assertNotIn(
            "deposit",
            [call.kwargs.get("action") for call in mock_check.call_args_list],
        )

    @patch("invoices.viewsets.check_saas_permission")
    def test_create_blocked_when_account_frozen(self, mock_check):
        """账户冻结时，充值账单创建应返回 403。"""

        mock_check.side_effect = APIError(ErrorCode.ACCOUNT_FROZEN)

        response = InvoiceViewSet.as_view({"post": "create"})(self._make_request())

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data["code"], ErrorCode.ACCOUNT_FROZEN.code)
