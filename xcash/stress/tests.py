import hashlib
import hmac
import importlib
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.core.exceptions import FieldDoesNotExist
from django.test import RequestFactory
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings
from django.urls import resolve
from hexbytes import HexBytes
from stress.bitcoin import BitcoinStressClient
from stress.bitcoin import _build_wallet_clients
from stress.bitcoin import send_btc
from stress.evm import send_erc20
from stress.evm import send_native
from stress.models import InvoiceStressCase
from stress.models import InvoiceStressCaseStatus
from stress.models import StressRun
from stress.models import StressRunStatus
from stress.payment import simulate_payment
from stress.service import _ANVIL_RECIPIENT_ADDRESSES
from stress.service import StressService
from stress.service import _require_stress_methods_ready
from stress.service import _setup_recipient_addresses
from stress.tasks import _execute
from stress.tasks import prepare_stress
from stress.views import _handle_webhook

from chains.models import Chain
from chains.models import ChainType
from chains.models import Wallet
from currencies.models import ChainToken
from currencies.models import Crypto
from invoices.models import Invoice
from projects.models import Project
from projects.models import RecipientAddress


@override_settings(STRESS_WEBHOOK_BASE_URL="http://localhost")
class StressServiceTests(SimpleTestCase):
    databases = {"default"}

    def test_create_invoice_posts_project_available_local_methods(self):
        project = SimpleNamespace(appid="app-1", hmac_key="secret")
        stress_run = SimpleNamespace(pk=12, project=project)
        case = SimpleNamespace(sequence=7, stress_run=stress_run)

        response = Mock()
        response.json.return_value = {"sys_no": "INV-1"}
        response.raise_for_status.return_value = None

        with (
            patch(
                "stress.service.Invoice.available_methods",
                return_value={
                    "BTC": ["bitcoin-local"],
                    "ETH": ["ethereum-local"],
                    "USDT": ["ethereum-local"],
                },
            ),
            patch.object(
                StressService,
                "_build_hmac_headers",
                return_value={"X-Test": "1"},
            ) as build_headers_mock,
            patch("stress.service.httpx.post", return_value=response) as post_mock,
        ):
            result = StressService.create_invoice(case)

        self.assertEqual(result, {"sys_no": "INV-1"})
        body = post_mock.call_args.kwargs["content"]
        payload = json.loads(body)
        self.assertEqual(
            payload["methods"],
            {
                "BTC": ["bitcoin-local"],
                "ETH": ["ethereum-local"],
                "USDT": ["ethereum-local"],
            },
        )
        self.assertEqual(payload["out_no"], "STRESS-12-7")
        build_headers_mock.assert_called_once_with(project, body)

    def test_create_invoice_raises_when_project_methods_incomplete(self):
        project = SimpleNamespace(appid="app-1", hmac_key="secret")
        stress_run = SimpleNamespace(pk=12, project=project)
        case = SimpleNamespace(sequence=7, stress_run=stress_run)

        with (
            patch(
                "stress.service.Invoice.available_methods",
                return_value={
                    "ETH": ["ethereum-local"],
                    "USDT": ["ethereum-local"],
                },
            ),
            patch("stress.service.httpx.post") as post_mock,
            self.assertRaisesMessage(
                RuntimeError,
                "Stress Project 收款地址未准备完整",
            ),
        ):
            StressService.create_invoice(case)

        post_mock.assert_not_called()

    def test_require_stress_methods_ready_returns_expected_methods(self):
        project = SimpleNamespace()

        with patch(
            "stress.service.Invoice.available_methods",
            return_value={
                "BTC": ["bitcoin-local"],
                "ETH": ["ethereum-local"],
                "USDT": ["ethereum-local"],
            },
        ):
            result = _require_stress_methods_ready(project)

        self.assertEqual(
            result,
            {
                "BTC": ["bitcoin-local"],
                "ETH": ["ethereum-local"],
                "USDT": ["ethereum-local"],
            },
        )

    def test_select_method_posts_one_fixed_local_method_without_fetching_invoice(self):
        project = SimpleNamespace(appid="app-1")
        stress_run = SimpleNamespace(project=project)
        case = SimpleNamespace(invoice_sys_no="INV-1", stress_run=stress_run)

        response = Mock()
        response.json.return_value = {
            "crypto": "USDT",
            "chain": "ethereum-local",
        }
        response.raise_for_status.return_value = None

        with (
            patch(
                "stress.service.random.choice",
                return_value=("USDT", "ethereum-local"),
            ) as choice_mock,
            patch("stress.service.httpx.get") as get_mock,
            patch("stress.service.httpx.post", return_value=response) as post_mock,
        ):
            result = StressService.select_method(case)

        self.assertEqual(
            result,
            {
                "crypto": "USDT",
                "chain": "ethereum-local",
            },
        )
        get_mock.assert_not_called()
        choice_mock.assert_called_once()
        payload = json.loads(post_mock.call_args.kwargs["content"])
        self.assertEqual(
            payload,
            {
                "crypto": "USDT",
                "chain": "ethereum-local",
            },
        )

    def test_prepare_creates_only_pay_cases(self):
        stress = StressRun(
            id=23,
            count=5,
            status=StressRunStatus.PREPARING,
        )
        stress.save = Mock()

        created_project = Project(pk=99)
        bulk_create_mock = Mock()

        with (
            patch(
                "stress.service.Project.objects.create", return_value=created_project
            ),
            patch("stress.service._setup_recipient_addresses"),
            patch(
                "stress.service.InvoiceStressCase.objects.bulk_create", bulk_create_mock
            ),
            patch("stress.service.random.gauss", return_value=0.0),
            patch("stress.service.random.shuffle"),
        ):
            StressService.prepare(stress)

        created_cases = bulk_create_mock.call_args.args[0]
        self.assertEqual(len(created_cases), 5)
        self.assertTrue(all(not hasattr(case, "scenario") for case in created_cases))

    def test_prepare_raises_when_recipient_setup_fails(self):
        stress = StressRun(
            id=23,
            count=5,
            status=StressRunStatus.PREPARING,
        )
        stress.save = Mock()

        with (
            patch("stress.service.Project.objects.create", return_value=Project(pk=99)),
            patch(
                "stress.service._setup_recipient_addresses",
                side_effect=RuntimeError("btc recipient missing"),
            ),
            patch(
                "stress.service.InvoiceStressCase.objects.bulk_create"
            ) as bulk_create_mock,
            self.assertRaisesMessage(RuntimeError, "btc recipient missing"),
        ):
            StressService.prepare(stress)

        bulk_create_mock.assert_not_called()

    def test_invoice_stress_case_model_has_no_scenario_field(self):
        with self.assertRaises(FieldDoesNotExist):
            InvoiceStressCase._meta.get_field("scenario")

    def test_execute_pays_without_scenario_field(self):
        case = SimpleNamespace(
            pk=1,
            status=InvoiceStressCaseStatus.CREATING,
            invoice_sys_no="",
            invoice_out_no="",
            crypto="",
            chain="",
            pay_address="",
            pay_amount=None,
            tx_hash="",
            payer_address="",
        )
        case.save = Mock()

        with (
            patch(
                "stress.tasks.StressService.create_invoice",
                return_value={"sys_no": "INV-1", "out_no": "OUT-1"},
            ),
            patch(
                "stress.tasks.StressService.select_method",
                return_value={
                    "crypto": "ETH",
                    "chain": "ethereum-local",
                    "pay_address": "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
                    "pay_amount": "1.23",
                },
            ),
            patch("stress.tasks.time.sleep"),
            patch(
                "stress.tasks._do_payment",
                return_value={
                    "tx_hash": "0xabc123",
                    "payer_address": "0x2000000000000000000000000000000000000002",
                },
            ) as do_payment_mock,
            patch("stress.tasks.check_webhook_timeout.apply_async") as apply_async_mock,
            patch("stress.tasks.StressService.on_case_finished"),
        ):
            _execute(case)

        do_payment_mock.assert_called_once_with(case)
        apply_async_mock.assert_called_once()
        self.assertEqual(case.status, InvoiceStressCaseStatus.PAID)
        self.assertEqual(case.tx_hash, "0xabc123")
        self.assertEqual(
            case.payer_address,
            "0x2000000000000000000000000000000000000002",
        )

    @override_settings(DEBUG=True)
    def test_stress_webhook_url_resolves_to_stress_view(self):
        import config.urls as project_urls

        reloaded_urls = importlib.reload(project_urls)

        match = resolve("/stress/webhook/", urlconf=reloaded_urls)

        self.assertEqual(match.view_name, "stress:webhook")

    @override_settings(STRESS_BTC_RPC_URL="http://xcash:xcash@localhost:18443")
    def test_bitcoin_stress_client_uses_ensured_wallet_client(self):
        wallet_client = Mock()
        wallet_client.get_new_address.return_value = "bcrt1payer"

        with (
            patch("stress.bitcoin.BitcoinRpcClient", return_value=Mock()) as rpc_cls,
            patch(
                "stress.bitcoin._ensure_wallet_client",
                return_value=wallet_client,
            ) as ensure_wallet_mock,
        ):
            result = BitcoinStressClient().get_new_address()

        self.assertEqual(result, "bcrt1payer")
        rpc_cls.assert_called_once_with("http://xcash:xcash@localhost:18443")
        self.assertEqual(ensure_wallet_mock.call_count, 2)
        self.assertEqual(
            ensure_wallet_mock.call_args_list[0].args[2],
            "xcash-miner",
        )
        self.assertEqual(
            ensure_wallet_mock.call_args_list[1].args[2],
            "xcash-miner",
        )
        wallet_client.get_new_address.assert_called_once_with(
            label="stress-recipient-xcash-miner",
            address_type="bech32",
        )

    @override_settings(STRESS_BTC_RPC_URL="http://xcash:xcash@localhost:18443")
    def test_build_wallet_clients_ensures_root_and_target_wallets(self):
        root_client = Mock()
        root_wallet_client = Mock()
        target_wallet_client = Mock()

        with (
            patch(
                "stress.bitcoin.BitcoinRpcClient", return_value=root_client
            ) as rpc_cls,
            patch(
                "stress.bitcoin._ensure_wallet_client",
                side_effect=[root_wallet_client, target_wallet_client],
            ) as ensure_wallet_mock,
        ):
            result = _build_wallet_clients(wallet_name="stress-case-1")

        self.assertEqual(
            result,
            (root_client, root_wallet_client, target_wallet_client),
        )
        rpc_cls.assert_called_once_with("http://xcash:xcash@localhost:18443")
        self.assertEqual(ensure_wallet_mock.call_count, 2)

    def test_send_btc_accepts_string_amount(self):
        root_client = Mock()
        root_wallet_client = Mock()
        payer_wallet_client = Mock()
        payer_wallet_client.get_new_address.return_value = "bcrt1payeraddress"
        payer_wallet_client.send_to_address.return_value = "btc-tx-hash"

        with (
            patch(
                "stress.bitcoin._ensure_wallet_client",
                side_effect=[root_wallet_client, payer_wallet_client],
            ),
            patch("stress.bitcoin._root_rpc_url", return_value="http://root-rpc"),
            patch("stress.bitcoin._root_wallet_name", return_value="xcash"),
            patch("stress.bitcoin.BitcoinRpcClient", return_value=root_client),
        ):
            result = send_btc(
                to="bcrt1qz252a6sxsamzl8sllmtcxtmsntkjek4z2vaktq",
                amount="0.01",
                wallet_name="stress-case-1",
            )

        self.assertEqual(result["tx_hash"], "btc-tx-hash")
        payer_wallet_client.send_to_address.assert_called_once_with(
            "bcrt1qz252a6sxsamzl8sllmtcxtmsntkjek4z2vaktq",
            Decimal("0.01"),
        )

    def test_send_btc_uses_dedicated_wallet(self):
        root_client = Mock()
        root_wallet_client = Mock()
        payer_wallet_client = Mock()
        payer_wallet_client.get_new_address.return_value = "bcrt1payeraddress"
        payer_wallet_client.send_to_address.return_value = "payment-tx"

        with (
            patch(
                "stress.bitcoin._ensure_wallet_client",
                side_effect=[root_wallet_client, payer_wallet_client],
                create=True,
            ) as ensure_wallet_mock,
            patch(
                "stress.bitcoin._root_rpc_url",
                return_value="http://root-rpc",
                create=True,
            ),
            patch(
                "stress.bitcoin._root_wallet_name",
                return_value="xcash",
                create=True,
            ),
            patch(
                "stress.bitcoin.BitcoinRpcClient",
                return_value=root_client,
                create=True,
            ),
        ):
            result = send_btc(
                to="bcrt1qz252a6sxsamzl8sllmtcxtmsntkjek4z2vaktq",
                amount=Decimal("0.01"),
                wallet_name="stress-case-1",
            )

        self.assertEqual(result["tx_hash"], "payment-tx")
        self.assertEqual(result["payer_address"], "bcrt1payeraddress")
        self.assertEqual(ensure_wallet_mock.call_count, 2)
        root_wallet_client.send_to_address.assert_called_once_with(
            "bcrt1payeraddress",
            Decimal("0.011"),
        )
        payer_wallet_client.send_to_address.assert_called_once_with(
            "bcrt1qz252a6sxsamzl8sllmtcxtmsntkjek4z2vaktq",
            Decimal("0.01"),
        )

    def test_send_native_uses_pending_nonce(self):
        payer = Mock()
        payer.address = "0x2000000000000000000000000000000000000002"
        payer.sign_transaction.return_value = SimpleNamespace(raw_transaction=b"raw")

        eth_api = Mock()
        eth_api.gas_price = 1
        eth_api.chain_id = 31337
        eth_api.get_transaction_count.return_value = 7
        eth_api.send_raw_transaction.return_value = HexBytes(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )
        w3 = Mock()
        w3.eth = eth_api
        w3.eth.account.create.return_value = payer
        w3.provider = Mock()
        w3.provider.make_request.return_value = {"result": True}

        with patch("stress.evm._get_w3", return_value=w3):
            send_native(
                to="0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
                amount=Decimal("1.23"),
            )

        eth_api.get_transaction_count.assert_called_once_with(
            payer.address,
            "pending",
        )
        w3.provider.make_request.assert_called_once()

    def test_send_native_uses_dedicated_payer_account(self):
        payer = Mock()
        payer.address = "0x2000000000000000000000000000000000000002"
        payer.sign_transaction.return_value = SimpleNamespace(raw_transaction=b"raw")

        eth_api = Mock()
        eth_api.gas_price = 1
        eth_api.chain_id = 31337
        eth_api.send_raw_transaction.return_value = HexBytes(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )
        w3 = Mock()
        w3.eth = eth_api
        w3.eth.account.create.return_value = payer
        w3.provider = Mock()
        w3.provider.make_request.return_value = {"result": True}

        with patch("stress.evm._get_w3", return_value=w3):
            result = send_native(
                to="0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
                amount=Decimal("1.23"),
            )

        w3.eth.account.create.assert_called_once_with()
        w3.provider.make_request.assert_called_once()
        self.assertEqual(result["payer_address"], payer.address)
        payment_tx = payer.sign_transaction.call_args.args[0]
        self.assertEqual(payment_tx["from"], payer.address)

    def test_send_erc20_uses_dedicated_payer_account(self):
        payer = Mock()
        payer.address = "0x2000000000000000000000000000000000000002"
        payer.sign_transaction.return_value = SimpleNamespace(raw_transaction=b"payer")

        contract = Mock()
        contract.functions.mint.return_value.build_transaction.side_effect = (
            lambda tx: tx
        )
        contract.functions.transfer.return_value.build_transaction.side_effect = (
            lambda tx: tx
        )

        eth_api = Mock()
        eth_api.gas_price = 1
        eth_api.chain_id = 31337
        eth_api.get_transaction_count.side_effect = [0, 1]
        eth_api.send_raw_transaction.side_effect = [
            HexBytes(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            HexBytes(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
        ]
        eth_api.contract.return_value = contract

        w3 = Mock()
        w3.eth = eth_api
        w3.eth.account.create.return_value = payer
        w3.provider = Mock()
        w3.provider.make_request.return_value = {"result": True}

        with (
            patch("stress.evm._get_w3", return_value=w3),
            patch(
                "stress.evm._require_contract",
                return_value="0x3000000000000000000000000000000000000003",
            ),
        ):
            result = send_erc20(
                token_address="0x3000000000000000000000000000000000000003",
                to="0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
                amount=Decimal("5"),
                decimals=6,
            )

        w3.eth.account.create.assert_called_once_with()
        w3.provider.make_request.assert_called_once()
        contract.functions.mint.assert_called_once_with(payer.address, 5_000_000)
        transfer_tx = payer.sign_transaction.call_args.args[0]
        self.assertEqual(transfer_tx["from"], payer.address)
        self.assertEqual(result["payer_address"], payer.address)

    def test_send_erc20_requires_existing_contract(self):
        w3 = Mock()
        w3.eth.get_code.return_value = b""

        with self.assertRaisesMessage(
            ValueError,
            "本地 ERC20 合约不存在，请先初始化本地链配置",
        ):
            from stress.evm import _require_contract

            _require_contract(
                w3,
                "0x3000000000000000000000000000000000000003",
            )

    def test_simulate_payment_dispatches_to_bitcoin_sender(self):
        chain_obj = SimpleNamespace(type=ChainType.BITCOIN, native_coin=None)
        with (
            patch("stress.payment.Chain.objects.get", return_value=chain_obj),
            patch("stress.payment.Crypto.objects.get"),
            patch(
                "stress.payment.send_btc",
                return_value={
                    "tx_hash": "btc-hash",
                    "payer_address": "bcrt1payer",
                },
            ) as send_btc_mock,
        ):
            result = simulate_payment(
                to_address="bcrt1target",
                chain_code="bitcoin-local",
                crypto_symbol="BTC",
                amount=Decimal("0.01"),
                payment_ref="case-1",
            )

        self.assertEqual(result["tx_hash"], "btc-hash")
        send_btc_mock.assert_called_once_with(
            to="bcrt1target",
            amount=Decimal("0.01"),
            wallet_name="stress-case-1",
        )

    def test_simulate_payment_dispatches_to_evm_native_sender(self):
        native_coin = SimpleNamespace(symbol="ETH")
        chain_obj = SimpleNamespace(type=ChainType.EVM, native_coin=native_coin)
        crypto_obj = Mock()
        crypto_obj.is_native = True
        crypto_obj.get_decimals.return_value = 18

        with (
            patch("stress.payment.Chain.objects.get", return_value=chain_obj),
            patch("stress.payment.Crypto.objects.get", return_value=crypto_obj),
            patch(
                "stress.payment.send_native",
                return_value={
                    "tx_hash": "0xnative",
                    "payer_address": "0xpayer",
                },
            ) as send_native_mock,
        ):
            result = simulate_payment(
                to_address="0xtarget",
                chain_code="ethereum-local",
                crypto_symbol="ETH",
                amount=Decimal("1.5"),
                payment_ref="case-2",
            )

        self.assertEqual(result["tx_hash"], "0xnative")
        send_native_mock.assert_called_once_with(
            to="0xtarget",
            amount=Decimal("1.5"),
            decimals=18,
        )

    def test_simulate_payment_dispatches_to_evm_erc20_sender(self):
        native_coin = SimpleNamespace(symbol="ETH")
        chain_obj = SimpleNamespace(type=ChainType.EVM, native_coin=native_coin)
        crypto_obj = Mock()
        crypto_obj.is_native = False
        crypto_obj.get_decimals.return_value = 6
        crypto_obj.address.return_value = "0xtoken"

        with (
            patch("stress.payment.Chain.objects.get", return_value=chain_obj),
            patch("stress.payment.Crypto.objects.get", return_value=crypto_obj),
            patch(
                "stress.payment.send_erc20",
                return_value={
                    "tx_hash": "0xerc20",
                    "payer_address": "0xpayer",
                },
            ) as send_erc20_mock,
        ):
            result = simulate_payment(
                to_address="0xtarget",
                chain_code="ethereum-local",
                crypto_symbol="USDT",
                amount=Decimal("25"),
                payment_ref="case-3",
            )

        self.assertEqual(result["tx_hash"], "0xerc20")
        send_erc20_mock.assert_called_once_with(
            token_address="0xtoken",
            to="0xtarget",
            amount=Decimal("25"),
            decimals=6,
        )


class StressRecipientSetupTests(TestCase):
    def setUp(self):
        self.eth, _ = Crypto.objects.update_or_create(
            symbol="ETH",
            defaults={
                "name": "Ethereum",
                "coingecko_id": "ethereum",
            },
        )
        self.btc, _ = Crypto.objects.update_or_create(
            symbol="BTC",
            defaults={
                "name": "Bitcoin",
                "coingecko_id": "bitcoin",
            },
        )
        self.usdt, _ = Crypto.objects.update_or_create(
            symbol="USDT",
            defaults={
                "name": "Tether USD",
                "decimals": 6,
                "coingecko_id": "tether",
                "prices": {"USD": "1"},
            },
        )
        self.ethereum_local, _ = Chain.objects.update_or_create(
            code="ethereum-local",
            defaults={
                "name": "Ethereum Local",
                "type": ChainType.EVM,
                "native_coin": self.eth,
                "chain_id": 31337,
                "rpc": "http://127.0.0.1:8545",
                "active": True,
            },
        )
        self.bitcoin_local, _ = Chain.objects.update_or_create(
            code="bitcoin-local",
            defaults={
                "name": "Bitcoin Local",
                "type": ChainType.BITCOIN,
                "native_coin": self.btc,
                "active": True,
            },
        )
        ChainToken.objects.update_or_create(
            chain=self.ethereum_local,
            crypto=self.eth,
            defaults={"address": "", "decimals": None},
        )
        ChainToken.objects.update_or_create(
            chain=self.bitcoin_local,
            crypto=self.btc,
            defaults={"address": "", "decimals": None},
        )
        ChainToken.objects.update_or_create(
            chain=self.ethereum_local,
            crypto=self.usdt,
            defaults={
                "address": "0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0",
                "decimals": 6,
            },
        )
        Project.objects.filter(name="Stress Target Project").delete()
        self.project = Project.objects.create(
            name="Stress Target Project",
            wallet=Wallet.objects.create(),
            webhook="http://localhost/stress/webhook/",
            ip_white_list="*",
            active=True,
        )

    @patch.dict("os.environ", {"BITCOIN_NETWORK": "regtest"})
    def test_setup_recipient_addresses_creates_local_recipients_without_templates(self):
        btc_invoice_address = "bcrt1q2507fuxge3y0sxd77vqz7yhangkm3wmvmpqxqn"

        with patch("stress.bitcoin.BitcoinStressClient") as client_cls:
            client_cls.return_value.get_new_address.return_value = btc_invoice_address

            _setup_recipient_addresses(self.project)

        recipients = list(
            RecipientAddress.objects.filter(project=self.project)
            .order_by("chain_type", "address")
            .values("chain_type", "address", "used_for_invoice", "used_for_deposit")
        )

        # BTC 充币已砍掉，只保留 BTC Invoice 收款地址
        self.assertEqual(
            recipients,
            [
                {
                    "chain_type": ChainType.BITCOIN.value,
                    "address": btc_invoice_address,
                    "used_for_invoice": True,
                    "used_for_deposit": False,
                },
                {
                    "chain_type": ChainType.EVM.value,
                    "address": _ANVIL_RECIPIENT_ADDRESSES[1],
                    "used_for_invoice": False,
                    "used_for_deposit": True,
                },
                {
                    "chain_type": ChainType.EVM.value,
                    "address": _ANVIL_RECIPIENT_ADDRESSES[0],
                    "used_for_invoice": True,
                    "used_for_deposit": False,
                },
            ],
        )
        self.assertEqual(
            Invoice.available_methods(self.project),
            {
                "BTC": ["bitcoin-local"],
                "ETH": ["ethereum-local"],
                "USDT": ["ethereum-local"],
            },
        )

    def test_setup_recipient_addresses_requires_bitcoin_recipient(self):
        with (
            patch(
                "stress.bitcoin.BitcoinStressClient",
                side_effect=RuntimeError("btc rpc unavailable"),
            ),
            self.assertRaisesMessage(
                RuntimeError,
                "创建 BTC 收款地址失败",
            ),
        ):
            _setup_recipient_addresses(self.project)


class FinalizeStressTimeoutTests(TestCase):
    def setUp(self):
        self.stress_run = StressRun.objects.create(
            name="timeout-test",
            count=5,
            status=StressRunStatus.RUNNING,
        )
        # 2 个终态 case + 3 个非终态 case
        InvoiceStressCase.objects.create(
            stress_run=self.stress_run,
            sequence=1,
            scheduled_offset=0,
            status=InvoiceStressCaseStatus.SUCCEEDED,
        )
        InvoiceStressCase.objects.create(
            stress_run=self.stress_run,
            sequence=2,
            scheduled_offset=1,
            status=InvoiceStressCaseStatus.FAILED,
            error="connection refused",
        )
        InvoiceStressCase.objects.create(
            stress_run=self.stress_run,
            sequence=3,
            scheduled_offset=2,
            status=InvoiceStressCaseStatus.PENDING,
        )
        InvoiceStressCase.objects.create(
            stress_run=self.stress_run,
            sequence=4,
            scheduled_offset=3,
            status=InvoiceStressCaseStatus.CREATING,
        )
        InvoiceStressCase.objects.create(
            stress_run=self.stress_run,
            sequence=5,
            scheduled_offset=4,
            status=InvoiceStressCaseStatus.PAID,
        )
        self.stress_run.succeeded = 1
        self.stress_run.failed = 1
        self.stress_run.save(update_fields=["succeeded", "failed"])

    def test_skips_non_terminal_cases_and_completes_run(self):
        from stress.tasks import finalize_stress_timeout

        finalize_stress_timeout.run(self.stress_run.pk)

        self.stress_run.refresh_from_db()
        self.assertEqual(self.stress_run.status, StressRunStatus.COMPLETED)
        self.assertEqual(self.stress_run.succeeded, 1)
        self.assertEqual(self.stress_run.failed, 1)
        self.assertEqual(self.stress_run.skipped, 3)
        self.assertIsNotNone(self.stress_run.finished_at)

        # 原终态 case 不受影响
        case1 = InvoiceStressCase.objects.get(stress_run=self.stress_run, sequence=1)
        self.assertEqual(case1.status, InvoiceStressCaseStatus.SUCCEEDED)
        case2 = InvoiceStressCase.objects.get(stress_run=self.stress_run, sequence=2)
        self.assertEqual(case2.status, InvoiceStressCaseStatus.FAILED)
        self.assertEqual(case2.error, "connection refused")

        # 非终态 case 被标记为 skipped
        for seq in (3, 4, 5):
            case = InvoiceStressCase.objects.get(
                stress_run=self.stress_run, sequence=seq
            )
            self.assertEqual(case.status, InvoiceStressCaseStatus.SKIPPED)
            self.assertEqual(case.error, "压测整轮超时，任务未执行")
            self.assertIsNotNone(case.finished_at)

    def test_noop_when_already_completed(self):
        from stress.tasks import finalize_stress_timeout

        self.stress_run.status = StressRunStatus.COMPLETED
        self.stress_run.save(update_fields=["status"])

        finalize_stress_timeout.run(self.stress_run.pk)

        self.stress_run.refresh_from_db()
        # skipped 不变，说明没有被二次处理
        self.assertEqual(self.stress_run.skipped, 0)

    def test_noop_when_all_cases_already_terminal(self):
        from stress.tasks import finalize_stress_timeout

        # 把所有非终态 case 手动设为终态
        InvoiceStressCase.objects.filter(
            stress_run=self.stress_run,
            status__in=[
                InvoiceStressCaseStatus.PENDING,
                InvoiceStressCaseStatus.CREATING,
                InvoiceStressCaseStatus.PAID,
            ],
        ).update(status=InvoiceStressCaseStatus.FAILED)

        finalize_stress_timeout.run(self.stress_run.pk)

        self.stress_run.refresh_from_db()
        # 没有 case 被 skip，状态仍为 running（由 on_case_finished 负责推进）
        self.assertEqual(self.stress_run.status, StressRunStatus.RUNNING)
        self.assertEqual(self.stress_run.skipped, 0)


class StressTaskTests(TestCase):
    def test_prepare_stress_marks_run_failed_when_prepare_raises(self):
        stress = StressRun.objects.create(
            name="prepare-failure",
            count=5,
            status=StressRunStatus.PREPARING,
        )

        with patch(
            "stress.tasks.StressService.prepare",
            side_effect=RuntimeError("btc rpc unavailable"),
        ):
            prepare_stress(stress.pk)

        stress.refresh_from_db()
        self.assertEqual(stress.status, StressRunStatus.FAILED)
        self.assertEqual(stress.error, "btc rpc unavailable")
        self.assertIsNotNone(stress.finished_at)


class StressWebhookTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.project = Project.objects.create(
            name="Stress Webhook Project",
            wallet=Wallet.objects.create(),
            webhook="http://localhost:8000/stress/webhook/",
            ip_white_list="*",
            active=True,
            hmac_key="stress-secret-key",
        )
        self.stress_run = StressRun.objects.create(
            name="stress-webhook",
            count=1,
            status=StressRunStatus.RUNNING,
            project=self.project,
        )
        self.case = InvoiceStressCase.objects.create(
            stress_run=self.stress_run,
            sequence=1,
            scheduled_offset=0,
            invoice_sys_no="INV-STRESS-1",
            invoice_out_no="STRESS-1-1",
            status=InvoiceStressCaseStatus.PAID,
        )

    def test_handle_webhook_accepts_actual_invoice_payload_without_status_field(self):
        payload = {
            "type": "invoice",
            "data": {
                "sys_no": self.case.invoice_sys_no,
                "out_no": self.case.invoice_out_no,
                "crypto": "ETH",
                "pay_address": "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
                "pay_amount": "1.23",
            },
            "tx": {
                "hash": "0xa04a8394076c7f7ad4a974fc462ba2a0e08e83c820f99bbe1ea7c8f3da6e7f52",
                "block": 1,
                "chain": "ethereum-local",
                "status": "confirmed",
                "is_confirmed": True,
            },
        }
        body = json.dumps(payload)
        nonce = "nonce-1"
        timestamp = "1710000000"
        signature = hmac.new(
            self.project.hmac_key.encode(),
            f"{nonce}{timestamp}{body}".encode(),
            hashlib.sha256,
        ).hexdigest()

        request = self.factory.post(
            "/stress/webhook/",
            data=body,
            content_type="application/json",
            HTTP_XC_NONCE=nonce,
            HTTP_XC_TIMESTAMP=timestamp,
            HTTP_XC_SIGNATURE=signature,
        )

        with patch("stress.views.time.time", return_value=int(timestamp)):
            _handle_webhook(request)

        self.case.refresh_from_db()
        self.assertEqual(self.case.status, InvoiceStressCaseStatus.SUCCEEDED)
        self.assertTrue(self.case.webhook_received)
