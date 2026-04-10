from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.db import IntegrityError
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone

from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import Wallet
from currencies.models import Crypto
from currencies.models import ChainToken
from currencies.models import Fiat
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0
from invoices.models import Invoice
from invoices.models import InvoiceStatus
from projects.models import Project
from projects.models import RecipientAddress
from tron.client import TronHttpClient
from tron.codec import TronAddressCodec
from tron.models import TronWatchCursor


@override_settings(TRON_RPC_TIMEOUT=3.0, TRON_API_KEY="tron-key")
class TronHttpClientTests(SimpleTestCase):
    @patch("tron.client.httpx.get")
    def test_get_latest_solid_block_number_reads_block_header_number(self, get_mock):
        get_mock.return_value.json.return_value = {
            "block_header": {"raw_data": {"number": 123456}}
        }
        get_mock.return_value.raise_for_status.return_value = None

        chain = SimpleNamespace(rpc="https://api.trongrid.io", code="tron-mainnet")
        client = TronHttpClient(chain=chain)

        latest_block = client.get_latest_solid_block_number()

        self.assertEqual(latest_block, 123456)
        get_mock.assert_called_once()

    @patch("tron.client.httpx.post")
    def test_get_transaction_info_by_id_posts_tx_hash(self, post_mock):
        post_mock.return_value.json.return_value = {"id": "a" * 64}
        post_mock.return_value.raise_for_status.return_value = None

        chain = SimpleNamespace(rpc="https://api.trongrid.io", code="tron-mainnet")
        client = TronHttpClient(chain=chain)

        payload = client.get_transaction_info_by_id("a" * 64)

        self.assertEqual(payload["id"], "a" * 64)
        _, kwargs = post_mock.call_args
        self.assertEqual(kwargs["json"], {"value": "a" * 64})

    @patch("tron.client.httpx.get")
    def test_list_confirmed_trc20_history_sends_contract_filter_and_fingerprint(
        self,
        get_mock,
    ):
        get_mock.return_value.json.return_value = {"data": [], "meta": {}}
        get_mock.return_value.raise_for_status.return_value = None

        chain = SimpleNamespace(rpc="https://api.trongrid.io")
        client = TronHttpClient(chain=chain)
        client.list_confirmed_trc20_history(
            address="TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb",
            contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            fingerprint="cursor-1",
        )

        _, kwargs = get_mock.call_args
        self.assertEqual(kwargs["headers"]["TRON-PRO-API-KEY"], "tron-key")
        self.assertEqual(kwargs["params"]["only_confirmed"], "true")
        self.assertEqual(kwargs["params"]["fingerprint"], "cursor-1")


class TronWatchCursorTests(TestCase):
    def test_cursor_is_unique_per_chain_and_watch_address(self):
        trx = Crypto.objects.create(
            name="TRON Cursor",
            symbol="TRX-CURSOR",
            coingecko_id="tron-cursor",
            decimals=6,
        )
        chain = Chain.objects.create(
            name="Tron Cursor Mainnet",
            code="tron-cursor-mainnet",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="https://api.trongrid.io",
            active=True,
        )
        TronWatchCursor.objects.create(
            chain=chain,
            watch_address="TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb",
        )

        with self.assertRaises(IntegrityError):
            TronWatchCursor.objects.create(
                chain=chain,
                watch_address="TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb",
            )


class TronUsdtPaymentScannerTests(TestCase):
    def setUp(self):
        self.usdt = Crypto.objects.create(
            name="Tether Tron",
            symbol="USDT",
            prices={"USD": "1"},
            coingecko_id="tether-tron-scan",
            decimals=6,
        )
        self.trx = Crypto.objects.create(
            name="TRON Scan Native",
            symbol="TRX-TRON-SCAN",
            coingecko_id="tron-scan-native",
            decimals=6,
        )
        self.chain = Chain.objects.create(
            name="Tron Scan Mainnet",
            code="tron-scan-mainnet",
            type=ChainType.TRON,
            native_coin=self.trx,
            rpc="https://api.trongrid.io",
            active=True,
        )
        self.usdt_mapping = ChainToken.objects.create(
            chain=self.chain,
            crypto=self.usdt,
            address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            decimals=6,
        )
        self.project = Project.objects.create(
            name="Tron Scan Project",
            wallet=Wallet.objects.create(),
        )
        Fiat.objects.get_or_create(code="USD")
        self.watch_address = "TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb"
        RecipientAddress.objects.create(
            name="tron-pay",
            project=self.project,
            chain_type=ChainType.TRON,
            address=self.watch_address,
            used_for_invoice=True,
            used_for_deposit=False,
        )
        self.sender_address = "TJRabPrwbZy45sbavfcjinPJC18kjpRTv8"

    def _build_trc20_log(self, *, to_address: str, raw_value: int) -> dict[str, object]:
        return {
            "address": TronAddressCodec.base58_to_hex41(self.usdt_mapping.address)[2:],
            "topics": [
                ERC20_TRANSFER_TOPIC0,
                "0x"
                + "0" * 24
                + TronAddressCodec.base58_to_hex41(self.sender_address)[-40:],
                "0x" + "0" * 24 + TronAddressCodec.base58_to_hex41(to_address)[-40:],
            ],
            "data": f"0x{raw_value:064x}",
        }

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_creates_observed_transfer_and_advances_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_trc20_history.return_value = {
            "data": [{"transaction_id": "a" * 64}],
            "meta": {},
        }
        client.get_transaction_info_by_id.return_value = {
            "id": "a" * 64,
            "blockNumber": 123450,
            "blockTimeStamp": 1710000000000,
            "receipt": {"result": "SUCCESS"},
            "log": [
                self._build_trc20_log(
                    to_address=self.watch_address,
                    raw_value=1_000_000,
                )
            ],
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.addresses_scanned, 1)
        self.assertEqual(summary.events_seen, 1)
        self.assertEqual(summary.created_transfers, 1)
        transfer = OnchainTransfer.objects.get(chain=self.chain)
        self.assertEqual(transfer.hash, "a" * 64)
        self.assertEqual(transfer.event_id, "trc20:0")
        self.assertEqual(transfer.amount, Decimal("1"))
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            watch_address=self.watch_address,
        )
        self.assertEqual(cursor.last_scanned_block, 123456)
        self.assertEqual(cursor.last_safe_block, 123456)
        self.assertEqual(cursor.last_event_fingerprint, f'{"a" * 64}:0')

    @patch("chains.tasks.confirm_transfer.delay")
    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.adapter.TronHttpClient.get_transaction_info_by_id")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_can_complete_invoice_via_existing_pipeline(
        self,
        client_cls,
        get_tx_info_mock,
        _enqueue_processing_mock,
        _confirm_delay_mock,
    ):
        from chains.tasks import confirm_transfer
        from tron.scanner import TronUsdtPaymentScanner

        invoice = Invoice.objects.create(
            project=self.project,
            out_no="tron-invoice-1",
            title="Tron Invoice",
            currency=self.usdt.symbol,
            amount=Decimal("1"),
            methods={self.usdt.symbol: [self.chain.code]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        invoice.select_method(self.usdt, self.chain)
        raw_value = int(invoice.pay_amount * Decimal("1000000"))
        transfer_time = invoice.started_at + timedelta(seconds=30)

        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_trc20_history.return_value = {
            "data": [{"transaction_id": "b" * 64}],
            "meta": {},
        }
        client.get_transaction_info_by_id.return_value = {
            "id": "b" * 64,
            "blockNumber": 123451,
            "blockTimeStamp": int(transfer_time.timestamp() * 1000),
            "receipt": {"result": "SUCCESS"},
            "log": [
                self._build_trc20_log(
                    to_address=invoice.pay_address,
                    raw_value=raw_value,
                )
            ],
        }
        get_tx_info_mock.return_value = {
            "id": "b" * 64,
            "receipt": {"result": "SUCCESS"},
        }

        TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        transfer = OnchainTransfer.objects.get(chain=self.chain, hash="b" * 64)
        transfer.process()
        confirm_transfer.run(transfer.pk)

        invoice.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.COMPLETED)


class TronTaskTests(TestCase):
    @patch("tron.tasks.scan_tron_chain.delay")
    def test_scan_active_tron_chains_only_dispatches_active_tron_chains(
        self,
        scan_delay_mock,
    ):
        from tron.tasks import scan_active_tron_chains

        native = Crypto.objects.create(
            name="Tron Task Native",
            symbol="TRX-TRON-TASK",
            coingecko_id="tron-task-native",
            decimals=6,
        )
        tron_chain = Chain.objects.create(
            code="tron-active",
            name="Tron Active",
            type=ChainType.TRON,
            rpc="https://api.trongrid.io",
            native_coin=native,
            active=True,
        )
        Chain.objects.create(
            code="tron-inactive",
            name="Tron Inactive",
            type=ChainType.TRON,
            rpc="https://api.trongrid.io",
            native_coin=native,
            active=False,
        )
        Chain.objects.create(
            code="eth-active",
            name="Ethereum Active",
            type=ChainType.EVM,
            chain_id=1,
            rpc="http://eth.active",
            native_coin=native,
            active=True,
        )

        scan_active_tron_chains.run()

        scan_delay_mock.assert_called_once_with(tron_chain.pk)
