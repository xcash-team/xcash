from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

import httpx
from django.contrib.admin.sites import AdminSite
from django.db import IntegrityError
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone
from tron.admin import TronWatchCursorAdmin
from tron.client import TronClientError
from tron.client import TronHttpClient
from tron.codec import TronAddressCodec
from tron.models import TronWatchCursor

from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import Wallet
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.models import Fiat
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0
from invoices.models import Invoice
from invoices.models import InvoiceStatus
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage


@override_settings(TRON_RPC_TIMEOUT=3.0)
class TronHttpClientTests(SimpleTestCase):
    @patch("tron.client.httpx.get")
    def test_get_latest_solid_block_number_reads_block_header_number(self, get_mock):
        get_mock.return_value.json.return_value = {
            "block_header": {"raw_data": {"number": 123456}}
        }
        get_mock.return_value.raise_for_status.return_value = None

        chain = SimpleNamespace(
            rpc="https://tron-mainnet.core.chainstack.com/token",
            code="tron-mainnet",
            tron_api_key="",
        )
        client = TronHttpClient(chain=chain)

        latest_block = client.get_latest_solid_block_number()

        self.assertEqual(latest_block, 123456)
        call_args, _kwargs = get_mock.call_args
        self.assertEqual(
            call_args[0],
            "https://api.trongrid.io/walletsolidity/getnowblock",
        )

    @patch("tron.client.httpx.post")
    def test_get_transaction_info_by_id_posts_tx_hash(self, post_mock):
        post_mock.return_value.json.return_value = {"id": "a" * 64}
        post_mock.return_value.raise_for_status.return_value = None

        chain = SimpleNamespace(
            rpc="https://api.trongrid.io",
            code="tron-mainnet",
            tron_api_key="",
        )
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

        chain = SimpleNamespace(rpc="https://api.trongrid.io", tron_api_key="tron-key")
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

    @patch("tron.client.httpx.get")
    def test_list_confirmed_contract_events_sends_block_filter_and_fingerprint(
        self,
        get_mock,
    ):
        get_mock.return_value.json.return_value = {"data": [], "meta": {}}
        get_mock.return_value.raise_for_status.return_value = None

        chain = SimpleNamespace(
            rpc="https://api.trongrid.io",
            code="tron-mainnet",
            tron_api_key="tron-key",
        )
        client = TronHttpClient(chain=chain)
        client.list_confirmed_contract_events(
            contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            event_name="Transfer",
            block_number=61840405,
            fingerprint="cursor-1",
        )

        call_args, kwargs = get_mock.call_args
        self.assertEqual(
            call_args[0],
            "https://api.trongrid.io/v1/contracts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/events",
        )
        self.assertEqual(kwargs["headers"]["TRON-PRO-API-KEY"], "tron-key")
        self.assertEqual(kwargs["params"]["event_name"], "Transfer")
        self.assertEqual(kwargs["params"]["block_number"], 61840405)
        self.assertEqual(kwargs["params"]["only_confirmed"], "true")
        self.assertEqual(kwargs["params"]["fingerprint"], "cursor-1")

    @patch("tron.client.httpx.get")
    def test_list_confirmed_contract_events_wraps_http_error(self, get_mock):
        get_mock.side_effect = httpx.HTTPError("boom")

        chain = SimpleNamespace(
            rpc="https://api.trongrid.io",
            code="tron-mainnet",
            tron_api_key="tron-key",
        )
        client = TronHttpClient(chain=chain)

        with self.assertRaisesMessage(
            TronClientError,
            "failed to fetch confirmed contract events from tron-mainnet",
        ):
            client.list_confirmed_contract_events(
                contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
                event_name="Transfer",
                block_number=61840405,
            )


class TronWatchCursorTests(TestCase):
    def test_enabling_tron_chain_creates_usdt_watch_cursor(self):
        trx = Crypto.objects.create(
            name="TRON Cursor Sync Native",
            symbol="TRX-CURSOR-SYNC",
            coingecko_id="tron-cursor-sync-native",
            decimals=6,
        )
        usdt = Crypto.objects.create(
            name="Tether Tron Cursor Sync",
            symbol="USDT",
            coingecko_id="tether-tron-cursor-sync",
            decimals=6,
        )
        chain = Chain.objects.create(
            name="Tron Cursor Sync Mainnet",
            code="tron-cursor-sync-mainnet",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="https://api.trongrid.io",
            active=False,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=chain,
            address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            decimals=6,
        )

        chain.active = True
        chain.save(update_fields=["active"])

        cursor = TronWatchCursor.objects.get(chain=chain)
        self.assertEqual(
            cursor.contract_address,
            "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
        )
        self.assertTrue(cursor.enabled)
        self.assertEqual(cursor.last_scanned_block, 0)

    def test_tron_chain_save_clears_generic_rpc(self):
        trx = Crypto.objects.create(
            name="TRON Cursor RPC Clear",
            symbol="TRX-CURSOR-RPC-CLEAR",
            coingecko_id="tron-cursor-rpc-clear",
            decimals=6,
        )

        chain = Chain.objects.create(
            name="Tron Cursor RPC Clear",
            code="tron-cursor-rpc-clear",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="https://api.trongrid.io",
            tron_api_key="  tron-key  ",
        )

        chain.refresh_from_db()
        self.assertEqual(chain.rpc, "")
        self.assertEqual(chain.tron_api_key, "tron-key")

    def test_cursor_is_unique_per_chain_and_contract_address(self):
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
            contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
        )

        with self.assertRaises(IntegrityError):
            TronWatchCursor.objects.create(
                chain=chain,
                contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            )


class TronWatchCursorAdminTests(TestCase):
    def setUp(self):
        self.trx = Crypto.objects.create(
            name="Tron Admin Test",
            symbol="TRX-ADMIN",
            coingecko_id="tron-admin-test",
            decimals=6,
        )
        self.chain = Chain.objects.create(
            name="Tron Admin Mainnet",
            code="tron-admin-mainnet",
            type=ChainType.TRON,
            native_coin=self.trx,
            rpc="https://api.trongrid.io",
            active=True,
            latest_block_number=66,
        )
        self.cursor = TronWatchCursor.objects.create(
            chain=self.chain,
            contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            last_scanned_block=11,
            last_safe_block=11,
            last_error="old error",
            last_error_at=timezone.now(),
        )
        self.other_cursor = TronWatchCursor.objects.create(
            chain=self.chain,
            contract_address="TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb",
            last_scanned_block=9,
            last_safe_block=9,
            enabled=False,
        )
        self.admin = TronWatchCursorAdmin(TronWatchCursor, AdminSite())
        self.admin.message_user = Mock()

    @patch("tron.admin.TronHttpClient")
    @patch.object(Chain, "get_latest_block_number", new_callable=PropertyMock)
    def test_sync_selected_to_latest_fetches_realtime_solid_block_number(
        self, get_latest_block_number_mock, client_cls
    ):
        get_latest_block_number_mock.side_effect = AssertionError(
            "should not fetch realtime block height"
        )
        Chain.objects.filter(pk=self.chain.pk).update(latest_block_number=77)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 88

        self.admin.sync_selected_to_latest(
            request=Mock(),
            queryset=TronWatchCursor.objects.filter(pk=self.cursor.pk),
        )

        self.cursor.refresh_from_db()
        self.other_cursor.refresh_from_db()
        self.chain.refresh_from_db()

        self.assertEqual(self.cursor.last_scanned_block, 88)
        self.assertEqual(self.cursor.last_safe_block, 88)
        self.assertEqual(self.cursor.last_error, "")
        self.assertIsNone(self.cursor.last_error_at)
        self.assertEqual(self.other_cursor.last_scanned_block, 9)
        self.assertEqual(self.chain.latest_block_number, 88)
        self.admin.message_user.assert_called_once()
        self.assertEqual(get_latest_block_number_mock.call_count, 0)
        client_cls.assert_called_once()
        self.assertEqual(client_cls.call_args.kwargs["chain"].pk, self.chain.pk)

    @patch("tron.admin.TronHttpClient")
    def test_sync_selected_to_latest_keeps_cursor_when_realtime_fetch_fails(
        self, client_cls
    ):
        client = client_cls.return_value
        client.get_latest_solid_block_number.side_effect = TronClientError(
            "latest failed"
        )

        self.admin.sync_selected_to_latest(
            request=Mock(),
            queryset=TronWatchCursor.objects.filter(pk=self.cursor.pk),
        )

        self.cursor.refresh_from_db()
        self.chain.refresh_from_db()

        self.assertEqual(self.cursor.last_scanned_block, 11)
        self.assertEqual(self.cursor.last_safe_block, 11)
        self.assertEqual(self.cursor.last_error, "old error")
        self.assertEqual(self.chain.latest_block_number, 66)
        self.admin.message_user.assert_called_once()


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
            usage=RecipientAddressUsage.INVOICE,
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

    def _build_contract_event(
        self,
        *,
        tx_hash: str,
        block_number: int,
        event_index: int = 0,
        to_address: str | None = None,
        raw_value: int = 1_000_000,
        timestamp_ms: int = 1710000000000,
    ) -> dict[str, object]:
        return {
            "block_number": block_number,
            "block_timestamp": timestamp_ms,
            "contract_address": self.usdt_mapping.address,
            "event_name": "Transfer",
            "event_index": event_index,
            "transaction_id": tx_hash,
            "result": {
                "from": "0x" + TronAddressCodec.base58_to_hex41(self.sender_address)[2:],
                "to": "0x"
                + TronAddressCodec.base58_to_hex41(to_address or self.watch_address)[2:],
                "value": str(raw_value),
            },
        }

    def _get_or_create_contract_cursor(self, *, last_scanned_block: int) -> TronWatchCursor:
        return TronWatchCursor.objects.create(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
            last_scanned_block=last_scanned_block,
            last_safe_block=last_scanned_block,
        )

    @override_settings(DEBUG=False)
    @patch("tron.scanner.TronHttpClient")
    def test_debug_false_first_scan_bootstraps_cursor_to_latest_block_without_fetching_history(
        self,
        client_cls,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 0)
        self.assertEqual(summary.filter_addresses, 1)
        client.list_confirmed_contract_events.assert_not_called()
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123456)
        self.assertEqual(cursor.last_safe_block, 123456)

    @override_settings(DEBUG=False)
    @patch("tron.scanner.TronHttpClient")
    def test_debug_false_resume_from_last_scanned_block(self, client_cls):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = {"data": [], "meta": {}}

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        client.list_confirmed_contract_events.assert_called_once_with(
            contract_address=self.usdt_mapping.address,
            event_name="Transfer",
            block_number=123456,
            fingerprint=None,
        )

    @override_settings(DEBUG=True)
    @patch("tron.scanner.TronHttpClient")
    def test_debug_true_restarts_from_latest_after_process_reset(self, client_cls):
        from tron.scanner import TronUsdtPaymentScanner

        TronUsdtPaymentScanner._debug_bootstrapped_cursors.clear()
        self._get_or_create_contract_cursor(last_scanned_block=123400)

        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123500
        TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        client.list_confirmed_contract_events.assert_not_called()

        client.reset_mock()
        client.get_latest_solid_block_number.return_value = 123501
        client.list_confirmed_contract_events.return_value = {"data": [], "meta": {}}
        TronUsdtPaymentScanner.scan_chain(chain=self.chain)
        client.list_confirmed_contract_events.assert_called_once()

        TronUsdtPaymentScanner._debug_bootstrapped_cursors.clear()
        client.reset_mock()
        client.get_latest_solid_block_number.return_value = 123510
        TronUsdtPaymentScanner.scan_chain(chain=self.chain)
        client.list_confirmed_contract_events.assert_not_called()

    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_records_cursor_error_when_latest_block_rpc_fails(
        self,
        client_cls,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        client = client_cls.return_value
        client.get_latest_solid_block_number.side_effect = TronClientError(
            "latest failed"
        )

        with self.assertRaisesMessage(TronClientError, "latest failed"):
            TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 0)
        self.assertEqual(cursor.last_safe_block, 0)
        self.assertEqual(cursor.last_error, "latest failed")
        self.assertIsNotNone(cursor.last_error_at)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_creates_observed_transfer_and_advances_contract_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = {
            "data": [
                self._build_contract_event(
                    tx_hash="a" * 64,
                    block_number=123456,
                )
            ],
            "meta": {},
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.filter_addresses, 1)
        self.assertEqual(summary.events_seen, 1)
        self.assertEqual(summary.created_transfers, 1)
        transfer = OnchainTransfer.objects.get(chain=self.chain)
        self.assertEqual(transfer.hash, "a" * 64)
        self.assertEqual(transfer.event_id, "trc20:0")
        self.assertEqual(transfer.amount, Decimal("1"))
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123456)
        self.assertEqual(cursor.last_safe_block, 123456)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_ignores_block_mismatch_contract_events(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = {
            "data": [
                self._build_contract_event(
                    tx_hash="c" * 64,
                    block_number=123457,
                )
            ],
            "meta": {},
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 0)
        self.assertEqual(summary.created_transfers, 0)
        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123456)
        self.assertEqual(cursor.last_safe_block, 123456)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_raises_and_does_not_fallback_when_contract_event_fetch_fails(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.side_effect = TronClientError("probe failed")

        with self.assertRaisesMessage(TronClientError, "probe failed"):
            TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())
        client.list_confirmed_trc20_history.assert_not_called()
        client.get_transaction_info_by_id.assert_not_called()
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123455)
        self.assertEqual(cursor.last_safe_block, 123455)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_raises_on_non_dict_contract_event_payload_and_keeps_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = []

        with self.assertRaisesMessage(TronClientError, "invalid contract events payload"):
            TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123455)
        self.assertEqual(cursor.last_safe_block, 123455)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_raises_on_invalid_contract_event_payload_shape_and_keeps_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = {
            "data": {"unexpected": "shape"},
            "meta": {},
        }

        with self.assertRaisesMessage(TronClientError, "invalid contract events payload"):
            TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123455)
        self.assertEqual(cursor.last_safe_block, 123455)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_stops_on_empty_page_even_if_fingerprint_exists(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.side_effect = [
            {
                "data": [],
                "meta": {"fingerprint": "page-2"},
            },
            AssertionError("empty page should stop pagination"),
        ]

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 0)
        self.assertEqual(summary.created_transfers, 0)
        self.assertEqual(client.list_confirmed_contract_events.call_count, 1)
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123456)
        self.assertEqual(cursor.last_safe_block, 123456)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_raises_on_repeated_contract_event_fingerprint_and_keeps_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.side_effect = [
            {
                "data": [
                    self._build_contract_event(
                        tx_hash="1" * 64,
                        block_number=123456,
                        event_index=0,
                    )
                ],
                "meta": {"fingerprint": "dup-page"},
            },
            {
                "data": [
                    self._build_contract_event(
                        tx_hash="2" * 64,
                        block_number=123456,
                        event_index=1,
                    )
                ],
                "meta": {"fingerprint": "dup-page"},
            },
            AssertionError("repeated fingerprint should not request more pages"),
        ]

        with self.assertRaisesMessage(
            TronClientError,
            "duplicate contract events fingerprint",
        ):
            TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())
        self.assertEqual(client.list_confirmed_contract_events.call_count, 2)
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123455)
        self.assertEqual(cursor.last_safe_block, 123455)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_raises_on_non_string_contract_event_fingerprint_and_keeps_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = {
            "data": [
                self._build_contract_event(
                    tx_hash="3" * 64,
                    block_number=123456,
                    event_index=0,
                )
            ],
            "meta": {"fingerprint": 123},
        }

        with self.assertRaisesMessage(
            TronClientError,
            "invalid contract events fingerprint",
        ):
            TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123455)
        self.assertEqual(cursor.last_safe_block, 123455)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_ignores_events_for_non_business_addresses(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.return_value = {
            "data": [
                self._build_contract_event(
                    tx_hash="d" * 64,
                    block_number=123456,
                    to_address=self.sender_address,
                )
            ],
            "meta": {},
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 0)
        self.assertEqual(summary.created_transfers, 0)
        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_ignores_events_without_event_index(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        event = self._build_contract_event(
            tx_hash="6" * 64,
            block_number=123456,
            event_index=9,
        )
        del event["event_index"]
        client.list_confirmed_contract_events.return_value = {
            "data": [event],
            "meta": {},
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 0)
        self.assertEqual(summary.created_transfers, 0)
        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_ignores_non_transfer_contract_events(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        event = self._build_contract_event(
            tx_hash="7" * 64,
            block_number=123456,
        )
        event["event_name"] = "Approval"
        client.list_confirmed_contract_events.return_value = {
            "data": [event],
            "meta": {},
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 0)
        self.assertEqual(summary.created_transfers, 0)
        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_ignores_events_from_non_target_contract(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        event = self._build_contract_event(
            tx_hash="8" * 64,
            block_number=123456,
        )
        event["contract_address"] = self.sender_address
        client.list_confirmed_contract_events.return_value = {
            "data": [event],
            "meta": {},
        }

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 0)
        self.assertEqual(summary.created_transfers, 0)
        self.assertFalse(OnchainTransfer.objects.filter(chain=self.chain).exists())

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_consumes_all_pages_in_same_block_before_advancing_cursor(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        from tron.scanner import TronUsdtPaymentScanner

        self._get_or_create_contract_cursor(last_scanned_block=123455)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123456
        client.list_confirmed_contract_events.side_effect = [
            {
                "data": [
                    self._build_contract_event(
                        tx_hash="e" * 64,
                        block_number=123456,
                        event_index=0,
                    )
                ],
                "meta": {"fingerprint": "page-2"},
            },
            {
                "data": [
                    self._build_contract_event(
                        tx_hash="f" * 64,
                        block_number=123456,
                        event_index=1,
                    )
                ],
                "meta": {},
            },
        ]

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, 1)
        self.assertEqual(summary.events_seen, 2)
        self.assertEqual(summary.created_transfers, 2)
        self.assertEqual(
            OnchainTransfer.objects.filter(chain=self.chain).order_by("hash").count(),
            2,
        )
        self.assertEqual(
            client.list_confirmed_contract_events.call_args_list,
            [
                (
                    (),
                    {
                        "contract_address": self.usdt_mapping.address,
                        "event_name": "Transfer",
                        "block_number": 123456,
                        "fingerprint": None,
                    },
                ),
                (
                    (),
                    {
                        "contract_address": self.usdt_mapping.address,
                        "event_name": "Transfer",
                        "block_number": 123456,
                        "fingerprint": "page-2",
                    },
                ),
            ],
        )
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, 123456)
        self.assertEqual(cursor.last_safe_block, 123456)

    @patch("chains.service.TransferService.enqueue_processing")
    @patch("tron.scanner.TronHttpClient")
    def test_scan_chain_caps_single_tick_advance_at_batch_size(
        self,
        client_cls,
        _enqueue_processing_mock,
    ):
        # 单 tick 内 Tron 推进的块数必须被 DEFAULT_TRON_SCAN_BATCH_SIZE 限制，
        # 避免大幅落后时 range(start, latest+1) 无界拖垮当次 beat task。
        from tron.scanner import DEFAULT_TRON_SCAN_BATCH_SIZE
        from tron.scanner import TronUsdtPaymentScanner

        start_cursor = 100_000
        latest_block = start_cursor + DEFAULT_TRON_SCAN_BATCH_SIZE * 4
        expected_end_block = start_cursor + DEFAULT_TRON_SCAN_BATCH_SIZE

        self._get_or_create_contract_cursor(last_scanned_block=start_cursor)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = latest_block
        client.list_confirmed_contract_events.return_value = {"data": [], "meta": {}}

        summary = TronUsdtPaymentScanner.scan_chain(chain=self.chain)

        self.assertEqual(summary.blocks_scanned, DEFAULT_TRON_SCAN_BATCH_SIZE)
        self.assertEqual(
            client.list_confirmed_contract_events.call_count,
            DEFAULT_TRON_SCAN_BATCH_SIZE,
        )
        cursor = TronWatchCursor.objects.get(
            chain=self.chain,
            contract_address=self.usdt_mapping.address,
        )
        self.assertEqual(cursor.last_scanned_block, expected_end_block)

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
        from tron.scanner import TronUsdtPaymentScanner

        from chains.tasks import confirm_transfer

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

        self._get_or_create_contract_cursor(last_scanned_block=123450)
        client = client_cls.return_value
        client.get_latest_solid_block_number.return_value = 123451
        client.list_confirmed_contract_events.return_value = {
            "data": [
                self._build_contract_event(
                    tx_hash="b" * 64,
                    block_number=123451,
                    timestamp_ms=int(transfer_time.timestamp() * 1000),
                    to_address=invoice.pay_address,
                    raw_value=raw_value,
                )
            ],
            "meta": {},
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
    @patch("tron.tasks.logger.info")
    @patch("tron.tasks.TronUsdtPaymentScanner.scan_chain")
    def test_scan_tron_chain_logs_filter_addresses_and_blocks_scanned(
        self,
        scan_chain_mock,
        logger_info_mock,
    ):
        from tron.scanner import TronScanSummary
        from tron.tasks import scan_tron_chain

        native = Crypto.objects.create(
            name="Tron Task Log Native",
            symbol="TRX-TRON-TASK-LOG",
            coingecko_id="tron-task-log-native",
            decimals=6,
        )
        tron_chain = Chain.objects.create(
            code="tron-log",
            name="Tron Log",
            type=ChainType.TRON,
            rpc="https://api.trongrid.io",
            tron_api_key="tron-key",
            native_coin=native,
            active=True,
        )
        scan_chain_mock.return_value = TronScanSummary(
            filter_addresses=3,
            blocks_scanned=7,
            events_seen=11,
            created_transfers=2,
        )

        scan_tron_chain.run(tron_chain.pk)

        logger_info_mock.assert_called_once_with(
            "Tron USDT 扫描完成",
            chain=tron_chain.code,
            filter_addresses=3,
            blocks_scanned=7,
            events_seen=11,
            created_transfers=2,
        )

    @patch("tron.tasks.TronUsdtPaymentScanner.scan_chain")
    def test_scan_tron_chain_skips_when_api_key_is_missing(
        self,
        scan_chain_mock,
    ):
        from tron.tasks import scan_tron_chain

        native = Crypto.objects.create(
            name="Tron Task Missing Key Native",
            symbol="TRX-TRON-TASK-MISSING-KEY",
            coingecko_id="tron-task-missing-key-native",
            decimals=6,
        )
        tron_chain = Chain.objects.create(
            code="tron-missing-key",
            name="Tron Missing Key",
            type=ChainType.TRON,
            rpc="https://api.trongrid.io",
            native_coin=native,
            active=True,
        )

        scan_tron_chain.run(tron_chain.pk)

        scan_chain_mock.assert_not_called()

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
            tron_api_key="tron-key",
            native_coin=native,
            active=True,
        )
        Chain.objects.create(
            code="tron-no-key",
            name="Tron No Key",
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
