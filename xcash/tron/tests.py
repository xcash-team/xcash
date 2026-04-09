from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from django.db import IntegrityError
from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings

from chains.models import Chain
from chains.models import ChainType
from currencies.models import Crypto
from tron.client import TronHttpClient
from tron.models import TronWatchCursor


@override_settings(TRON_RPC_TIMEOUT=3.0, TRON_API_KEY="tron-key")
class TronHttpClientTests(SimpleTestCase):
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
