import threading
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.contrib.admin.sites import AdminSite
from django.core.cache import cache
from django.db import connections
from django.db import close_old_connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.test import override_settings
from django.utils import timezone
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskFailureReason
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from chains.models import TxHash
from chains.models import Wallet
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from common.consts import ERC20_TRANSFER_GAS
from currencies.models import ChainToken
from currencies.models import Crypto
from evm.admin import EvmScanCursorAdmin
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.erc20 import EvmErc20TransferScanner
from evm.scanner.native import EvmNativeDirectScanner
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcError
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage



class EvmAdapterTests(TestCase):
    def test_tx_result_returns_confirmed_when_status_is_one(self):
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"status": 1}),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.CONFIRMED)

    def test_tx_result_returns_failed_when_status_is_zero(self):
        # 链上执行失败（revert）应返回 FAILED，而不是和 pending / not found 混为一类。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"status": 0}),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.FAILED)

    def test_tx_result_returns_confirming_when_transaction_not_found(self):
        from web3.exceptions import TransactionNotFound

        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(
                    side_effect=TransactionNotFound("0x" + "ab" * 32),
                ),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.CONFIRMING)

    def test_tx_result_returns_confirming_when_receipt_is_none(self):
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value=None),
            ),
        )

        from chains.adapters import TxCheckStatus
        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertEqual(result, TxCheckStatus.CONFIRMING)

    def test_tx_result_returns_exception_when_receipt_missing_status(self):
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(return_value={"transactionHash": "0x01"}),
            ),
        )

        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertIsInstance(result, RuntimeError)

    def test_tx_result_returns_exception_on_rpc_error(self):
        # RPC 调用异常（网络问题等）应返回异常对象，由上层决定是否重试。
        chain = Chain(
            code="eth",
            name="Ethereum",
            type=ChainType.EVM,
            chain_id=1,
            native_coin=Crypto(name="Ethereum", symbol="ETH", coingecko_id="ethereum"),
        )
        rpc_error = ConnectionError("node unreachable")
        chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(
                get_transaction_receipt=Mock(side_effect=rpc_error),
            ),
        )

        from evm.adapter import EvmAdapter

        result = EvmAdapter.tx_result(chain, "0x" + "ab" * 32)
        self.assertIsInstance(result, ConnectionError)
