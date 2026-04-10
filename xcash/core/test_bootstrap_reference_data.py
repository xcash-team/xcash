import os
from unittest.mock import patch

from django.test import TestCase
from django.test import override_settings
from web3 import Web3

from chains.models import Chain
from chains.models import ChainType
from core.signals import bootstrap_reference_data_after_migrate
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.models import Fiat


class ReferenceDataBootstrapSignalTests(TestCase):
    def tearDown(self):
        from core import signals

        signals._BOOTSTRAPPED_DATABASE_ALIASES.clear()
        super().tearDown()

    @override_settings(DEBUG=False, AUTO_BOOTSTRAP_REFERENCE_DATA=True)
    @patch.dict(
        os.environ,
        {
            "DEFAULT_CHAIN_BOOTSTRAP_PROFILE": "auto",
            "BITCOIN_NETWORK": "mainnet",
        },
        clear=False,
    )
    def test_post_migrate_bootstraps_public_reference_data_by_default(self):
        bootstrap_reference_data_after_migrate(sender=None, using="default")

        self.assertTrue(Fiat.objects.filter(code="USD").exists())
        self.assertTrue(Crypto.objects.filter(symbol="ETH").exists())
        self.assertTrue(Crypto.objects.filter(symbol="TRX").exists())
        self.assertTrue(Crypto.objects.filter(symbol="BNB").exists())
        self.assertTrue(Crypto.objects.filter(symbol="POL").exists())
        self.assertTrue(Crypto.objects.get(symbol="TRX").is_native)

        eth_chain = Chain.objects.get(code="ethereum-mainnet")
        bsc_chain = Chain.objects.get(code="bsc-mainnet")
        polygon_chain = Chain.objects.get(code="polygon-mainnet")
        btc_chain = Chain.objects.get(code="bitcoin-mainnet")
        tron_chain = Chain.objects.get(code="tron-mainnet")

        self.assertEqual(eth_chain.type, ChainType.EVM)
        self.assertEqual(bsc_chain.type, ChainType.EVM)
        self.assertEqual(polygon_chain.type, ChainType.EVM)
        self.assertEqual(btc_chain.type, ChainType.BITCOIN)
        self.assertEqual(tron_chain.type, ChainType.TRON)
        self.assertFalse(eth_chain.active)
        self.assertFalse(bsc_chain.active)
        self.assertFalse(polygon_chain.active)
        self.assertFalse(btc_chain.active)
        self.assertFalse(tron_chain.active)
        self.assertEqual(eth_chain.rpc, "")
        self.assertEqual(bsc_chain.rpc, "")
        self.assertEqual(polygon_chain.rpc, "")
        self.assertEqual(btc_chain.rpc, "")
        self.assertEqual(tron_chain.rpc, "")
        self.assertIsNone(tron_chain.chain_id)
        self.assertIsNone(tron_chain.is_poa)
        self.assertEqual(tron_chain.confirm_block_count, 0)
        self.assertTrue(
            ChainToken.objects.filter(
                chain=eth_chain,
                crypto__symbol="ETH",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=btc_chain,
                crypto__symbol="BTC",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=bsc_chain,
                crypto__symbol="BNB",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=polygon_chain,
                crypto__symbol="POL",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=tron_chain,
                crypto__symbol="TRX",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=eth_chain,
                crypto__symbol="USDT",
                address=Web3.to_checksum_address(
                    "0xdAC17F958D2ee523a2206206994597C13D831ec7"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=eth_chain,
                crypto__symbol="USDC",
                address=Web3.to_checksum_address(
                    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=eth_chain,
                crypto__symbol="DAI",
                address=Web3.to_checksum_address(
                    "0x6B175474E89094C44Da98b954EedeAC495271d0F"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=bsc_chain,
                crypto__symbol="USDT",
                address=Web3.to_checksum_address(
                    "0x55d398326f99059fF775485246999027B3197955"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=bsc_chain,
                crypto__symbol="USDC",
                address=Web3.to_checksum_address(
                    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=bsc_chain,
                crypto__symbol="DAI",
                address=Web3.to_checksum_address(
                    "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=polygon_chain,
                crypto__symbol="USDT",
                address=Web3.to_checksum_address(
                    "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=polygon_chain,
                crypto__symbol="USDC",
                address=Web3.to_checksum_address(
                    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=polygon_chain,
                crypto__symbol="DAI",
                address=Web3.to_checksum_address(
                    "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=tron_chain,
                crypto__symbol="USDT",
                address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            ).exists()
        )

    @override_settings(DEBUG=True, AUTO_BOOTSTRAP_REFERENCE_DATA=True)
    @patch("core.default_data.has_standard_erc20_interface", return_value=True)
    @patch.dict(
        os.environ,
        {
            "DEFAULT_CHAIN_BOOTSTRAP_PROFILE": "auto",
            "BITCOIN_NETWORK": "mainnet",
            "LOCAL_EVM_CHAIN_CODE": "ethereum-local",
            "LOCAL_EVM_CHAIN_NAME": "Ethereum Local",
            "LOCAL_EVM_RPC": "http://127.0.0.1:8545",
            "LOCAL_EVM_CHAIN_ID": "31337",
            "LOCAL_BTC_CHAIN_CODE": "bitcoin-local",
            "LOCAL_BTC_CHAIN_NAME": "Bitcoin Local",
            "LOCAL_BTC_RPC": "http://xcash:xcash@127.0.0.1:18443/wallet/xcash",
            "LOCAL_EVM_USDT_ADDRESS": "0x00000000000000000000000000000000000000B1",
            "LOCAL_EVM_USDC_ADDRESS": "0x00000000000000000000000000000000000000B2",
            "LOCAL_EVM_DAI_ADDRESS": "0x00000000000000000000000000000000000000B3",
        },
        clear=False,
    )
    def test_post_migrate_bootstraps_local_reference_data_in_debug(self, _has_erc20):
        bootstrap_reference_data_after_migrate(sender=None, using="default")

        evm_chain = Chain.objects.get(code="ethereum-local")
        btc_chain = Chain.objects.get(code="bitcoin-local")

        self.assertEqual(evm_chain.type, ChainType.EVM)
        self.assertEqual(evm_chain.chain_id, 31337)
        self.assertEqual(evm_chain.confirm_block_count, 1)
        self.assertEqual(btc_chain.type, ChainType.BITCOIN)
        self.assertEqual(btc_chain.confirm_block_count, 1)
        self.assertTrue(
            ChainToken.objects.filter(
                chain=evm_chain,
                crypto__symbol="ETH",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=btc_chain,
                crypto__symbol="BTC",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=evm_chain,
                crypto__symbol="USDT",
                address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000B1"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=evm_chain,
                crypto__symbol="USDC",
                address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000B2"
                ),
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=evm_chain,
                crypto__symbol="DAI",
                address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000B3"
                ),
            ).exists()
        )

    @override_settings(AUTO_BOOTSTRAP_REFERENCE_DATA=True)
    def test_post_migrate_bootstraps_each_database_only_once_per_process(self):
        with (
            patch("core.signals._reference_tables_ready", return_value=True),
            patch("core.signals.ensure_default_reference_data") as bootstrap_mock,
        ):
            bootstrap_reference_data_after_migrate(sender=None, using="default")
            bootstrap_reference_data_after_migrate(sender=None, using="default")

        bootstrap_mock.assert_called_once_with(using="default")
