from os import environ
from unittest.mock import patch

from bip_utils import Bip44Coins
from django.test import RequestFactory
from django.test import SimpleTestCase
from django.test import override_settings

from bitcoin.adapter import BitcoinAdapter
from bitcoin.network import get_active_bitcoin_network
from common.middlewares import XcashMiddleware
from common.utils.bitcoin import is_valid_bitcoin_address


class BitcoinAddressValidationTests(SimpleTestCase):
    @patch.dict(environ, {"BITCOIN_NETWORK": "mainnet"}, clear=False)
    def test_base58_address_requires_real_checksum(self):
        # 真实 checksum 校验必须能识别“形状正确但最后一位被篡改”的假地址。
        self.assertTrue(is_valid_bitcoin_address("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"))
        self.assertFalse(is_valid_bitcoin_address("1BoatSLRHtKNngkdXEeobR76b53LETtpy1"))

    @patch.dict(environ, {"BITCOIN_NETWORK": "mainnet"}, clear=False)
    def test_bech32_address_requires_real_checksum(self):
        # bech32 校验同样不能只看前缀与字符集。
        self.assertTrue(
            is_valid_bitcoin_address("bc1qz252a6sxsamzl8sllmtcxtmsntkjek4z2vaktq")
        )
        self.assertFalse(
            is_valid_bitcoin_address("bc1qz252a6sxsamzl8sllmtcxtmsntkjek4z2vaktp")
        )

    @patch.dict(environ, {"BITCOIN_NETWORK": "mainnet"}, clear=False)
    def test_adapter_reuses_common_bitcoin_validation(self):
        # 适配器层必须和模型字段共用同一套 BTC 校验规则。
        self.assertTrue(
            BitcoinAdapter.validate_address("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy")
        )
        self.assertFalse(
            BitcoinAdapter.validate_address("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNL1")
        )

    @patch.dict(environ, {"BITCOIN_NETWORK": "regtest"}, clear=False)
    def test_regtest_network_accepts_testnet_style_base58_address(self):
        # regtest 使用 testnet 的 base58 版本；本地联调时必须接受这类地址。
        self.assertTrue(is_valid_bitcoin_address("mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn"))
        self.assertFalse(is_valid_bitcoin_address("1BoatSLRHtKNngkdXEeobR76b53LETtpyT"))

    @patch.dict(environ, {"BITCOIN_NETWORK": "regtest"}, clear=False)
    def test_signer_wallet_switches_bip44_coin_for_regtest(self):
        # signer 与主应用共享同一份 Bitcoin 网络配置，切到 regtest 时必须落到 testnet 的 BIP44 coin。
        self.assertEqual(
            get_active_bitcoin_network().bip44_coin, Bip44Coins.BITCOIN_TESTNET
        )
        self.assertEqual(get_active_bitcoin_network().name, "regtest")


class TrustedProxyClientIpTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    @override_settings(TRUSTED_PROXY_IPS=["127.0.0.1", "::1"])
    def test_trusted_proxy_can_forward_x_real_ip(self):
        # 只有来自受信代理的请求，才允许把 X-Real-IP 作为真实客户端地址。
        request = self.factory.get(
            "/v1/demo",
            headers={"X-Real-IP": "203.0.113.9"},
            REMOTE_ADDR="127.0.0.1",
        )

        self.assertEqual(XcashMiddleware._client_ip(request), "203.0.113.9")

    @override_settings(TRUSTED_PROXY_IPS=["127.0.0.1", "::1"])
    def test_untrusted_source_cannot_spoof_x_real_ip(self):
        # 源站直连时即使带了 X-Real-IP，也只能回退到实际 TCP 来源地址。
        request = self.factory.get(
            "/v1/demo",
            headers={"X-Real-IP": "203.0.113.9"},
            REMOTE_ADDR="198.51.100.7",
        )

        self.assertEqual(XcashMiddleware._client_ip(request), "198.51.100.7")
