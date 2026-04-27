from django.test import RequestFactory
from django.test import SimpleTestCase
from django.test import override_settings

from common.middlewares import XcashMiddleware


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
