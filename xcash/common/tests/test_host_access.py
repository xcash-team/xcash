import importlib
import os
import sys

from django.http import HttpResponse
from django.test import RequestFactory
from django.test import SimpleTestCase
from django.test import override_settings

from common.host_access import normalize_ip_host
from common.middlewares import InternalApiHostRestrictionMiddleware


class NormalizeIpHostTests(SimpleTestCase):
    def test_normalize_ip_host_strips_whitespace(self):
        self.assertEqual(
            normalize_ip_host(" 192.168.1.10 "),
            "192.168.1.10",
        )

    def test_normalize_ip_host_allows_blank_value(self):
        self.assertEqual(normalize_ip_host(""), "")

    def test_normalize_ip_host_rejects_invalid_value(self):
        with self.assertRaisesMessage(ValueError, "INTERNAL_API_IP"):
            normalize_ip_host("not-an-ip")


class ProductionInternalApiHostSettingsTests(SimpleTestCase):
    def tearDown(self):
        sys.modules.pop("config.settings.production", None)
        super().tearDown()

    def test_internal_api_ip_defaults_to_loopback(self):
        original = os.environ.pop("INTERNAL_API_IP", None)
        self.addCleanup(self._restore_internal_api_ip, original)
        os.environ.setdefault("DJANGO_SECRET_KEY", "test-secret-key")

        production = importlib.import_module("config.settings.production")

        self.assertEqual(production.INTERNAL_API_ALLOWED_IP, "127.0.0.1")
        self.assertIn("127.0.0.1", production.ALLOWED_HOSTS)

    @staticmethod
    def _restore_internal_api_ip(original):
        if original is None:
            os.environ.pop("INTERNAL_API_IP", None)
            return

        os.environ["INTERNAL_API_IP"] = original


@override_settings(
    ALLOWED_HOSTS=["merchant.example.com", "192.168.1.10"],
    INTERNAL_API_ALLOWED_IP="192.168.1.10",
)
class InternalApiHostRestrictionMiddlewareTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.middleware = InternalApiHostRestrictionMiddleware(
            lambda request: HttpResponse("ok")
        )

    def test_ip_host_can_access_internal_api_path(self):
        request = self.factory.get(
            "/internal/v1/projects",
            HTTP_HOST="192.168.1.10:6688",
        )

        response = self.middleware(request)

        self.assertEqual(response.status_code, 200)

    def test_ip_host_cannot_access_admin_path(self):
        request = self.factory.get(
            "/admin/login/",
            HTTP_HOST="192.168.1.10:6688",
        )

        response = self.middleware(request)

        self.assertEqual(response.status_code, 404)

    def test_domain_host_is_not_restricted(self):
        request = self.factory.get(
            "/admin/login/",
            HTTP_HOST="merchant.example.com",
        )

        response = self.middleware(request)

        self.assertEqual(response.status_code, 200)
