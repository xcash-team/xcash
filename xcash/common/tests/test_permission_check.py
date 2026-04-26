from unittest.mock import patch, Mock

import httpx
from django.core.cache import cache
from django.test import TestCase, override_settings

from common.exceptions import APIError
from common.error_codes import ErrorCode


@override_settings(
    INTERNAL_API_TOKEN="xcash-saas-token",
    SAAS_CALLBACK_URL="http://saas",
)
class CheckSaasPermissionTest(TestCase):
    def setUp(self):
        cache.clear()

    @patch("common.permission_check.httpx.Client")
    def test_caches_successful_response(self, mock_client_cls):
        from common.permission_check import check_saas_permission
        from common.exceptions import APIError

        mock_resp = Mock()
        mock_resp.json.return_value = {
            "appid": "XC-a",
            "frozen": False,
            "enable_deposit": True,
            "enable_withdrawal": True,
        }
        mock_resp.raise_for_status.return_value = None
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp

        check_saas_permission(appid="XC-a", action="deposit")  # 第一次调
        check_saas_permission(appid="XC-a", action="deposit")  # 第二次应命中缓存

        # 只调一次 SaaS
        self.assertEqual(
            mock_client_cls.return_value.__enter__.return_value.post.call_count, 1,
        )

    @patch("common.permission_check.httpx.Client")
    def test_denies_disabled_feature(self, mock_client_cls):
        from common.permission_check import check_saas_permission

        mock_resp = Mock()
        mock_resp.json.return_value = {
            "appid": "XC-disabled",
            "frozen": False,
            "enable_deposit": True,
            "enable_withdrawal": False,
        }
        mock_resp.raise_for_status.return_value = None
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp

        check_saas_permission(appid="XC-disabled", action="deposit")  # OK

        self.assertRaises(APIError, check_saas_permission, appid="XC-disabled", action="withdrawal")

    @patch("common.permission_check.httpx.Client")
    def test_denies_frozen_user(self, mock_client_cls):
        from common.permission_check import check_saas_permission

        mock_resp = Mock()
        mock_resp.json.return_value = {
            "appid": "XC-frozen",
            "frozen": True,
            "enable_deposit": True,
            "enable_withdrawal": True,
        }
        mock_resp.raise_for_status.return_value = None
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp

        self.assertRaises(APIError, check_saas_permission, appid="XC-frozen", action="deposit")

    @patch("common.permission_check.httpx.Client")
    def test_uses_stale_cache_on_saas_unavailable(self, mock_client_cls):
        """SaaS 第一次返回成功，第二次超时 → 用 stale 缓存。"""
        from common.permission_check import check_saas_permission

        ok_resp = Mock()
        ok_resp.json.return_value = {
            "appid": "XC-stale",
            "frozen": False,
            "enable_deposit": True,
            "enable_withdrawal": False,
        }
        ok_resp.raise_for_status.return_value = None

        # 第一次成功，缓存写入
        mock_client_cls.return_value.__enter__.return_value.post.return_value = ok_resp
        check_saas_permission(appid="XC-stale", action="deposit")

        # 模拟 60 秒后正常缓存过期，但 stale 仍在
        cache.delete("saas:permission:XC-stale")

        # 第二次 SaaS 超时
        mock_client_cls.return_value.__enter__.return_value.post.side_effect = httpx.ConnectError("boom")
        # 应该用 stale 缓存判定
        check_saas_permission(appid="XC-stale", action="deposit")  # 不抛异常

        self.assertRaises(APIError, check_saas_permission, appid="XC-stale", action="withdrawal")

    @patch("common.permission_check.httpx.Client")
    def test_fail_closed_on_cold_start_with_saas_unavailable(self, mock_client_cls):
        from common.permission_check import check_saas_permission

        mock_client_cls.return_value.__enter__.return_value.post.side_effect = httpx.ConnectError("boom")

        self.assertRaises(APIError, check_saas_permission, appid="XC-cold", action="deposit")

    @override_settings(INTERNAL_API_TOKEN="")
    def test_no_token_means_self_hosted_pass_through(self):
        """INTERNAL_API_TOKEN 为空（自托管模式）：直接放行。"""
        from common.permission_check import check_saas_permission

        # 不应抛异常，不应调用 SaaS
        check_saas_permission(appid="XC-a", action="withdrawal")

    @patch("common.permission_check.httpx.Client")
    def test_saas_returns_4xx_treated_as_unavailable(self, mock_client_cls):
        """SaaS 返回 4xx（如 token 错误）应走 fail-closed，与 connect_error 等价。"""
        from common.permission_check import check_saas_permission

        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Forbidden", request=Mock(), response=Mock(status_code=403),
        )
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp

        # 冷启动 + SaaS 返回 4xx → fail-closed
        self.assertRaises(APIError, check_saas_permission, appid="XC-4xx", action="deposit")

    def test_missing_appid_raises_invalid_appid(self):
        """appid=None 直接抛 INVALID_APPID，不走 SaaS 调用。"""
        from common.permission_check import check_saas_permission

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(appid=None, action="deposit")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_APPID)

    def test_empty_appid_raises_invalid_appid(self):
        """appid='' 也走 INVALID_APPID。"""
        from common.permission_check import check_saas_permission

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(appid="", action="deposit")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_APPID)
