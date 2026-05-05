from __future__ import annotations

import time
from unittest.mock import Mock
from unittest.mock import patch

import httpx
from django.core.cache import cache
from django.test import TestCase
from django.test import override_settings

from common.error_codes import ErrorCode
from common.exceptions import APIError
from common.permission_check import _refresh_saas_permission
from common.permission_check import check_saas_permission


@override_settings(
    INTERNAL_API_TOKEN="xcash-saas-token",
    SAAS_CALLBACK_URL="http://saas",
)
class CheckSaasPermissionTest(TestCase):
    """check_saas_permission 主入口的行为测试。

    新策略：完全基于本地缓存判定；缺缓存或 stale 缓存只派发后台刷新，不阻塞主链路。
    """

    def setUp(self):
        cache.clear()

    @patch("common.permission_check._refresh_saas_permission.delay")
    def test_cold_start_passes_through_and_schedules_refresh(self, mock_delay):
        """无缓存 → 默认放行 + 派发刷新任务。"""

        check_saas_permission(appid="XC-new", action="deposit")  # 不抛
        mock_delay.assert_called_once_with(appid="XC-new")

    @patch("common.permission_check._refresh_saas_permission.delay")
    def test_fresh_cache_no_refresh(self, mock_delay):
        """命中新缓存（fetched_at < 60s）→ 不派发刷新。"""

        cache.set(
            "saas:permission:XC-a",
            {"frozen": False, "enable_deposit_withdrawal": True, "_fetched_at": time.time()},
            None,
        )

        check_saas_permission(appid="XC-a", action="deposit")
        mock_delay.assert_not_called()

    @patch("common.permission_check._refresh_saas_permission.delay")
    def test_stale_cache_triggers_refresh_but_uses_cache(self, mock_delay):
        """命中 stale 缓存（fetched_at > 60s）→ 派发刷新，本次仍按旧缓存判定。"""

        cache.set(
            "saas:permission:XC-a",
            {"frozen": False, "enable_deposit_withdrawal": True, "_fetched_at": time.time() - 120},
            None,
        )

        check_saas_permission(appid="XC-a", action="deposit")  # 旧缓存说放行
        mock_delay.assert_called_once_with(appid="XC-a")

    @patch("common.permission_check._refresh_saas_permission.delay")
    def test_refresh_lock_dedupes_within_window(self, mock_delay):
        """同一 appid 在锁窗口内多次触发，只派发一次刷新任务。"""

        # 3 次 cold start，预期只派发 1 次
        check_saas_permission(appid="XC-dup", action="deposit")
        check_saas_permission(appid="XC-dup", action="deposit")
        check_saas_permission(appid="XC-dup", action="deposit")

        self.assertEqual(mock_delay.call_count, 1)

    @patch("common.permission_check._refresh_saas_permission.delay")
    def test_refresh_lock_is_per_appid(self, mock_delay):
        """不同 appid 的刷新锁互不干扰。"""

        check_saas_permission(appid="XC-a", action="deposit")
        check_saas_permission(appid="XC-b", action="deposit")

        self.assertEqual(mock_delay.call_count, 2)

    def test_frozen_user_denied(self):
        """缓存里 frozen=True → 拒绝。"""

        cache.set(
            "saas:permission:XC-frozen",
            {"frozen": True, "enable_deposit_withdrawal": True, "_fetched_at": time.time()},
            None,
        )

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(appid="XC-frozen", action="deposit")
        self.assertEqual(ctx.exception.error_code, ErrorCode.ACCOUNT_FROZEN)

    def test_disabled_deposit_withdrawal_feature_denies_both_actions(self):
        """缓存里 enable_deposit_withdrawal=False → deposit/withdrawal 都拒绝。"""

        cache.set(
            "saas:permission:XC-d",
            {
                "frozen": False,
                "enable_deposit_withdrawal": False,
                "_fetched_at": time.time(),
            },
            None,
        )

        with self.assertRaises(APIError) as deposit_ctx:
            check_saas_permission(appid="XC-d", action="deposit")
        self.assertEqual(deposit_ctx.exception.error_code, ErrorCode.FEATURE_NOT_ENABLED)

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(appid="XC-d", action="withdrawal")
        self.assertEqual(ctx.exception.error_code, ErrorCode.FEATURE_NOT_ENABLED)

    def test_allowed_chain_and_crypto_pass(self):
        """缓存里 chain/token 白名单同时命中 → 放行。"""

        cache.set(
            "saas:permission:XC-allowed",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": ["ethereum-mainnet", "bsc-mainnet"],
                "allowed_crypto_symbols": ["USDT", "USDC"],
                "_fetched_at": time.time(),
            },
            None,
        )

        check_saas_permission(
            appid="XC-allowed",
            action="deposit",
            chain_code="ethereum-mainnet",
            crypto_symbol="USDT",
        )

    def test_empty_chain_and_crypto_whitelists_mean_all_supported(self):
        """SaaS 传空白名单时按未限制处理，兼容未设置的 Tier。"""

        cache.set(
            "saas:permission:XC-empty-whitelist",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": [],
                "allowed_crypto_symbols": [],
                "_fetched_at": time.time(),
            },
            None,
        )

        check_saas_permission(
            appid="XC-empty-whitelist",
            action="deposit",
            chain_code="bsc-mainnet",
            crypto_symbol="ETH",
        )

    def test_disallowed_chain_denied(self):
        """缓存里 chain 不在白名单 → 拒绝该 chain/token 组合。"""

        cache.set(
            "saas:permission:XC-chain-denied",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": ["ethereum-mainnet"],
                "allowed_crypto_symbols": ["USDT", "USDC"],
                "_fetched_at": time.time(),
            },
            None,
        )

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(
                appid="XC-chain-denied",
                action="deposit",
                chain_code="bsc-mainnet",
                crypto_symbol="USDT",
            )
        self.assertEqual(ctx.exception.error_code, ErrorCode.FEATURE_NOT_ENABLED)

    def test_disallowed_crypto_denied(self):
        """缓存里 crypto 不在白名单 → 拒绝该 chain/token 组合。"""

        cache.set(
            "saas:permission:XC-crypto-denied",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": ["ethereum-mainnet"],
                "allowed_crypto_symbols": ["USDT"],
                "_fetched_at": time.time(),
            },
            None,
        )

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(
                appid="XC-crypto-denied",
                action="withdrawal",
                chain_code="ethereum-mainnet",
                crypto_symbol="USDC",
            )
        self.assertEqual(ctx.exception.error_code, ErrorCode.FEATURE_NOT_ENABLED)

    def test_missing_chain_token_args_keeps_feature_only_check(self):
        """未传 chain/token 时保持旧行为，只校验功能开关。"""

        cache.set(
            "saas:permission:XC-feature-only",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "allowed_chain_codes": ["ethereum-mainnet"],
                "allowed_crypto_symbols": ["USDT"],
                "_fetched_at": time.time(),
            },
            None,
        )

        check_saas_permission(appid="XC-feature-only", action="deposit")

    def test_single_feature_flag_allows_withdrawal(self):
        """withdrawal 也只读取 enable_deposit_withdrawal，不再要求独立开关。"""

        cache.set(
            "saas:permission:XC-single-flag",
            {
                "frozen": False,
                "enable_deposit_withdrawal": True,
                "_fetched_at": time.time(),
            },
            None,
        )

        check_saas_permission(appid="XC-single-flag", action="withdrawal")

    @override_settings(INTERNAL_API_TOKEN="")
    @patch("common.permission_check._refresh_saas_permission.delay")
    def test_self_hosted_pass_through_no_refresh(self, mock_delay):
        """INTERNAL_API_TOKEN 为空（自托管）：直接放行，且不派发任务。"""

        check_saas_permission(appid="XC-a", action="withdrawal")
        mock_delay.assert_not_called()

    def test_missing_appid_raises_invalid_appid(self):
        """appid=None 直接抛 INVALID_APPID。"""

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(appid=None, action="deposit")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_APPID)

    def test_empty_appid_raises_invalid_appid(self):
        """appid='' 也走 INVALID_APPID。"""

        with self.assertRaises(APIError) as ctx:
            check_saas_permission(appid="", action="deposit")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_APPID)


@override_settings(
    INTERNAL_API_TOKEN="xcash-saas-token",
    SAAS_CALLBACK_URL="http://saas",
)
class RefreshSaasPermissionTaskTest(TestCase):
    """_refresh_saas_permission celery 任务本体的行为测试。"""

    def setUp(self):
        cache.clear()

    @patch("common.permission_check.httpx.Client")
    def test_task_writes_cache_with_fetched_at(self, mock_client_cls):
        """任务成功 → 缓存被覆写，含 _fetched_at 时间戳。"""

        mock_resp = Mock()
        mock_resp.json.return_value = {
            "appid": "XC-r",
            "frozen": False,
            "enable_deposit_withdrawal": True,
        }
        mock_resp.raise_for_status.return_value = None
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp

        before = time.time()
        _refresh_saas_permission.run(appid="XC-r")
        after = time.time()

        cached = cache.get("saas:permission:XC-r")
        self.assertIsNotNone(cached)
        self.assertTrue(cached["enable_deposit_withdrawal"])
        self.assertNotIn("enable_deposit", cached)
        self.assertNotIn("enable_withdrawal", cached)
        self.assertIn("_fetched_at", cached)
        self.assertGreaterEqual(cached["_fetched_at"], before)
        self.assertLessEqual(cached["_fetched_at"], after)

    @patch("common.permission_check.httpx.Client")
    def test_task_failure_keeps_old_cache(self, mock_client_cls):
        """任务调 SaaS 失败 → 旧缓存原封不动，方便后续主调用继续兜底。"""

        old = {"frozen": False, "enable_deposit_withdrawal": True, "_fetched_at": time.time() - 100}
        cache.set("saas:permission:XC-keep", old, None)

        mock_client_cls.return_value.__enter__.return_value.post.side_effect = httpx.ConnectError("boom")
        _refresh_saas_permission.run(appid="XC-keep")

        self.assertEqual(cache.get("saas:permission:XC-keep"), old)

    @patch("common.permission_check.httpx.Client")
    def test_task_4xx_treated_as_failure(self, mock_client_cls):
        """SaaS 返回 4xx（如 token 错误）→ 同样视作失败，不破坏旧缓存。"""

        old = {"frozen": False, "enable_deposit_withdrawal": True, "_fetched_at": time.time() - 100}
        cache.set("saas:permission:XC-4xx", old, None)

        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Forbidden", request=Mock(), response=Mock(status_code=403),
        )
        mock_client_cls.return_value.__enter__.return_value.post.return_value = mock_resp

        _refresh_saas_permission.run(appid="XC-4xx")

        self.assertEqual(cache.get("saas:permission:XC-4xx"), old)

    @override_settings(INTERNAL_API_TOKEN="")
    @patch("common.permission_check.httpx.Client")
    def test_task_skips_when_no_token(self, mock_client_cls):
        """自托管模式下任务被错误派发也不会调 SaaS。"""

        _refresh_saas_permission.run(appid="XC-x")

        mock_client_cls.assert_not_called()
