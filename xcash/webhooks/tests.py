from unittest.mock import MagicMock
from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase
from django.utils import timezone

from chains.models import Wallet
from core.models import PLATFORM_SETTINGS_CACHE_KEY
from core.models import PlatformSettings
from projects.models import Project
from webhooks.models import DeliveryAttempt
from webhooks.models import WebhookEvent
from webhooks.service import WebhookService
from webhooks.tasks import deliver_event
from webhooks.tasks import next_backoff


def _make_project(**kwargs):
    defaults = {
        "name": f"Demo-{Project.objects.count()}",
        "wallet": Wallet.objects.create(),
        "webhook": "https://merchant.example.com/hook",
        "webhook_open": True,
    }
    defaults.update(kwargs)
    return Project.objects.create(**defaults)


class WebhookServiceTests(TestCase):
    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        super().tearDown()

    @patch("webhooks.tasks.deliver_event.delay")
    def test_create_event_enqueues_delivery_after_commit(self, deliver_event_mock):
        # webhook 事件创建后必须显式在 on_commit 派发投递任务，而不是依赖 model signal。
        project = _make_project()

        with self.captureOnCommitCallbacks(execute=True):
            event = WebhookService.create_event(
                project=project,
                payload={"type": "deposit", "data": {"foo": "bar"}},
            )

        deliver_event_mock.assert_called_once_with(event.pk)

    def test_next_backoff_uses_platform_settings_cap(self):
        # Webhook 退避上限应可由平台参数中心调整，避免固定 120 秒无法匹配实际值守策略。
        PlatformSettings.objects.create(webhook_delivery_max_backoff_seconds=20)

        self.assertEqual(next_backoff(1), 4)
        self.assertEqual(next_backoff(10), 20)


class DeliverEventTests(TestCase):
    """覆盖 deliver_event 各核心分支。"""

    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        cache.clear()
        super().tearDown()

    def _create_event(self, project=None, **kwargs):
        if project is None:
            project = _make_project()
        return WebhookEvent.objects.create(
            project=project,
            payload={"type": "test", "data": {}},
            **kwargs,
        )

    # ── 成功路径 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_deliver_success_marks_event_succeeded(self, mock_http):
        mock_http.return_value = (True, 200, {}, "ok", "", 50)
        event = self._create_event()

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.SUCCEEDED)
        self.assertIsNotNone(event.delivered_at)
        self.assertEqual(event.last_error, "")

    @patch("webhooks.tasks._execute_http_delivery")
    def test_deliver_success_resets_failed_count(self, mock_http):
        mock_http.return_value = (True, 200, {}, "ok", "", 50)
        project = _make_project(failed_count=5)
        event = self._create_event(project=project)

        deliver_event(event.pk)

        project.refresh_from_db()
        self.assertEqual(project.failed_count, 0)
        self.assertTrue(project.webhook_open)

    @patch("webhooks.tasks._execute_http_delivery")
    def test_deliver_success_creates_attempt(self, mock_http):
        mock_http.return_value = (True, 200, {}, "ok", "", 50)
        event = self._create_event()

        deliver_event(event.pk)

        self.assertEqual(DeliveryAttempt.objects.filter(event=event).count(), 1)
        attempt = DeliveryAttempt.objects.get(event=event)
        self.assertTrue(attempt.ok)
        self.assertEqual(attempt.try_number, 1)

    # ── 失败路径：5xx 可重试 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_5xx_retryable_sets_schedule_locked(self, mock_http):
        mock_http.return_value = (False, 500, {}, "Internal Server Error", "", 50)
        event = self._create_event()

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.PENDING)
        self.assertIsNotNone(event.schedule_locked_until)
        self.assertGreater(event.schedule_locked_until, timezone.now())

    # ── 失败路径：4xx 不可重试 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_4xx_marks_event_failed(self, mock_http):
        mock_http.return_value = (False, 404, {}, "Not Found", "", 50)
        event = self._create_event()

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.FAILED)

    # ── 失败路径：3xx 不可重试（修复后行为）──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_3xx_marks_event_failed(self, mock_http):
        """3xx 重定向不应被视为可重试，httpx 不跟随重定向，重试无意义。"""
        mock_http.return_value = (False, 301, {}, "", "", 50)
        event = self._create_event()

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.FAILED)

    # ── 网络错误可重试 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_network_error_retryable(self, mock_http):
        mock_http.return_value = (False, None, None, "", "ConnectError: ...", 5000)
        event = self._create_event()

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.PENDING)
        self.assertIsNotNone(event.schedule_locked_until)

    # ── 熔断机制 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_breaker_trips_after_threshold(self, mock_http):
        """连续失败达到阈值后自动关闭项目 webhook。"""
        PlatformSettings.objects.create(webhook_delivery_breaker_threshold=2)
        mock_http.return_value = (False, 500, {}, "error", "", 50)
        project = _make_project(failed_count=1)
        event = self._create_event(project=project)

        deliver_event(event.pk)

        project.refresh_from_db()
        self.assertFalse(project.webhook_open)
        self.assertEqual(project.failed_count, 2)
        # 熔断后事件不可重试，直接标记失败
        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.FAILED)

    # ── 幂等：非 PENDING 跳过 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_skip_non_pending_event(self, mock_http):
        event = self._create_event(status=WebhookEvent.Status.SUCCEEDED)

        deliver_event(event.pk)

        mock_http.assert_not_called()

    # ── webhook 未配置 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_no_webhook_url_marks_failed(self, mock_http):
        project = _make_project(webhook="")
        event = self._create_event(project=project)

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.FAILED)
        self.assertIn("not configured", event.last_error)
        mock_http.assert_not_called()

    # ── webhook_open=False ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_webhook_closed_marks_failed(self, mock_http):
        project = _make_project(webhook_open=False)
        event = self._create_event(project=project)

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.FAILED)
        self.assertIn("not open", event.last_error)
        mock_http.assert_not_called()

    # ── 超出重试次数 ──

    @patch("webhooks.tasks._execute_http_delivery")
    def test_exceeds_max_retries_marks_failed(self, mock_http):
        PlatformSettings.objects.create(webhook_delivery_max_retries=1)
        mock_http.return_value = (False, 500, {}, "error", "", 50)
        event = self._create_event()
        # 模拟已有 1 次尝试
        DeliveryAttempt.objects.create(
            event=event,
            try_number=1,
            request_headers={},
            request_body="{}",
            duration_ms=50,
        )

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.FAILED)


class WebhookDeliveryPolicyTests(TestCase):
    """验证 GET_QUERY 与 POST_JSON 两种投递方式的分派逻辑。"""

    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        super().tearDown()

    @patch("webhooks.tasks._execute_http_delivery")
    def test_get_query_delivery_uses_event_delivery_url_and_success_text(self, mock_http):
        mock_http.return_value = (True, 200, {}, "success", "", 30)
        project = _make_project(webhook="https://native.example.com/hook")
        event = WebhookEvent.objects.create(
            project=project,
            payload={"pid": "1001", "trade_status": "TRADE_SUCCESS"},
            delivery_url="https://merchant.example.com/notify",
            delivery_method=WebhookEvent.DeliveryMethod.GET_QUERY,
            expected_response_body="success",
        )

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.SUCCEEDED)
        call_kwargs = mock_http.call_args.kwargs
        self.assertEqual(call_kwargs["request_url"], "https://merchant.example.com/notify")
        self.assertEqual(call_kwargs["method"], "GET")
        self.assertEqual(call_kwargs["params"], {"pid": "1001", "trade_status": "TRADE_SUCCESS"})
        self.assertEqual(call_kwargs["expected_response_body"], "success")
        # 默认未配置出口代理，请求 header 中不应出现代理转发字段
        self.assertNotIn("CF-Worker-Destination", call_kwargs["headers"])
        self.assertNotIn("CF-Worker-Key", call_kwargs["headers"])

    @patch("webhooks.tasks._egress_proxy_key", "proxy-key-secret")
    @patch("webhooks.tasks._egress_proxy_url", "https://proxy.example.com/forward")
    @patch("webhooks.tasks._execute_http_delivery")
    def test_get_query_uses_egress_proxy_when_configured(self, mock_http):
        """配置了出口代理后，GET 类型 webhook 必须走代理转发，避免暴露真实 IP / SSRF。"""
        # 任务在投递完成后会从 headers 字典中 pop 掉代理鉴权字段，避免落库；
        # 为了断言"调用 _execute_http_delivery 时刻"的 header，必须在 side_effect 里做快照。
        captured = {}

        def _capture(**kwargs):
            captured["request_url"] = kwargs["request_url"]
            captured["method"] = kwargs["method"]
            captured["params"] = kwargs["params"]
            captured["headers"] = dict(kwargs["headers"])
            return (True, 200, {}, "success", "", 30)

        mock_http.side_effect = _capture
        project = _make_project()
        event = WebhookEvent.objects.create(
            project=project,
            payload={"pid": "1001", "trade_status": "TRADE_SUCCESS"},
            delivery_url="https://merchant.example.com/notify",
            delivery_method=WebhookEvent.DeliveryMethod.GET_QUERY,
            expected_response_body="success",
        )

        deliver_event(event.pk)

        # 请求 URL 改为代理地址，原商户 URL 通过 header 传递给代理
        self.assertEqual(captured["request_url"], "https://proxy.example.com/forward")
        self.assertEqual(
            captured["headers"]["CF-Worker-Destination"],
            "https://merchant.example.com/notify",
        )
        self.assertEqual(captured["headers"]["CF-Worker-Key"], "proxy-key-secret")
        # GET 方法和 query payload 不变，签名校验交给商户端的 EPay MD5
        self.assertEqual(captured["method"], "GET")
        self.assertEqual(captured["params"], {"pid": "1001", "trade_status": "TRADE_SUCCESS"})

    @patch("webhooks.tasks._egress_proxy_url", None)
    @patch("webhooks.tasks._execute_http_delivery")
    def test_get_query_direct_when_proxy_not_configured(self, mock_http):
        """未配置出口代理时按原状直连商户 URL。"""
        mock_http.return_value = (True, 200, {}, "success", "", 30)
        project = _make_project()
        event = WebhookEvent.objects.create(
            project=project,
            payload={"pid": "1001"},
            delivery_url="https://merchant.example.com/notify",
            delivery_method=WebhookEvent.DeliveryMethod.GET_QUERY,
            expected_response_body="success",
        )

        deliver_event(event.pk)

        call_kwargs = mock_http.call_args.kwargs
        self.assertEqual(call_kwargs["request_url"], "https://merchant.example.com/notify")
        self.assertNotIn("CF-Worker-Destination", call_kwargs["headers"])
        self.assertNotIn("CF-Worker-Key", call_kwargs["headers"])

    @patch("webhooks.tasks._egress_proxy_key", "proxy-key-secret")
    @patch("webhooks.tasks._egress_proxy_url", "https://proxy.example.com/forward")
    @patch("webhooks.tasks._execute_http_delivery")
    def test_get_query_attempt_log_strips_proxy_credentials(self, mock_http):
        """代理鉴权 header 不能写入 DeliveryAttempt 日志，避免密钥泄漏。"""
        mock_http.return_value = (True, 200, {}, "success", "", 30)
        project = _make_project()
        event = WebhookEvent.objects.create(
            project=project,
            payload={"pid": "1001"},
            delivery_url="https://merchant.example.com/notify",
            delivery_method=WebhookEvent.DeliveryMethod.GET_QUERY,
            expected_response_body="success",
        )

        deliver_event(event.pk)

        attempt = DeliveryAttempt.objects.get(event=event)
        self.assertIsNone(attempt.request_headers.get("CF-Worker-Key"))
        self.assertIsNone(attempt.request_headers.get("CF-Worker-Destination"))

    @patch("webhooks.tasks._execute_http_delivery")
    def test_native_json_delivery_keeps_existing_ok_contract(self, mock_http):
        mock_http.return_value = (True, 200, {}, "ok", "", 30)
        project = _make_project()
        event = WebhookEvent.objects.create(
            project=project,
            payload={"type": "invoice", "data": {"sys_no": "INV-1"}},
        )

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.SUCCEEDED)
        call_kwargs = mock_http.call_args.kwargs
        self.assertEqual(call_kwargs["method"], "POST")
        self.assertEqual(call_kwargs["expected_response_body"], "ok")

    @patch("webhooks.tasks._execute_http_delivery")
    def test_get_query_works_without_project_webhook_url(self, mock_http):
        """EPay 事件使用 event.delivery_url，项目不需要配置原生 webhook。"""
        mock_http.return_value = (True, 200, {}, "success", "", 30)
        project = _make_project(webhook="")
        event = WebhookEvent.objects.create(
            project=project,
            payload={"pid": "1001"},
            delivery_url="https://merchant.example.com/notify",
            delivery_method=WebhookEvent.DeliveryMethod.GET_QUERY,
            expected_response_body="success",
        )

        deliver_event(event.pk)

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.SUCCEEDED)


class WebhookResponseMatchingTests(TestCase):
    """验证 _execute_http_delivery 的响应文本匹配宽容度。

    商户的 PHP/Java 框架 echo "success" 时经常带 \\n / \\r\\n / BOM 或前后空白，
    严格相等会把这些合法响应误判为失败；strip 后精确匹配兼顾兼容性与严格度。
    """

    def _run(self, resp_text: str, expected: str = "success") -> bool:
        from webhooks.tasks import _execute_http_delivery

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.text = resp_text

        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = mock_response
        mock_client.post.return_value = mock_response

        with patch("webhooks.tasks.httpx.Client", return_value=mock_client):
            ok, *_ = _execute_http_delivery(
                request_url="https://example.com",
                method="POST",
                headers={},
                body_str="{}",
                expected_response_body=expected,
            )
        return ok

    def test_exact_match_is_ok(self):
        self.assertTrue(self._run("success"))

    def test_trailing_newline_is_ok(self):
        self.assertTrue(self._run("success\n"))

    def test_crlf_is_ok(self):
        self.assertTrue(self._run("success\r\n"))

    def test_surrounding_whitespace_is_ok(self):
        self.assertTrue(self._run("  success  "))

    def test_case_mismatch_is_failure(self):
        self.assertFalse(self._run("Success"))

    def test_extra_content_is_failure(self):
        self.assertFalse(self._run("success ok"))

    def test_empty_is_failure(self):
        self.assertFalse(self._run(""))


class SignalTests(TestCase):
    """测试 projects signal 中重置 webhook 事件的行为。"""

    def test_reopen_webhook_resets_failed_events_with_schedule_cleared(self):
        """通过 Project.save() 重新打开 webhook 时，FAILED 事件应重置为 PENDING 且清除 schedule_locked_until。"""
        project = _make_project(webhook_open=False, failed_count=5)
        event = WebhookEvent.objects.create(
            project=project,
            payload={"type": "test"},
            status=WebhookEvent.Status.FAILED,
            schedule_locked_until=timezone.now() + timezone.timedelta(hours=1),
        )

        project.webhook_open = True
        project.save()

        event.refresh_from_db()
        self.assertEqual(event.status, WebhookEvent.Status.PENDING)
        self.assertIsNone(event.schedule_locked_until)

        project.refresh_from_db()
        self.assertEqual(project.failed_count, 0)
