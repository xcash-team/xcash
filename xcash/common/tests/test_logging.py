import logging

from django.test import SimpleTestCase

from common.logger import ExcludeStaticFilesRequestFilter


class LoggingTest(SimpleTestCase):
    def test_static_request_filter_blocks_static_paths(self):
        log_filter = ExcludeStaticFilesRequestFilter()

        static_record = logging.LogRecord(
            name="django.server",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='"%s" %s %s',
            args=("GET /static/admin.css HTTP/1.1", "200", "123"),
            exc_info=None,
        )
        admin_record = logging.LogRecord(
            name="django.server",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='"%s" %s %s',
            args=("GET /admin/ HTTP/1.1", "200", "123"),
            exc_info=None,
        )
        api_record = logging.LogRecord(
            name="django.server",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='"%s" %s %s',
            args=("GET /api/v1/projects/ HTTP/1.1", "200", "123"),
            exc_info=None,
        )

        self.assertIs(log_filter.filter(static_record), False)
        self.assertIs(log_filter.filter(admin_record), True)
        self.assertIs(log_filter.filter(api_record), True)

    def test_django_server_logger_keeps_default_handler_and_uses_static_request_filter(self):
        from config.settings.base import LOGGING

        self.assertIn("django.server", LOGGING["handlers"])
        self.assertEqual(
            LOGGING["handlers"]["django.server"]["formatter"],
            "django.server",
        )
        self.assertIn(
            "exclude_static_files_requests",
            LOGGING["handlers"]["django.server"]["filters"],
        )
        self.assertEqual(
            LOGGING["loggers"]["django.server"]["handlers"],
            ["django.server"],
        )
        self.assertIs(
            LOGGING["filters"]["exclude_static_files_requests"]["()"],
            ExcludeStaticFilesRequestFilter,
        )
