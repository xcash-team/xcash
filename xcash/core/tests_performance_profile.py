import os
import subprocess
import sys
from dataclasses import fields
from pathlib import Path
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase

from config.performance import PerformanceProfile
from config.performance import get_bool_default
from config.performance import get_int
from config.performance import get_int_default
from config.performance import profile_name


class PerformanceProfileTests(SimpleTestCase):
    def test_default_profile_is_low(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(profile_name(), "low")
            self.assertEqual(get_int("GUNICORN_WORKERS", "django_workers"), 1)
            self.assertEqual(get_int("CELERY_WORKER_CONCURRENCY", "celery_worker_concurrency"), 2)

    def test_middle_profile_targets_four_core_server(self):
        with patch.dict(os.environ, {"PERFORMANCE": "middle"}, clear=True):
            self.assertEqual(get_int("GUNICORN_WORKERS", "django_workers"), 3)
            self.assertEqual(get_int("GUNICORN_THREADS", "django_threads"), 3)
            self.assertEqual(get_int("SIGNER_GUNICORN_WORKERS", "signer_workers"), 2)
            self.assertEqual(get_int("CELERY_WORKER_CONCURRENCY", "celery_worker_concurrency"), 6)
            self.assertEqual(get_int("CELERY_EVM_ERC20_SCAN_SCHEDULE_SECONDS", "evm_scan_seconds"), 10)

    def test_high_profile_targets_eight_core_server(self):
        with patch.dict(os.environ, {"PERFORMANCE": "high"}, clear=True):
            self.assertEqual(get_int("GUNICORN_WORKERS", "django_workers"), 5)
            self.assertEqual(get_int("GUNICORN_THREADS", "django_threads"), 4)
            self.assertEqual(get_int("SIGNER_GUNICORN_WORKERS", "signer_workers"), 3)
            self.assertEqual(get_int("CELERY_WORKER_CONCURRENCY", "celery_worker_concurrency"), 12)
            self.assertEqual(get_int("CELERY_TRON_SCAN_SCHEDULE_SECONDS", "tron_scan_seconds"), 6)

    def test_explicit_env_overrides_profile_value(self):
        with patch.dict(
            os.environ,
            {"PERFORMANCE": "middle", "CELERY_WORKER_CONCURRENCY": "9"},
            clear=True,
        ):
            self.assertEqual(get_int("CELERY_WORKER_CONCURRENCY", "celery_worker_concurrency"), 9)

    def test_profile_only_contains_machine_sizing_fields(self):
        self.assertEqual(
            {field.name for field in fields(PerformanceProfile)},
            {
                "django_workers",
                "django_threads",
                "signer_workers",
                "celery_worker_concurrency",
                "evm_scan_seconds",
                "tron_scan_seconds",
                "bitcoin_scan_seconds",
                "bitcoin_watch_sync_seconds",
            },
        )

    def test_low_impact_defaults_are_fixed_outside_profile(self):
        with patch.dict(os.environ, {"PERFORMANCE": "high"}, clear=True):
            self.assertEqual(get_int_default("CELERY_WEBHOOK_EVENTS_SCHEDULE_SECONDS", 15), 15)
            self.assertEqual(get_int_default("CELERY_EVM_BROADCAST_SCHEDULE_SECONDS", 8), 8)
            self.assertEqual(get_int_default("CELERY_CRYPTO_PRICE_REFRESH_SCHEDULE_SECONDS", 120), 120)
            self.assertFalse(get_bool_default("CELERY_RESULT_EXTENDED", default=False))
            self.assertTrue(get_bool_default("CELERY_TASK_IGNORE_RESULT", default=True))

    def test_explicit_env_overrides_fixed_default(self):
        with patch.dict(os.environ, {"PERFORMANCE": "high", "CELERY_WEBHOOK_EVENTS_SCHEDULE_SECONDS": "7"}, clear=True):
            self.assertEqual(get_int_default("CELERY_WEBHOOK_EVENTS_SCHEDULE_SECONDS", 15), 7)

    def test_invalid_profile_fails_fast(self):
        with (
            patch.dict(os.environ, {"PERFORMANCE": "tiny"}, clear=True),
            self.assertRaises(ImproperlyConfigured),
        ):
            profile_name()

    def test_shell_env_outputs_profile_values(self):
        env = {
            **os.environ,
            "PERFORMANCE": "high",
            "DJANGO_SETTINGS_MODULE": "config.settings.test",
        }
        performance_script = Path(__file__).resolve().parents[2] / "config" / "performance.py"
        completed = subprocess.run(
            [sys.executable, str(performance_script), "shell-env", "web"],
            check=True,
            env=env,
            capture_output=True,
            text=True,
        )

        self.assertIn("export GUNICORN_WORKERS=5", completed.stdout)
        self.assertIn("export GUNICORN_THREADS=4", completed.stdout)
