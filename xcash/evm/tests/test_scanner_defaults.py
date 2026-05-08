from django.test import TestCase

from config.celery import EVM_ERC20_SCAN_SCHEDULE_SECONDS
from config.celery import EVM_NATIVE_SCAN_SCHEDULE_SECONDS
from config.celery import app
from config.celery import evm_tasks
from evm.scanner.constants import DEFAULT_ERC20_SCAN_REPLAY_BLOCKS
from evm.scanner.constants import DEFAULT_NATIVE_SCAN_BATCH_SIZE
from evm.scanner.constants import DEFAULT_NATIVE_SCAN_REPLAY_BLOCKS


class EvmScannerDefaultsTests(TestCase):
    def test_native_scan_uses_expected_default_batch_size(self):
        self.assertEqual(DEFAULT_NATIVE_SCAN_BATCH_SIZE, 32)

    def test_erc20_scan_uses_small_fixed_replay_window(self):
        self.assertEqual(DEFAULT_ERC20_SCAN_REPLAY_BLOCKS, 2)

    def test_native_scan_uses_small_fixed_replay_window(self):
        self.assertEqual(DEFAULT_NATIVE_SCAN_REPLAY_BLOCKS, 2)

    def test_evm_scanner_schedules_are_split_by_scan_type(self):
        self.assertEqual(EVM_ERC20_SCAN_SCHEDULE_SECONDS, 16)
        self.assertEqual(EVM_NATIVE_SCAN_SCHEDULE_SECONDS, 16)
        self.assertEqual(
            evm_tasks["scan_active_evm_erc20_chains"]["task"],
            "evm.tasks.scan_active_evm_erc20_chains",
        )
        self.assertEqual(
            evm_tasks["scan_active_evm_erc20_chains"]["schedule"],
            EVM_ERC20_SCAN_SCHEDULE_SECONDS,
        )
        self.assertEqual(
            evm_tasks["scan_active_evm_native_chains"]["task"],
            "evm.tasks.scan_active_evm_native_chains",
        )
        self.assertEqual(
            evm_tasks["scan_active_evm_native_chains"]["schedule"],
            EVM_NATIVE_SCAN_SCHEDULE_SECONDS,
        )
        self.assertIn(
            "scan_active_evm_erc20_chains",
            app.conf.beat_schedule,
        )
        self.assertIn(
            "scan_active_evm_native_chains",
            app.conf.beat_schedule,
        )
        self.assertNotIn("scan_active_evm_chains", app.conf.beat_schedule)
