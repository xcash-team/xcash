import threading
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.contrib.admin.sites import AdminSite
from django.core.cache import cache
from django.db import connections
from django.db import close_old_connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.test import override_settings
from django.utils import timezone
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskFailureReason
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from chains.models import TxHash
from chains.models import Wallet
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from common.consts import ERC20_TRANSFER_GAS
from currencies.models import ChainToken
from currencies.models import Crypto
from evm.admin import EvmScanCursorAdmin
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.erc20 import EvmErc20TransferScanner
from evm.scanner.native import EvmNativeDirectScanner
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcError
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage



class EvmTaskQueueTests(TestCase):
    queue_lock_key = "dispatch_due_evm_broadcast_tasks-locked"

    def setUp(self):
        self._clear_singleton_locks()
        self.wallet = Wallet.objects.create()
        self.native = Crypto.objects.create(
            name="Ethereum Queue",
            symbol="ETHQ",
            coingecko_id="ethereum-queue",
        )
        self.chain = Chain.objects.create(
            code="ethq",
            name="Ethereum Queue",
            type=ChainType.EVM,
            chain_id=1,
            rpc="http://ethq.local",
            native_coin=self.native,
            active=True,
        )
        self.addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f1"
            ),
        )

    def _clear_singleton_locks(self):
        cache.delete(self.queue_lock_key)

    def _create_evm_task(
        self,
        *,
        tx_hash: str,
        stage: str,
        result: str,
        nonce: int | None = None,
        address: Address | None = None,
    ) -> EvmBroadcastTask:
        # 任务级测试直接手工落库，聚焦"队列如何挑任务"和"终局任务是否被错误重播"。
        task_address = address or self.addr
        next_nonce = self._next_test_nonce(task_address)
        if nonce is not None and nonce > next_nonce:
            # 触发器要求 nonce 连续，自动填充中间的空洞
            self._fill_nonce_gap(task_address, next_nonce, nonce)
        target_nonce = next_nonce if nonce is None else nonce
        base_task = BroadcastTask.objects.create(
            chain=self.chain,
            address=task_address,
            transfer_type=TransferType.Withdrawal,
            crypto=self.native,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f2"
            ),
            amount=Decimal("1"),
            tx_hash=tx_hash,
            stage=stage,
            result=result,
        )
        return EvmBroadcastTask.objects.create(
            base_task=base_task,
            address=task_address,
            chain=self.chain,
            nonce=target_nonce,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000f2"),
            value=0,
            gas=21_000,
            gas_price=1,
        )

    def _next_test_nonce(self, address: Address) -> int:
        from django.db.models import Max

        max_nonce = EvmBroadcastTask.objects.filter(
            address=address, chain=self.chain
        ).aggregate(m=Max("nonce"))["m"]
        return 0 if max_nonce is None else max_nonce + 1

    def _fill_nonce_gap(self, address: Address, start: int, end: int) -> None:
        """填充 [start, end) 区间的 nonce，满足触发器连续性约束。"""
        for n in range(start, end):
            filler_base = BroadcastTask.objects.create(
                chain=self.chain,
                address=address,
                transfer_type=TransferType.Withdrawal,
                stage=BroadcastTaskStage.FINALIZED,
                result=BroadcastTaskResult.SUCCESS,
            )
            EvmBroadcastTask.objects.create(
                base_task=filler_base,
                address=address,
                chain=self.chain,
                nonce=n,
                to=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000f2"
                ),
                value=0,
                gas=21_000,
                gas_price=1,
            )

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_skips_finalized_broadcast_task(self, broadcast_mock):
        # 已终局的链上任务不应再次广播，否则会把成功/失败终态重新拉回执行面。
        from evm.tasks import broadcast_evm_task

        broadcast_task = self._create_evm_task(
            tx_hash="0x" + "a" * 64,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.SUCCESS,
        )

        broadcast_evm_task.run(broadcast_task.pk)

        broadcast_mock.assert_not_called()

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_skips_pending_chain_to_avoid_immediate_rebroadcast(
        self, broadcast_mock
    ):
        # 普通 Celery 广播入口只负责 QUEUED 首次发送；PENDING_CHAIN 重播必须走
        # coordinator 的超时收口路径，避免重复消息绕过重播间隔。
        from evm.tasks import broadcast_evm_task

        broadcast_task = self._create_evm_task(
            tx_hash="0x" + "aa" * 32,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
        )

        broadcast_evm_task.run(broadcast_task.pk)

        broadcast_mock.assert_not_called()

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_dispatches_only_queued_unknown_tasks(
        self, delay_mock
    ):
        # dispatch 只放行 QUEUED 任务；PENDING_CHAIN / recent / finalized 不应被选中。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=2,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f4"
            ),
        )

        due_queued = self._create_evm_task(
            tx_hash="0x" + "b" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        # PENDING_CHAIN 任务不应被 dispatch 重新选中（已在 mempool 中等待确认）。
        self._create_evm_task(
            tx_hash="0x" + "c" * 64,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
            address=other_addr,
        )
        recent_task = self._create_evm_task(
            tx_hash="0x" + "d" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        finalized_task = self._create_evm_task(
            tx_hash="0x" + "e" * 64,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.SUCCESS,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        fresh_created_at = timezone.now()
        EvmBroadcastTask.objects.filter(pk=due_queued.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(pk=recent_task.pk).update(
            created_at=fresh_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(pk=finalized_task.pk).update(
            created_at=stale_created_at,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            {call.args[0] for call in delay_mock.call_args_list},
            {due_queued.pk},
        )

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_skips_when_lower_queued_nonce_exists(
        self,
        broadcast_mock,
    ):
        # 同账户更高 nonce 在更低 QUEUED nonce 存在时不应越过广播，保证 nonce 按顺序进入 mempool。
        from evm.tasks import broadcast_evm_task

        self._create_evm_task(
            tx_hash="0x" + "1" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "2" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        broadcast_evm_task.run(higher_task.pk)

        broadcast_mock.assert_not_called()

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_task_allows_higher_nonce_after_lower_task_enters_pending_confirm(
        self,
        broadcast_mock,
    ):
        # 一旦更低 nonce 已被链上观察到并进入 PENDING_CONFIRM，说明该 nonce 已消费，不应继续阻断后续 nonce。
        from evm.tasks import broadcast_evm_task

        self._create_evm_task(
            tx_hash="0x" + "11" * 32,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "12" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        broadcast_evm_task.run(higher_task.pk)

        broadcast_mock.assert_called_once()

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_dispatches_only_lowest_queued_nonce_per_account(
        self, delay_mock
    ):
        # 队列层只应放行每个账户当前最小 QUEUED nonce，避免高 nonce 在前序缺口存在时被反复重试。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=1,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f3"
            ),
        )
        lower_task = self._create_evm_task(
            tx_hash="0x" + "3" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=5,
        )
        blocked_higher_task = self._create_evm_task(
            tx_hash="0x" + "4" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=6,
        )
        other_account_task = self._create_evm_task(
            tx_hash="0x" + "5" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
            address=other_addr,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(
            pk__in=[lower_task.pk, blocked_higher_task.pk, other_account_task.pk]
        ).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            {call.args[0] for call in delay_mock.call_args_list},
            {lower_task.pk, other_account_task.pk},
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_treats_pending_confirm_as_nonce_consumed(
        self,
        delay_mock,
    ):
        # SQL 选取最小阻塞 nonce 时，不应把已进入 PENDING_CONFIRM 的前序任务继续当作缺口。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        lower_confirming_task = self._create_evm_task(
            tx_hash="0x" + "13" * 32,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "14" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        stale_attempt_at = timezone.now() - timedelta(minutes=5)
        EvmBroadcastTask.objects.filter(pk=lower_confirming_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=stale_attempt_at,
        )
        EvmBroadcastTask.objects.filter(pk=higher_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            [call.args[0] for call in delay_mock.call_args_list],
            [higher_task.pk],
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_due_evm_broadcast_tasks_avoids_slice_starvation_from_blocked_high_nonces(
        self, delay_mock
    ):
        # SQL 层应直接挑每账户最小未收口 nonce，避免更高 nonce 候选占满 slice 后被 Python 层全部跳过。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=3,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000f5"
            ),
        )
        lower_task = self._create_evm_task(
            tx_hash="0x" + "6" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        blocked_tasks = [
            self._create_evm_task(
                tx_hash=f"0x{i:064x}",
                stage=BroadcastTaskStage.QUEUED,
                result=BroadcastTaskResult.UNKNOWN,
                nonce=i,
            )
            for i in range(2, 10)
        ]
        other_account_task = self._create_evm_task(
            tx_hash="0x" + "7" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
            address=other_addr,
        )

        older_created_at = timezone.now() - timedelta(seconds=12)
        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(pk=lower_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(
            pk__in=[task.pk for task in blocked_tasks]
        ).update(
            created_at=older_created_at,
            last_attempt_at=None,
        )
        EvmBroadcastTask.objects.filter(pk=other_account_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            {call.args[0] for call in delay_mock.call_args_list},
            {lower_task.pk, other_account_task.pk},
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_clear_singleton_locks_allows_queue_dispatch_after_stale_lock(
        self,
        delay_mock,
    ):
        # singleton 锁残留会让队列任务直接返回；测试夹具必须主动清理，避免用例依赖外部缓存状态。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        cache.set(self.queue_lock_key, "true", 60)
        due_task = self._create_evm_task(
            tx_hash="0x" + "f" * 64,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        EvmBroadcastTask.objects.filter(pk=due_task.pk).update(
            created_at=timezone.now() - timedelta(seconds=8),
            last_attempt_at=None,
        )

        self._clear_singleton_locks()
        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            [call.args[0] for call in delay_mock.call_args_list],
            [due_task.pk],
        )

    # ── Nonce 流水线测试 ──────────────────────────────────────────────

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_allows_when_lower_nonce_is_pending_chain(
        self,
        broadcast_mock,
    ):
        # 低 nonce 已提交到 mempool (PENDING_CHAIN) 时，高 nonce 允许广播。
        from evm.tasks import broadcast_evm_task

        self._create_evm_task(
            tx_hash="0x" + "a1" * 32,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )
        higher_task = self._create_evm_task(
            tx_hash="0x" + "a2" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=2,
        )

        broadcast_evm_task.run(higher_task.pk)

        broadcast_mock.assert_called_once()

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_blocks_when_pipeline_full(
        self,
        broadcast_mock,
    ):
        # 同地址同链 PENDING_CHAIN 达到 EVM_PIPELINE_DEPTH 时阻断新广播。
        from evm.constants import EVM_PIPELINE_DEPTH
        from evm.tasks import broadcast_evm_task

        for i in range(EVM_PIPELINE_DEPTH):
            self._create_evm_task(
                tx_hash=f"0x{i:064x}",
                stage=BroadcastTaskStage.PENDING_CHAIN,
                result=BroadcastTaskResult.UNKNOWN,
                nonce=i,
            )
        next_task = self._create_evm_task(
            tx_hash="0x" + "b1" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=EVM_PIPELINE_DEPTH,
        )

        broadcast_evm_task.run(next_task.pk)

        broadcast_mock.assert_not_called()

    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_broadcast_resumes_after_pipeline_slot_freed(
        self,
        broadcast_mock,
    ):
        # pipeline 有空位后恢复广播。
        from evm.constants import EVM_PIPELINE_DEPTH
        from evm.tasks import broadcast_evm_task

        pending_tasks = []
        for i in range(EVM_PIPELINE_DEPTH):
            pending_tasks.append(
                self._create_evm_task(
                    tx_hash=f"0x{i:064x}",
                    stage=BroadcastTaskStage.PENDING_CHAIN,
                    result=BroadcastTaskResult.UNKNOWN,
                    nonce=i,
                )
            )
        next_task = self._create_evm_task(
            tx_hash="0x" + "c1" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=EVM_PIPELINE_DEPTH,
        )

        # 模拟一笔完成，腾出 pipeline 空位
        first = pending_tasks[0]
        BroadcastTask.objects.filter(pk=first.base_task_id).update(
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.SUCCESS,
        )

        broadcast_evm_task.run(next_task.pk)

        broadcast_mock.assert_called_once()

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_allows_queued_when_pipeline_has_room(self, delay_mock):
        # 同地址已有 PENDING_CHAIN 但未满时，dispatch 仍放行最低 QUEUED nonce。
        from django.utils import timezone

        from evm.tasks import dispatch_due_evm_broadcast_tasks

        self._create_evm_task(
            tx_hash="0x" + "d1" * 32,
            stage=BroadcastTaskStage.PENDING_CHAIN,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=0,
        )
        queued_task = self._create_evm_task(
            tx_hash="0x" + "d2" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(pk=queued_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        self.assertEqual(
            [call.args[0] for call in delay_mock.call_args_list],
            [queued_task.pk],
        )

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_blocks_when_pipeline_full(self, delay_mock):
        # pipeline 已满时 dispatch 不选该地址的 QUEUED 任务。
        from django.utils import timezone

        from evm.constants import EVM_PIPELINE_DEPTH
        from evm.tasks import dispatch_due_evm_broadcast_tasks

        for i in range(EVM_PIPELINE_DEPTH):
            self._create_evm_task(
                tx_hash=f"0x{0xE0 + i:064x}",
                stage=BroadcastTaskStage.PENDING_CHAIN,
                result=BroadcastTaskResult.UNKNOWN,
                nonce=i,
            )
        blocked_task = self._create_evm_task(
            tx_hash="0x" + "e1" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=EVM_PIPELINE_DEPTH,
        )

        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(pk=blocked_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        delay_mock.assert_not_called()

    @patch("evm.tasks.broadcast_evm_task.delay")
    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_chain_dispatch_triggers_next_queued_after_broadcast(
        self,
        broadcast_mock,
        delay_mock,
    ):
        # 广播成功后应链式调度同地址下一个 QUEUED nonce，无需等待下一轮 dispatch 周期。
        from evm.tasks import broadcast_evm_task

        current_task = self._create_evm_task(
            tx_hash="0x" + "f1" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=0,
        )
        next_task = self._create_evm_task(
            tx_hash="0x" + "f2" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )

        def mark_pending(*args, **kwargs):
            BroadcastTask.objects.filter(pk=current_task.base_task_id).update(
                stage=BroadcastTaskStage.PENDING_CHAIN,
            )

        broadcast_mock.side_effect = mark_pending

        broadcast_evm_task.run(current_task.pk)

        broadcast_mock.assert_called_once()
        delay_mock.assert_called_once_with(next_task.pk)

    @patch("evm.tasks.broadcast_evm_task.delay")
    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_chain_dispatch_skips_when_current_task_remains_queued(
        self,
        broadcast_mock,
        delay_mock,
    ):
        # pre-flight 阻断会让当前任务保持 QUEUED 并依赖 last_attempt_at 节流；
        # 链式调度不能立刻把同一个最低 nonce 再投递一次。
        from evm.tasks import broadcast_evm_task

        current_task = self._create_evm_task(
            tx_hash="0x" + "f5" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=0,
        )
        self._create_evm_task(
            tx_hash="0x" + "f6" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=1,
        )

        broadcast_evm_task.run(current_task.pk)

        broadcast_mock.assert_called_once()
        delay_mock.assert_not_called()

    @patch("evm.tasks.broadcast_evm_task.delay")
    @patch("evm.tasks.EvmBroadcastTask.broadcast")
    def test_chain_dispatch_stops_when_pipeline_full(
        self,
        broadcast_mock,
        delay_mock,
    ):
        # pipeline 满时链式调度不应继续派发。
        from evm.constants import EVM_PIPELINE_DEPTH
        from evm.tasks import broadcast_evm_task

        # 创建 EVM_PIPELINE_DEPTH - 1 个已在 mempool 的任务
        for i in range(EVM_PIPELINE_DEPTH - 1):
            self._create_evm_task(
                tx_hash=f"0x{0xF0 + i:064x}",
                stage=BroadcastTaskStage.PENDING_CHAIN,
                result=BroadcastTaskResult.UNKNOWN,
                nonce=i,
            )
        # 当前任务广播后 pipeline 刚好满
        current_task = self._create_evm_task(
            tx_hash="0x" + "f3" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=EVM_PIPELINE_DEPTH - 1,
        )
        # 还有一个排队中的任务
        self._create_evm_task(
            tx_hash="0x" + "f4" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            nonce=EVM_PIPELINE_DEPTH,
        )

        def mark_pending(*args, **kwargs):
            BroadcastTask.objects.filter(pk=current_task.base_task_id).update(
                stage=BroadcastTaskStage.PENDING_CHAIN,
            )

        broadcast_mock.side_effect = mark_pending

        broadcast_evm_task.run(current_task.pk)

        broadcast_mock.assert_called_once()
        # pipeline 满，不应链式调度下一个
        delay_mock.assert_not_called()

    def _setup_deposit_with_pending_recharge(
        self,
        *,
        recharge_stage: str,
        recharge_result: str = BroadcastTaskResult.UNKNOWN,
        recharge_failure_reason: str = "",
        recharged_at=None,
    ):
        """为"地址正在等 gas"场景构造 fixture：返回 (collection_task, recharge_entry).

        复用此 helper 的 3 个测试只在 `recharge_stage / recharged_at` 处有差异，
        其余编排保持一致，便于对比三种触发条件下的 dispatch 决策。
        """
        from deposits.models import DepositAddress
        from deposits.models import GasRecharge
        from projects.models import Project
        from users.models import Customer

        project = Project.objects.create(
            name="dispatch-pending-recharge-project",
            wallet=self.wallet,
            webhook="https://example.com/webhook",
        )
        customer = Customer.objects.create(
            project=project, uid="dispatch-pending-recharge"
        )
        deposit_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=1,
            address_index=10,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000003e1"
            ),
        )
        deposit_address_record = DepositAddress.objects.create(
            customer=customer,
            chain_type=ChainType.EVM,
            address=deposit_addr,
        )
        vault_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=11,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000003e2"
            ),
        )

        # 待 dispatch 的 collection 任务，address = deposit 地址
        collection_task = self._create_evm_task(
            tx_hash="0x" + "aa" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            address=deposit_addr,
        )
        stale_created_at = timezone.now() - timedelta(seconds=8)
        EvmBroadcastTask.objects.filter(pk=collection_task.pk).update(
            created_at=stale_created_at,
            last_attempt_at=None,
        )

        # 关联的 gas-recharge 任务，address = Vault。
        # 先用 QUEUED/UNKNOWN 通过 BroadcastTask.full_clean() 校验，再 update() 到目标状态；
        # 这是唯一绕过"FAILED 必须带 failure_reason"约束的写法，同时能精确构造终局组合。
        recharge_task = self._create_evm_task(
            tx_hash="0x" + "bb" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            address=vault_addr,
        )
        BroadcastTask.objects.filter(pk=recharge_task.base_task_id).update(
            stage=recharge_stage,
            result=recharge_result,
            failure_reason=recharge_failure_reason,
        )
        recharge_entry = GasRecharge.objects.create(
            deposit_address=deposit_address_record,
            broadcast_task=recharge_task.base_task,
            recharged_at=recharged_at,
        )
        return collection_task, recharge_entry

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_skips_task_whose_address_has_active_pending_gas_recharge(
        self, delay_mock
    ):
        # 同地址已有活跃中（未到账、broadcast_task 未 finalized）的 GasRecharge 时，
        # dispatch 必须暂不投递该地址的 collection，让 picker 名额让给其它地址，
        # 避免"collection 反复 pre-flight 不过占队头 → Vault gas-recharge 永远排不上"的死锁。
        from evm.tasks import dispatch_due_evm_broadcast_tasks

        collection_task, _ = self._setup_deposit_with_pending_recharge(
            recharge_stage=BroadcastTaskStage.QUEUED,
            recharged_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        picked_pks = {call.args[0] for call in delay_mock.call_args_list}
        self.assertNotIn(collection_task.pk, picked_pks)

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_ignores_pending_gas_recharge_on_other_chain(self, delay_mock):
        # DepositAddress 按 chain_type 复用 EVM 地址；其它 EVM 链的补 gas 任务
        # 不能阻塞当前链同地址的归集任务。
        from deposits.models import DepositAddress
        from deposits.models import GasRecharge
        from projects.models import Project
        from users.models import Customer
        from evm.tasks import dispatch_due_evm_broadcast_tasks

        other_native = Crypto.objects.create(
            name="Ethereum Queue Other",
            symbol="ETHQO",
            coingecko_id="ethereum-queue-other",
        )
        other_chain = Chain.objects.create(
            code="ethq-other",
            name="Ethereum Queue Other",
            type=ChainType.EVM,
            chain_id=2,
            rpc="http://ethq-other.local",
            native_coin=other_native,
            active=True,
        )
        project = Project.objects.create(
            name="dispatch-cross-chain-recharge-project",
            wallet=self.wallet,
            webhook="https://example.com/webhook",
        )
        customer = Customer.objects.create(
            project=project, uid="dispatch-cross-chain-recharge"
        )
        deposit_addr = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=1,
            address_index=20,
            address=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000004e1"
            ),
        )
        deposit_address_record = DepositAddress.objects.create(
            customer=customer,
            chain_type=ChainType.EVM,
            address=deposit_addr,
        )
        collection_task = self._create_evm_task(
            tx_hash="0x" + "ab" * 32,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
            address=deposit_addr,
        )
        EvmBroadcastTask.objects.filter(pk=collection_task.pk).update(
            created_at=timezone.now() - timedelta(seconds=8),
            last_attempt_at=None,
        )
        other_chain_recharge = BroadcastTask.objects.create(
            chain=other_chain,
            address=self.addr,
            transfer_type=TransferType.GasRecharge,
            crypto=other_native,
            stage=BroadcastTaskStage.QUEUED,
            result=BroadcastTaskResult.UNKNOWN,
        )
        GasRecharge.objects.create(
            deposit_address=deposit_address_record,
            broadcast_task=other_chain_recharge,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        picked_pks = {call.args[0] for call in delay_mock.call_args_list}
        self.assertIn(collection_task.pk, picked_pks)

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_resumes_task_after_gas_recharge_recharged_at_written(
        self, delay_mock
    ):
        # recharged_at 已写入代表 gas 已实际到账（_dispatch_business_confirm 里设置），
        # 本地 balance 必然充足，dispatch 应当恢复对该地址的调度。
        from evm.tasks import dispatch_due_evm_broadcast_tasks

        collection_task, _ = self._setup_deposit_with_pending_recharge(
            recharge_stage=BroadcastTaskStage.FINALIZED,
            recharge_result=BroadcastTaskResult.SUCCESS,
            recharged_at=timezone.now(),
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        picked_pks = {call.args[0] for call in delay_mock.call_args_list}
        self.assertIn(collection_task.pk, picked_pks)

    @patch("evm.tasks.broadcast_evm_task.delay")
    def test_dispatch_resumes_task_when_gas_recharge_finalized_failed(
        self, delay_mock
    ):
        # gas-recharge 终局失败（broadcast_task.stage=FINALIZED+FAILED）意味着 recharged_at
        # 永远不会被写入；若仍按"recharged_at IS NULL"一刀切，本地址会被永久阻塞。
        # 因此 filter 只在 broadcast_task 处于活跃阶段时才跳过。
        from evm.tasks import dispatch_due_evm_broadcast_tasks

        collection_task, _ = self._setup_deposit_with_pending_recharge(
            recharge_stage=BroadcastTaskStage.FINALIZED,
            recharge_result=BroadcastTaskResult.FAILED,
            recharge_failure_reason=BroadcastTaskFailureReason.INSUFFICIENT_BALANCE,
            recharged_at=None,
        )

        with self.captureOnCommitCallbacks(execute=True):
            dispatch_due_evm_broadcast_tasks.run()

        picked_pks = {call.args[0] for call in delay_mock.call_args_list}
        self.assertIn(collection_task.pk, picked_pks)
