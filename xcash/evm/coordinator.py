from __future__ import annotations

import structlog
from django.db import transaction as db_transaction
from django.utils import timezone
from web3.exceptions import TransactionNotFound

from chains.adapters import TxCheckStatus
from chains.models import BroadcastTask
from chains.models import BroadcastTaskFailureReason
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import TransferType
from chains.models import TxHash
from common.time import ago
from evm.constants import EVM_PENDING_REBROADCAST_TIMEOUT
from evm.models import EvmBroadcastTask

logger = structlog.get_logger()


class InternalEvmTaskCoordinator:
    """协调内部 EVM 任务的链上终局状态。

    对 PENDING_CHAIN 超过阈值仍未终局的任务，遍历所有历史 tx_hash 查询 receipt：
    - 查到 receipt (status=1) -> 直接推进成功终局
    - 查到 receipt (status=0) -> 标记失败终局
    - 所有 hash 均无 receipt -> 交易已被 mempool 丢弃，重新广播
    """

    @classmethod
    def reconcile_chain(cls, *, chain: Chain) -> int:
        failed_count = 0
        queryset = (
            EvmBroadcastTask.objects.select_related("base_task", "address")
            .filter(
                chain=chain,
                completed=False,
                base_task__stage=BroadcastTaskStage.PENDING_CHAIN,
                base_task__result=BroadcastTaskResult.UNKNOWN,
                last_attempt_at__lt=ago(seconds=EVM_PENDING_REBROADCAST_TIMEOUT),
            )
            .order_by("address_id", "nonce", "created_at")
        )

        for evm_task in queryset:
            if not evm_task.base_task_id:
                continue

            status, tx_hash = cls._find_receipt_across_hashes(evm_task=evm_task)
            if isinstance(status, Exception):
                logger.warning(
                    "EVM 任务超时收口查链失败",
                    chain=chain.code,
                    address=evm_task.address.address,
                    nonce=evm_task.nonce,
                    error=str(status),
                )
                continue

            if status == TxCheckStatus.CONFIRMED:
                assert tx_hash is not None  # CONFIRMED 分支一定携带命中的 hash
                cls._finalize_confirmed_task(evm_task=evm_task, tx_hash=tx_hash)
            elif status == TxCheckStatus.FAILED:
                if cls._finalize_failed_task(evm_task=evm_task):
                    failed_count += 1
            else:
                # 所有历史 hash 都找不到 receipt，交易已被 mempool 丢弃，重新广播。
                try:
                    evm_task.broadcast()
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "PENDING_CHAIN 超时重新广播失败",
                        chain=chain.code,
                        address=evm_task.address.address,
                        nonce=evm_task.nonce,
                    )
                else:
                    logger.info(
                        "PENDING_CHAIN 超时且无链上记录，已重新广播",
                        chain=chain.code,
                        address=evm_task.address.address,
                        nonce=evm_task.nonce,
                    )

        return failed_count

    @staticmethod
    def _find_receipt_across_hashes(
        *, evm_task: EvmBroadcastTask
    ) -> tuple[TxCheckStatus | Exception, str | None]:
        """遍历任务的所有历史 tx_hash 查找链上 receipt。

        返回 (status, tx_hash):
        - 找到 receipt -> (CONFIRMED 或 FAILED, 命中的 hash)
        - 全部未找到 -> (CONFIRMING, None)
        - RPC 异常 -> (Exception, None)
        """
        hashes = set(
            TxHash.objects.filter(
                broadcast_task=evm_task.base_task
            ).values_list("hash", flat=True)
        )
        current_hash = evm_task.base_task.tx_hash
        if current_hash:
            hashes.add(current_hash)

        for tx_hash in hashes:
            try:
                receipt = evm_task.chain.w3.eth.get_transaction_receipt(tx_hash)
            except TransactionNotFound:
                continue
            except Exception as exc:  # noqa: BLE001
                return exc, None

            if receipt is None:
                continue

            status = receipt.get("status")
            if status == 1:
                return TxCheckStatus.CONFIRMED, tx_hash
            if status == 0:
                return TxCheckStatus.FAILED, tx_hash
            return RuntimeError("EVM receipt status missing or invalid"), None

        return TxCheckStatus.CONFIRMING, None

    @staticmethod
    @db_transaction.atomic
    def _finalize_confirmed_task(
        *, evm_task: EvmBroadcastTask, tx_hash: str
    ) -> None:
        """链上已确认成功但 scanner 未观测到时，由协调器直接推进终局。

        直接按 base_task pk 更新，不通过 hash 反查，避免与 scanner 路径竞争
        或因 hash 映射异常导致误操作其他任务。update() 的 result=UNKNOWN 过滤
        保证与 scanner 的 mark_finalized_success 天然互斥。
        """
        from withdrawals.service import WithdrawalService

        locked_task = EvmBroadcastTask.objects.select_for_update().get(pk=evm_task.pk)
        if not locked_task.base_task_id:
            return

        base_task = locked_task.base_task
        if (
            locked_task.completed
            or base_task.stage == BroadcastTaskStage.FINALIZED
            or base_task.result != BroadcastTaskResult.UNKNOWN
        ):
            return

        updated = BroadcastTask.objects.filter(
            pk=base_task.pk,
            result=BroadcastTaskResult.UNKNOWN,
        ).exclude(
            stage=BroadcastTaskStage.FINALIZED,
        ).update(
            tx_hash=tx_hash,
            stage=BroadcastTaskStage.FINALIZED,
            result=BroadcastTaskResult.SUCCESS,
            failure_reason="",
            updated_at=timezone.now(),
        )
        if not updated:
            return

        EvmBroadcastTask.objects.filter(pk=locked_task.pk, completed=False).update(
            completed=True
        )
        if base_task.transfer_type == TransferType.Withdrawal:
            WithdrawalService.confirm_withdrawal_by_task(broadcast_task=base_task)

        logger.info(
            "协调器直接推进链上成功终局",
            chain=evm_task.chain.code,
            address=evm_task.address.address,
            nonce=evm_task.nonce,
            tx_hash=tx_hash,
        )

    @staticmethod
    @db_transaction.atomic
    def _finalize_failed_task(*, evm_task: EvmBroadcastTask) -> bool:
        from withdrawals.service import WithdrawalService

        locked_task = EvmBroadcastTask.objects.select_for_update().get(pk=evm_task.pk)
        if not locked_task.base_task_id:
            return False

        base_task = locked_task.base_task
        if (
            locked_task.completed
            or base_task.stage != BroadcastTaskStage.PENDING_CHAIN
            or base_task.result != BroadcastTaskResult.UNKNOWN
        ):
            return False

        updated = BroadcastTask.mark_finalized_failed(
            task_id=base_task.pk,
            reason=BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )
        if not updated:
            return False

        EvmBroadcastTask.objects.filter(pk=locked_task.pk, completed=False).update(
            completed=True
        )
        if base_task.transfer_type == TransferType.Withdrawal:
            WithdrawalService.fail_withdrawal(broadcast_task=base_task)
        return True
