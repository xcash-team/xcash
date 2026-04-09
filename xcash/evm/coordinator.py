from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import structlog
from django.db import transaction as db_transaction
from django.utils import timezone
from web3 import Web3
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
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0

logger = structlog.get_logger()


def _to_hex(value: object) -> str:
    """将 bytes / HexBytes / str 统一转为无 0x 前缀的十六进制字符串。"""
    if hasattr(value, "hex"):
        hex_value = value.hex()
    elif isinstance(value, str):
        hex_value = value.removeprefix("0x")
    else:
        hex_value = str(value)
    return hex_value.removeprefix("0x")


def _parse_erc20_transfer_log(*, receipt: dict, tx_hash: str) -> dict | None:
    """从 receipt 日志中解析 ERC-20 Transfer 事件。

    返回 {"from_address", "to_address", "value", "event_id"} 或 None。
    仅匹配第一条 Transfer 日志（一笔内部广播交易只对应一次 token transfer）。
    """
    for log in receipt.get("logs") or []:
        topics = list(log.get("topics") or [])
        if len(topics) < 3:
            continue

        topic0_hex = _to_hex(topics[0]).lower()
        # ERC20_TRANSFER_TOPIC0 带 0x 前缀，比较时统一去掉。
        if topic0_hex != ERC20_TRANSFER_TOPIC0.removeprefix("0x").lower():
            continue

        from_address = Web3.to_checksum_address(f"0x{_to_hex(topics[1])[-40:]}")
        to_address = Web3.to_checksum_address(f"0x{_to_hex(topics[2])[-40:]}")

        raw_data = _to_hex(log.get("data", "0x0"))
        if not raw_data:
            continue
        value = Decimal(int(raw_data, 16))

        log_index = log.get("logIndex", 0)
        if isinstance(log_index, str):
            log_index = int(log_index, 16) if log_index.startswith("0x") else int(log_index)

        return {
            "from_address": from_address,
            "to_address": to_address,
            "value": value,
            "event_id": f"erc20:{log_index}",
        }

    return None


class InternalEvmTaskCoordinator:
    """协调内部 EVM 任务的链上终局状态。

    对 PENDING_CHAIN 超过阈值仍未终局的任务，遍历所有历史 tx_hash 查询 receipt：
    - 查到 receipt (status=1) -> 直接推进成功终局
    - 查到 receipt (status=0) -> 标记失败终局
    - 所有 hash 均无 receipt -> 交易已被 mempool 丢弃，重新广播
    """

    @classmethod
    def reconcile_chain(cls, *, chain: Chain) -> None:
        queryset = (
            EvmBroadcastTask.objects.select_related("base_task", "address")
            .filter(
                chain=chain,
                base_task__stage=BroadcastTaskStage.PENDING_CHAIN,
                base_task__result=BroadcastTaskResult.UNKNOWN,
                last_attempt_at__lt=ago(seconds=EVM_PENDING_REBROADCAST_TIMEOUT),
            )
            .order_by("address_id", "nonce", "created_at")
        )

        for evm_task in queryset:
            if not evm_task.base_task_id:
                continue

            status, tx_hash, receipt = cls._find_receipt_across_hashes(evm_task=evm_task)
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
                assert receipt is not None
                try:
                    cls._observe_confirmed_transaction(
                        evm_task=evm_task, tx_hash=tx_hash, receipt=receipt,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "协调器观察确认交易失败",
                        chain=chain.code,
                        address=evm_task.address.address,
                        nonce=evm_task.nonce,
                        tx_hash=tx_hash,
                    )
                    continue
            elif status == TxCheckStatus.FAILED:
                cls._finalize_failed_task(evm_task=evm_task)
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

    @staticmethod
    def _find_receipt_across_hashes(
        *, evm_task: EvmBroadcastTask
    ) -> tuple[TxCheckStatus | Exception, str | None, dict | None]:
        """遍历任务的所有历史 tx_hash 查找链上 receipt。

        返回 (status, tx_hash, receipt):
        - 找到 receipt -> (CONFIRMED 或 FAILED, 命中的 hash, receipt)
        - 全部未找到 -> (CONFIRMING, None, None)
        - RPC 异常 -> (Exception, None, None)
        """
        hashes = set(
            TxHash.objects.filter(
                broadcast_task=evm_task.base_task
            ).values_list("hash", flat=True)
        )

        for tx_hash in hashes:
            try:
                receipt = evm_task.chain.w3.eth.get_transaction_receipt(tx_hash)
            except TransactionNotFound:
                continue
            except Exception as exc:  # noqa: BLE001
                return exc, None, None

            if receipt is None:
                continue

            status = receipt.get("status")
            if status == 1:
                return TxCheckStatus.CONFIRMED, tx_hash, dict(receipt)
            if status == 0:
                return TxCheckStatus.FAILED, tx_hash, None
            return RuntimeError("EVM receipt status missing or invalid"), None, None

        return TxCheckStatus.CONFIRMING, None, None

    @staticmethod
    def _observe_confirmed_transaction(
        *, evm_task: EvmBroadcastTask, tx_hash: str, receipt: dict,
    ) -> None:
        """链上已确认但 scanner 未观测到时，构建 ObservedTransferPayload 喂回扫描器管线。

        不再直接推进终局，而是走统一的 TransferService.create_observed_transfer 入口，
        让后续业务逻辑（匹配 Invoice/Deposit/Withdrawal、确认等）由扫描器管线统一处理。
        """
        from chains.service import ObservedTransferPayload
        from chains.service import TransferService

        chain = evm_task.chain
        base_task = evm_task.base_task
        w3 = chain.w3

        # 获取 block（for timestamp）和 transaction（for native tx details）
        block = w3.eth.get_block(receipt["blockNumber"])
        tx = w3.eth.get_transaction(tx_hash)

        timestamp = int(block["timestamp"])
        occurred_at = datetime.fromtimestamp(
            timestamp, tz=timezone.get_current_timezone(),
        )
        block_number = int(receipt["blockNumber"])

        is_native = base_task.crypto == chain.native_coin

        if is_native:
            # 原生币转账：from/to/value 从 transaction 对象取
            from_address = Web3.to_checksum_address(str(tx["from"]))
            to_address = Web3.to_checksum_address(str(tx["to"]))
            value = Decimal(int(tx["value"]))
            decimals = chain.native_coin.get_decimals(chain)
            amount = value.scaleb(-decimals)
            event_id = "native:tx"
        else:
            # ERC-20 转账：从 receipt.logs 解析 Transfer 事件
            parsed = _parse_erc20_transfer_log(receipt=receipt, tx_hash=tx_hash)
            if parsed is None:
                logger.warning(
                    "协调器未在 receipt 中找到 ERC-20 Transfer 日志",
                    chain=chain.code,
                    tx_hash=tx_hash,
                )
                return
            from_address = parsed["from_address"]
            to_address = parsed["to_address"]
            value = parsed["value"]
            decimals = base_task.crypto.get_decimals(chain)
            amount = value.scaleb(-decimals)
            event_id = parsed["event_id"]

        observed = ObservedTransferPayload(
            chain=chain,
            block=block_number,
            tx_hash=tx_hash,
            event_id=event_id,
            from_address=from_address,
            to_address=to_address,
            crypto=base_task.crypto,
            value=value,
            amount=amount,
            timestamp=timestamp,
            occurred_at=occurred_at,
            source="evm-coordinator",
        )
        TransferService.create_observed_transfer(observed=observed)

        logger.info(
            "协调器构建观察载荷并喂回扫描器管线",
            chain=chain.code,
            address=evm_task.address.address,
            nonce=evm_task.nonce,
            tx_hash=tx_hash,
            event_id=event_id,
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
            base_task.stage != BroadcastTaskStage.PENDING_CHAIN
            or base_task.result != BroadcastTaskResult.UNKNOWN
        ):
            return False

        updated = BroadcastTask.mark_finalized_failed(
            task_id=base_task.pk,
            reason=BroadcastTaskFailureReason.EXECUTION_REVERTED,
        )
        if not updated:
            return False

        if base_task.transfer_type == TransferType.Withdrawal:
            WithdrawalService.fail_withdrawal(broadcast_task=base_task)
        return True
