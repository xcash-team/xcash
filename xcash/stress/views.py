# xcash/stress/views.py
import hashlib
import hmac as hmac_mod
import json
import time
from dataclasses import dataclass

import structlog
from django.db import models
from django.db import transaction
from django.http import HttpResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from common.consts import NONCE_HEADER
from common.consts import SIGNATURE_HEADER
from common.consts import TIMESTAMP_HEADER

from .models import DepositStressCase
from .models import DepositStressCaseStatus
from .models import InvoiceStressCase
from .models import InvoiceStressCaseStatus
from .models import WithdrawalStressCase
from .models import WithdrawalStressCaseStatus
from .service import StressService

logger = structlog.get_logger()

_TIMESTAMP_TOLERANCE = 60


@dataclass
class _VerifyResult:
    sig_ok: bool
    payload_ok: bool
    nonce_ok: bool
    ts_ok: bool
    errors: list[str]

    @property
    def all_ok(self) -> bool:
        return self.sig_ok and self.payload_ok and self.nonce_ok and self.ts_ok


@csrf_exempt
@require_POST
def stress_webhook_view(request):
    """接收 xcash webhook 推送，验证并推进 InvoiceStressCase 状态。
    始终返回 "ok" 避免触发 webhook 重试干扰测试。
    """
    try:
        _handle_webhook(request)
    except Exception:
        logger.exception("stress.webhook.unhandled_error")

    return HttpResponse("ok", content_type="text/plain")


def _parse_request(request):
    """解析请求头和 body，返回 (nonce, timestamp_str, signature, body_str, payload) 或 None。"""
    nonce = request.headers.get(NONCE_HEADER, "")
    timestamp_str = request.headers.get(TIMESTAMP_HEADER, "")
    signature = request.headers.get(SIGNATURE_HEADER, "")
    body_str = request.body.decode("utf-8")

    try:
        payload = json.loads(body_str)
    except json.JSONDecodeError:
        logger.warning("stress.webhook.invalid_json")
        return None

    data = payload.get("data", {})
    # stress case 对 deposit 仍以 hash 标识；其他类型以 sys_no 标识
    event_type = payload.get("type", "")
    if event_type == "deposit":
        if not data.get("hash"):
            logger.warning("stress.webhook.missing_hash")
            return None
    elif not data.get("sys_no"):
        logger.warning("stress.webhook.missing_sys_no")
        return None

    return nonce, timestamp_str, signature, body_str, payload


def _verify_webhook(
    *, nonce, timestamp_str, signature, body_str, data, it: InvoiceStressCase
) -> _VerifyResult:
    """执行四项 webhook 验证，返回验证结果。"""
    project = it.stress_run.project
    errors = []

    # 1. 签名验证
    message = f"{nonce}{timestamp_str}{body_str}"
    expected_sig = hmac_mod.new(
        project.hmac_key.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    sig_ok = hmac_mod.compare_digest(signature, expected_sig)
    if not sig_ok:
        errors.append("签名验证失败")

    # 2. Payload 匹配
    payload_ok = (
        data.get("sys_no") == it.invoice_sys_no
        and data.get("out_no") == it.invoice_out_no
    )
    if not payload_ok:
        errors.append(
            f"Payload 不匹配: sys_no={data.get('sys_no')}, out_no={data.get('out_no')}"
        )

    # 3. Nonce 唯一性
    nonce_ok = nonce not in it.webhook_received_nonces
    if not nonce_ok:
        errors.append(f"Nonce 重复: {nonce}")

    # 4. Timestamp 合理性
    try:
        ts = int(timestamp_str)
        now_ts = int(time.time())
        ts_ok = abs(now_ts - ts) <= _TIMESTAMP_TOLERANCE
    except (ValueError, TypeError):
        ts_ok = False
    if not ts_ok:
        errors.append(f"时间戳不合理: {timestamp_str}")

    return _VerifyResult(
        sig_ok=sig_ok,
        payload_ok=payload_ok,
        nonce_ok=nonce_ok,
        ts_ok=ts_ok,
        errors=errors,
    )


def _update_stress_case(case, nonce, result: _VerifyResult):
    """原子更新 InvoiceStressCase 状态，返回是否实际更新。"""
    with transaction.atomic():
        case = InvoiceStressCase.objects.select_for_update().get(pk=case.pk)

        if case.status not in (
            InvoiceStressCaseStatus.PAID,
            InvoiceStressCaseStatus.WEBHOOK_OK,
        ):
            return None

        case.webhook_received = True
        case.webhook_signature_ok = result.sig_ok
        case.webhook_payload_ok = result.payload_ok
        case.webhook_nonce_ok = result.nonce_ok
        case.webhook_timestamp_ok = result.ts_ok

        if nonce and nonce not in case.webhook_received_nonces:
            case.webhook_received_nonces = [*case.webhook_received_nonces, nonce]

        if result.all_ok:
            case.status = InvoiceStressCaseStatus.SUCCEEDED
        else:
            case.status = InvoiceStressCaseStatus.FAILED
            case.error = "; ".join(result.errors)
        case.finished_at = timezone.now()

        case.save(
            update_fields=[
                "webhook_received",
                "webhook_signature_ok",
                "webhook_payload_ok",
                "webhook_nonce_ok",
                "webhook_timestamp_ok",
                "webhook_received_nonces",
                "status",
                "error",
                "finished_at",
            ]
        )

    return case


def _handle_webhook(request):
    parsed = _parse_request(request)
    if parsed is None:
        return

    nonce, timestamp_str, signature, body_str, payload = parsed

    # 按 payload 顶层字段区分业务类型
    event_type = payload.get("type", "")
    if event_type == "withdrawal":
        _handle_withdrawal_webhook(
            nonce=nonce,
            timestamp_str=timestamp_str,
            signature=signature,
            body_str=body_str,
            payload=payload,
        )
        return

    if event_type == "deposit":
        _handle_deposit_webhook(
            nonce=nonce,
            timestamp_str=timestamp_str,
            signature=signature,
            body_str=body_str,
            payload=payload,
        )
        return

    # 默认走 Invoice 逻辑
    _handle_invoice_webhook(
        nonce=nonce,
        timestamp_str=timestamp_str,
        signature=signature,
        body_str=body_str,
        payload=payload,
    )


def _handle_invoice_webhook(*, nonce, timestamp_str, signature, body_str, payload):
    """处理 Invoice 类型的 Webhook 回调。"""
    data = payload.get("data", {})
    sys_no = data.get("sys_no", "")
    is_final = data.get("confirmed", False)
    if not is_final:
        logger.info(
            "stress.webhook.skipped_non_final", sys_no=sys_no, confirmed=data.get("confirmed")
        )
        return

    try:
        case = InvoiceStressCase.objects.select_related("stress_run__project").get(
            invoice_sys_no=sys_no
        )
    except InvoiceStressCase.DoesNotExist:
        logger.warning("stress.webhook.no_matching_case", sys_no=sys_no)
        return

    if case.status not in (
        InvoiceStressCaseStatus.PAID,
        InvoiceStressCaseStatus.WEBHOOK_OK,
    ):
        logger.info("stress.webhook.skipped", sys_no=sys_no, status=case.status)
        return

    result = _verify_webhook(
        nonce=nonce,
        timestamp_str=timestamp_str,
        signature=signature,
        body_str=body_str,
        data=data,
        it=case,
    )

    updated = _update_stress_case(case, nonce, result)
    if updated is None:
        return

    StressService.on_case_finished(updated)

    logger.info(
        "stress.webhook.processed",
        sys_no=sys_no,
        all_ok=result.all_ok,
        errors=result.errors or None,
    )


def _handle_withdrawal_webhook(*, nonce, timestamp_str, signature, body_str, payload):
    """处理 Withdrawal 类型的 Webhook 回调。"""
    data = payload.get("data", {})
    sys_no = data.get("sys_no", "")
    # 只处理终态 webhook
    if not data.get("confirmed"):
        logger.info(
            "stress.withdrawal_webhook.skipped_non_final",
            sys_no=sys_no,
            confirmed=data.get("confirmed"),
        )
        return

    try:
        case = WithdrawalStressCase.objects.select_related("stress_run__project").get(
            withdrawal_sys_no=sys_no
        )
    except WithdrawalStressCase.DoesNotExist:
        logger.warning("stress.withdrawal_webhook.no_matching_case", sys_no=sys_no)
        return

    if case.status != WithdrawalStressCaseStatus.CONFIRMING:
        logger.info(
            "stress.withdrawal_webhook.skipped",
            sys_no=sys_no,
            status=case.status,
        )
        return

    result = _verify_withdrawal_webhook(
        nonce=nonce,
        timestamp_str=timestamp_str,
        signature=signature,
        body_str=body_str,
        data=data,
        case=case,
    )

    updated = _update_withdrawal_case(case, nonce, result)
    if updated is None:
        return

    StressService.on_case_finished(updated)

    logger.info(
        "stress.withdrawal_webhook.processed",
        sys_no=sys_no,
        all_ok=result.all_ok,
        errors=result.errors or None,
    )


def _verify_withdrawal_webhook(
    *, nonce, timestamp_str, signature, body_str, data, case: WithdrawalStressCase
) -> _VerifyResult:
    """执行四项提币 webhook 验证。"""
    project = case.stress_run.project
    errors = []

    # 1. 签名验证
    message = f"{nonce}{timestamp_str}{body_str}"
    expected_sig = hmac_mod.new(
        project.hmac_key.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    sig_ok = hmac_mod.compare_digest(signature, expected_sig)
    if not sig_ok:
        errors.append("签名验证失败")

    # 2. Payload 匹配
    payload_ok = (
        data.get("sys_no") == case.withdrawal_sys_no
        and data.get("out_no") == case.withdrawal_out_no
    )
    if not payload_ok:
        errors.append(
            f"Payload 不匹配: sys_no={data.get('sys_no')}, out_no={data.get('out_no')}"
        )

    # 3. Nonce 唯一性
    nonce_ok = nonce not in case.webhook_received_nonces
    if not nonce_ok:
        errors.append(f"Nonce 重复: {nonce}")

    # 4. Timestamp 合理性
    try:
        ts = int(timestamp_str)
        now_ts = int(time.time())
        ts_ok = abs(now_ts - ts) <= _TIMESTAMP_TOLERANCE
    except (ValueError, TypeError):
        ts_ok = False
    if not ts_ok:
        errors.append(f"时间戳不合理: {timestamp_str}")

    return _VerifyResult(
        sig_ok=sig_ok,
        payload_ok=payload_ok,
        nonce_ok=nonce_ok,
        ts_ok=ts_ok,
        errors=errors,
    )


def _update_withdrawal_case(case, nonce, result: _VerifyResult):
    """原子更新 WithdrawalStressCase 状态。"""
    with transaction.atomic():
        case = WithdrawalStressCase.objects.select_for_update().get(pk=case.pk)

        if case.status != WithdrawalStressCaseStatus.CONFIRMING:
            return None

        case.webhook_received = True
        case.webhook_signature_ok = result.sig_ok
        case.webhook_payload_ok = result.payload_ok
        case.webhook_nonce_ok = result.nonce_ok
        case.webhook_timestamp_ok = result.ts_ok

        if nonce and nonce not in case.webhook_received_nonces:
            case.webhook_received_nonces = [*case.webhook_received_nonces, nonce]

        if result.all_ok:
            case.status = WithdrawalStressCaseStatus.SUCCEEDED
        else:
            case.status = WithdrawalStressCaseStatus.FAILED
            case.error = "; ".join(result.errors)
        case.finished_at = timezone.now()

        case.save(
            update_fields=[
                "webhook_received",
                "webhook_signature_ok",
                "webhook_payload_ok",
                "webhook_nonce_ok",
                "webhook_timestamp_ok",
                "webhook_received_nonces",
                "status",
                "error",
                "finished_at",
            ]
        )

    return case


# ── 充币 Webhook ─────────────────────────────────────────────


def _check_and_trigger_collection(stress_run_id: int) -> None:
    """延迟导入避免循环引用，检查是否触发归集验证。"""
    from .tasks import _maybe_trigger_collection_verification  # noqa: PLC0415

    _maybe_trigger_collection_verification(stress_run_id)


def _handle_deposit_webhook(*, nonce, timestamp_str, signature, body_str, payload):
    """处理 Deposit 类型的 Webhook 回调。"""
    data = payload.get("data", {})
    tx_hash = data.get("hash", "")
    # 只处理 completed 终态 webhook
    if not data.get("confirmed"):
        logger.info(
            "stress.deposit_webhook.skipped_non_final",
            tx_hash=tx_hash,
            confirmed=data.get("confirmed"),
        )
        return

    # webhook 的 hash 带 0x 前缀，case.tx_hash 可能不带，需要两种形式都尝试匹配
    tx_hash_bare = tx_hash.removeprefix("0x")
    try:
        case = DepositStressCase.objects.select_related("stress_run__project").get(
            models.Q(tx_hash=tx_hash) | models.Q(tx_hash=tx_hash_bare),
        )
    except DepositStressCase.DoesNotExist:
        logger.warning("stress.deposit_webhook.no_matching_case", tx_hash=tx_hash)
        return

    if case.status != DepositStressCaseStatus.PAID:
        logger.info(
            "stress.deposit_webhook.skipped",
            tx_hash=tx_hash,
            status=case.status,
        )
        return

    result = _verify_deposit_webhook(
        nonce=nonce,
        timestamp_str=timestamp_str,
        signature=signature,
        body_str=body_str,
        data=data,
        case=case,
    )

    updated = _update_deposit_case(case, nonce, result)
    if updated is None:
        return

    # 归集验证失败的 case 直接终结
    if updated.status == DepositStressCaseStatus.FAILED:
        StressService.on_case_finished(updated)

    # 检查是否所有 deposit cases 都通过了 webhook 阶段，触发归集验证
    _check_and_trigger_collection(case.stress_run_id)

    logger.info(
        "stress.deposit_webhook.processed",
        tx_hash=tx_hash,
        all_ok=result.all_ok,
        errors=result.errors or None,
    )


def _verify_deposit_webhook(
    *, nonce, timestamp_str, signature, body_str, data, case: DepositStressCase
) -> _VerifyResult:
    """执行四项充币 webhook 验证。"""
    project = case.stress_run.project
    errors = []

    # 1. 签名验证
    message = f"{nonce}{timestamp_str}{body_str}"
    expected_sig = hmac_mod.new(
        project.hmac_key.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    sig_ok = hmac_mod.compare_digest(signature, expected_sig)
    if not sig_ok:
        errors.append("签名验证失败")

    # 2. Payload 匹配：hash（忽略 0x 前缀差异）和 uid 必须与 case 一致
    data_hash = (data.get("hash") or "").removeprefix("0x")
    case_hash = case.tx_hash.removeprefix("0x")
    payload_ok = (
        data_hash == case_hash
        and data.get("uid") == case.customer_uid
    )
    if not payload_ok:
        errors.append(
            f"Payload 不匹配: hash={data.get('hash')}, uid={data.get('uid')}"
        )

    # 3. Nonce 唯一性
    nonce_ok = nonce not in case.webhook_received_nonces
    if not nonce_ok:
        errors.append(f"Nonce 重复: {nonce}")

    # 4. Timestamp 合理性
    try:
        ts = int(timestamp_str)
        now_ts = int(time.time())
        ts_ok = abs(now_ts - ts) <= _TIMESTAMP_TOLERANCE
    except (ValueError, TypeError):
        ts_ok = False
    if not ts_ok:
        errors.append(f"时间戳不合理: {timestamp_str}")

    return _VerifyResult(
        sig_ok=sig_ok,
        payload_ok=payload_ok,
        nonce_ok=nonce_ok,
        ts_ok=ts_ok,
        errors=errors,
    )


def _update_deposit_case(case, nonce, result: _VerifyResult):
    """原子更新 DepositStressCase 状态。

    webhook 验证通过 → WEBHOOK_OK（等待归集验证），
    验证失败 → FAILED（直接终结）。
    """
    with transaction.atomic():
        case = DepositStressCase.objects.select_for_update().get(pk=case.pk)

        if case.status != DepositStressCaseStatus.PAID:
            return None

        case.webhook_received = True
        case.webhook_signature_ok = result.sig_ok
        case.webhook_payload_ok = result.payload_ok
        case.webhook_nonce_ok = result.nonce_ok
        case.webhook_timestamp_ok = result.ts_ok

        if nonce and nonce not in case.webhook_received_nonces:
            case.webhook_received_nonces = [*case.webhook_received_nonces, nonce]

        if result.all_ok:
            # 不设 finished_at，等待归集验证阶段完成
            case.status = DepositStressCaseStatus.WEBHOOK_OK
        else:
            case.status = DepositStressCaseStatus.FAILED
            case.error = "; ".join(result.errors)
            case.finished_at = timezone.now()

        case.save(
            update_fields=[
                "webhook_received",
                "webhook_signature_ok",
                "webhook_payload_ok",
                "webhook_nonce_ok",
                "webhook_timestamp_ok",
                "webhook_received_nonces",
                "status",
                "error",
                "finished_at",
            ]
        )

    return case
