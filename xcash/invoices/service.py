from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import structlog
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from chains.models import ConfirmMode
from chains.models import TransferType
from chains.service import ChainService
from chains.service import TransferService
from common.utils.math import format_decimal_stripped
from currencies.service import CryptoService
from currencies.service import FiatService
from common.internal_callback import send_internal_callback
from webhooks.service import WebhookService

from .exceptions import InvoiceStatusError
from .models import Invoice
from .models import InvoiceProtocol
from .models import InvoicePaySlot
from .models import InvoicePaySlotDiscardReason
from .models import InvoicePaySlotStatus
from .models import InvoiceStatus

if TYPE_CHECKING:
    from chains.models import OnchainTransfer

logger = structlog.get_logger()


class InvoiceService:
    @staticmethod
    def refresh_initial_worth(invoice: Invoice) -> None:
        """在账单创建后立即固化基础 worth，避免继续依赖隐式 post_save signal。"""
        usd = FiatService.get_by_code("USD")

        if invoice.crypto and invoice.pay_amount:
            worth = invoice.crypto.to_fiat(fiat=usd, amount=invoice.pay_amount)
        elif invoice.is_crypto_fixed:
            crypto = CryptoService.get_by_symbol(invoice.currency)
            worth = crypto.to_fiat(fiat=usd, amount=invoice.amount)
        else:
            fiat = FiatService.get_by_code(invoice.currency)
            worth = invoice.amount * fiat.fiat_price(usd)

        # worth 只更新自身字段，直接 update 可避免把整行实例再次写回。
        Invoice.objects.filter(pk=invoice.pk).update(
            worth=worth,
            updated_at=timezone.now(),
        )
        invoice.refresh_from_db()

    @staticmethod
    def try_auto_select_single_method(invoice: Invoice) -> None:
        """仅当 methods 唯一时自动分配 pay slot，替代历史 post_save signal。"""
        methods = invoice.methods or {}
        if len(methods) != 1:
            return

        symbol, chain_codes = next(iter(methods.items()))
        if len(chain_codes) != 1:
            return

        try:
            crypto = CryptoService.get_by_symbol(symbol)
            chain = ChainService.get_by_code(chain_codes[0])
            invoice.select_method(crypto, chain)
        except ObjectDoesNotExist:
            logger.warning(
                "initialize_invoice: resource missing",
                symbol=symbol,
                chain=chain_codes[0],
            )
        except Invoice.InvoiceAllocationError as exc:
            logger.warning("initialize_invoice allocation failed", detail=str(exc))

    @staticmethod
    def schedule_expiration_check(invoice: Invoice) -> None:
        """在事务提交后注册过期检查任务，避免回滚后仍派发悬空 Celery 任务。"""
        from functools import partial

        from .tasks import check_expired

        # 提前捕获值，避免闭包延迟绑定陷阱。
        eta = invoice.expires_at + timedelta(seconds=1)
        dispatch = partial(check_expired.apply_async, (invoice.id,), eta=eta)
        transaction.on_commit(dispatch)

    @classmethod
    def initialize_invoice(cls, invoice: Invoice) -> Invoice:
        """账单创建后的显式初始化入口：worth、自动支付方式、过期任务。"""
        cls.refresh_initial_worth(invoice)
        cls.try_auto_select_single_method(invoice)
        cls.schedule_expiration_check(invoice)
        return invoice

    @staticmethod
    def build_webhook_payload(invoice: Invoice) -> dict:
        """构建 webhook 推送给商户的 payload，与 model 层解耦。

        将 payload 结构集中在 service 层管理，便于未来版本化或按场景差异化。
        """
        return {
            "type": "invoice",
            "data": {
                "sys_no": invoice.sys_no,
                "out_no": invoice.out_no,
                "crypto": invoice.crypto.symbol if invoice.crypto else None,
                "chain": invoice.transfer.chain.code if invoice.transfer_id else None,
                "pay_address": invoice.pay_address,
                "pay_amount": (
                    format_decimal_stripped(invoice.pay_amount)
                    if invoice.pay_amount is not None
                    else None
                ),
                "hash": invoice.transfer.hash if invoice.transfer_id else None,
                "block": invoice.transfer.block if invoice.transfer_id else None,
                "confirmed": invoice.status == InvoiceStatus.COMPLETED,
            },
        }

    @staticmethod
    @transaction.atomic
    def try_match_invoice(
        transfer: OnchainTransfer,
    ):
        # 第一步：不加锁地找到候选槽位，仅用于定位归属的 Invoice ID。
        # 避免先锁 PaySlot 再锁 Invoice 的顺序——select_method 先锁 Invoice 再锁
        # PaySlot，两者顺序相反会在并发时形成死锁。
        candidate = (
            InvoicePaySlot.objects.filter(
                chain=transfer.chain,
                crypto=transfer.crypto,
                pay_address=transfer.to_address,
                pay_amount=transfer.amount,
                invoice__started_at__lte=transfer.datetime,
                invoice__expires_at__gte=transfer.datetime,
                invoice__status__in=[InvoiceStatus.WAITING, InvoiceStatus.EXPIRED],
            )
            .filter(
                Q(status=InvoicePaySlotStatus.ACTIVE)
                | Q(
                    status=InvoicePaySlotStatus.DISCARDED,
                    discard_reason=InvoicePaySlotDiscardReason.EXPIRED,
                )
            )
            .order_by("-version", "-created_at", "-pk")
            .values("pk", "invoice_id")
            .first()
        )
        if candidate is None:
            return False

        # 第二步：先锁 Invoice（与 select_method 保持相同的加锁顺序，防死锁）。
        invoice = (
            Invoice.objects.select_for_update()
            .select_related("project")
            .get(pk=candidate["invoice_id"])
        )

        # 第三步：Invoice 锁住后再锁 PaySlot，并重新验证槽位状态（防止锁外失效）。
        pay_slot = (
            InvoicePaySlot.objects.select_for_update()
            .select_related("invoice", "invoice__project", "crypto", "chain")
            .filter(
                pk=candidate["pk"],
            )
            .filter(
                Q(status=InvoicePaySlotStatus.ACTIVE)
                | Q(
                    status=InvoicePaySlotStatus.DISCARDED,
                    discard_reason=InvoicePaySlotDiscardReason.EXPIRED,
                )
            )
            .first()
        )
        if pay_slot is None:
            # 锁住 Invoice 后发现槽位已被其他事务处理，放弃本次匹配。
            return False

        invoice._sync_snapshot_from_slot(pay_slot)
        confirm_mode = (
            ConfirmMode.QUICK
            if invoice.project.fast_confirm_threshold > invoice.worth
            else ConfirmMode.FULL
        )
        transfer = TransferService.assign_type_and_mode(
            transfer, TransferType.Invoice, confirm_mode
        )

        matched_at = timezone.now()
        # 命中任一槽位后，账单就只认这次付款，其余仍 active 的旧槽位立即作废。
        InvoicePaySlot.objects.filter(
            invoice=invoice, status=InvoicePaySlotStatus.ACTIVE
        ).exclude(pk=pay_slot.pk).update(
            status=InvoicePaySlotStatus.DISCARDED,
            discard_reason=InvoicePaySlotDiscardReason.SETTLED,
            discarded_at=matched_at,
            updated_at=matched_at,
        )
        InvoicePaySlot.objects.filter(pk=pay_slot.pk).update(
            status=InvoicePaySlotStatus.MATCHED,
            discard_reason=None,
            matched_at=matched_at,
            discarded_at=None,
            updated_at=matched_at,
        )
        pay_slot.refresh_from_db()
        # 账单状态更新不依赖 post_save 副作用，直接 update 可避免实例整行回写。
        Invoice.objects.filter(pk=invoice.pk).update(
            transfer_id=transfer.pk,
            status=InvoiceStatus.CONFIRMING,
            updated_at=timezone.now(),
        )
        invoice.refresh_from_db()

        if (
            invoice.project.pre_notify
            and invoice.protocol == InvoiceProtocol.NATIVE
            and confirm_mode == ConfirmMode.FULL
        ):
            WebhookService.create_event(
                project=invoice.project,
                payload=InvoiceService.build_webhook_payload(invoice),
            )

        return True

    @classmethod
    @transaction.atomic
    def confirm_invoice(
        cls,
        invoice: Invoice,
    ):
        # 必须在本方法内对 Invoice 加行锁，不能仅依赖调用方（OnchainTransfer.confirm）持有
        # OnchainTransfer 锁——其他调用路径可能绕开 OnchainTransfer 锁直接调用此方法。
        invoice = (
            Invoice.objects.select_for_update()
            .select_related("project")
            .get(pk=invoice.pk)
        )
        if invoice.status != InvoiceStatus.CONFIRMING:
            raise InvoiceStatusError(f"Invoice must be confirming, {invoice.sys_no}")

        # 账单确认不依赖 save() 信号，直接 update 可减少并发覆盖面。
        Invoice.objects.filter(pk=invoice.pk).update(
            status=InvoiceStatus.COMPLETED,
            updated_at=timezone.now(),
        )
        invoice.refresh_from_db()

        if invoice.protocol == InvoiceProtocol.EPAY_V1:
            from .epay_service import EpaySubmitService

            EpaySubmitService.enqueue_paid_notify(invoice)
        elif invoice.protocol == InvoiceProtocol.NATIVE:
            WebhookService.create_event(
                project=invoice.project,
                payload=cls.build_webhook_payload(invoice),
            )
        # 设计决策：开源版本不计算内部手续费或月成交量统计，
        # 账单状态机在 COMPLETED 即为终局，无需后续财务核算步骤。

        send_internal_callback(
            event="invoice.confirmed",
            appid=invoice.project.appid,
            sys_no=invoice.sys_no,
            worth=str(invoice.worth),
            currency=invoice.crypto.symbol,
        )

    @classmethod
    @transaction.atomic
    def drop_invoice(
        cls,
        invoice: Invoice,
    ):
        # 必须在本方法内对 Invoice 加行锁，防止 confirm_invoice 与 drop_invoice
        # 并发执行导致状态错乱（如 drop 回退 WAITING 的同时 confirm 已推送 COMPLETED webhook）。
        invoice = (
            Invoice.objects.select_for_update()
            .select_related("project")
            .get(pk=invoice.pk)
        )
        if invoice.status != InvoiceStatus.CONFIRMING:
            raise InvoiceStatusError("Invoice must be confirming")

        matched_slot = (
            invoice.pay_slots.filter(status=InvoicePaySlotStatus.MATCHED)
            .order_by("-matched_at", "-version", "-pk")
            .first()
        )
        if matched_slot is not None:
            # 尝试重激活已匹配的槽位；若唯一约束冲突（同地址+金额组合已被其他账单占用），
            # 则放弃重激活，仅清理快照字段。
            reactivated_at = timezone.now()
            try:
                InvoicePaySlot.objects.filter(pk=matched_slot.pk).update(
                    status=InvoicePaySlotStatus.ACTIVE,
                    discard_reason=None,
                    matched_at=None,
                    discarded_at=None,
                    updated_at=reactivated_at,
                )
                matched_slot.refresh_from_db()
                invoice._sync_snapshot_from_slot(matched_slot)
            except IntegrityError:
                # 唯一约束冲突：另一张账单已占用该地址+金额组合，无法重激活。
                # 清空快照字段，让账单回到"未选择支付方式"的初始状态。
                logger.warning(
                    "drop_invoice: slot reactivation conflict, clearing snapshot",
                    invoice=invoice.sys_no,
                    slot_pk=matched_slot.pk,
                )
                Invoice.objects.filter(pk=invoice.pk).update(
                    crypto=None,
                    chain=None,
                    pay_address=None,
                    pay_amount=None,
                    updated_at=reactivated_at,
                )
        else:
            # matched_slot 不存在时（异常场景），同样清空快照字段避免显示过期的支付信息。
            Invoice.objects.filter(pk=invoice.pk).update(
                crypto=None,
                chain=None,
                pay_address=None,
                pay_amount=None,
                updated_at=timezone.now(),
            )

        # 账单回退状态：若已过期则恢复为 EXPIRED 而非 WAITING，避免僵尸账单。
        rollback_status = (
            InvoiceStatus.EXPIRED
            if invoice.expires_at <= timezone.now()
            else InvoiceStatus.WAITING
        )
        Invoice.objects.filter(pk=invoice.pk).update(
            status=rollback_status,
            transfer=None,
            updated_at=timezone.now(),
        )
        invoice.refresh_from_db()
