from __future__ import annotations

from collections import defaultdict

from django.contrib import admin
from django.contrib import messages
from django.db import transaction
from django.utils import timezone


class SyncScanCursorToLatestActionMixin:
    """为扫描游标后台提供“追平到最新区块”批量动作。"""

    def get_sync_latest_block(self, *, chain) -> int:
        return chain.latest_block_number

    @admin.action(description="启用所选扫描游标")
    def enable_selected_scanners(self, request, queryset) -> None:
        selected_ids = list(queryset.values_list("pk", flat=True))
        if not selected_ids:
            self.message_user(request, "未选中任何扫描游标", level=messages.WARNING)
            return

        updated_count = queryset.model.objects.filter(pk__in=selected_ids).update(
            enabled=True
        )
        self.message_user(
            request,
            f"已启用 {updated_count} 个扫描游标",
            level=messages.SUCCESS,
        )

    @admin.action(description="暂停所选扫描游标")
    def disable_selected_scanners(self, request, queryset) -> None:
        selected_ids = list(queryset.values_list("pk", flat=True))
        if not selected_ids:
            self.message_user(request, "未选中任何扫描游标", level=messages.WARNING)
            return

        updated_count = queryset.model.objects.filter(pk__in=selected_ids).update(
            enabled=False
        )
        self.message_user(
            request,
            f"已暂停 {updated_count} 个扫描游标",
            level=messages.SUCCESS,
        )

    @admin.action(description="追平到最新区块")
    def sync_selected_to_latest(self, request, queryset) -> None:
        selected_cursors = list(
            queryset.select_related("chain").order_by("chain_id", "pk")
        )
        if not selected_cursors:
            self.message_user(request, "未选中任何扫描游标", level=messages.WARNING)
            return

        cursor_ids_by_chain_id: dict[int, list[int]] = defaultdict(list)
        chains_by_id = {}
        for cursor in selected_cursors:
            cursor_ids_by_chain_id[cursor.chain_id].append(cursor.pk)
            chains_by_id[cursor.chain_id] = cursor.chain

        success_count = 0
        updated_at = timezone.now()

        for chain_id, cursor_ids in cursor_ids_by_chain_id.items():
            chain = chains_by_id[chain_id]
            try:
                latest_block = self.get_sync_latest_block(chain=chain)
            except Exception as exc:  # noqa: BLE001
                self.message_user(
                    request,
                    f"{chain.code} 获取最新区块失败，已跳过 {len(cursor_ids)} 个扫描游标：{exc}",
                    level=messages.ERROR,
                )
                continue
            safe_block = max(0, latest_block - chain.confirm_block_count)
            with transaction.atomic():
                queryset.model.objects.filter(pk__in=cursor_ids).update(
                    last_scanned_block=latest_block,
                    last_safe_block=safe_block,
                    last_error="",
                    last_error_at=None,
                    updated_at=updated_at,
                )
            success_count += len(cursor_ids)

        if success_count:
            self.message_user(
                request,
                f"已将 {success_count} 个扫描游标追平到链上最新区块",
                level=messages.SUCCESS,
            )
