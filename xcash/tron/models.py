from __future__ import annotations

from django.db import models
from django.utils.translation import gettext_lazy as _

from common.fields import AddressField


class TronWatchCursor(models.Model):
    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.CASCADE,
        related_name="tron_watch_cursors",
        verbose_name=_("链"),
    )
    watch_address = AddressField(_("监听地址"))
    last_scanned_block = models.PositiveIntegerField(_("已扫描到的区块"), default=0)
    last_safe_block = models.PositiveIntegerField(_("安全区块"), default=0)
    last_event_fingerprint = models.CharField(
        _("最近事件指纹"),
        max_length=128,
        blank=True,
        default="",
    )
    enabled = models.BooleanField(_("启用"), default=True)
    last_error = models.CharField(_("最近错误"), max_length=255, blank=True, default="")
    last_error_at = models.DateTimeField(_("最近错误时间"), blank=True, null=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("chain", "watch_address"),
                name="uniq_tron_watch_cursor_chain_watch_address",
            ),
        ]
        ordering = ("chain_id", "watch_address")
        verbose_name = _("Tron 扫描游标")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"{self.chain.code}:{self.watch_address}"
