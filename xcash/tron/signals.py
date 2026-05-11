from __future__ import annotations

from django.db import transaction
from django.db.models.signals import post_delete
from django.db.models.signals import post_save
from django.dispatch import receiver

from chains.models import ChainType
from projects.models import RecipientAddress
from tron.watchers import clear_tron_filter_addresses_cache
from tron.watchers import refresh_tron_filter_addresses


def _refresh_tron_filter_addresses_on_commit() -> None:
    # 同步清缓存：让其它进程立即停止读到陈旧集合；
    # commit 后重新预热：避免事务尚未可见时把脏数据写回缓存。
    clear_tron_filter_addresses_cache()
    transaction.on_commit(refresh_tron_filter_addresses)


@receiver(post_save, sender=RecipientAddress)
@receiver(post_delete, sender=RecipientAddress)
def refresh_tron_filter_addresses_when_recipient_address_changes(
    sender,
    instance: RecipientAddress,
    **kwargs,
):
    # 只对 Tron 链上的 RecipientAddress 变更失效缓存，避免 EVM 写入触发无用刷新。
    if instance.chain_type != ChainType.TRON:
        return
    _refresh_tron_filter_addresses_on_commit()
