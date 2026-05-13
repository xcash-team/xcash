from __future__ import annotations

from django.db import transaction
from django.db.models.signals import post_delete
from django.db.models.signals import post_save
from django.db.models.signals import pre_save
from django.dispatch import receiver
from tron.watchers import clear_tron_filter_addresses_cache
from tron.watchers import refresh_tron_filter_addresses

from chains.models import ChainType
from projects.models import RecipientAddress


def _refresh_tron_filter_addresses_on_commit() -> None:
    def _refresh() -> None:
        clear_tron_filter_addresses_cache()
        refresh_tron_filter_addresses()

    # 缓存失效必须等事务提交后再重建，避免其它 scanner 在事务未提交时
    # 抢先按旧 DB 快照刷新缓存并推进无 replay 的 Tron 游标。
    transaction.on_commit(_refresh)


@receiver(pre_save, sender=RecipientAddress)
def remember_old_chain_type_for_tron_filter_cache(
    sender,
    instance: RecipientAddress,
    **kwargs,
):
    if instance.pk is None:
        instance._old_chain_type = None
        return
    instance._old_chain_type = (
        RecipientAddress.objects.filter(pk=instance.pk)
        .values_list("chain_type", flat=True)
        .first()
    )


@receiver(post_save, sender=RecipientAddress)
@receiver(post_delete, sender=RecipientAddress)
def refresh_tron_filter_addresses_when_recipient_address_changes(
    sender,
    instance: RecipientAddress,
    **kwargs,
):
    # 旧值或新值任一为 Tron 都要失效；否则 TRON -> EVM 修改会留下陈旧观察地址。
    old_chain_type = getattr(instance, "_old_chain_type", None)
    if instance.chain_type != ChainType.TRON and old_chain_type != ChainType.TRON:
        return
    _refresh_tron_filter_addresses_on_commit()
