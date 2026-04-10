"""Deposit.transfer: SET_NULL+nullable → CASCADE+non-nullable。

Transfer 是 Deposit 的存在依据，不应允许为空。
链上回滚时 Transfer 和 Deposit 应同步删除，保持本地与链上一致。
"""

import django.db.models.deletion
from django.db import migrations, models


def cleanup_orphan_deposits(apps, schema_editor):
    """删除 transfer 为空的脏数据（项目未上线，仅防御性清理）。"""
    Deposit = apps.get_model("deposits", "Deposit")
    Deposit.objects.filter(transfer__isnull=True).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("deposits", "0003_alter_depositaddress_chain_type"),
    ]

    operations = [
        migrations.RunPython(cleanup_orphan_deposits, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="deposit",
            name="transfer",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                to="chains.onchaintransfer",
                verbose_name="链上转账",
            ),
        ),
    ]
