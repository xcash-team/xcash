# 系统级 lazy create 的历史数据补齐：为所有尚未绑定 EpayMerchant 的 Project
# 自动分配 pid (从 PID_BASELINE=1688 起步) 与 16 位随机 secret_key。
# 这样上线后所有项目（包括迁移前已存在的）都具备 EPay 收款身份。

import secrets

from django.db import migrations


PID_BASELINE = 1688
SECRET_KEY_LENGTH = 16


def _generate_secret_key() -> str:
    # token_urlsafe(12) 稳定产出 16 位 base64url 字符串，与 EpayMerchant
    # SECRET_KEY_LENGTH 对齐；data migration 不依赖 runtime model，
    # 故就近实现一份等价逻辑。
    return secrets.token_urlsafe(12)


def backfill_epay_merchants(apps, schema_editor):
    Project = apps.get_model("projects", "Project")
    EpayMerchant = apps.get_model("invoices", "EpayMerchant")

    missing = list(
        Project.objects.using(schema_editor.connection.alias)
        .filter(epay_merchant__isnull=True)
        .order_by("pk")
    )
    if not missing:
        return

    max_pid = (
        EpayMerchant.objects.using(schema_editor.connection.alias)
        .order_by("-pid")
        .values_list("pid", flat=True)
        .first()
    )
    next_pid = max(max_pid or 0, PID_BASELINE - 1) + 1
    if max_pid is None:
        next_pid = PID_BASELINE

    for project in missing:
        EpayMerchant.objects.using(schema_editor.connection.alias).create(
            project=project,
            pid=next_pid,
            secret_key=_generate_secret_key(),
            active=True,
        )
        next_pid += 1


def reverse_noop(apps, schema_editor):
    # 反向迁移不删 EpayMerchant：删除会丢失商户密钥，且与 OneToOne 历史业务订单关联。
    # 真要回滚就走 0007 之前的状态，让运维显式判断。
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("invoices", "0007_remove_epaymerchant_default_currency"),
    ]

    operations = [
        migrations.RunPython(backfill_epay_merchants, reverse_noop),
    ]
