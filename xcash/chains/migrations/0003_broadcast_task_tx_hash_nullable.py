from django.db import migrations

import common.fields


class Migration(migrations.Migration):
    dependencies = [
        ("chains", "0002_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="broadcasttask",
            name="tx_hash",
            field=common.fields.HashField(
                blank=True,
                db_index=True,
                max_length=100,
                null=True,
                unique=False,
                verbose_name="交易哈希",
            ),
        ),
    ]
