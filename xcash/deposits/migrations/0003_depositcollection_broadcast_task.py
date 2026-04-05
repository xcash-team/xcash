from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("chains", "0003_broadcast_task_tx_hash_nullable"),
        ("deposits", "0002_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="depositcollection",
            name="broadcast_task",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="deposit_collection",
                to="chains.broadcasttask",
                verbose_name="链上任务",
            ),
        ),
    ]
