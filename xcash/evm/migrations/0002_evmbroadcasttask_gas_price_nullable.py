from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("evm", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="evmbroadcasttask",
            name="gas_price",
            field=models.PositiveBigIntegerField(
                blank=True,
                null=True,
                verbose_name="Gas Price",
            ),
        ),
    ]
