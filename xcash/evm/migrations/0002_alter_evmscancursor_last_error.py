from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("evm", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="evmscancursor",
            name="last_error",
            field=models.TextField(blank=True, default="", verbose_name="最近错误"),
        ),
    ]
