from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ("risk", "0002_alter_riskassessment_source"),
    ]

    operations = [
        migrations.AlterField(
            model_name="riskassessment",
            name="risk_detail",
            field=models.JSONField(blank=True, default=list, verbose_name="风险详情"),
        ),
        migrations.AddField(
            model_name="riskassessment",
            name="skip_reason",
            field=models.CharField(
                blank=True,
                choices=[
                    ("unsupported_chain", "链或币种暂不支持"),
                    ("provider_not_configured", "未配置 provider"),
                ],
                db_index=True,
                default="",
                max_length=32,
                verbose_name="跳过原因",
            ),
        ),
    ]
