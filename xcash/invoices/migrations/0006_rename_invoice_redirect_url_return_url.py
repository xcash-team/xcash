# Generated for renaming Invoice.redirect_url to Invoice.return_url.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("invoices", "0005_alter_epaymerchant_default_currency_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="invoice",
            old_name="redirect_url",
            new_name="return_url",
        ),
        migrations.AlterField(
            model_name="invoice",
            name="return_url",
            field=models.URLField(blank=True, verbose_name="支付成功后同步跳转地址"),
        ),
    ]
