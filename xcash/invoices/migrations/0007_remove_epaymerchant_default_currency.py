# 移除 EpayMerchant.default_currency：改由 EPay 提交接口逐次指定 currency，
# 并要求 currency 必须命中 currencies.Fiat 表中的某个法币代码。

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("invoices", "0006_rename_invoice_redirect_url_return_url"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="epaymerchant",
            name="default_currency",
        ),
    ]
