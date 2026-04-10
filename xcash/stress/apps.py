from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class StressConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "stress"
    verbose_name = _("压力测试")
