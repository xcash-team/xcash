from django.apps import AppConfig


class TronConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "tron"
    verbose_name = "Tron"

    def ready(self):
        import tron.signals  # noqa: F401, PLC0415
