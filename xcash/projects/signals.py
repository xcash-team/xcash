from django.db.models.signals import pre_save
from django.dispatch import receiver

from chains.models import Wallet
from projects.models import Project


@receiver(pre_save, sender=Project)
def project_pre_created(sender, instance: Project, **kwargs):
    if (
        not Project.objects.filter(pk=instance.pk).exists()
        and instance.wallet_id is None
    ):
        instance.wallet = Wallet.generate()


@receiver(pre_save, sender=Project)
def clear_failed_count(sender, instance: Project, **kwargs):
    if Project.objects.filter(pk=instance.pk).exists():
        old_instance = Project.objects.get(pk=instance.pk)
        if not old_instance.webhook_open and instance.webhook_open:
            instance.failed_count = 0
            from webhooks.models import WebhookEvent

            WebhookEvent.objects.filter(
                project=instance, status=WebhookEvent.Status.FAILED
            ).update(
                status=WebhookEvent.Status.PENDING,
                schedule_locked_until=None,
                delivery_locked_until=None,
            )
