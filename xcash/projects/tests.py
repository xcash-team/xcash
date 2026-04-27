from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib import admin
from django.contrib.sessions.middleware import SessionMiddleware
from django.core.exceptions import PermissionDenied
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.test import override_settings
from django.test.client import RequestFactory
from django.urls import reverse
from django.utils import timezone
from django_otp.plugins.otp_totp.models import TOTPDevice

from chains.models import Chain
from chains.models import ChainType
from chains.test_signer import build_test_remote_signer_backend
from common.admin import ModelAdmin
from currencies.models import Crypto
from projects.admin import PaymentAddressInline
from projects.admin import ProjectAdmin
from projects.models import Project
from projects.models import RecipientAddressUsage
from projects.models import RecipientAddress
from users.models import User
from users.otp import ADMIN_OTP_VERIFIED_AT_SESSION_KEY

_PROJECT_TEST_PATCHERS = []


def setUpModule():
    backend = build_test_remote_signer_backend()
    for target in ("chains.signer.get_signer_backend",):
        patcher = patch(target, return_value=backend)
        patcher.start()
        _PROJECT_TEST_PATCHERS.append(patcher)


def tearDownModule():
    while _PROJECT_TEST_PATCHERS:
        _PROJECT_TEST_PATCHERS.pop().stop()


@override_settings(
    ALERTS_TELEGRAM_BOT_TOKEN="telegram-token",
    ALERTS_TELEGRAM_API_BASE="https://api.telegram.org",
    ALERTS_TELEGRAM_TIMEOUT=3.0,
    ALERTS_REPEAT_INTERVAL_MINUTES=30,
)
class ProjectAdminTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.user = User.objects.create_user(
            username="project-owner", password="secret"
        )
        self.project = Project.objects.create(name="Owner Project")
        self.crypto = Crypto.objects.create(
            name="Ethereum Project",
            symbol="ETHP",
            coingecko_id="ethereum-project",
        )
        self.chain = Chain.objects.create(
            name="Ethereum Project",
            code="eth-project",
            type=ChainType.EVM,
            native_coin=self.crypto,
            chain_id=403,
            rpc="http://localhost:8545",
            active=True,
        )

    def _force_verified_admin_login(self, username: str, *, verified_at=None) -> User:
        admin_user = User.objects.create_superuser(username=username, password="secret")
        device = TOTPDevice.objects.create(user=admin_user, name="Admin TOTP")
        self.client.force_login(admin_user)
        session = self.client.session
        session["otp_device_id"] = device.persistent_id
        session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = (
            verified_at or timezone.now()
        ).isoformat()
        session.save()
        return admin_user

    def _force_verified_project_owner_login(self, *, verified_at=None) -> TOTPDevice:
        device = TOTPDevice.objects.create(user=self.user, name="Owner Admin TOTP")
        self.client.force_login(self.user)
        session = self.client.session
        session["otp_device_id"] = device.persistent_id
        session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = (
            verified_at or timezone.now()
        ).isoformat()
        session.save()
        return device

    def _build_project_owner_request(self, *, verified_at):
        device = TOTPDevice.objects.create(user=self.user, name="Owner Admin TOTP")
        request = self.factory.post(
            reverse("admin:projects_project_change", args=[self.project.pk])
        )
        SessionMiddleware(lambda req: None).process_request(request)
        request.session["otp_device_id"] = device.persistent_id
        request.session[ADMIN_OTP_VERIFIED_AT_SESSION_KEY] = verified_at.isoformat()
        request.session.save()
        request.user = self.user
        request.user.otp_device = device
        return request

    def test_project_admin_save_model_requires_fresh_otp_for_withdrawal_policy_change(
        self,
    ):
        admin_instance = ProjectAdmin(Project, admin.site)
        request = self._build_project_owner_request(
            verified_at=timezone.now() - timedelta(minutes=16)
        )
        form = SimpleNamespace(changed_data=["withdrawal_single_limit"])

        with self.assertRaises(PermissionDenied):
            admin_instance.save_model(request, self.project, form=form, change=True)

    def test_project_admin_save_model_allows_non_sensitive_change_without_fresh_otp(
        self,
    ):
        admin_instance = ProjectAdmin(Project, admin.site)
        request = self._build_project_owner_request(
            verified_at=timezone.now() - timedelta(minutes=16)
        )
        form = SimpleNamespace(changed_data=["name"])

        with (
            patch.object(
                ModelAdmin,
                "save_model",
                autospec=True,
            ) as save_model_mock,
            patch.object(
                admin_instance,
                "_require_fresh_project_change_otp",
                autospec=True,
            ) as otp_mock,
        ):
            admin_instance.save_model(request, self.project, form=form, change=True)

        otp_mock.assert_not_called()
        save_model_mock.assert_called_once()

    def test_payment_address_inline_form_validates_without_usage_field(self):
        request = self.factory.get("/admin/projects/project/add/")
        request.user = self.user

        inline = PaymentAddressInline(Project, admin.site)
        formset_class = inline.get_formset(request, self.project)
        form = formset_class.form(
            data={
                "name": "Invoice Inline",
                "chain_type": ChainType.EVM,
                "address": "0x52908400098527886E0F7030069857D2E4169EE7",
            },
            instance=RecipientAddress(project=self.project),
        )

        self.assertTrue(form.is_valid(), form.errors)


class RecipientAddressCapabilityTests(TestCase):
    def setUp(self):
        self.project = Project.objects.create(name="Recipient Capability Project")

    def test_name_is_optional_when_saving_recipient_address(self):
        recipient = RecipientAddress.objects.create(
            project=self.project,
            chain_type=ChainType.TRON,
            address="TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT",
            usage=RecipientAddressUsage.INVOICE,
        )

        self.assertEqual(recipient.name, "")

    def test_clean_allows_tron_invoice_address(self):
        recipient = RecipientAddress(
            name="Tron Invoice",
            project=self.project,
            chain_type=ChainType.TRON,
            address="TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT",
            usage=RecipientAddressUsage.INVOICE,
        )

        recipient.clean()

    def test_clean_rejects_tron_collection_address(self):
        recipient = RecipientAddress(
            name="Tron Collection",
            project=self.project,
            chain_type=ChainType.TRON,
            address="TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT",
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )

        with self.assertRaises(ValidationError) as ctx:
            recipient.clean()

        self.assertIn("chain_type", ctx.exception.message_dict)

    def test_clean_rejects_bitcoin_collection_address(self):
        recipient = RecipientAddress(
            name="Bitcoin Collection",
            project=self.project,
            chain_type=ChainType.BITCOIN,
            address="1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )

        with self.assertRaises(ValidationError) as ctx:
            recipient.clean()

        self.assertIn("chain_type", ctx.exception.message_dict)
        self.assertEqual(
            ctx.exception.message_dict["chain_type"],
            ["当前版本归集地址仅支持 EVM。"],
        )

    def test_invoice_recipient_queryset_filters_by_usage(self):
        from projects.service import ProjectService

        RecipientAddress.objects.create(
            name="Invoice Recipient",
            project=self.project,
            chain_type=ChainType.EVM,
            address="0x52908400098527886E0F7030069857D2E4169EE7",
            usage=RecipientAddressUsage.INVOICE,
        )
        RecipientAddress.objects.create(
            name="Collection Recipient",
            project=self.project,
            chain_type=ChainType.EVM,
            address="0x8617E340B3D01FA5F11F306F4090FD50E238070D",
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )

        qs = ProjectService.invoice_recipients(self.project)

        self.assertEqual(qs.count(), 1)
        self.assertEqual(qs.first().usage, RecipientAddressUsage.INVOICE)
