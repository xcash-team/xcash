from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from django import forms
from django.utils.translation import gettext_lazy as _
from unfold.widgets import UnfoldAdminDecimalFieldWidget
from unfold.widgets import UnfoldAdminSelectWidget
from unfold.widgets import UnfoldAdminTextInputWidget

from common.consts import MAX_INVOICE_DURATION
from common.consts import MIN_INVOICE_DURATION
from currencies.service import CryptoService
from currencies.service import FiatService
from projects.models import Project

from .models import Invoice
from .widgets import CurrencySelectWidget

DEFAULT_MANUAL_DURATION_MINUTES = 10


if TYPE_CHECKING:
    from collections.abc import Iterable

    from django.utils.functional import Promise


class ManualInvoiceAdminForm(forms.ModelForm):
    REQUIRED_FIELDS: tuple[str, ...] = ("project", "title", "currency", "amount")
    OPTIONAL_FIELDS: tuple[str, ...] = ("out_no", "duration")
    FIELDSETS = (
        (
            _("必填"),
            {
                "fields": REQUIRED_FIELDS,
                "classes": ("wide",),
            },
        ),
        (
            _("可选"),
            {
                "fields": OPTIONAL_FIELDS,
                "classes": ("wide",),
            },
        ),
    )

    duration = forms.IntegerField(
        required=False,
        min_value=MIN_INVOICE_DURATION,
        max_value=MAX_INVOICE_DURATION,
        label=_("有效期（分钟）"),
        help_text=_("留空时使用默认有效期，当前默认值为 %(default)s 分钟。")
        % {"default": DEFAULT_MANUAL_DURATION_MINUTES},
        widget=UnfoldAdminTextInputWidget(
            attrs={
                "type": "number",
                "min": str(MIN_INVOICE_DURATION),
                "max": str(MAX_INVOICE_DURATION),
            }
        ),
    )

    class Meta:
        model = Invoice
        fields = ("project", "title", "out_no", "currency", "amount")
        widgets = {
            "project": UnfoldAdminSelectWidget(attrs={"class": "w-full"}),
            "title": UnfoldAdminTextInputWidget(),
            "out_no": UnfoldAdminTextInputWidget(),
            "amount": UnfoldAdminDecimalFieldWidget(),
        }

    def __init__(self, *args, **kwargs):
        project_queryset = kwargs.pop("project_queryset", None)
        super().__init__(*args, **kwargs)

        project_choice_field = self._setup_project_field(project_queryset)

        duration_field = self.fields.get("duration")
        if duration_field is not None:
            duration_field.initial = DEFAULT_MANUAL_DURATION_MINUTES

        ordered_fields = list(self.REQUIRED_FIELDS + self.OPTIONAL_FIELDS)
        self.order_fields(ordered_fields)

        self._configure_out_no_field()
        self._configure_amount_field()

        original_currency_field = self.fields.get("currency")
        if original_currency_field is None:
            return

        crypto_codes = self._collect_crypto_codes(project_choice_field)
        fiat_codes = self._collect_fiat_codes()

        currency_field = self._build_currency_field(
            original_currency_field=original_currency_field,
            fiat_codes=fiat_codes,
            crypto_codes=crypto_codes,
        )

        self.fields["currency"] = currency_field

    def _setup_project_field(
        self, project_queryset: Iterable[Project] | None
    ) -> forms.ModelChoiceField | None:
        project_field = self.fields.get("project")
        if not isinstance(project_field, forms.ModelChoiceField):
            return None
        if project_queryset is not None:
            project_field.queryset = project_queryset
        return project_field

    def _collect_crypto_codes(
        self, project_field: forms.ModelChoiceField | None
    ) -> set[str]:
        project = self._resolve_project(project_field)
        if project is not None:
            return self._available_symbols(project)

        if project_field is None or project_field.queryset is None:
            return set()

        return {
            symbol.upper()
            for candidate in project_field.queryset
            for symbol in Invoice.available_methods(candidate)
        }

    def _build_currency_field(
        self,
        *,
        original_currency_field: forms.Field,
        fiat_codes: set[str],
        crypto_codes: set[str],
    ) -> forms.ChoiceField:
        choices = self._build_currency_choices(fiat_codes, crypto_codes)

        currency_field = forms.ChoiceField(
            choices=choices,
            label=original_currency_field.label,
            required=True,
            widget=CurrencySelectWidget(
                fiat_codes=fiat_codes,
            ),
            help_text=_("若选择法币，支付时将按实时汇率自动换算为应付加密货币的数量。"),
        )
        currency_field.initial = original_currency_field.initial

        return currency_field

    def _configure_out_no_field(self) -> None:
        out_no_field = self.fields.get("out_no")
        if out_no_field is None:
            return
        out_no_field.required = False
        out_no_field.help_text = _("留空时系统会自动生成商户单号。")

    def _configure_amount_field(self) -> None:
        amount_field = self.fields.get("amount")
        if amount_field is None:
            return
        amount_field.min_value = Decimal("0.00000001")
        amount_field.widget.attrs.setdefault("step", "0.00000001")

    def _resolve_project(
        self, project_field: forms.ModelChoiceField | None
    ) -> Project | None:
        if project_field is None or project_field.queryset is None:
            return None

        queryset = project_field.queryset

        def fetch(raw_value) -> Project | None:
            if not raw_value:
                return None
            try:
                return queryset.get(pk=raw_value)
            except (Project.DoesNotExist, ValueError, TypeError):
                return None

        if self.is_bound:
            return fetch(self.data.get(self.add_prefix("project")))

        initial_value = self.initial.get("project")
        if isinstance(initial_value, Project):
            return initial_value

        candidate = fetch(initial_value)
        if candidate is not None:
            return candidate

        if getattr(self.instance, "pk", None):
            return self.instance.project

        return None

    @staticmethod
    def _available_symbols(project: Project) -> set[str]:
        return {symbol.upper() for symbol in Invoice.available_methods(project)}

    def _build_choice_group(
        self,
        *,
        label: str | Promise,
        codes: set[str],
        is_fiat: bool,
    ) -> tuple[str | Promise, list[tuple[str, str]]] | None:
        ordered_codes = self._order_codes(codes)
        if not ordered_codes:
            return None

        return (
            label,
            [
                (code, self._build_label(code, is_fiat=is_fiat))
                for code in ordered_codes
            ],
        )

    @staticmethod
    def _order_codes(codes: set[str]) -> list[str]:
        if not codes:
            return []
        return sorted(codes)

    @staticmethod
    def _collect_fiat_codes() -> set[str]:
        codes = FiatService.list_all().values_list("code", flat=True)
        return {code.upper() for code in codes}

    def _build_currency_choices(
        self, fiat_codes: set[str], crypto_codes: set[str]
    ) -> list[tuple[str | Promise, list[tuple[str, str]]]]:
        choices: list[tuple[str | Promise, list[tuple[str, str]]]] = []

        fiat_group = self._build_choice_group(
            label=_("法币"),
            codes=fiat_codes,
            is_fiat=True,
        )
        if fiat_group is not None:
            choices.append(fiat_group)

        crypto_group = self._build_choice_group(
            label=_("加密货币"),
            codes=crypto_codes,
            is_fiat=False,
        )
        if crypto_group is not None:
            choices.append(crypto_group)

        return choices

    @staticmethod
    def _build_label(code: str, *, is_fiat: bool) -> str:
        kind = _("法币") if is_fiat else _("加密货币")
        return f"{code} · {kind}"

    def clean_out_no(self):
        value = self.cleaned_data.get("out_no")
        if not value:
            return ""

        normalized = value.strip()
        project = self.cleaned_data.get("project")
        if (
            project
            and Invoice.objects.filter(project=project, out_no=normalized).exists()
        ):
            raise forms.ValidationError(_("该商户单号已存在，请重新填写。"))
        return normalized

    def clean_currency(self):
        value = self.cleaned_data["currency"].strip().upper()
        if not value:
            raise forms.ValidationError(_("请输入货币代码"))
        if not (CryptoService.exists(value) or FiatService.exists(value)):
            raise forms.ValidationError(_("当前货币暂不支持"))
        return value

    def clean(self):
        cleaned_data = super().clean()
        project = cleaned_data.get("project")
        currency = cleaned_data.get("currency")
        if not project or not currency:
            return cleaned_data

        available_methods = Invoice.available_methods(project)
        if not available_methods:
            raise forms.ValidationError(
                _("当前项目暂无可用支付方式。请确保已设置支付地址。")
            )

        if CryptoService.exists(currency):
            crypto_methods = available_methods.get(currency)
            if not crypto_methods:
                raise forms.ValidationError(_("该加密货币暂无可用链"))
            cleaned_data["methods"] = {currency: crypto_methods}
        else:
            cleaned_data["methods"] = available_methods

        return cleaned_data

    def clean_duration(self):
        duration = self.cleaned_data.get("duration")
        if duration in (None, ""):
            return None

        if not (MIN_INVOICE_DURATION <= duration <= MAX_INVOICE_DURATION):
            raise forms.ValidationError(
                _("有效期需在 %(min)s 至 %(max)s 分钟之间")
                % {"min": MIN_INVOICE_DURATION, "max": MAX_INVOICE_DURATION}
            )

        return duration
