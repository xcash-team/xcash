from django import forms
from django.contrib import admin
from django.contrib.admin import helpers
from django.contrib.admin.utils import flatten_fieldsets
from django.core.exceptions import PermissionDenied
from django.core.exceptions import ValidationError
from django.forms.formsets import all_valid
from django.utils.html import format_html
from django.utils.html import format_html_join
from django.utils.translation import gettext_lazy as _
from unfold.admin import TabularInline
from unfold.decorators import display
from unfold.widgets import UnfoldAdminTextInputWidget

from alerts.admin import ProjectTelegramAlertConfigInline
from alerts.models import ProjectTelegramAlertConfig
from chains.adapters import AdapterFactory
from chains.capabilities import ChainProductCapabilityService
from chains.models import Address
from chains.models import AddressUsage
from chains.models import Chain
from chains.models import ChainType
from common.admin import ModelAdmin
from projects.models import Project
from projects.models import RecipientAddressUsage
from projects.models import RecipientAddress
from users.forms import OTPVerifyForm
from users.models import AdminAccessLog
from users.otp import AdminOTPRequiredError
from users.otp import get_fresh_admin_sensitive_action_context
from users.otp import get_pending_admin_user
from users.otp import get_primary_totp_device
from users.otp import record_admin_access
from users.otp import refresh_admin_otp_verification
from users.otp import set_pending_admin_otp
from users.otp import verify_otp_token

# Register your models here.


class ProjectForm(forms.ModelForm):
    class Meta:
        model = Project
        fields = ("ip_white_list",)

    def __init__(self, *args, **kwargs):
        # 从 kwargs 中提取用户
        self.user = kwargs.pop("user", None)
        super().__init__(*args, **kwargs)

    def clean_ip_white_list(self):
        """
        检查设置的白名单IP 地址或网络是否合法
        :return: None
        """
        ip_white_list = self.cleaned_data.get("ip_white_list", "").strip()

        if not ip_white_list or ip_white_list == "*":
            return ip_white_list

        from common.utils.security import is_ip_or_network

        if not all(is_ip_or_network(addr) for addr in ip_white_list.split(",")):
            raise forms.ValidationError(_("IP 白名单格式错误."))

        return ip_white_list


class ProjectHmacKeyWidget(UnfoldAdminTextInputWidget):
    input_type = "password"

    class Media:
        js = ("projects/js/hmac_key_toggle.js",)

    def __init__(self, attrs=None):
        super().__init__(attrs=attrs)

        classes = self.attrs.get("class", "").split()
        if "pr-12" not in classes:
            classes.append("pr-12")
        self.attrs["class"] = " ".join(classes)

        self.attrs.setdefault("data-password-toggle-input", "true")
        self.attrs.setdefault("autocomplete", "off")

    def render(self, name, value, attrs=None, renderer=None):
        input_html = super().render(name, value, attrs=attrs, renderer=renderer)
        button_html = format_html(
            '<button type="button" '
            'class="flex items-center justify-center text-gray-400 hover:text-gray-600 '
            "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 "
            'focus-visible:outline-primary-500 dark:text-gray-500 dark:hover:text-gray-300" '
            'style="position:absolute;top:50%;right:0.5rem;transform:translateY(-50%);" '
            'data-password-toggle-button aria-label="{}" aria-pressed="false">'
            '<span class="material-symbols-outlined text-lg" data-password-toggle-icon '
            'data-hidden-label="visibility_off" data-visible-label="visibility">visibility_off</span>'
            "</button>",
            _("显示或隐藏密钥"),
        )

        return format_html(
            '<div class="max-w-2xl" data-password-toggle '
            'style="position:relative;max-width:42rem;">{}{}</div>',
            input_html,
            button_html,
        )


class RecipientAddressInlineForm(forms.ModelForm):
    """支付地址 / 归集地址 inline 共用表单，包含地址格式校验和跨项目占用检查。"""

    allowed_chain_types = frozenset(ChainType.values)

    class Meta:
        model = RecipientAddress
        fields = ("name", "chain_type", "address")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["chain_type"].choices = [
            choice
            for choice in ChainType.choices
            if choice[0] in self.allowed_chain_types
        ]

    def clean(self):
        cleaned_data = super().clean()
        chain_type = cleaned_data.get("chain_type")
        address = cleaned_data.get("address")
        if not chain_type or not address:
            return cleaned_data

        if chain_type not in self.allowed_chain_types:
            raise ValidationError(_("当前用途不支持该地址格式"))

        adapter = AdapterFactory.get_adapter(chain_type=chain_type)
        if not adapter.validate_address(address=address):
            raise forms.ValidationError(_("地址格式错误"))

        # inline 场景下 project 由 parent 自动注入，不在 cleaned_data 里；
        # 用 instance.project_id 或 parent_instance 来做跨项目占用检查。
        project = getattr(self.instance, "project", None)
        qs = RecipientAddress.objects.filter(address=address)
        if project:
            qs = qs.exclude(project=project)
        if qs.exists():
            raise ValidationError(_("地址已被其他项目占用"))

        return cleaned_data

    def clean_address(self):
        address = self.cleaned_data.get("address")
        if address and Address.objects.filter(address=address).exists():
            raise ValidationError(_("不能设置为系统内账户"))
        return address


class _RecipientAddressInline(TabularInline):
    """支付地址 / 归集地址 inline 基类，子类只需声明 model 和用途字段。"""

    form = RecipientAddressInlineForm
    extra = 0
    fields = ("name", "chain_type", "address")
    usage = ""
    allowed_chain_types = frozenset(ChainType.values)

    def get_queryset(self, request):
        return super().get_queryset(request).filter(usage=self.usage)

    def get_formset(self, request, obj=None, **kwargs):
        base_form = self.form

        class InlineForm(base_form):
            allowed_chain_types = self.allowed_chain_types

        kwargs["form"] = InlineForm
        formset = super().get_formset(request, obj, **kwargs)
        formset.form.base_fields["chain_type"].choices = [
            choice
            for choice in ChainType.choices
            if choice[0] in self.allowed_chain_types
        ]
        return formset


class PaymentAddressInline(_RecipientAddressInline):
    model = RecipientAddress
    usage = RecipientAddressUsage.INVOICE
    allowed_chain_types = (
        ChainProductCapabilityService.INVOICE_RECIPIENT_CHAIN_TYPES
    )
    verbose_name = _("支付地址")
    verbose_name_plural = _("支付地址")


class CollectionAddressInline(_RecipientAddressInline):
    model = RecipientAddress
    usage = RecipientAddressUsage.DEPOSIT_COLLECTION
    allowed_chain_types = (
        ChainProductCapabilityService.COLLECTION_RECIPIENT_CHAIN_TYPES
    )
    verbose_name = _("归集地址")
    verbose_name_plural = _("归集地址")


@admin.register(Project)
class ProjectAdmin(ModelAdmin):
    change_form_outer_after_template = "admin/includes/project_change_otp_modal.html"
    SENSITIVE_PROJECT_FIELDS = frozenset(
        {
            "withdrawal_review_required",
            "withdrawal_review_exempt_limit",
            "withdrawal_single_limit",
            "withdrawal_daily_limit",
        }
    )
    form = ProjectForm
    inlines = (
        PaymentAddressInline,
        CollectionAddressInline,
        ProjectTelegramAlertConfigInline,
    )
    list_display = (
        "name",
        "appid",
        "display_ready_status",
        "display_withdrawal_policy",
        "webhook",
        "failed_count",
        "webhook_open",
        "active",
    )
    list_editable = ("active",)
    list_filter = (
        "active",
        "webhook_open",
        "withdrawal_review_required",
    )
    search_fields = ("name", "appid", "webhook")

    def _require_fresh_project_change_otp(self, request):
        cached_context = getattr(request, "_project_sensitive_action_context", None)
        if cached_context is not None:
            return cached_context

        context = get_fresh_admin_sensitive_action_context(
            request=request,
            source="admin_project_change",
        )
        request._project_sensitive_action_context = context
        return context

    def _project_form_changes_sensitive_fields(self, form) -> bool:
        # 只有提币风控字段真正变化时才要求 fresh OTP，避免普通项目配置编辑被一刀切。
        if form is None:
            return False
        changed_fields = set(getattr(form, "changed_data", ()) or ())
        return bool(changed_fields & self.SENSITIVE_PROJECT_FIELDS)

    def _project_post_changes_sensitive_fields(self, request, obj: Project) -> bool:
        # changeform_view 需要在进入保存前预判风险级别，这里复用 admin form 的变更比较逻辑。
        form_class = self.get_form(request, obj)
        bound_form = form_class(request.POST, request.FILES, instance=obj)
        return self._project_form_changes_sensitive_fields(bound_form)

    def _build_otp_modal_hidden_fields(self, request):
        hidden_fields = []
        for key, values in request.POST.lists():
            if key in {"csrfmiddlewaretoken", "token", "_otp_modal_submit"}:
                continue
            hidden_fields.extend({"name": key, "value": value} for value in values)
        return hidden_fields

    def _render_project_changeform_with_otp_modal(
        self,
        request,
        object_id,
        form_url="",
        extra_context=None,
        *,
        form: OTPVerifyForm,
    ):
        obj = self.get_object(request, object_id)
        if obj is None:
            return self._get_obj_does_not_exist_redirect(request, self.opts, object_id)

        fieldsets = self.get_fieldsets(request, obj)
        model_form_class = self.get_form(
            request,
            obj,
            change=True,
            fields=flatten_fieldsets(fieldsets),
        )
        bound_form = model_form_class(request.POST, request.FILES, instance=obj)
        formsets, inline_instances = self._create_formsets(
            request, bound_form.instance, change=True
        )
        bound_form.is_valid()
        all_valid(formsets)

        readonly_fields = self.get_readonly_fields(request, obj)
        admin_form = helpers.AdminForm(
            bound_form,
            list(fieldsets),
            self.get_prepopulated_fields(request, obj),
            readonly_fields,
            model_admin=self,
        )
        media = self.media + admin_form.media
        inline_formsets = self.get_inline_formsets(
            request, formsets, inline_instances, obj
        )
        for inline_formset in inline_formsets:
            media += inline_formset.media

        modal_context = {
            **self.admin_site.each_context(request),
            "title": _("Change %s") % self.opts.verbose_name,
            "subtitle": str(obj),
            "adminform": admin_form,
            "object_id": object_id,
            "original": obj,
            "is_popup": False,
            "to_field": None,
            "media": media,
            "inline_admin_formsets": inline_formsets,
            "errors": helpers.AdminErrorList(bound_form, formsets),
            "preserved_filters": self.get_preserved_filters(request),
            "otp_modal_open": True,
            "otp_verify_form": form,
            "otp_modal_locked_title": _("继续保存前需要重新验证"),
            "otp_modal_locked_text": _(
                "提币风控属于高风险配置。请输入一次两步验证码后继续保存。"
            ),
            "otp_modal_submit_label": _("验证并保存"),
            "otp_modal_hidden_fields": self._build_otp_modal_hidden_fields(request),
        }
        modal_context.update(extra_context or {})
        # 这里显式复用 admin change form 渲染链路，保证项目页字段和 inline 在弹窗出现时仍保持原始填写状态。
        return self.render_change_form(
            request,
            modal_context,
            add=False,
            change=True,
            obj=obj,
            form_url=form_url,
        )

    def _handle_project_change_modal_verification(
        self, request, object_id, form_url="", extra_context=None
    ):
        pending_user = get_pending_admin_user(request)
        if pending_user is None or pending_user.pk != request.user.pk:
            raise PermissionDenied("当前会话缺少可用的两步验证上下文")

        device = get_primary_totp_device(user=pending_user, confirmed=True)
        if device is None:
            raise PermissionDenied("当前账号尚未绑定两步验证设备")

        form = OTPVerifyForm(request.POST)
        if not form.is_valid():
            return self._render_project_changeform_with_otp_modal(
                request,
                object_id,
                form_url,
                extra_context,
                form=form,
            )
        if not verify_otp_token(device, form.cleaned_data["token"]):
            record_admin_access(
                request=request,
                action=AdminAccessLog.Action.OTP_VERIFY,
                result=AdminAccessLog.Result.FAILED,
                user=pending_user,
                reason="project_change_modal_invalid_token",
            )
            form.add_error("token", _("两步验证码无效，请检查设备时间或重新输入。"))
            return self._render_project_changeform_with_otp_modal(
                request,
                object_id,
                form_url,
                extra_context,
                form=form,
            )

        record_admin_access(
            request=request,
            action=AdminAccessLog.Action.OTP_VERIFY,
            result=AdminAccessLog.Result.SUCCEEDED,
            user=pending_user,
            reason="project_change_modal_verified",
        )
        refresh_admin_otp_verification(request, user=pending_user, device=device)
        request._project_sensitive_action_context = (
            get_fresh_admin_sensitive_action_context(
                request=request, source="admin_project_change"
            )
        )
        return super().changeform_view(request, object_id, form_url, extra_context)

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        if db_field.name == "hmac_key":
            kwargs["widget"] = ProjectHmacKeyWidget()
        return super().formfield_for_dbfield(db_field, request, **kwargs)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        if request.method == "POST" and object_id:
            project = self.get_object(request, object_id)
            if project is not None and self._project_post_changes_sensitive_fields(
                request, project
            ):
                if request.POST.get("_otp_modal_submit") == "1":
                    return self._handle_project_change_modal_verification(
                        request,
                        object_id,
                        form_url,
                        extra_context,
                    )
                try:
                    # 只有资金风控字段变化时才在入口处前置 OTP，保持修改体验与风险等级匹配。
                    self._require_fresh_project_change_otp(request)
                except AdminOTPRequiredError:
                    set_pending_admin_otp(
                        request,
                        user=request.user,
                        next_path=request.get_full_path(),
                    )
                    return self._render_project_changeform_with_otp_modal(
                        request,
                        object_id,
                        form_url,
                        extra_context,
                        form=OTPVerifyForm(),
                    )
        return super().changeform_view(request, object_id, form_url, extra_context)

    def save_model(self, request, obj, form, change):
        if change and self._project_form_changes_sensitive_fields(form):
            # changelist_editable 等入口不会经过 changeform_view，save_model 这里仍需保留风控字段兜底校验。
            try:
                self._require_fresh_project_change_otp(request)
            except AdminOTPRequiredError as exc:
                raise PermissionDenied(str(exc)) from exc
        super().save_model(request, obj, form, change)

    def get_form(self, request, obj=None, **kwargs):
        form_class = super().get_form(request, obj, **kwargs)

        class RequestForm(form_class):
            def __init__(self, *args, **kwargs):
                kwargs["user"] = request.user
                super().__init__(*args, **kwargs)

        return RequestForm

    def get_inline_instances(self, request, obj=None):
        if obj is None:
            return []
        return super().get_inline_instances(request, obj=obj)

    def save_related(self, request, form, formsets, change):
        # 收币地址 inline 需在保存前注入用途标记位；重写 save_related 以便同时访问 formsets 和 inline 实例。
        form.save_m2m()
        inline_instances = self.get_inline_instances(request, form.instance)
        for formset, inline in zip(formsets, inline_instances, strict=False):
            usage = getattr(inline, "usage", None)
            if usage:
                instances = formset.save(commit=False)
                for instance in instances:
                    instance.usage = usage
                    instance.save()
                for obj in formset.deleted_objects:
                    obj.delete()
                formset.save_m2m()
            elif formset.model is ProjectTelegramAlertConfig:
                instances = formset.save(commit=False)
                for instance in instances:
                    instance.project = form.instance
                    if instance.pk:
                        instance.updated_by = request.user
                    else:
                        instance.created_by = request.user
                        instance.updated_by = request.user
                    instance.save()
                formset.save_m2m()
            else:
                formset.save()

    def get_readonly_fields(self, request, obj=None):
        if obj:  # 修改项目
            return (
                "wallet",
                "appid",
                "failed_count",
                "display_vault_addresses",
                "display_ready_detail",
            )
        # 新建项目
        return (
            "wallet",
            "appid",
        )

    def get_fieldsets(self, request, obj=None):
        if obj is None:
            return self.add_fieldsets
        return self.edit_fieldsets

    add_fieldsets = (
        (
            _("基本信息"),
            {
                "fields": (
                    "name",
                    "webhook",
                ),
            },
        ),
        (
            _("自动归集"),
            {
                "fields": (
                    "gather_worth",
                    "gather_period",
                ),
            },
        ),
        ("安全", {"fields": ("ip_white_list",)}),
    )
    edit_fieldsets = (
        (
            _("项目状态"),
            {
                "classes": ("wide",),
                "fields": ("display_ready_detail",),
            },
        ),
        (
            _("基本信息"),
            {
                "fields": (
                    "name",
                    "appid",
                    "wallet",
                    "fast_confirm_threshold",
                ),
            },
        ),
        (
            _("自动归集"),
            {
                "fields": (
                    "gather_worth",
                    "gather_period",
                ),
            },
        ),
        (
            _("金库"),
            {
                "fields": ("display_vault_addresses",),
            },
        ),
        (
            _("安全"),
            {
                "fields": (
                    "hmac_key",
                    "ip_white_list",
                ),
            },
        ),
        (
            _("通知"),
            {
                "fields": (
                    "webhook",
                    "failed_count",
                    "webhook_open",
                ),
            },
        ),
        (
            _("提币风控"),
            {
                "fields": (
                    "withdrawal_review_required",
                    "withdrawal_review_exempt_limit",
                    "withdrawal_single_limit",
                    "withdrawal_daily_limit",
                ),
            },
        ),
    )

    def has_delete_permission(self, request, obj=None):
        return False  # 禁止删除

    @display(
        description=_("就绪"),
        label={
            "已就绪": "success",
            "未就绪": "danger",
        },
    )
    def display_ready_status(self, instance: Project):
        ready, _ = instance.is_ready
        return "已就绪" if ready else "未就绪"

    @display(description=_("提币风控"))
    def display_withdrawal_policy(self, instance: Project):
        # 管理端列表直接展示当前项目提币策略摘要，避免进入详情页后才知道审核和限额设置。
        review = _("审核") if instance.withdrawal_review_required else _("直发")
        exempt_limit = instance.withdrawal_review_exempt_limit or "-"
        single_limit = instance.withdrawal_single_limit or "-"
        daily_limit = instance.withdrawal_daily_limit or "-"
        return (
            f"{review} / 免审:{exempt_limit} / 单笔:{single_limit} / 单日:{daily_limit}"
        )

    @display(description=_("金库地址"))
    def display_vault_addresses(self, instance: Project):
        rows = []
        for chain_type, chain_label in ChainType.choices:
            if chain_type != ChainType.EVM:
                continue
            chain_names = list(
                Chain.objects.filter(type=chain_type).values_list("name", flat=True)
            )
            try:
                vault_address = instance.wallet.get_address(
                    chain_type=chain_type,
                    usage=AddressUsage.Vault,
                )
                rows.append(
                    (
                        chain_label,
                        " / ".join(chain_names) or "-",
                        vault_address.address,
                    )
                )
            except RuntimeError:
                rows.append(
                    (
                        chain_label,
                        " / ".join(chain_names) or "-",
                        "-",
                    )
                )

        body = format_html_join(
            "",
            (
                "<tr>"
                '<td class="px-3 py-2 font-medium">{}</td>'
                '<td class="px-3 py-2">{}</td>'
                '<td class="px-3 py-2 font-mono break-all">{}</td>'
                "</tr>"
            ),
            rows,
        )
        return format_html(
            '<div class="overflow-auto">'
            '<table class="min-w-full divide-y divide-base-200 text-sm">'
            "<thead>"
            "<tr>"
            '<th class="px-3 py-2 text-left">{}</th>'
            '<th class="px-3 py-2 text-left">{}</th>'
            '<th class="px-3 py-2 text-left">{}</th>'
            "</tr>"
            "</thead>"
            "<tbody>{}</tbody>"
            "</table>"
            "</div>",
            _("地址格式"),
            _("适用链"),
            _("金库地址"),
            body,
        )

    @display(description=_("项目状态"))
    def display_ready_detail(self, instance: Project):
        ready, errors = instance.is_ready
        if ready:
            return format_html(
                '<div class="flex items-center gap-2 py-2">'
                '<span class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-green-100 dark:bg-green-900/30">'
                '<span class="material-symbols-outlined text-green-600 dark:text-green-400" style="font-size:16px">check_circle</span>'
                "</span>"
                '<span class="text-green-600 dark:text-green-400 font-semibold text-base">{}</span>'
                "</div>",
                _("所有检查项已通过，项目可正常运行"),
            )
        items = format_html_join(
            "",
            '<li class="flex items-center gap-2 py-1">'
            '<span class="material-symbols-outlined text-red-500 dark:text-red-400" style="font-size:16px">cancel</span>'
            "<span>{}</span>"
            "</li>",
            ((e,) for e in errors),
        )
        return format_html(
            '<div class="py-2">'
            '<div class="flex items-center gap-2 mb-2">'
            '<span class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-red-100 dark:bg-red-900/30">'
            '<span class="material-symbols-outlined text-red-500 dark:text-red-400" style="font-size:16px">error</span>'
            "</span>"
            '<span class="text-red-600 dark:text-red-400 font-semibold text-base">{}</span>'
            "</div>"
            '<ul class="ml-8 space-y-0.5 text-sm text-red-600 dark:text-red-400">{}</ul>'
            "</div>",
            _("项目未就绪，请处理以下问题"),
            items,
        )
