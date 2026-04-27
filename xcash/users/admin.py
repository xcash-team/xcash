from django.contrib import admin
from django.contrib import messages
from django.contrib.admin.options import IS_POPUP_VAR
from django.contrib.admin.utils import unquote
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.core.exceptions import PermissionDenied
from django.http import Http404
from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import path
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.utils.html import escape
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.debug import sensitive_post_parameters
from django_otp.plugins.otp_totp.models import TOTPDevice
from unfold.decorators import display
from unfold.forms import AdminPasswordChangeForm

from common.admin import ModelAdmin
from common.admin import ReadOnlyModelAdmin
from projects.models import Project

from .forms import AdminUserOTPChangeForm
from .forms import UserAdminChangeForm
from .forms import UserAdminCreationForm
from .models import AdminAccessLog
from .models import Customer
from .models import User
from .otp import activate_pending_totp_device
from .otp import build_totp_qr_data_url
from .otp import get_or_create_pending_totp_device
from .otp import get_primary_totp_device
from .otp import get_totp_secret
from .otp import record_admin_access
from .otp import refresh_admin_otp_verification
from .otp import verify_otp_token

csrf_protect_m = method_decorator(csrf_protect)
sensitive_post_parameters_m = method_decorator(sensitive_post_parameters())


@admin.register(User)
class UserAdmin(BaseUserAdmin, ModelAdmin):
    # 用户模型已切换为 username 登录，这里同步移除已失效的 edition/balance/account 配置。
    form = UserAdminChangeForm
    add_form = UserAdminCreationForm
    change_password_form = AdminPasswordChangeForm
    change_user_otp_template = "admin/users/user/change_otp.html"
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "username",
                    "password",
                    "display_otp_management",
                )
            },
        ),
        (
            _("权限"),
            {
                "fields": (
                    "is_active",
                    "is_staff",
                    "is_superuser",
                ),
            },
        ),
        (_("重要日期"), {"fields": ("last_login", "date_joined")}),
    )
    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": ("username", "password1", "password2"),
            },
        ),
    )
    list_display = [
        "username",
        "display_otp_enabled",
        "is_superuser",
        "is_staff",
        "is_active",
    ]
    search_fields = ["username"]
    ordering = ("id",)
    readonly_fields = (*BaseUserAdmin.readonly_fields, "display_otp_management")

    @display(description=_("两步验证"))
    def display_otp_enabled(self, obj: User):
        # 直接展示是否已绑定 TOTP，便于后台排查哪些资金账号仍未完成两步验证。
        return TOTPDevice.objects.devices_for_user(obj, confirmed=True).exists()

    @admin.display(description=_("两步验证管理"))
    def display_otp_management(self, obj: User):
        confirmed_device = get_primary_totp_device(user=obj, confirmed=True)
        pending_device = get_primary_totp_device(user=obj, confirmed=False)
        if pending_device is not None:
            status_text = _("存在待确认的新设置")
            action_text = _("继续确认两步验证")
        elif confirmed_device is not None:
            status_text = _("已开启")
            action_text = _("修改两步验证")
        else:
            status_text = _("未开启")
            action_text = _("设置两步验证")

        # 在用户详情页直接放管理入口，避免管理员只能手动拼接 URL 才能进入密钥重置页面。
        return format_html(
            '{}<div class="mt-3"><a href="{}" class="inline-flex items-center rounded-default border border-base-200 px-3 py-2 text-sm font-medium">{}</a></div>',
            status_text,
            reverse(
                f"{self.admin_site.name}:{self.opts.app_label}_{self.opts.model_name}_otp_change",
                args=(obj.pk,),
            ),
            action_text,
        )

    def get_urls(self):
        return [
            path(
                "<object_id>/otp/",
                self.admin_site.admin_view(self.user_change_otp),
                name=f"{self.opts.app_label}_{self.opts.model_name}_otp_change",
            ),
            *super().get_urls(),
        ]

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)

    def _get_otp_change_user(self, request, object_id: str) -> User:
        user = self.get_object(request, unquote(object_id))
        if not self.has_change_permission(request, user):
            raise PermissionDenied
        if user is None:
            raise Http404(
                _("%(name)s object with primary key %(key)r does not exist.")
                % {
                    "name": self.opts.verbose_name,
                    "key": escape(object_id),
                }
            )
        return user

    @staticmethod
    def _validate_current_identity_for_otp_change(
        *,
        form: AdminUserOTPChangeForm,
        request,
        current_device,
    ) -> bool:
        if not request.user.check_password(form.cleaned_data["current_password"]):
            record_admin_access(
                request=request,
                action=AdminAccessLog.Action.OTP_ROTATE,
                result=AdminAccessLog.Result.FAILED,
                user=request.user,
                reason="admin_user_change_otp_invalid_password",
            )
            form.add_error("current_password", _("当前密码不正确。"))
            return False

        if current_device is not None and not verify_otp_token(
            current_device, form.cleaned_data["current_token"]
        ):
            record_admin_access(
                request=request,
                action=AdminAccessLog.Action.OTP_ROTATE,
                result=AdminAccessLog.Result.FAILED,
                user=request.user,
                reason="admin_user_change_otp_invalid_current_token",
            )
            form.add_error(
                "current_token",
                _("当前两步验证码无效，请检查设备时间或重新输入。"),
            )
            return False

        return True

    @staticmethod
    def _validate_new_otp_device(
        *,
        form: AdminUserOTPChangeForm,
        request,
        user: User,
        device,
    ) -> bool:
        if verify_otp_token(device, form.cleaned_data["token"]):
            return True

        record_admin_access(
            request=request,
            action=AdminAccessLog.Action.OTP_ROTATE,
            result=AdminAccessLog.Result.FAILED,
            user=request.user,
            reason=f"admin_user_change_otp_invalid_token:user:{user.pk}",
        )
        form.add_error("token", _("两步验证码无效，请检查设备时间或重新输入。"))
        return False

    def _render_user_change_otp_page(
        self,
        *,
        request,
        user: User,
        form: AdminUserOTPChangeForm,
        form_url: str,
        device,
        is_self_service: bool,
    ):
        context = {
            **self.admin_site.each_context(request),
            "title": _("修改两步验证: %s") % escape(user.get_username()),
            "form_url": form_url,
            "form": form,
            "is_popup": (IS_POPUP_VAR in request.POST or IS_POPUP_VAR in request.GET),
            "is_popup_var": IS_POPUP_VAR,
            "opts": self.opts,
            "original": user,
            "otp_secret": get_totp_secret(device=device),
            # 页面直接复用后台内生成的二维码，保持与登录绑定页一致，不依赖外部服务。
            "otp_qr_data_url": build_totp_qr_data_url(config_url=device.config_url),
            "require_self_password_confirmation": is_self_service,
            "require_self_old_otp_confirmation": is_self_service,
        }
        request.current_app = self.admin_site.name
        return TemplateResponse(request, self.change_user_otp_template, context)

    @sensitive_post_parameters_m
    @csrf_protect_m
    def user_change_otp(self, request, object_id, form_url=""):
        user = self._get_otp_change_user(request, object_id)
        is_self_service = request.user.pk == user.pk
        if not is_self_service and not request.user.is_superuser:
            # 代他人重置 OTP 属于账户接管级别的高风险动作，当前版本只允许 superuser 执行。
            raise PermissionDenied("只有超级管理员可以重置其他用户的两步验证")

        device = get_or_create_pending_totp_device(user=user)
        current_device = get_primary_totp_device(user=user, confirmed=True)
        if is_self_service and current_device is None:
            # 自己换绑 OTP 必须校验旧设备；若旧设备缺失，说明会话与账户状态不一致，应阻断并人工排查。
            raise PermissionDenied("当前账号缺少已绑定的两步验证设备，无法执行自助换绑")
        if request.method == "POST":
            form = AdminUserOTPChangeForm(
                request.POST,
                require_current_password=is_self_service,
                require_existing_token=is_self_service,
            )
            if form.is_valid():
                identity_valid = (
                    not is_self_service
                    or self._validate_current_identity_for_otp_change(
                        form=form,
                        request=request,
                        current_device=current_device,
                    )
                )
                if identity_valid and self._validate_new_otp_device(
                    form=form,
                    request=request,
                    user=user,
                    device=device,
                ):
                    # 新设备验证码校验通过后才替换旧密钥，避免目标用户在确认前被提前踢出 OTP 登录链路。
                    device = activate_pending_totp_device(
                        user=user,
                        device=device,
                        device_name=form.cleaned_data.get("device_name") or device.name,
                    )
                    record_admin_access(
                        request=request,
                        action=AdminAccessLog.Action.OTP_ROTATE,
                        result=AdminAccessLog.Result.SUCCEEDED,
                        user=request.user,
                        reason=f"admin_user_change_otp_confirmed:user:{user.pk}",
                    )
                    self.log_change(
                        request,
                        user,
                        _("已更新该用户的两步验证设置"),
                    )
                    if request.user.pk == user.pk:
                        # 管理员修改自己的 OTP 密钥后，当前后台会话也要切换到新设备，否则 session 会引用已删除旧设备。
                        refresh_admin_otp_verification(
                            request,
                            user=user,
                            device=device,
                        )
                    messages.success(request, _("两步验证更新成功。"))
                    return HttpResponseRedirect(
                        reverse(
                            f"{self.admin_site.name}:{self.opts.app_label}_{self.opts.model_name}_change",
                            args=(user.pk,),
                        )
                    )
        else:
            form = AdminUserOTPChangeForm(
                initial={"device_name": device.name},
                require_current_password=is_self_service,
                require_existing_token=is_self_service,
            )

        return self._render_user_change_otp_page(
            request=request,
            user=user,
            form=form,
            form_url=form_url,
            device=device,
            is_self_service=is_self_service,
        )


@admin.register(Customer)
class CustomerAdmin(ReadOnlyModelAdmin):
    list_display = ("uid", "created_at")


class ProjectListFilter(admin.SimpleListFilter):
    title = _("项目")
    parameter_name = "project"

    def lookups(self, request, model_admin):
        return tuple(
            (project.pk, project.name) for project in Project.objects.order_by("name")
        )

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(project_id=self.value())
        return queryset


@admin.register(AdminAccessLog)
class AdminAccessLogAdmin(ReadOnlyModelAdmin):
    list_display = ("created_at", "username_snapshot", "action", "result", "ip")
    list_filter = ("action", "result", "created_at")
    search_fields = ("username_snapshot", "ip", "reason")
