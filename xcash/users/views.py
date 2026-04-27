from django.contrib import admin
from django.contrib import messages
from django.contrib.auth import authenticate
from django.http import Http404
from django.shortcuts import redirect
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils.translation import gettext_lazy as _
from django.views.generic import FormView
from django.views.generic import View


def _safe_next_path(request) -> str:
    """从请求中提取 next 参数，仅允许同站相对路径，防止 Open Redirect。"""
    next_path = request.POST.get("next") or request.GET.get("next") or "/"
    if url_has_allowed_host_and_scheme(next_path, allowed_hosts=None):
        return next_path
    return "/"


from .forms import LoginForm
from .forms import OTPSetupForm
from .forms import OTPVerifyForm
from .models import AdminAccessLog
from .models import User
from .otp import build_totp_qr_data_url
from .otp import complete_admin_otp_login
from .otp import get_or_create_pending_totp_device
from .otp import get_pending_admin_next_path
from .otp import get_pending_admin_user
from .otp import get_primary_totp_device
from .otp import get_totp_secret
from .otp import record_admin_access
from .otp import set_pending_admin_otp
from .otp import verify_otp_token


class AdminContextMixin:
    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)  # noqa
        ctx.update(admin.site.each_context(self.request))  # noqa
        # 认证模板统一依赖 app_path，显式补齐后可复用现有登录页结构。
        ctx["app_path"] = self.request.get_full_path()
        return ctx

    def dispatch(self, request, *args, **kwargs):
        # 已完成 OTP 的后台用户无需再次走登录/绑定页面。
        if (
            request.user.is_authenticated
            and request.user.is_staff
            and request.user.is_verified()
            and request.path != "/logout/"
        ):
            return redirect("/")
        return super().dispatch(request, *args, **kwargs)


class LoginView(AdminContextMixin, FormView):
    form_class = LoginForm
    template_name = "auth/login.html"
    success_url = "/"

    def form_valid(self, form):
        username = form.cleaned_data["username"]
        password = form.cleaned_data["password"]

        user: User | None = authenticate(
            self.request, username=username, password=password
        )
        if user is not None:
            next_path = _safe_next_path(self.request)
            set_pending_admin_otp(self.request, user=user, next_path=next_path)
            record_admin_access(
                request=self.request,
                action=AdminAccessLog.Action.PASSWORD_LOGIN,
                result=AdminAccessLog.Result.SUCCEEDED,
                user=user,
                reason="password_ok",
            )
            if get_primary_totp_device(user=user, confirmed=True) is not None:
                return redirect("users:otp_verify")
            return redirect("users:otp_setup")
        else:
            record_admin_access(
                request=self.request,
                action=AdminAccessLog.Action.PASSWORD_LOGIN,
                result=AdminAccessLog.Result.FAILED,
                reason="invalid_credentials",
            )
            form.add_error(None, _("用户名或密码错误。"))
            return self.form_invalid(form)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["title"] = _("登录")
        # 登录成功后的目标地址需要显式回传到表单，避免 OTP 完成后丢失跳转上下文。
        context["next"] = _safe_next_path(self.request)
        return context


class OTPContextMixin(AdminContextMixin):
    pending_user: User | None = None

    def prepare_otp_state(self):
        return None

    def dispatch(self, request, *args, **kwargs):
        self.pending_user = get_pending_admin_user(request)
        if self.pending_user is None:
            messages.error(request, _("请先完成用户名和密码登录。"))
            return redirect("users:login")
        prepared_response = self.prepare_otp_state()
        if prepared_response is not None:
            return prepared_response
        return super().dispatch(request, *args, **kwargs)

    def get_success_url(self):
        return get_pending_admin_next_path(self.request)


class OTPSetupView(OTPContextMixin, FormView):
    form_class = OTPSetupForm
    template_name = "auth/otp_setup.html"

    def prepare_otp_state(self):
        if get_primary_totp_device(user=self.pending_user, confirmed=True) is not None:
            return redirect("users:otp_verify")
        self.device = get_or_create_pending_totp_device(user=self.pending_user)
        return None

    def form_valid(self, form):
        if form.cleaned_data.get("device_name"):
            self.device.name = form.cleaned_data["device_name"]
            self.device.save(update_fields=["name"])
        if not verify_otp_token(self.device, form.cleaned_data["token"]):
            record_admin_access(
                request=self.request,
                action=AdminAccessLog.Action.OTP_SETUP,
                result=AdminAccessLog.Result.FAILED,
                user=self.pending_user,
                reason="invalid_token",
            )
            form.add_error("token", _("两步验证码无效，请检查设备时间或重新输入。"))
            return self.form_invalid(form)

        self.device.confirmed = True
        self.device.save(update_fields=["confirmed"])
        record_admin_access(
            request=self.request,
            action=AdminAccessLog.Action.OTP_SETUP,
            result=AdminAccessLog.Result.SUCCEEDED,
            user=self.pending_user,
            reason="device_confirmed",
        )
        return complete_admin_otp_login(
            self.request,
            user=self.pending_user,
            device=self.device,
        )

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["title"] = _("绑定两步验证")
        context["otp_secret"] = get_totp_secret(device=self.device)
        # 绑定页优先展示本地生成的二维码，手动密钥只作为扫码失败时的备用方案。
        context["otp_qr_data_url"] = build_totp_qr_data_url(
            config_url=self.device.config_url
        )
        context["next"] = self.get_success_url()
        return context


class OTPVerifyView(OTPContextMixin, FormView):
    form_class = OTPVerifyForm
    template_name = "auth/otp_verify.html"

    def prepare_otp_state(self):
        self.device = get_primary_totp_device(user=self.pending_user, confirmed=True)
        if self.device is None:
            return redirect("users:otp_setup")
        return None

    def form_valid(self, form):
        if not verify_otp_token(self.device, form.cleaned_data["token"]):
            record_admin_access(
                request=self.request,
                action=AdminAccessLog.Action.OTP_VERIFY,
                result=AdminAccessLog.Result.FAILED,
                user=self.pending_user,
                reason="invalid_token",
            )
            form.add_error("token", _("两步验证码无效，请检查设备时间或重新输入。"))
            return self.form_invalid(form)

        record_admin_access(
            request=self.request,
            action=AdminAccessLog.Action.OTP_VERIFY,
            result=AdminAccessLog.Result.SUCCEEDED,
            user=self.pending_user,
            reason="otp_verified",
        )
        return complete_admin_otp_login(
            self.request,
            user=self.pending_user,
            device=self.device,
        )

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["title"] = _("验证两步验证码")
        context["next"] = self.get_success_url()
        return context


class SignupDisabledView(View):
    def dispatch(self, request, *args, **kwargs):
        # 注册能力已下线，显式返回 404 可避免请求继续落到 admin 默认路由。
        raise Http404(_("注册页面不存在。"))
