from django.urls import path
from django_smart_ratelimit import rate_limit

from .otp import get_admin_otp_ratelimit_key
from .views import LoginView
from .views import OTPSetupView
from .views import OTPVerifyView
from .views import SignupDisabledView

app_name = "users"

urlpatterns = [
    # 注册入口保留为显式 404，避免未命中 users.urls 后继续被 admin 默认路由兜底。
    path("signup", SignupDisabledView.as_view(), name="signup_disabled"),
    path(
        "login",
        rate_limit(
            key="ip",
            rate="100/h",
            skip_if=lambda req: req.method != "POST",
        )(LoginView.as_view()),
        name="login",
    ),
    path(
        "otp/setup",
        rate_limit(
            key=get_admin_otp_ratelimit_key,
            rate="30/h",
            skip_if=lambda req: req.method != "POST",
        )(OTPSetupView.as_view()),
        name="otp_setup",
    ),
    path(
        "otp/verify",
        rate_limit(
            key=get_admin_otp_ratelimit_key,
            rate="60/h",
            skip_if=lambda req: req.method != "POST",
        )(OTPVerifyView.as_view()),
        name="otp_verify",
    ),
]
