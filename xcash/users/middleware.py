from __future__ import annotations

from django.conf import settings

from users.otp import downgrade_unverified_admin_session


class AdminOTPRequiredMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.exempt_prefixes = tuple(
            prefix
            for prefix in (
                # 登录端点已统一为无尾斜杠，豁免判定使用无斜杠前缀以覆盖 /login 本身。
                "/login",
                "/otp/",
                "/logout/",
                "/i18n/",
                "/v1/",
                settings.STATIC_URL,
            )
            if prefix
        )

    def __call__(self, request):
        user = getattr(request, "user", None)
        if (
            user is not None
            and user.is_authenticated
            and user.is_staff
            and not user.is_verified()
            and not request.path.startswith(self.exempt_prefixes)
        ):
            # Admin 资金后台只接受完成 OTP 的会话；未验证会话统一降级并重定向到 OTP 步骤。
            return downgrade_unverified_admin_session(request, user=user)
        return self.get_response(request)
