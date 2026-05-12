# ruff: noqa
from django.conf import settings
from django.contrib import admin
from django.urls import include, reverse_lazy
from django.urls import path
from django.views.generic import RedirectView

from core.dashboard import operational_inspection_view, signer_overview_view
from invoices.epay_views import EpaySubmitView
from invoices.views import payment_view

urlpatterns = [
    path("v1/", include("config.api_v1")),
    path("epay/submit.php", EpaySubmitView.as_view(), name="epay-submit"),
    # 支付前端 SPA：返回 index.html，由 React 根据 sys_no 渲染支付页
    path("pay/<str:sys_no>", payment_view, name="payment-invoice"),
    path("i18n/", include("django.conf.urls.i18n")),
    # 自定义登录 / OTP 路由需要先于 admin.site.urls 注册，才能接管默认 /login/ 入口。
    path("", include("users.urls")),
    path(
        "operations/inspection",
        # 改动原因：为“异常巡检”提供独立后台页，避免继续复用 admin 首页。
        admin.site.admin_view(operational_inspection_view),
        name="operational-inspection",
    ),
    path(
        "signer/overview",
        admin.site.admin_view(signer_overview_view),
        name="signer-overview",
    ),
]

if settings.INTERNAL_API_TOKEN:
    urlpatterns += [path("internal/v1/", include("internal_api.urls"))]

if settings.DEBUG and "stress" in settings.INSTALLED_APPS:
    # stress webhook 必须优先于 admin catch-all 注册，否则 /stress/webhook/ 会被后台路由吞掉。
    urlpatterns += [path("stress/", include("stress.urls"))]

urlpatterns += [
    # Admin authentication URLs (需要在admin.site.urls之前)
    path("", admin.site.urls),
]

if settings.DEBUG:
    if "debug_toolbar" in settings.INSTALLED_APPS:
        import debug_toolbar

        urlpatterns = [path("__debug__/", include(debug_toolbar.urls))] + urlpatterns
