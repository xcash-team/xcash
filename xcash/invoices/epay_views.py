import structlog
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import redirect
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django_smart_ratelimit import rate_limit

from .epay_service import EpaySubmitError
from .epay_service import EpaySubmitService

logger = structlog.get_logger()


def _epay_submit_rate(_group, _request):
    # 把 rate 包成 callable，让测试可以通过 override_settings(EPAY_SUBMIT_RATE_LIMIT=...) 调阈值。
    return getattr(settings, "EPAY_SUBMIT_RATE_LIMIT", "60/m")


# /epay/submit.php 是公开匿名入口，攻击者用有效 pid + 错误 sign 反复打就会持续触发
# EpayMerchant.objects.get + serializer 校验 + 签名计算，对 DB 形成查询型 DoS。
# 这里按 IP 维度做 60/min 软限流，超限直接 429（block=True），不阻断正常商户。
@method_decorator(csrf_exempt, name="dispatch")
@method_decorator(
    rate_limit(key="ip", rate=_epay_submit_rate, block=True),
    name="dispatch",
)
class EpaySubmitView(View):
    http_method_names = ["get", "post"]

    def get(self, request):
        return self._submit(request, request.GET)

    def post(self, request):
        return self._submit(request, request.POST)

    def _submit(self, request, params):
        try:
            invoice = EpaySubmitService.submit(params)
        except EpaySubmitError as exc:
            # 对外统一回 "fail"，避免商户/攻击者根据错误细节区分
            # "pid 存在但签名错"与"pid 不存在"，造成 pid 枚举或验证规则泄漏。
            # 详情通过 structlog 写到服务端日志，便于排查。
            logger.warning(
                "epay submit rejected",
                pid=params.get("pid", ""),
                client_ip=request.META.get("REMOTE_ADDR", ""),
                error=str(exc),
            )
            return HttpResponse(
                "fail",
                status=400,
                content_type="text/plain; charset=utf-8",
            )

        return redirect("payment-invoice", sys_no=invoice.sys_no)
