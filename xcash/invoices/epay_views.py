import structlog
from django.http import HttpResponse
from django.shortcuts import redirect
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from .epay_service import EpaySubmitError
from .epay_service import EpaySubmitService

logger = structlog.get_logger()


@method_decorator(csrf_exempt, name="dispatch")
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
