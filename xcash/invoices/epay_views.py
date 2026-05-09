from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from .epay_service import EpaySubmitError
from .epay_service import EpaySubmitService


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
            return HttpResponse(
                f"fail:{exc}",
                status=400,
                content_type="text/plain; charset=utf-8",
            )

        pay_path = reverse("payment-invoice", kwargs={"sys_no": invoice.sys_no})
        return HttpResponseRedirect(request.build_absolute_uri(pay_path))
