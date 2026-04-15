from django.urls import include
from django.urls import path
from internal_api.viewsets.currencies import InternalChainViewSet
from internal_api.viewsets.currencies import InternalCryptoViewSet
from internal_api.viewsets.projects import ProjectViewSet
from rest_framework.routers import SimpleRouter

router = SimpleRouter(trailing_slash=False)
router.register("projects", ProjectViewSet)
router.register("currencies", InternalCryptoViewSet, basename="internal-crypto")
router.register("chains", InternalChainViewSet, basename="internal-chain")

# 嵌套在 /projects/{appid}/ 下的业务端点
from internal_api.viewsets.deposits import InternalDepositViewSet
from internal_api.viewsets.invoices import InternalInvoiceViewSet
from internal_api.viewsets.withdrawals import InternalWithdrawalViewSet

project_router = SimpleRouter(trailing_slash=False)
project_router.register("invoices", InternalInvoiceViewSet, basename="internal-invoice")
project_router.register("deposits", InternalDepositViewSet, basename="internal-deposit")
project_router.register(
    "withdrawals", InternalWithdrawalViewSet, basename="internal-withdrawal"
)

app_name = "internal_api"
urlpatterns = [
    *router.urls,
    path(
        "projects/<str:project_appid>/",
        include(project_router.urls),
    ),
]
