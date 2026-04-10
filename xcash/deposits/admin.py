from django.contrib import admin
from django.utils import timezone
from unfold.decorators import display

from common.admin import ReadOnlyModelAdmin
from core.monitoring import OperationalRiskService
from deposits.models import Deposit
from deposits.models import DepositAddress
from deposits.models import DepositCollection
from deposits.models import GasRecharge


class DepositCollectionStateFilter(admin.SimpleListFilter):
    title = "归集状态"
    parameter_name = "collection_state"

    def lookups(self, request, model_admin):
        return (
            ("uncollected", "未归集"),
            ("collecting", "归集中"),
            ("collected", "已归集"),
            ("stalled", "归集超时"),
        )

    def queryset(self, request, queryset):
        value = self.value()
        if value == "uncollected":
            return queryset.filter(collection__isnull=True)
        if value == "collecting":
            return queryset.filter(
                collection__isnull=False, collection__collected_at__isnull=True
            )
        if value == "collected":
            return queryset.filter(collection__collected_at__isnull=False)
        if value == "stalled":
            return queryset.filter(
                collection__isnull=False,
                collection__collected_at__isnull=True,
                collection__updated_at__lte=timezone.now()
                - OperationalRiskService.deposit_collection_timeout(),
            )
        return queryset


@admin.register(Deposit)
class DepositAdmin(ReadOnlyModelAdmin):
    list_display = (
        "sys_no",
        "display_project",
        "customer",
        "display_chain",
        "display_crypto",
        "display_amount",
        "display_status",
        "display_collection_state",
        "display_attention",
        "created_at",
    )
    search_fields = (
        "sys_no",
        "customer__uid",
        "transfer__hash",
        "collection__collection_hash",
    )
    list_filter = (
        "status",
        "transfer__crypto",
        "transfer__chain",
        DepositCollectionStateFilter,
    )
    readonly_fields = (
        "sys_no",
        "customer",
        "transfer",
        "worth",
        "status",
        "collection",
        "created_at",
        "updated_at",
    )
    fieldsets = (
        (
            "基本信息",
            {
                "fields": (
                    "sys_no",
                    "customer",
                    "transfer",
                    "worth",
                    "status",
                )
            },
        ),
        (
            "归集信息",
            {"fields": ("collection",)},
        ),
        (
            "时间",
            {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    @display(
        description="状态",
        label={
            "确认中": "info",
            "已完成": "success",
        },
    )
    def display_status(self, instance: Deposit):
        return instance.get_status_display()

    @display(description="项目")
    def display_project(self, instance: Deposit):
        return instance.customer.project

    @display(description="链")
    def display_chain(self, instance: Deposit):
        return instance.transfer.chain.code

    @display(description="币种")
    def display_crypto(self, instance: Deposit):
        return instance.transfer.crypto.symbol

    @display(description="数量")
    def display_amount(self, instance: Deposit):
        return instance.transfer.amount

    @display(
        description="归集",
        label={
            "未归集": "warning",
            "归集中": "info",
            "已归集": "success",
        },
    )
    def display_collection_state(self, instance: Deposit):
        if not instance.collection_id:
            return "未归集"
        if instance.collection.collected_at:
            return "已归集"
        return "归集中"

    @display(
        description="巡检",
        label={
            "正常": "success",
            "超时": "danger",
        },
    )
    def display_attention(self, instance: Deposit):
        if self._is_collection_stalled(instance):
            return "超时"
        return "正常"

    @staticmethod
    def _is_collection_stalled(instance: Deposit) -> bool:
        """判断归集是否超时：已关联 collection 但未确认且超过超时阈值。"""
        if not instance.collection_id or instance.collection.collected_at:
            return False
        timeout = OperationalRiskService.deposit_collection_timeout()
        return instance.collection.updated_at <= timezone.now() - timeout

    def get_queryset(self, request):
        # 预加载关联对象，避免 list_display 各列逐行 N+1 查询
        return (
            super()
            .get_queryset(request)
            .select_related(
                "collection",
                "customer__project",
                "transfer__chain",
                "transfer__crypto",
            )
        )


class DepositInline(admin.TabularInline):
    model = Deposit
    extra = 0
    can_delete = False
    verbose_name = "充币"
    verbose_name_plural = "充币"
    fields = (
        "sys_no",
        "customer",
        "display_chain",
        "display_crypto",
        "display_amount",
        "status",
        "created_at",
    )
    readonly_fields = fields

    def has_add_permission(self, request, obj=None):
        return False

    @display(description="链")
    def display_chain(self, instance: Deposit):
        return instance.transfer.chain.code

    @display(description="币种")
    def display_crypto(self, instance: Deposit):
        return instance.transfer.crypto.symbol

    @display(description="数量")
    def display_amount(self, instance: Deposit):
        return instance.transfer.amount


@admin.register(DepositCollection)
class DepositCollectionAdmin(ReadOnlyModelAdmin):
    list_display = (
        "collection_hash",
        "transfer",
        "collected_at",
        "created_at",
        "updated_at",
    )
    search_fields = ("collection_hash",)
    readonly_fields = (
        "collection_hash",
        "transfer",
        "broadcast_task",
        "collected_at",
        "created_at",
        "updated_at",
    )
    inlines = (DepositInline,)

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .select_related("transfer", "broadcast_task")
            .prefetch_related(
                "deposits__transfer__chain",
                "deposits__transfer__crypto",
                "deposits__customer",
            )
        )


@admin.register(GasRecharge)
class GasRechargeAdmin(ReadOnlyModelAdmin):
    list_display = (
        "id",
        "display_customer",
        "display_chain",
        "display_status",
        "recharged_at",
        "created_at",
    )
    list_filter = ("deposit_address__chain_type",)
    readonly_fields = (
        "deposit_address",
        "broadcast_task",
        "transfer",
        "recharged_at",
        "created_at",
        "updated_at",
    )

    @display(description="客户")
    def display_customer(self, instance: GasRecharge):
        return instance.deposit_address.customer.uid

    @display(description="链类型")
    def display_chain(self, instance: GasRecharge):
        return instance.deposit_address.chain_type

    @display(
        description="状态",
        label={
            "待上链": "warning",
            "已到账": "success",
        },
    )
    def display_status(self, instance: GasRecharge):
        if instance.recharged_at:
            return "已到账"
        return "待上链"

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .select_related(
                "deposit_address__customer",
                "broadcast_task",
                "transfer",
            )
        )


@admin.register(DepositAddress)
class DepositAddressAdmin(ReadOnlyModelAdmin):
    list_display = (
        "uid",
        "address",
    )
    search_fields = ("address__address", "customer__uid")
    list_display_links = None

    @display(description="UID", label=True)
    def uid(self, instance: DepositAddress):
        return instance.customer.uid
