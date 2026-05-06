from django import forms
from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from unfold.decorators import display

from chains.models import Address
from chains.models import BroadcastTask
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import Wallet
from common.admin import ModelAdmin
from common.admin import ReadOnlyModelAdmin
from common.utils.math import format_decimal_stripped


# Register your models here.


class ChainAdminForm(forms.ModelForm):
    class Meta:
        model = Chain
        fields = "__all__"  # noqa: DJ007


@admin.register(Chain)
class ChainAdmin(ModelAdmin):
    form = ChainAdminForm
    list_display = (
        "name",
        "type",
        "native_coin",
        "latest_block_number",
    )

    base_fieldsets = (
        (
            _("基本信息"),
            {
                "fields": (
                    "name",
                    "code",
                    "type",
                    "native_coin",
                    "confirm_block_count",
                    "active",
                )
            },
        ),
    )
    evm_fieldsets = (
        (
            "EVM",
            {
                "fields": (
                    "rpc",
                    "chain_id",
                    "base_transfer_gas",
                    "erc20_transfer_gas",
                    "evm_log_max_block_range",
                )
            },
        ),
    )
    bitcoin_fieldsets = (
        (
            "Bitcoin",
            {
                "fields": (
                    "rpc",
                )
            },
        ),
    )
    tron_fieldsets = (
        (
            "Tron",
            {
                "fields": (
                    "tron_api_key",
                )
            },
        ),
    )

    readonly_fields = ("chain_id",)

    def get_fieldsets(self, request, obj=None):
        if obj is None:
            return (
                *self.base_fieldsets,
                *self.evm_fieldsets,
                *self.tron_fieldsets,
            )
        if obj.type == ChainType.EVM:
            return (*self.base_fieldsets, *self.evm_fieldsets)
        if obj.type == ChainType.BITCOIN:
            return (*self.base_fieldsets, *self.bitcoin_fieldsets)
        if obj.type == ChainType.TRON:
            return (*self.base_fieldsets, *self.tron_fieldsets)
        return self.base_fieldsets


@admin.register(Wallet)
class WalletAdmin(ReadOnlyModelAdmin):
    list_display = ("__str__",)


@admin.register(Address)
class AddressAdmin(ReadOnlyModelAdmin):
    list_display = (
        "address",
        "usage",
    )
    readonly_fields = (
        "address",
        "wallet",
        "usage",
        "chain_type",
        "bip44_account",
        "address_index",
    )


@admin.register(OnchainTransfer)
class TransferAdmin(ReadOnlyModelAdmin):
    search_fields = ("hash",)
    readonly_fields = ("display_crypto", "display_chain")

    list_display = (
        "from_address",
        "to_address",
        "chain",
        "crypto",
        "amount",
        "datetime",
        "type",
        "display_status",
    )

    fields = (
        "from_address",
        "to_address",
        "display_chain",
        "display_crypto",
        "value",
        "amount",
        "block",
        "hash",
        "datetime",
        "timestamp",
        "type",
    )

    @display(description=_("加密货币"))  # noqa
    def display_crypto(self, obj: OnchainTransfer):
        return obj.crypto.symbol

    @display(description=_("链"))  # noqa
    def display_chain(self, obj: OnchainTransfer):
        return obj.chain.name

    @display(
        description="状态",
        label={
            "确认中": "info",
            "已确认": "success",
            "已失效": "",
        },
    )
    def display_status(self, instance: OnchainTransfer):
        return instance.get_status_display()


@admin.register(BroadcastTask)
class BroadcastTaskAdmin(ReadOnlyModelAdmin):
    # BroadcastTask 是跨链统一锚点；后台只做观察与排障，禁止人工修改，避免破坏 stage/result/failure_reason 三元一致约束。
    ordering = ("-created_at",)
    list_display = (
        "display_address",
        "display_chain",
        "display_transfer_type",
        "display_crypto",
        "display_recipient",
        "display_amount",
        "display_tx_hash",
        "display_status",
        "display_failure_reason",
        "created_at",
    )
    list_filter = ("stage", "result", "transfer_type", "chain")
    list_select_related = ("address", "chain", "crypto")
    search_fields = ("tx_hash", "address__address", "recipient")

    @admin.display(ordering="address__address", description=_("地址"))
    def display_address(self, obj: BroadcastTask):
        return obj.address

    @admin.display(ordering="chain__name", description=_("网络"))
    def display_chain(self, obj: BroadcastTask):
        return obj.chain

    @admin.display(ordering="transfer_type", description=_("类型"))
    def display_transfer_type(self, obj: BroadcastTask):
        return obj.get_transfer_type_display()

    @admin.display(ordering="crypto__symbol", description=_("代币"))
    def display_crypto(self, obj: BroadcastTask):
        return obj.crypto or "—"

    @admin.display(ordering="recipient", description=_("收款地址"))
    def display_recipient(self, obj: BroadcastTask):
        return obj.recipient or "—"

    @admin.display(description=_("数量"))
    def display_amount(self, obj: BroadcastTask):
        if obj.amount is None:
            return "—"
        return format_decimal_stripped(obj.amount)

    @admin.display(ordering="tx_hash", description=_("交易哈希"))
    def display_tx_hash(self, obj: BroadcastTask):
        return obj.tx_hash or "—"

    @display(
        description=_("状态"),
        label={
            "待广播": "warning",
            "待上链": "warning",
            "确认中": "info",
            "成功": "success",
            "失败": "danger",
            "已终结": "info",
        },
    )
    def display_status(self, instance: BroadcastTask):
        # BroadcastTask.display_status 已将 stage/result 融合为面向运营的单字段语义，
        # 这里沿用同一来源避免后台与业务代码的展示口径漂移。
        return instance.display_status

    @admin.display(ordering="failure_reason", description=_("失败原因"))
    def display_failure_reason(self, obj: BroadcastTask):
        # 仅失败终态任务会有失败原因；UNKNOWN / SUCCESS 情况下返回占位符避免列视觉空白。
        return obj.get_failure_reason_display() if obj.failure_reason else "—"
