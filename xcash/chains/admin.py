from django import forms
from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from unfold.decorators import display

from chains.models import Address
from chains.models import Chain
from chains.models import OnchainTransfer
from chains.models import Wallet
from common.admin import ModelAdmin
from common.admin import ReadOnlyModelAdmin


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

    fieldsets = (
        (
            _("基本信息"),
            {
                "fields": (
                    "name",
                    "code",
                    "type",
                    "native_coin",
                    "rpc",
                    "confirm_block_count",
                    "active",
                )
            },
        ),
        (
            "EVM",
            {
                "fields": (
                    "chain_id",
                    "base_transfer_gas",
                    "erc20_transfer_gas",
                )
            },
        ),
    )

    readonly_fields = ("chain_id",)


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
