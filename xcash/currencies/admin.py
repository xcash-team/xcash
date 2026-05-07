from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from unfold.admin import TabularInline
from unfold.decorators import display

from common.admin import ModelAdmin
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.models import Fiat


@admin.action(description="同步并删除占位符代币")
def merge_placeholder_crypto(modeladmin, request, queryset):
    """将占位符 Crypto 原子性地合并到选中的真实代币。

    使用约定：同时勾选一个 active=True 的目标代币和一或多个 active=False 的占位符，
    action 自动从选中项中识别目标，无需额外字段或中间表单。

    在单个事务内原子完成（无窗口期，防止 Balance 漏更新）：
    1. 占位符的所有 ChainToken 改指向目标代币
    2. 占位符关联的全部 OnchainTransfer.crypto（含 CONFIRMING 状态）更新为目标代币
    3. 删除占位符
    """
    from django.contrib import messages
    from django.db import transaction

    from chains.models import OnchainTransfer

    targets = queryset.filter(active=True)
    placeholders = queryset.filter(active=False)

    if targets.count() != 1:
        modeladmin.message_user(
            request,
            "请同时选中恰好一个已激活的目标代币和若干占位符",
            level=messages.WARNING,
        )
        return
    if not placeholders.exists():
        modeladmin.message_user(
            request, "未选中任何占位符（active=False）", level=messages.WARNING
        )
        return

    target = targets.first()
    merged_count = 0

    for placeholder in placeholders:
        try:
            with transaction.atomic():
                # 步骤 1：ChainToken 改指向目标代币（与步骤 2 同一事务，无窗口期）
                for ct in ChainToken.objects.filter(crypto=placeholder):
                    if ChainToken.objects.filter(
                        crypto=target, chain=ct.chain
                    ).exists():
                        modeladmin.message_user(
                            request,
                            f"{placeholder.symbol}：目标代币在 {ct.chain.code} 上已有部署记录，跳过该链",
                            level=messages.WARNING,
                        )
                        continue
                    ct.crypto = target
                    # 占位符合并只改写 crypto 外键，不依赖 save() 信号，直接 update 更收敛。
                    ChainToken.objects.filter(pk=ct.pk).update(crypto=target)

                # 步骤 2：全量更新 Transfer（含 CONFIRMING），确保 Balance 能正确关联
                OnchainTransfer.objects.filter(crypto=placeholder).update(crypto=target)

                # 步骤 3：删除占位符
                placeholder.delete()

            merged_count += 1
        except Exception as e:
            modeladmin.message_user(
                request, f"{placeholder.symbol} 合并失败：{e}", level=messages.ERROR
            )

    if merged_count:
        modeladmin.message_user(
            request,
            f"成功将 {merged_count} 个占位符合并到 {target.symbol}",
            level=messages.SUCCESS,
        )


class ChainTokenInline(TabularInline):
    model = ChainToken
    extra = 0
    verbose_name = _("链上部署")
    verbose_name_plural = _("链上部署")
    fields = ("chain", "address", "decimals")


@admin.register(Crypto)
class CryptoAdmin(ModelAdmin):
    inlines = (ChainTokenInline,)

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return "symbol", "decimals", "prices"
        return ()

    list_display = (
        "name",
        "symbol",
        "supported_chains",
        "display_type",
        "decimals",
        "active",
    )
    list_filter = ("active",)
    actions = [merge_placeholder_crypto]

    @display(
        description="类型",
        label={
            "原生币": "warning",
            "代币": "info",
        },
    )
    def display_type(self, instance: Crypto):
        if instance.is_native:
            return "原生币"
        return "代币"


@admin.register(Fiat)
class FiatAdmin(ModelAdmin):
    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ("code",)
        return ()

    list_display = (
        "code",
        "icon",
    )
