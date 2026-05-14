"""core app 仅保留后台看板、系统初始化入口与平台级运行参数。"""

from decimal import Decimal

from django.core.cache import cache
from django.core.validators import MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

PLATFORM_SETTINGS_CACHE_KEY = "core:platform_settings:singleton"


class PlatformSettings(models.Model):
    """平台级运行参数中心。

    第一性原则：只有“上线后可能调整、且不属于密钥/部署机密”的运行参数，
    才应该进入后台由超管维护。
    """

    singleton_key = models.PositiveSmallIntegerField(
        _("单例键"),
        default=1,
        unique=True,
        editable=False,
    )
    admin_session_timeout_minutes = models.PositiveIntegerField(
        _("后台会话超时(分钟)"),
        default=10,
        validators=[MinValueValidator(1)],
        help_text=_("管理员登录后台后，超过该时间无操作需重新登录。"),
    )
    admin_sensitive_action_otp_max_age_seconds = models.PositiveIntegerField(
        _("敏感动作两步验证超时(秒)"),
        default=900,
        validators=[MinValueValidator(60)],
        help_text=_(
            "超过该时间后，提币审批、Signer 运营等高风险动作需要重新验证两步验证码。"
        ),
    )
    alerts_repeat_interval_minutes = models.PositiveIntegerField(
        _("告警重复发送间隔(分钟)"),
        default=30,
        validators=[MinValueValidator(1)],
        help_text=_("同一项目同一告警在该时间窗内不重复发送。"),
    )
    open_native_scanner = models.BooleanField(
        _("开启 EVM 原生币扫描"),
        default=False,
        help_text=_(
            "全局控制 EVM 原生币直转扫描；关闭时 EVM 原生币支付、充币地址和提币入口不可用。"
        ),
    )
    webhook_delivery_breaker_threshold = models.PositiveIntegerField(
        _("Webhook 熔断阈值"),
        default=30,
        validators=[MinValueValidator(1)],
        help_text=_("连续失败达到该次数后，自动关闭项目 Webhook 投递。"),
    )
    webhook_delivery_max_retries = models.PositiveIntegerField(
        _("Webhook 最大重试次数"),
        default=5,
        validators=[MinValueValidator(1)],
        help_text=_("单个 Webhook 事件最多自动重试的次数。"),
    )
    webhook_delivery_max_backoff_seconds = models.PositiveIntegerField(
        _("Webhook 最大退避秒数"),
        default=120,
        validators=[MinValueValidator(1)],
        help_text=_("Webhook 自动重试的指数退避上限秒数。"),
    )
    reviewing_withdrawal_timeout_minutes = models.PositiveIntegerField(
        _("审核中提币超时(分钟)"),
        default=30,
        validators=[MinValueValidator(1)],
        help_text=_("审核中提币超过该时间仍未处理时，进入异常巡检。"),
    )
    pending_withdrawal_timeout_minutes = models.PositiveIntegerField(
        _("待提币超时(分钟)"),
        default=15,
        validators=[MinValueValidator(1)],
        help_text=_("已批准但仍未进入链上确认流程的提币，超过该时间后进入异常巡检。"),
    )
    confirming_withdrawal_timeout_minutes = models.PositiveIntegerField(
        _("确认中提币超时(分钟)"),
        default=30,
        validators=[MinValueValidator(1)],
        help_text=_("链上确认中提币超过该时间仍未完成时，进入异常巡检。"),
    )
    deposit_collection_timeout_minutes = models.PositiveIntegerField(
        _("归集超时(分钟)"),
        default=30,
        validators=[MinValueValidator(1)],
        help_text=_("充币归集超过该时间仍未完成时，进入异常巡检。"),
    )
    webhook_event_timeout_minutes = models.PositiveIntegerField(
        _("Webhook 堆积超时(分钟)"),
        default=15,
        validators=[MinValueValidator(1)],
        help_text=_("待投递 Webhook 事件超过该时间仍未送达时，进入异常巡检。"),
    )
    risk_marking_enabled = models.BooleanField(
        _("开启风险标记"),
        default=False,
        help_text=_("开启后对高于风险查询阈值的账单和充币查询外部地址风险。"),
    )
    risk_marking_threshold_usd = models.DecimalField(
        _("风险查询阈值(USD)"),
        max_digits=16,
        decimal_places=6,
        default=Decimal("0"),
        validators=[MinValueValidator(Decimal("0"))],
        help_text=_("账单或充币价值大于该阈值时查询风险。"),
    )
    risk_marking_cache_seconds = models.PositiveIntegerField(
        _("风险查询缓存秒数"),
        default=3600,
        validators=[MinValueValidator(1)],
        help_text=_("同一地址风险查询成功后在 Django 缓存中保留的秒数。"),
    )
    risk_marking_force_refresh_threshold_usd = models.DecimalField(
        _("风险查询强制刷新阈值(USD)"),
        max_digits=16,
        decimal_places=6,
        default=Decimal("10000"),
        validators=[MinValueValidator(Decimal("0"))],
        help_text=_("本次业务价值大于该阈值时跳过缓存直接查询。"),
    )
    quicknode_misttrack_endpoint_url = models.URLField(
        _("QuickNode MistTrack Endpoint"),
        blank=True,
        default="",
        help_text=_("QuickNode MistTrack add-on 的 JSON-RPC endpoint URL。"),
    )
    created_by = models.ForeignKey(
        "users.User",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="created_platform_settings",
        verbose_name=_("创建人"),
    )
    updated_by = models.ForeignKey(
        "users.User",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="updated_platform_settings",
        verbose_name=_("更新人"),
    )
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        verbose_name = _("平台运行参数")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return str(_("平台运行参数"))

    def save(self, *args, **kwargs):
        # 强制收口为单例记录，避免后台误建第二份配置导致读取口径分叉。
        self.singleton_key = 1
        super().save(*args, **kwargs)
        if not self.open_native_scanner:
            self._delete_evm_native_scan_cursors()
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)

    def delete(self, *args, **kwargs):
        raise RuntimeError("平台运行参数不允许删除")

    @staticmethod
    def _delete_evm_native_scan_cursors() -> None:
        """全局关闭 native 扫描时清理游标，重新开启后从新游标开始。"""
        from evm.models import EvmScanCursor
        from evm.models import EvmScanCursorType

        EvmScanCursor.objects.filter(
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        ).delete()
