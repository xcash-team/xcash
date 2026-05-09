from django.db import models
from django.utils.translation import gettext_lazy as _
from shortuuid.django_fields import ShortUUIDField

# Create your models here.


class WebhookEvent(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", _("待投递")
        SUCCEEDED = "succeeded", _("已送达")
        FAILED = "failed", _("投递失败")

    class DeliveryMethod(models.TextChoices):
        POST_JSON = "post_json", _("POST JSON")
        GET_QUERY = "get_query", _("GET Query")

    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.CASCADE,
        verbose_name=_("项目"),
    )
    nonce = ShortUUIDField(
        editable=False,
        help_text=_("唯一。由于存在多次重试机制，此nonce用于幂等去重。"),
        unique=True,
    )
    payload = models.JSONField(verbose_name=_("内容"))
    delivery_url = models.URLField(_("投递地址"), blank=True, default="")
    delivery_method = models.CharField(
        _("投递方式"),
        choices=DeliveryMethod,
        default=DeliveryMethod.POST_JSON,
        max_length=16,
    )
    expected_response_body = models.CharField(
        _("成功响应内容"),
        max_length=32,
        default="ok",
    )

    status = models.CharField(
        _("状态"),
        choices=Status.choices,
        default=Status.PENDING,
    )
    schedule_locked_until = models.DateTimeField(
        verbose_name=_("下次投递"), null=True, blank=True
    )
    last_error = models.TextField(
        verbose_name=_("投递报错信息"), blank=True, default=""
    )
    delivered_at = models.DateTimeField(
        verbose_name=_("送达时间"),
        blank=True,
        null=True,
    )

    created_at = models.DateTimeField(verbose_name=_("创建时间"), auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        verbose_name = _("事件")
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.nonce

    @property
    def is_pending(self) -> bool:
        return self.status == self.Status.PENDING

    @property
    def is_succeeded(self) -> bool:
        return self.status == self.Status.SUCCEEDED

    @property
    def is_failed(self) -> bool:
        return self.status == self.Status.FAILED


class DeliveryAttempt(models.Model):
    event = models.ForeignKey(
        WebhookEvent,
        on_delete=models.CASCADE,
        related_name="attempts",
        verbose_name=_("事件"),
    )
    try_number = models.PositiveSmallIntegerField(verbose_name=_("次数"))

    # 请求与响应
    request_headers = models.JSONField()
    request_body = models.TextField()

    response_status = models.PositiveIntegerField(null=True)
    response_headers = models.JSONField(null=True)
    response_body = models.TextField(blank=True, default="")

    duration_ms = models.PositiveIntegerField(verbose_name=_("耗时(ms)"))

    # 结果
    ok = models.BooleanField(default=False, verbose_name=_("成功"))
    error = models.TextField(
        blank=True, default="", verbose_name=_("错误"), editable=False
    )

    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("创建时间"))

    class Meta:
        ordering = ("-created_at",)
        verbose_name = _("投递日志")
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"DeliveryAttempt(event={self.event_id}, try={self.try_number}, ok={self.ok})"
