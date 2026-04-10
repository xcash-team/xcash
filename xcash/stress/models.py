from django.db import models
from django.utils.translation import gettext_lazy as _


class StressRunStatus(models.TextChoices):
    DRAFT = "draft", _("草稿")
    PREPARING = "preparing", _("准备中")
    FAILED = "failed", _("准备失败")
    READY = "ready", _("就绪")
    RUNNING = "running", _("运行中")
    COMPLETED = "completed", _("已完成")


class InvoiceStressCaseStatus(models.TextChoices):
    PENDING = "pending", _("等待执行")
    CREATING = "creating", _("创建账单中")
    CREATED = "created", _("账单已创建")
    PAYING = "paying", _("支付中")
    PAID = "paid", _("已支付")
    WEBHOOK_OK = "webhook_ok", _("Webhook 验证通过")
    SUCCEEDED = "succeeded", _("成功")
    FAILED = "failed", _("失败")
    SKIPPED = "skipped", _("跳过")


class WithdrawalStressCaseStatus(models.TextChoices):
    PENDING = "pending", _("等待执行")
    CREATING = "creating", _("创建提币中")
    CREATED = "created", _("提币单已创建")
    CONFIRMING = "confirming", _("等待链上确认")
    SUCCEEDED = "succeeded", _("成功")
    FAILED = "failed", _("失败")
    SKIPPED = "skipped", _("跳过")


class StressRun(models.Model):
    name = models.CharField(_("名称"), max_length=128)
    count = models.PositiveIntegerField(_("支付模拟次数"), default=0)
    withdrawal_count = models.PositiveIntegerField(_("提币模拟次数"), default=0)
    status = models.CharField(
        _("状态"),
        max_length=16,
        choices=StressRunStatus,
        default=StressRunStatus.DRAFT,
    )
    project = models.OneToOneField(
        "projects.Project",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        verbose_name=_("专用项目"),
    )

    succeeded = models.PositiveIntegerField(_("成功"), default=0)
    failed = models.PositiveIntegerField(_("失败"), default=0)
    skipped = models.PositiveIntegerField(_("跳过"), default=0)
    error = models.TextField(_("错误信息"), blank=True)

    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    started_at = models.DateTimeField(_("开始时间"), null=True, blank=True)
    finished_at = models.DateTimeField(_("完成时间"), null=True, blank=True)

    class Meta:
        verbose_name = _("压力测试")
        verbose_name_plural = _("压力测试")
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.name} ({self.get_status_display()})"

    @property
    def total_finished(self):
        return self.succeeded + self.failed + self.skipped


class InvoiceStressCase(models.Model):
    stress_run = models.ForeignKey(
        StressRun,
        on_delete=models.CASCADE,
        related_name="cases",
        verbose_name=_("测试轮次"),
    )
    sequence = models.PositiveIntegerField(_("序号"))
    scheduled_offset = models.FloatField(
        _("调度偏移(秒)"),
        help_text=_("相对于 Stress.started_at 的偏移秒数"),
    )

    # 执行过程中填充
    invoice_sys_no = models.CharField(_("系统单号"), max_length=64, blank=True)
    invoice_out_no = models.CharField(_("商户单号"), max_length=64, blank=True)
    crypto = models.CharField(_("币种"), max_length=32, blank=True)
    chain = models.CharField(_("链"), max_length=32, blank=True)
    pay_address = models.CharField(_("支付地址"), max_length=256, blank=True)
    pay_amount = models.DecimalField(
        _("支付金额"),
        max_digits=36,
        decimal_places=18,
        null=True,
        blank=True,
    )
    payer_address = models.CharField(_("付款地址"), max_length=256, blank=True)
    tx_hash = models.CharField(_("交易哈希"), max_length=128, blank=True)

    # Webhook 验证结果
    webhook_received = models.BooleanField(_("已收到 Webhook"), default=False)
    webhook_signature_ok = models.BooleanField(_("签名验证"), default=False)
    webhook_payload_ok = models.BooleanField(_("Payload 验证"), default=False)
    webhook_nonce_ok = models.BooleanField(_("Nonce 验证"), default=False)
    webhook_timestamp_ok = models.BooleanField(_("时间戳验证"), default=False)
    webhook_received_nonces = models.JSONField(
        _("已收到的 Nonce 列表"), default=list, blank=True
    )

    status = models.CharField(
        _("状态"),
        max_length=16,
        choices=InvoiceStressCaseStatus,
        default=InvoiceStressCaseStatus.PENDING,
    )
    error = models.TextField(_("错误信息"), blank=True)

    started_at = models.DateTimeField(_("开始时间"), null=True, blank=True)
    finished_at = models.DateTimeField(_("完成时间"), null=True, blank=True)

    class Meta:
        verbose_name = _("账单测试")
        verbose_name_plural = _("账单测试")
        ordering = ["stress_run", "sequence"]

    def __str__(self):
        return f"#{self.sequence} {self.get_status_display()}"


class WithdrawalStressCase(models.Model):
    stress_run = models.ForeignKey(
        StressRun,
        on_delete=models.CASCADE,
        related_name="withdrawal_cases",
        verbose_name=_("测试轮次"),
    )
    sequence = models.PositiveIntegerField(_("序号"))
    scheduled_offset = models.FloatField(
        _("调度偏移(秒)"),
        help_text=_("相对于 Stress.started_at 的偏移秒数"),
    )

    # 执行过程中填充
    withdrawal_sys_no = models.CharField(_("系统单号"), max_length=64, blank=True)
    withdrawal_out_no = models.CharField(_("商户单号"), max_length=64, blank=True)
    crypto = models.CharField(_("币种"), max_length=32, blank=True)
    chain = models.CharField(_("链"), max_length=32, blank=True)
    to_address = models.CharField(_("目的地址"), max_length=256, blank=True)
    amount = models.DecimalField(
        _("提币金额"),
        max_digits=36,
        decimal_places=18,
        null=True,
        blank=True,
    )
    tx_hash = models.CharField(_("交易哈希"), max_length=128, blank=True)

    # Webhook 验证结果
    webhook_received = models.BooleanField(_("已收到 Webhook"), default=False)
    webhook_signature_ok = models.BooleanField(_("签名验证"), default=False)
    webhook_payload_ok = models.BooleanField(_("Payload 验证"), default=False)
    webhook_nonce_ok = models.BooleanField(_("Nonce 验证"), default=False)
    webhook_timestamp_ok = models.BooleanField(_("时间戳验证"), default=False)
    webhook_received_nonces = models.JSONField(
        _("已收到的 Nonce 列表"), default=list, blank=True
    )

    status = models.CharField(
        _("状态"),
        max_length=16,
        choices=WithdrawalStressCaseStatus,
        default=WithdrawalStressCaseStatus.PENDING,
    )
    error = models.TextField(_("错误信息"), blank=True)

    started_at = models.DateTimeField(_("开始时间"), null=True, blank=True)
    finished_at = models.DateTimeField(_("完成时间"), null=True, blank=True)

    class Meta:
        verbose_name = _("提币测试")
        verbose_name_plural = _("提币测试")
        ordering = ["stress_run", "sequence"]

    def __str__(self):
        return f"WD#{self.sequence} {self.get_status_display()}"
