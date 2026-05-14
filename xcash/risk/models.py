from __future__ import annotations

from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _


class RiskSource(models.TextChoices):
    QUICKNODE_MISTTRACK = "quicknode_misttrack", _("QuickNode MistTrack")


class RiskLevel(models.TextChoices):
    LOW = "Low", _("Low")
    MODERATE = "Moderate", _("Moderate")
    HIGH = "High", _("High")
    SEVERE = "Severe", _("Severe")


class RiskAssessmentStatus(models.TextChoices):
    PENDING = "pending", _("待查询")
    SUCCESS = "success", _("查询成功")
    FAILED = "failed", _("查询失败")
    SKIPPED = "skipped", _("已跳过")


class RiskTargetType(models.TextChoices):
    INVOICE = "invoice", _("账单")
    DEPOSIT = "deposit", _("充币")


class RiskAssessment(models.Model):
    source = models.CharField(
        _("数据来源"),
        choices=RiskSource,
        max_length=32,
        default=RiskSource.QUICKNODE_MISTTRACK,
        db_index=True,
    )
    status = models.CharField(
        _("查询状态"),
        choices=RiskAssessmentStatus,
        max_length=16,
        default=RiskAssessmentStatus.PENDING,
        db_index=True,
    )
    target_type = models.CharField(
        _("目标类型"),
        choices=RiskTargetType,
        max_length=16,
        db_index=True,
    )
    invoice = models.OneToOneField(
        "invoices.Invoice",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="risk_assessment",
        verbose_name=_("账单"),
    )
    deposit = models.OneToOneField(
        "deposits.Deposit",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="risk_assessment",
        verbose_name=_("充币"),
    )
    address = models.CharField(_("查询地址"), max_length=128, db_index=True)
    tx_hash = models.CharField(_("交易哈希"), max_length=128, blank=True, default="")
    risk_level = models.CharField(  # noqa: DJ001
        _("风险等级"),
        choices=RiskLevel,
        max_length=16,
        null=True,
        blank=True,
        db_index=True,
    )
    risk_score = models.DecimalField(
        _("风险分数"),
        max_digits=6,
        decimal_places=2,
        null=True,
        blank=True,
    )
    detail_list = models.JSONField(_("风险原因列表"), default=list, blank=True)
    risk_detail = models.JSONField(_("风险详情"), default=dict, blank=True)
    risk_report_url = models.URLField(_("风险报告链接"), blank=True, default="")
    raw_response = models.JSONField(_("原始响应摘要"), default=dict, blank=True)
    error_message = models.TextField(_("错误摘要"), blank=True, default="")
    checked_at = models.DateTimeField(_("查询完成时间"), null=True, blank=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        ordering = ("-created_at",)
        verbose_name = _("风险评估")
        verbose_name_plural = _("风险评估")
        constraints = [
            models.CheckConstraint(
                name="risk_assessment_exactly_one_target",
                condition=(
                    (
                        models.Q(invoice__isnull=False)
                        & models.Q(deposit__isnull=True)
                    )
                    | (
                        models.Q(invoice__isnull=True)
                        & models.Q(deposit__isnull=False)
                    )
                ),
            ),
            models.CheckConstraint(
                name="risk_assessment_target_type_matches_target",
                condition=(
                    (
                        models.Q(target_type=RiskTargetType.INVOICE)
                        & models.Q(invoice__isnull=False)
                        & models.Q(deposit__isnull=True)
                    )
                    | (
                        models.Q(target_type=RiskTargetType.DEPOSIT)
                        & models.Q(invoice__isnull=True)
                        & models.Q(deposit__isnull=False)
                    )
                ),
            ),
        ]

    def __str__(self) -> str:
        return f"{self.get_target_type_display()} {self.address}"

    def clean(self):
        super().clean()
        has_invoice = self.invoice_id is not None
        has_deposit = self.deposit_id is not None
        if has_invoice == has_deposit:
            raise ValidationError(_("风险评估必须且只能关联一个业务目标。"))
