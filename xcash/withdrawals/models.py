from django.db import models
from django.utils.translation import gettext_lazy as _

from common.fields import AddressField
from common.fields import HashField
from common.fields import SysNoField
from common.utils.math import format_decimal_stripped


class VaultFunding(models.Model):
    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.PROTECT,
        verbose_name=_("项目"),
    )
    transfer = models.OneToOneField(
        "chains.OnchainTransfer",
        on_delete=models.SET_NULL,
        verbose_name=_("链上转账"),
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = _("金库注资")
        verbose_name_plural = _("金库注资")

    def __str__(self):
        return f"{self.project_id}:{self.transfer_id}"


class WithdrawalStatus(models.TextChoices):
    # 提币请求已创建，等待人工审核
    REVIEWING = "reviewing", _("审核中")
    # 审核通过（或免审核），等待链上任务执行
    PENDING = "pending", _("待执行")
    # 交易已上链，等待区块链确认数达标
    CONFIRMING = "confirming", _("确认中")
    # 交易确认数达标，提币成功
    COMPLETED = "completed", _("已完成")
    # 人工审核拒绝（管理员在 REVIEWING 阶段主动拒绝）
    REJECTED = "rejected", _("已拒绝")
    # 链上交易最终失败（BroadcastTask 确认 FINALIZED + FAILED）
    FAILED = "failed", _("已失败")


class Withdrawal(models.Model):
    sys_no = SysNoField(prefix="WDR-")
    customer = models.ForeignKey(
        "users.Customer",
        on_delete=models.PROTECT,
        verbose_name=_("用户"),
        blank=True,
        null=True,
    )
    crypto = models.ForeignKey(
        "currencies.Crypto",
        on_delete=models.PROTECT,
        verbose_name=_("代币"),
    )
    amount = models.DecimalField(_("数量"), max_digits=32, decimal_places=8)
    worth = models.DecimalField(
        _("价值(USD)"),
        max_digits=16,
        decimal_places=6,
        default=0,
    )
    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.CASCADE,
        verbose_name=_("项目"),
    )
    chain = models.ForeignKey(
        "chains.Chain",
        on_delete=models.PROTECT,
        verbose_name=_("链"),
        blank=True,
        null=True,
    )
    out_no = models.CharField(_("商户单号"), max_length=128)
    to = AddressField(verbose_name=_("收币地址"))
    # 审核态提币尚未签名，因此 hash 允许为空；真正上链后再回填真实交易哈希。
    hash = HashField(verbose_name=_("哈希"), unique=False, blank=True, null=True)
    # 提币统一锚定跨链 BroadcastTask；hash 仅保留对外展示，不再承担主关联职责。
    broadcast_task = models.OneToOneField(
        "chains.BroadcastTask",
        on_delete=models.PROTECT,
        verbose_name=_("链上任务"),
        blank=True,
        null=True,
    )
    status = models.CharField(
        choices=WithdrawalStatus,
        default=WithdrawalStatus.PENDING,
        verbose_name=_("状态"),
    )
    reviewed_by = models.ForeignKey(
        "users.User",
        on_delete=models.PROTECT,
        verbose_name=_("审核人"),
        related_name="reviewed_withdrawals",
        blank=True,
        null=True,
    )
    reviewed_at = models.DateTimeField(_("审核时间"), blank=True, null=True)
    transfer = models.OneToOneField(
        "chains.OnchainTransfer",
        on_delete=models.SET_NULL,
        verbose_name=_("链上转账"),
        blank=True,
        null=True,
    )

    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
        constraints = [
            models.UniqueConstraint(
                fields=("project", "out_no"),
                name="uniq_withdrawal_project_out_no",
            ),
        ]
        # 提币审核已切到“项目归属即权限边界”，不再维护额外 approve/reject 细粒度权限。
        verbose_name = _("提币")
        verbose_name_plural = _("提币")

    def __str__(self):
        return self.out_no

    @property
    def content(self):
        """Webhook payload，包含提币关键信息供商户侧消费。"""
        data = {
            "sys_no": self.sys_no,
            "out_no": self.out_no,
            "chain": self.chain.code if self.chain else "",
            "hash": (
                self.broadcast_task.tx_hash if self.broadcast_task_id else self.hash
            ),
            "amount": format_decimal_stripped(self.amount),
            "crypto": self.crypto.symbol,
            "status": self.status,
        }
        # customer 为可选字段（API 侧未必传入）
        if self.customer_id:
            data["uid"] = self.customer.uid
        return {
            "type": "withdrawal",
            "data": data,
        }


class WithdrawalReviewLog(models.Model):
    class Action(models.TextChoices):
        APPROVED = "approved", _("已批准")
        REJECTED = "rejected", _("已拒绝")

    withdrawal = models.ForeignKey(
        "withdrawals.Withdrawal",
        on_delete=models.CASCADE,
        related_name="review_logs",
        verbose_name=_("提币"),
    )
    project = models.ForeignKey(
        "projects.Project",
        on_delete=models.CASCADE,
        verbose_name=_("项目"),
    )
    actor = models.ForeignKey(
        "users.User",
        on_delete=models.PROTECT,
        related_name="withdrawal_review_logs",
        verbose_name=_("操作人"),
    )
    action = models.CharField(
        choices=Action,
        verbose_name=_("操作"),
    )
    from_status = models.CharField(
        choices=WithdrawalStatus,
        verbose_name=_("原状态"),
    )
    to_status = models.CharField(
        choices=WithdrawalStatus,
        verbose_name=_("目标状态"),
    )
    note = models.TextField(_("备注"), blank=True, default="")
    snapshot = models.JSONField(_("快照"), default=dict, blank=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        verbose_name = _("提币审核日志")
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.withdrawal_id}:{self.action}:{self.actor_id}"
