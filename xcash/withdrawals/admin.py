from django.contrib import admin
from django.contrib import messages
from django.core.exceptions import PermissionDenied
from django.db.models import Count
from django.db.models import Q
from django.http import QueryDict
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from unfold.decorators import display

from common.admin import ModelAdmin
from common.admin import ReadOnlyModelAdmin
from core.monitoring import OperationalRiskService
from users.forms import OTPVerifyForm
from users.models import AdminAccessLog
from users.otp import AdminOTPRequiredError
from users.otp import get_fresh_admin_approval_context
from users.otp import get_pending_admin_user
from users.otp import get_primary_totp_device
from users.otp import record_admin_access
from users.otp import refresh_admin_otp_verification
from users.otp import set_pending_admin_otp
from users.otp import verify_otp_token
from withdrawals.models import VaultFunding
from withdrawals.models import Withdrawal
from withdrawals.models import WithdrawalReviewLog
from withdrawals.models import WithdrawalStatus
from withdrawals.service import WithdrawalService

# Register your models here.


class WithdrawalAttentionFilter(admin.SimpleListFilter):
    title = _("巡检状态")
    parameter_name = "attention"

    def lookups(self, request, model_admin):
        return (
            ("normal", _("正常")),
            ("stalled", _("超时")),
        )

    def queryset(self, request, queryset):
        now = timezone.now()
        stalled_q = (
            Q(
                status=WithdrawalStatus.REVIEWING,
                updated_at__lte=now
                - OperationalRiskService.reviewing_withdrawal_timeout(),
            )
            | Q(
                status=WithdrawalStatus.PENDING,
                updated_at__lte=now
                - OperationalRiskService.pending_withdrawal_timeout(),
            )
            | Q(
                status=WithdrawalStatus.CONFIRMING,
                updated_at__lte=now
                - OperationalRiskService.confirming_withdrawal_timeout(),
            )
        )
        if self.value() == "stalled":
            return queryset.filter(stalled_q)
        if self.value() == "normal":
            return queryset.exclude(stalled_q)
        return queryset


class WithdrawalReviewLogInline(admin.TabularInline):
    model = WithdrawalReviewLog
    extra = 0
    can_delete = False
    fields = ("actor", "action", "from_status", "to_status", "note", "created_at")
    readonly_fields = fields

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(VaultFunding)
class VaultFundingAdmin(ReadOnlyModelAdmin):
    list_display = (
        "project",
        "transfer_chain",
        "transfer_crypto",
        "transfer_amount",
        "transfer_hash",
        "transfer_status",
        "transfer_datetime",
    )
    list_filter = ("project",)
    list_select_related = ("project", "transfer", "transfer__chain", "transfer__crypto")

    @admin.display(description=_("链"))
    def transfer_chain(self, obj):
        return obj.transfer.chain if obj.transfer else "-"

    @admin.display(description=_("代币"))
    def transfer_crypto(self, obj):
        return obj.transfer.crypto if obj.transfer else "-"

    @admin.display(description=_("数量"))
    def transfer_amount(self, obj):
        return obj.transfer.amount if obj.transfer else "-"

    @admin.display(description=_("哈希"))
    def transfer_hash(self, obj):
        return obj.transfer.hash if obj.transfer else "-"

    @admin.display(description=_("状态"))
    def transfer_status(self, obj):
        return obj.transfer.get_status_display() if obj.transfer else "-"

    @admin.display(description=_("时间"))
    def transfer_datetime(self, obj):
        return obj.transfer.datetime if obj.transfer else "-"


@admin.register(Withdrawal)
class WithdrawalAdmin(ModelAdmin):
    list_after_template = "admin/includes/withdrawal_action_otp_modal.html"
    list_display = (
        "project",
        "out_no",
        "customer",
        "to",
        "crypto",
        "chain",
        "amount",
        "worth",
        "display_status",
        "display_attention",
        "display_review_log_count",
        "reviewed_by",
        "reviewed_at",
        "created_at",
    )
    readonly_fields = (
        "project",
        "sys_no",
        "out_no",
        "customer",
        "chain",
        "crypto",
        "amount",
        "worth",
        "to",
        "hash",
        "status",
        "reviewed_by",
        "reviewed_at",
        "transfer",
        "broadcast_task",
        "created_at",
        "updated_at",
    )
    fieldsets = (
        (
            _("基本信息"),
            {
                "fields": (
                    "project",
                    "sys_no",
                    "out_no",
                    "customer",
                    "chain",
                    "crypto",
                    "amount",
                    "worth",
                    "to",
                    "status",
                )
            },
        ),
        (
            _("审核信息"),
            {
                "fields": (
                    "reviewed_by",
                    "reviewed_at",
                )
            },
        ),
        (
            _("链上信息"),
            {
                "fields": (
                    "hash",
                    "broadcast_task",
                    "transfer",
                )
            },
        ),
        (
            _("时间"),
            {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )
    search_fields = (
        "out_no",
        "hash",
        "to",
        "project__name",
    )
    list_filter = (
        "chain",
        "crypto",
        "status",
        "reviewed_by",
        WithdrawalAttentionFilter,
    )
    actions = (
        "approve_selected_withdrawals",
        "reject_selected_withdrawals",
    )
    inlines = (WithdrawalReviewLogInline,)

    SENSITIVE_REVIEW_ACTIONS = {
        "approve_selected_withdrawals": "admin_bulk_approve",
        "reject_selected_withdrawals": "admin_bulk_reject",
    }

    def get_queryset(self, request):
        # 提币已改为“商户自审自己项目”，因此非超管继续沿用 owner 过滤，超管仍由底层 admin 返回全量数据。
        queryset = super().get_queryset(request)
        # 提币列表需要同时展示审核日志数量，直接注入聚合避免列表页逐行 count()。
        return queryset.annotate(review_log_total=Count("review_logs"))

    def get_search_results(self, request, queryset, search_term):
        queryset, use_distinct = super().get_search_results(
            request, queryset, search_term
        )
        if not search_term:
            return queryset, use_distinct

        # customer__uid 关系查询对 Django admin 是合法的，但部分 IDE 会误报“无法解析 admin 字段”。
        # 这里改为显式补充搜索结果，既保留按客户 UID 检索能力，也消除静态检查噪音。
        customer_uid_queryset = self.get_queryset(request).filter(
            customer__uid__icontains=search_term
        )
        return queryset | customer_uid_queryset, use_distinct

    def changelist_view(self, request, extra_context=None):
        selected_action = request.POST.get("action")
        if (
            request.method == "POST"
            and selected_action in self.SENSITIVE_REVIEW_ACTIONS
        ):
            # 先在入口处把 action 显式传给 OTP 分支，避免后续再从 POST 强取造成类型告警。
            modal_response = self._handle_sensitive_review_otp(
                request, action=selected_action
            )
            if modal_response is not None:
                return modal_response
        return super().changelist_view(request, extra_context=extra_context)

    def _handle_sensitive_review_otp(self, request, *, action: str):
        action_source = self.SENSITIVE_REVIEW_ACTIONS[action]
        if request.POST.get("_otp_modal_submit") == "1":
            return self._verify_sensitive_review_otp(request, source=action_source)

        try:
            request._withdrawal_approval_context = get_fresh_admin_approval_context(
                request=request, source=action_source
            )
        except AdminOTPRequiredError:
            set_pending_admin_otp(
                request,
                user=request.user,
                next_path=request.get_full_path(),
            )
            return self._render_changelist_with_otp_modal(
                request,
                form=OTPVerifyForm(),
            )
        else:
            return None

    def _verify_sensitive_review_otp(self, request, *, source: str):
        pending_user = get_pending_admin_user(request)
        if pending_user is None or pending_user.pk != request.user.pk:
            raise PermissionDenied("当前会话缺少可用的两步验证上下文")

        device = get_primary_totp_device(user=pending_user, confirmed=True)
        if device is None:
            raise PermissionDenied("当前账号尚未绑定两步验证设备")

        form = OTPVerifyForm(request.POST)
        if not form.is_valid():
            return self._render_changelist_with_otp_modal(request, form=form)
        if not verify_otp_token(device, form.cleaned_data["token"]):
            record_admin_access(
                request=request,
                action=AdminAccessLog.Action.OTP_VERIFY,
                result=AdminAccessLog.Result.FAILED,
                user=pending_user,
                reason="withdrawal_action_modal_invalid_token",
            )
            form.add_error("token", _("两步验证码无效，请检查设备时间或重新输入。"))
            return self._render_changelist_with_otp_modal(request, form=form)

        record_admin_access(
            request=request,
            action=AdminAccessLog.Action.OTP_VERIFY,
            result=AdminAccessLog.Result.SUCCEEDED,
            user=pending_user,
            reason="withdrawal_action_modal_verified",
        )
        refresh_admin_otp_verification(request, user=pending_user, device=device)
        request._withdrawal_approval_context = get_fresh_admin_approval_context(
            request=request, source=source
        )
        return None

    def _render_changelist_with_otp_modal(self, request, *, form: OTPVerifyForm):
        extra_context = {
            "otp_modal_open": True,
            "otp_verify_form": form,
            "otp_modal_locked_title": _("继续执行前需要重新验证"),
            "otp_modal_locked_text": _(
                "提币审批属于高风险动作。请输入一次两步验证码后继续执行。"
            ),
            "otp_modal_submit_label": _("验证并执行操作"),
            "otp_modal_hidden_fields": self._build_otp_modal_hidden_fields(request),
        }
        original_method = request.method
        original_post = request.POST
        request.method = "GET"
        request.POST = QueryDict(mutable=True)
        try:
            return super().changelist_view(request, extra_context=extra_context)
        finally:
            request.method = original_method
            request.POST = original_post

    def _build_otp_modal_hidden_fields(self, request):
        hidden_fields = []
        for key, values in request.POST.lists():
            if key in {"csrfmiddlewaretoken", "token", "_otp_modal_submit"}:
                continue
            hidden_fields.extend({"name": key, "value": value} for value in values)
        return hidden_fields

    @display(
        description="状态",
        label={
            "审核中": "warning",
            "待执行": "warning",
            "确认中": "info",
            "已完成": "success",
            "已拒绝": "danger",
            "已失败": "danger",
        },
    )
    def display_status(self, instance: Withdrawal):
        return instance.get_status_display()

    @display(
        description="巡检",
        label={
            "正常": "success",
            "超时": "danger",
        },
    )
    def display_attention(self, instance: Withdrawal):
        now = timezone.now()
        if (
            (
                instance.status == WithdrawalStatus.REVIEWING
                and instance.updated_at
                <= now - OperationalRiskService.reviewing_withdrawal_timeout()
            )
            or (
                instance.status == WithdrawalStatus.PENDING
                and instance.updated_at
                <= now - OperationalRiskService.pending_withdrawal_timeout()
            )
            or (
                instance.status == WithdrawalStatus.CONFIRMING
                and instance.updated_at
                <= now - OperationalRiskService.confirming_withdrawal_timeout()
            )
        ):
            return "超时"
        return "正常"

    @display(description="审核日志")
    def display_review_log_count(self, instance: Withdrawal):
        return getattr(instance, "review_log_total", instance.review_logs.count())

    @admin.action(description=_("批准提币"))
    def approve_selected_withdrawals(self, request, queryset):
        try:
            # 提币审批属于高风险动作，进入 service 前必须拿到近期 OTP 验证上下文。
            approval_context = getattr(
                request, "_withdrawal_approval_context", None
            ) or get_fresh_admin_approval_context(
                request=request,
                source="admin_bulk_approve",
            )
        except AdminOTPRequiredError as exc:
            self.message_user(request, str(exc), level=messages.ERROR)
            return
        approved = 0
        skipped = 0
        denied = 0
        for withdrawal_id in queryset.values_list("pk", flat=True):
            try:
                WithdrawalService.approve_withdrawal(
                    withdrawal_id=withdrawal_id,
                    reviewer=request.user,
                    note=str(_("后台批量批准")),
                    approval_context=approval_context,
                )
                approved += 1
            except PermissionError:
                denied += 1
            except ValueError:
                # 仅 REVIEWING 状态允许批准，其余状态跳过，避免后台批量操作误改已上链单据。
                skipped += 1
        if approved:
            self.message_user(
                request, _("已批准 %(count)s 笔提币") % {"count": approved}
            )
        if skipped:
            self.message_user(
                request,
                _("已跳过 %(count)s 笔非审核中提币") % {"count": skipped},
                level=messages.WARNING,
            )
        if denied:
            self.message_user(
                request,
                _("已拒绝执行 %(count)s 笔无权限提币") % {"count": denied},
                level=messages.ERROR,
            )

    @admin.action(description=_("拒绝提币"))
    def reject_selected_withdrawals(self, request, queryset):
        try:
            # 拒绝同样属于资金审批动作，沿用同一套 OTP 新鲜度约束。
            approval_context = getattr(
                request, "_withdrawal_approval_context", None
            ) or get_fresh_admin_approval_context(
                request=request,
                source="admin_bulk_reject",
            )
        except AdminOTPRequiredError as exc:
            self.message_user(request, str(exc), level=messages.ERROR)
            return
        rejected = 0
        skipped = 0
        denied = 0
        for withdrawal_id in queryset.values_list("pk", flat=True):
            try:
                WithdrawalService.reject_withdrawal(
                    withdrawal_id=withdrawal_id,
                    reviewer=request.user,
                    note=str(_("后台批量拒绝")),
                    approval_context=approval_context,
                )
                rejected += 1
            except PermissionError:
                denied += 1
            except ValueError:
                skipped += 1
        if rejected:
            self.message_user(
                request, _("已拒绝 %(count)s 笔提币") % {"count": rejected}
            )
        if skipped:
            self.message_user(
                request,
                _("已跳过 %(count)s 笔非审核中提币") % {"count": skipped},
                level=messages.WARNING,
            )
        if denied:
            self.message_user(
                request,
                _("已拒绝执行 %(count)s 笔无权限提币") % {"count": denied},
                level=messages.ERROR,
            )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(WithdrawalReviewLog)
class WithdrawalReviewLogAdmin(ReadOnlyModelAdmin):
    list_display = (
        "withdrawal",
        "project",
        "actor",
        "action",
        "from_status",
        "to_status",
        "created_at",
    )
    readonly_fields = (
        "withdrawal",
        "project",
        "actor",
        "action",
        "from_status",
        "to_status",
        "note",
        "snapshot",
        "created_at",
    )
    search_fields = (
        "withdrawal__out_no",
        "actor__username",
    )
    list_filter = ("action", "from_status", "to_status")
