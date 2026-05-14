from django.contrib import admin
from risk.models import RiskAssessment

from common.admin import ReadOnlyModelAdmin


@admin.register(RiskAssessment)
class RiskAssessmentAdmin(ReadOnlyModelAdmin):
    list_display = (
        "id",
        "target_type",
        "source",
        "status",
        "risk_level",
        "risk_score",
        "address",
        "tx_hash",
        "checked_at",
        "created_at",
    )
    list_filter = ("source", "status", "risk_level", "target_type")
    search_fields = (
        "address",
        "tx_hash",
        "invoice__sys_no",
        "deposit__sys_no",
    )
    readonly_fields = (
        "source",
        "status",
        "target_type",
        "invoice",
        "deposit",
        "address",
        "tx_hash",
        "risk_level",
        "risk_score",
        "detail_list",
        "risk_detail",
        "risk_report_url",
        "raw_response",
        "error_message",
        "checked_at",
        "created_at",
        "updated_at",
    )
    fieldsets = (
        (
            "目标",
            {
                "fields": (
                    "target_type",
                    "invoice",
                    "deposit",
                    "address",
                    "tx_hash",
                )
            },
        ),
        (
            "风险结果",
            {
                "fields": (
                    "source",
                    "status",
                    "risk_level",
                    "risk_score",
                    "detail_list",
                    "risk_detail",
                    "risk_report_url",
                    "raw_response",
                    "error_message",
                )
            },
        ),
        ("时间", {"fields": ("checked_at", "created_at", "updated_at")}),
    )
