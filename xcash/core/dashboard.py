import json

from django.contrib import admin
from django.core.exceptions import PermissionDenied
from django.shortcuts import render
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from django.views.generic import RedirectView

from core.dashboard_metrics import build_dashboard_metrics
from core.dashboard_metrics import build_signer_dashboard_summary
from core.monitoring import OperationalRiskService
from users.forms import OTPVerifyForm
from users.models import AdminAccessLog
from users.otp import AdminOTPRequiredError
from users.otp import complete_admin_otp_login
from users.otp import get_fresh_admin_sensitive_action_context
from users.otp import get_pending_admin_user
from users.otp import get_primary_totp_device
from users.otp import record_admin_access
from users.otp import set_pending_admin_otp


class HomeView(RedirectView):
    pattern_name = "admin:index"


def _build_environment_badge(signer_summary: dict | None, risk_summary: dict) -> list[str]:
    """为后台顶部角标生成轻量状态摘要，避免复用完整首页聚合。"""
    if not signer_summary or not signer_summary.get("available"):
        return [_("Signer异常"), "danger"]

    health = signer_summary.get("health") or {}
    if not health.get("healthy", True) or not _signer_auth_configured(signer_summary):
        return [_("Signer异常"), "danger"]

    if risk_summary["stalled_webhook_event_count"] > 0:
        return [_("存在高风险告警"), "danger"]

    pending_count = (
        risk_summary["stalled_withdrawal_count"]
        + risk_summary["stalled_deposit_collection_count"]
    )
    if pending_count > 0:
        return [_("%(count)s项待处理") % {"count": pending_count}, "warning"]

    return [_("运行正常"), "success"]


def environment_callback(request):
    signer_summary = build_signer_dashboard_summary()
    # 顶部 environment badge 只需要判断是否可安全操作后台，
    # 这里仅读取 signer 健康与巡检计数，避免触发完整首页统计查询。
    risk_summary = OperationalRiskService.build_summary(limit=0)
    return _build_environment_badge(signer_summary, risk_summary)


def _fmt_usd(amount) -> str:
    return f"$ {amount:,.2f}"


def _signer_auth_configured(signer_summary: dict | None) -> bool:
    """兼容 signer 健康摘要字段演进，避免后台页因字段改名直接报错。"""
    if not signer_summary or not signer_summary.get("available"):
        return False
    health = signer_summary.get("health") or {}
    # signer 服务已将旧字段 signer_shared_secret 统一收口为 auth_configured，
    # 这里保留向后兼容，避免主应用与 signer 灰度升级期间出现管理页 500。
    return bool(
        health.get("auth_configured", health.get("signer_shared_secret", False))
    )


def _build_operational_inspection_payload(metrics, signer_summary):
    # 改动原因：首页摘要与独立巡检页必须共用同一套异常组装逻辑，避免两个入口出现口径漂移。
    inspection_sections = []
    attention_items = []

    failed_attempt_rows = [
        {
            "level": _("高"),
            "title": _("Webhook 投递失败"),
            "description": _("项目 %(project)s 在 %(time)s 投递失败：HTTP %(status)s")
            % {
                "project": attempt.event.project.name,
                "time": attempt.created_at.strftime("%m-%d %H:%M"),
                "status": attempt.response_status or "-",
            },
            "href": reverse("admin:webhooks_deliveryattempt_change", args=[attempt.pk]),
        }
        for attempt in metrics["recent_failed_attempts"]
    ]
    inspection_sections.append(
        {
            "title": _("Webhook 投递失败"),
            "subtitle": _("近24小时失败回调明细"),
            "count": len(failed_attempt_rows),
            "rows": failed_attempt_rows,
            "empty_text": _("近24小时没有新的投递失败"),
        }
    )
    attention_items.extend(failed_attempt_rows)

    stalled_invoice_rows = [
        {
            "level": _("中"),
            "title": _("账单长时间确认中"),
            "description": _("%(project)s / %(sys_no)s / %(crypto)s-%(chain)s")
            % {
                "project": invoice.project.name,
                "sys_no": invoice.sys_no,
                "crypto": invoice.crypto.symbol if invoice.crypto else "-",
                "chain": invoice.chain.code if invoice.chain else "-",
            },
            "href": reverse("admin:invoices_invoice_change", args=[invoice.pk]),
        }
        for invoice in metrics["recent_stalled_invoices"]
    ]
    inspection_sections.append(
        {
            "title": _("账单确认巡检"),
            "subtitle": _("长时间处于确认中的账单"),
            "count": len(stalled_invoice_rows),
            "rows": stalled_invoice_rows,
            "empty_text": _("当前没有长时间确认中的账单"),
        }
    )
    attention_items.extend(stalled_invoice_rows)

    stalled_withdrawal_rows = [
        {
            "level": _("中"),
            "title": _("提币长时间未完成"),
            "description": _("%(project)s / %(out_no)s / %(crypto)s-%(chain)s")
            % {
                "project": withdrawal.project.name,
                "out_no": withdrawal.out_no,
                "crypto": withdrawal.crypto.symbol,
                "chain": withdrawal.chain.code if withdrawal.chain else "-",
            },
            "href": reverse(
                "admin:withdrawals_withdrawal_change", args=[withdrawal.pk]
            ),
        }
        for withdrawal in metrics["recent_stalled_withdrawals"]
    ]
    inspection_sections.append(
        {
            "title": _("提币执行巡检"),
            "subtitle": _("长时间未完成的提币"),
            "count": len(stalled_withdrawal_rows),
            "rows": stalled_withdrawal_rows,
            "empty_text": _("当前没有卡住的提币单"),
        }
    )
    attention_items.extend(stalled_withdrawal_rows)

    stalled_deposit_rows = [
        {
            "level": _("中"),
            "title": _("归集长时间未完成"),
            "description": _("%(project)s / %(sys_no)s / %(crypto)s-%(chain)s")
            % {
                "project": (
                    deposit.customer.project.name if deposit.customer_id else "-"
                ),
                "sys_no": deposit.sys_no,
                "crypto": deposit.transfer.crypto.symbol,
                "chain": deposit.transfer.chain.code,
            },
            "href": reverse("admin:deposits_deposit_change", args=[deposit.pk]),
        }
        for deposit in metrics["recent_stalled_deposit_collections"]
    ]
    inspection_sections.append(
        {
            "title": _("归集巡检"),
            "subtitle": _("长时间未完成的归集任务"),
            "count": len(stalled_deposit_rows),
            "rows": stalled_deposit_rows,
            "empty_text": _("当前没有卡住的归集任务"),
        }
    )
    attention_items.extend(stalled_deposit_rows)

    stalled_webhook_rows = [
        {
            "level": _("高"),
            "title": _("Webhook 长时间未送达"),
            "description": _("%(project)s / %(nonce)s / 创建于 %(time)s")
            % {
                "project": event.project.name,
                "nonce": event.nonce,
                "time": event.created_at.strftime("%m-%d %H:%M"),
            },
            "href": reverse("admin:webhooks_webhookevent_change", args=[event.pk]),
        }
        for event in metrics["recent_stalled_webhook_events"]
    ]
    inspection_sections.append(
        {
            "title": _("Webhook 堆积巡检"),
            "subtitle": _("创建后长时间未送达的事件"),
            "count": len(stalled_webhook_rows),
            "rows": stalled_webhook_rows,
            "empty_text": _("当前没有堆积中的 Webhook 事件"),
        }
    )
    attention_items.extend(stalled_webhook_rows)

    if signer_summary:
        signer_rows = []
        if signer_summary["available"]:
            signer_rows.extend(
                {
                    "level": _("高") if anomaly["status"] == "failed" else _("中"),
                    "title": _("Signer 请求异常"),
                    "description": _(
                        "%(endpoint)s / wallet=%(wallet)s / %(code)s / %(time)s"
                    )
                    % {
                        "endpoint": anomaly["endpoint"],
                        "wallet": anomaly["wallet_id"] or "-",
                        "code": anomaly["error_code"] or "-",
                        "time": anomaly["created_at"][5:16].replace("T", " "),
                    },
                    "href": None,
                }
                for anomaly in signer_summary["recent_anomalies"]
            )
            empty_text = _("近1小时没有新的 Signer 异常")
        else:
            signer_rows.append(
                {
                    "level": _("高"),
                    "title": _("Signer 服务不可达"),
                    "description": signer_summary["detail"],
                    "href": None,
                }
            )
            empty_text = _("Signer 服务当前不可用")
        inspection_sections.append(
            {
                "title": _("Signer 巡检"),
                "subtitle": _("签名服务健康度与近期异常"),
                "count": len(signer_rows),
                "rows": signer_rows,
                "empty_text": empty_text,
            }
        )
        attention_items.extend(signer_rows)

    return {
        "attention_items": attention_items,
        "inspection_sections": inspection_sections,
    }


def _build_operational_inspection_summary_cards(snapshot, signer_summary):
    # 改动原因：独立巡检页需要先给出风险摘要，用户不必逐段滚动才能判断当前是否有异常。
    summary_cards = [
        {
            "title": _("账单确认风险"),
            "metric": snapshot["confirming_count"],
            "subtitle": _("确认中 %(count)s 笔，临近超时 %(soon)s 笔")
            % {
                "count": snapshot["confirming_count"],
                "soon": snapshot["expiring_soon_count"],
            },
            "tone": "bg-amber-50",
        },
        {
            "title": _("提币巡检"),
            "metric": snapshot["stalled_withdrawal_count"],
            "subtitle": _("卡住提币 %(stalled)s 笔，审核中 %(reviewing)s 笔")
            % {
                "stalled": snapshot["stalled_withdrawal_count"],
                "reviewing": snapshot["reviewing_withdrawal_count"],
            },
            "tone": "bg-rose-50",
        },
        {
            "title": _("归集巡检"),
            "metric": snapshot["stalled_deposit_collection_count"],
            "subtitle": _("长时间未完成归集 %(count)s 笔")
            % {"count": snapshot["stalled_deposit_collection_count"]},
            "tone": "bg-orange-50",
        },
        {
            "title": _("Webhook 巡检"),
            "metric": snapshot["stalled_webhook_event_count"],
            "subtitle": _("待投递 %(pending)s 条，失败事件 %(failed)s 条")
            % {
                "pending": snapshot["pending_events_count"],
                "failed": snapshot["failed_events_count"],
            },
            "tone": "bg-sky-50",
        },
    ]
    if signer_summary:
        if signer_summary["available"]:
            summary_cards.append(
                {
                    "title": _("Signer 巡检"),
                    "metric": len(signer_summary["recent_anomalies"]),
                    "subtitle": _("近1小时失败 %(failed)s 次，限流 %(limited)s 次")
                    % {
                        "failed": signer_summary["requests_last_hour"]["failed"],
                        "limited": signer_summary["requests_last_hour"]["rate_limited"],
                    },
                    "tone": "bg-indigo-50",
                }
            )
        else:
            summary_cards.append(
                {
                    "title": _("Signer 巡检"),
                    "metric": _("不可用"),
                    "subtitle": signer_summary["detail"],
                    "tone": "bg-gray-100",
                }
            )
    return summary_cards


def dashboard_callback(request, context):
    # analytics app 已退役，首页实时指标改由 core 内部服务直接提供。
    metrics = build_dashboard_metrics()
    snapshot = metrics["snapshot"]
    chart_rows = metrics["chart_rows"]
    signer_summary = metrics.get("signer_summary")
    inspection_payload = _build_operational_inspection_payload(metrics, signer_summary)

    # 后台首页改为实时经营看板，优先展示商户最关心的成交、转化、积压和失败指标。
    snapshot_cards = [
        {
            "title": _("今日成交额"),
            "metric": _fmt_usd(snapshot["today_completed_worth"]),
            "subtitle": _("今日成功账单 %(count)s 笔")
            % {"count": snapshot["today_completed_count"]},
            "tone": "bg-emerald-50",
        },
        {
            "title": _("7日成交额"),
            "metric": _fmt_usd(snapshot["rolling_7d_completed_worth"]),
            "subtitle": _("近7日成功账单 %(count)s 笔")
            % {"count": snapshot["rolling_7d_completed_count"]},
            "tone": "bg-sky-50",
        },
        {
            "title": _("30日成交额"),
            "metric": _fmt_usd(snapshot["rolling_30d_completed_worth"]),
            "subtitle": _("近30日成功账单 %(count)s 笔")
            % {"count": snapshot["rolling_30d_completed_count"]},
            "tone": "bg-indigo-50",
        },
        {
            "title": _("30日支付转化率"),
            "metric": f"{snapshot['conversion_rate_30d']}%",
            "subtitle": _("近30日共创建账单 %(count)s 笔")
            % {"count": snapshot["created_30d_count"]},
            "tone": "bg-amber-50",
        },
        {
            "title": _("待确认收款"),
            "metric": _fmt_usd(snapshot["confirming_worth"]),
            "subtitle": _("确认中 %(count)s 笔")
            % {"count": snapshot["confirming_count"]},
            "tone": "bg-orange-50",
        },
        {
            "title": _("Webhook 健康度"),
            "metric": f"{snapshot['webhook_success_rate_7d']}%",
            "subtitle": _("近7日投递 %(total)s 次，失败投递 %(failed)s 次")
            % {
                "total": snapshot["webhook_attempt_total_7d"],
                "failed": snapshot["webhook_attempt_failed_7d"],
            },
            "tone": "bg-rose-50",
        },
    ]

    backlog_rows = [
        {
            "label": _("待支付账单"),
            "value": snapshot["waiting_count"],
            "detail": _fmt_usd(snapshot["waiting_worth"]),
            "href": f"{reverse('admin:invoices_invoice_changelist')}?status__exact=waiting",
        },
        {
            "label": _("确认中账单"),
            "value": snapshot["confirming_count"],
            "detail": _fmt_usd(snapshot["confirming_worth"]),
            "href": f"{reverse('admin:invoices_invoice_changelist')}?status__exact=confirming",
        },
        {
            "label": _("审核中提币"),
            "value": snapshot["reviewing_withdrawal_count"],
            "detail": _("等待人工审核"),
            "href": f"{reverse('admin:withdrawals_withdrawal_changelist')}?status__exact=reviewing",
        },
        {
            "label": _("待执行提币"),
            "value": snapshot["pending_withdrawal_count"],
            "detail": _("等待系统构建/广播"),
            "href": f"{reverse('admin:withdrawals_withdrawal_changelist')}?status__exact=pending",
        },
        {
            "label": _("确认中提币"),
            "value": snapshot["confirming_withdrawal_count"],
            "detail": _("链上已上链，等待确认"),
            "href": f"{reverse('admin:withdrawals_withdrawal_changelist')}?status__exact=confirming",
        },
        {
            "label": _("待投递事件"),
            "value": snapshot["pending_events_count"],
            "detail": _("等待 Webhook 调度"),
            "href": f"{reverse('admin:webhooks_webhookevent_changelist')}?status__exact=pending",
        },
        {
            "label": _("失败事件"),
            "value": snapshot["failed_events_count"],
            "detail": _("需要人工检查或重投"),
            "href": f"{reverse('admin:webhooks_webhookevent_changelist')}?status__exact=failed",
        },
    ]

    health_cards = [
        {
            "title": _("提币执行"),
            "metric": _fmt_usd(snapshot["completed_withdrawal_worth_30d"]),
            "subtitle": _("近30日成功 %(done)s 笔，拒绝 %(rejected)s 笔")
            % {
                "done": snapshot["completed_withdrawal_count_30d"],
                "rejected": snapshot["rejected_withdrawal_count_30d"],
            },
        },
        {
            "title": _("Webhook 投递"),
            "metric": _("%(ok)s / %(total)s 成功")
            % {
                "ok": snapshot["webhook_attempt_ok_7d"],
                "total": snapshot["webhook_attempt_total_7d"],
            },
            "subtitle": _("近7日成功率 %(rate)s%%")
            % {"rate": snapshot["webhook_success_rate_7d"]},
        },
        {
            "title": _("任务巡检"),
            "metric": _("%(wdr)s / %(dep)s / %(wh)s")
            % {
                "wdr": snapshot["stalled_withdrawal_count"],
                "dep": snapshot["stalled_deposit_collection_count"],
                "wh": snapshot["stalled_webhook_event_count"],
            },
            "subtitle": _("卡住提币 / 卡住归集 / 超时回调"),
        },
    ]
    if signer_summary:
        if signer_summary["available"]:
            # signer 作为独立签名服务，首页只展示健康与异常摘要，不把敏感细节暴露给运营界面。
            health_cards.append(
                {
                    "title": _("Signer 服务"),
                    "metric": _("%(healthy)s / %(failed)s / %(rate_limited)s")
                    % {
                        "healthy": signer_summary["requests_last_hour"]["succeeded"],
                        "failed": signer_summary["requests_last_hour"]["failed"],
                        "rate_limited": signer_summary["requests_last_hour"][
                            "rate_limited"
                        ],
                    },
                    "subtitle": _(
                        "近1小时成功 / 失败 / 限流，请求总数 %(total)s，冻结钱包 %(frozen)s 个"
                    )
                    % {
                        "total": signer_summary["requests_last_hour"]["total"],
                        "frozen": signer_summary["wallets"]["frozen"],
                    },
                }
            )
        else:
            health_cards.append(
                {
                    "title": _("Signer 服务"),
                    "metric": _("不可用"),
                    "subtitle": signer_summary["detail"],
                }
            )

    context.update(
        {
            "snapshot_cards": snapshot_cards,
            "backlog_rows": backlog_rows,
            "health_cards": health_cards,
            "top_projects": [
                {
                    "name": row["project__name"],
                    "gmv": _fmt_usd(row["gmv"]),
                    "completed_orders": row["completed_orders"],
                    "conversion_rate": (
                        f"{(row['completed_orders'] / row['total_orders'] * 100):.1f}%"
                        if row["total_orders"]
                        else "0.0%"
                    ),
                    "waiting_orders": row["waiting_orders"],
                    "confirming_orders": row["confirming_orders"],
                }
                for row in metrics["top_projects"]
            ],
            "payment_methods": [
                {
                    "label": f"{row['crypto__symbol']} / {row['chain__code']}",
                    "gmv": _fmt_usd(row["gmv"]),
                    "order_count": row["order_count"],
                }
                for row in metrics["payment_methods"]
            ],
            "attention_items": inspection_payload["attention_items"][:8],
            "chart": json.dumps(
                {
                    "labels": [row["label"] for row in chart_rows],
                    "datasets": [
                        {
                            "label": str(_("完成金额(USD)")),
                            "type": "line",
                            "yAxisID": "y",
                            "data": [
                                float(row["completed_worth"]) for row in chart_rows
                            ],
                            "backgroundColor": "#0f766e",
                            "borderColor": "#0f766e",
                            "tension": 0.35,
                        },
                        {
                            "label": str(_("创建账单数")),
                            "type": "bar",
                            "yAxisID": "y1",
                            "data": [row["created_count"] for row in chart_rows],
                            "backgroundColor": "#93c5fd",
                            "borderColor": "#60a5fa",
                        },
                        {
                            "label": str(_("超时账单数")),
                            "type": "bar",
                            "yAxisID": "y1",
                            "data": [row["expired_count"] for row in chart_rows],
                            "backgroundColor": "#fdba74",
                            "borderColor": "#fb923c",
                        },
                    ],
                },
            ),
        },
    )
    return context


def operational_inspection_view(request):
    # 改动原因：“异常巡检”菜单需要落到独立页面，而不是继续回到 admin 首页。
    metrics = build_dashboard_metrics()
    signer_summary = metrics.get("signer_summary")
    inspection_payload = _build_operational_inspection_payload(metrics, signer_summary)
    overview_context = admin.site.each_context(request)
    overview_context.update(
        {
            "title": _("异常巡检"),
            "inspection_summary_cards": _build_operational_inspection_summary_cards(
                metrics["snapshot"],
                signer_summary,
            ),
            "inspection_sections": inspection_payload["inspection_sections"],
            "attention_items_count": len(inspection_payload["attention_items"]),
        }
    )
    return render(request, "admin/operational_inspection.html", overview_context)


def _render_signer_otp_modal(request, *, form: OTPVerifyForm):
    overview_context = admin.site.each_context(request)
    overview_context.update(
        {
            "title": _("Signer 运营"),
            "signer_summary": None,
            "signer_health_rows": [],
            "otp_verify_form": form,
            "otp_modal_open": True,
            "otp_modal_locked_title": _("继续查看前需要重新验证"),
            "otp_modal_locked_text": _(
                "Signer 运营属于高敏感后台入口。请输入一次两步验证码后继续查看。"
            ),
        }
    )
    return render(request, "admin/signer_overview.html", overview_context)


def _handle_signer_modal_verification(request):
    pending_user = get_pending_admin_user(request)
    if pending_user is None or pending_user.pk != request.user.pk:
        raise PermissionDenied("当前会话缺少可用的两步验证上下文")

    device = get_primary_totp_device(user=pending_user, confirmed=True)
    if device is None:
        raise PermissionDenied("当前账号尚未绑定两步验证设备")

    form = OTPVerifyForm(request.POST)
    if not form.is_valid():
        return _render_signer_otp_modal(request, form=form)

    if not device.verify_token(form.cleaned_data["token"]):
        record_admin_access(
            request=request,
            action=AdminAccessLog.Action.OTP_VERIFY,
            result=AdminAccessLog.Result.FAILED,
            user=pending_user,
            reason="modal_invalid_token",
        )
        form.add_error("token", _("两步验证码无效，请检查设备时间或重新输入。"))
        return _render_signer_otp_modal(request, form=form)

    record_admin_access(
        request=request,
        action=AdminAccessLog.Action.OTP_VERIFY,
        result=AdminAccessLog.Result.SUCCEEDED,
        user=pending_user,
        reason="modal_sensitive_action_verified",
    )
    return complete_admin_otp_login(
        request,
        user=pending_user,
        device=device,
    )


def signer_overview_view(request):
    if not request.user.is_superuser:
        raise PermissionDenied("只有超管可以查看 signer 运营页")

    if request.method == "POST":
        return _handle_signer_modal_verification(request)

    try:
        # Signer 运营页属于系统级敏感观测入口，要求超管近期完成过 OTP。
        get_fresh_admin_sensitive_action_context(
            request=request,
            source="signer_overview",
        )
    except AdminOTPRequiredError:
        # 对已登录超管更合理的行为是页面内弹出 OTP 二次验证，而不是跳整页或直接 403。
        set_pending_admin_otp(
            request,
            user=request.user,
            next_path=request.get_full_path(),
        )
        return _render_signer_otp_modal(request, form=OTPVerifyForm())

    signer_summary = build_signer_dashboard_summary()
    overview_context = admin.site.each_context(request)
    overview_context.update(
        {
            "title": _("Signer 运营"),
            "signer_summary": signer_summary,
            "signer_health_rows": [
                {
                    "label": _("数据库"),
                    "value": (
                        _("正常")
                        if signer_summary
                        and signer_summary["available"]
                        and signer_summary["health"]["database"]
                        else _("异常")
                    ),
                },
                {
                    "label": _("缓存"),
                    "value": (
                        _("正常")
                        if signer_summary
                        and signer_summary["available"]
                        and signer_summary["health"]["cache"]
                        else _("异常")
                    ),
                },
                {
                    "label": _("共享密钥"),
                    "value": (
                        _("已配置")
                        if _signer_auth_configured(signer_summary)
                        else _("未配置")
                    ),
                },
            ],
        }
    )
    return render(request, "admin/signer_overview.html", overview_context)
