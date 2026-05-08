from pathlib import Path

from django.conf import settings
from django.http import Http404
from django.http import HttpResponse


def payment_view(request, sys_no=None):
    """
    托管支付前端 SPA（pay-fronted）。

    所有 /pay/* 请求都返回同一个 index.html，由 React 根据 URL 中的
    sys_no 读取对应 Invoice 并渲染支付页。静态资源（JS/CSS）由
    反向代理通过 /static/pay/ 直接托管，不经过此 view。
    """
    index_html = Path(settings.BASE_DIR) / "pay-fronted" / "dist" / "index.html"
    try:
        content = index_html.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise Http404(
            "支付前端尚未构建，请在 pay-fronted/ 目录下执行 pnpm build。"
        ) from exc
    return HttpResponse(content, content_type="text/html; charset=utf-8")
