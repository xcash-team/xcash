from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx
from risk.models import RiskLevel


@dataclass(frozen=True)
class MistTrackRiskResult:
    risk_level: str
    risk_score: Decimal | None
    detail_list: list[Any]
    risk_detail: list[dict[str, Any]]
    risk_report_url: str
    raw_response: dict[str, Any]


_DEFAULT_TIMEOUT = 5.0
_MAX_ATTEMPTS = 3
_BASE_BACKOFF = 0.5

# 用于擦除异常消息中可能携带的 secret（api_key/QuickNode endpoint token）
_API_KEY_REGEX = re.compile(r"(api_key=)[^&\s'\"]+", re.IGNORECASE)


def _scrub_secrets(message: str) -> str:
    return _API_KEY_REGEX.sub(r"\1***", message)


def _scrub_url(url: str) -> str:
    # QuickNode endpoint 形如 https://<subdomain>.quiknode.pro/<token>/，token 即 secret。
    # 仅保留 scheme+host，去掉 path/query 避免泄露 token。
    parsed = httpx.URL(url)
    return f"{parsed.scheme}://{parsed.host}"


def _request_with_retry(
    method: str,
    url: str,
    *,
    request_kwargs: dict[str, Any],
    error_label: str,
    scrub_message: bool,
) -> httpx.Response:
    """统一的带指数退避重试的 HTTP 调用。
    重试策略：仅对网络错误与 5xx 重试，4xx 直接抛出。最多 3 次，退避 0.5s/1s/2s + jitter。
    异常消息中的 api_key/URL token 在抛出前被擦除。
    """
    last_exc: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            response = httpx.request(method, url, **request_kwargs)
        except httpx.HTTPError as exc:
            last_exc = exc
            if attempt == _MAX_ATTEMPTS - 1:
                break
        else:
            status = response.status_code
            if status < 500:
                if status >= 400:
                    # 4xx 不重试，但要擦除响应中可能存在的敏感字符
                    body = response.text[:200]
                    msg = f"{error_label} HTTP {status}: {body}"
                    if scrub_message:
                        msg = _scrub_secrets(msg)
                    raise RuntimeError(msg)
                return response
            last_exc = RuntimeError(f"{error_label} HTTP {status}")
            if attempt == _MAX_ATTEMPTS - 1:
                break

        backoff = _BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.25)  # noqa: S311
        time.sleep(backoff)

    msg = f"{error_label} request failed: {last_exc}"
    if scrub_message:
        msg = _scrub_secrets(msg)
    raise RuntimeError(msg)


def _coerce_risk_detail(value: Any) -> list[dict[str, Any]]:
    """统一 risk_detail 为 list[dict]。
    - V3 官方返回 list[dict]
    - QuickNode add-on 历史返回 dict（如 {"sanction": 1}），适配为单元素 list 以保持类型不变
    - 其他情况返回空 list
    """
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict) and value:
        return [value]
    return []


class QuicknodeMistTrackClient:
    def __init__(self, *, endpoint_url: str):
        self.endpoint_url = endpoint_url

    def address_risk_score(self, *, chain: str, address: str) -> MistTrackRiskResult:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mt_addressRiskScore",
            "params": [{"chain": chain, "address": address}],
        }
        response = _request_with_retry(
            "POST",
            self.endpoint_url,
            request_kwargs={"json": payload, "timeout": _DEFAULT_TIMEOUT},
            # 不在 error_label 中泄露 endpoint，外层日志按 host 摘要
            error_label=f"QuickNode MistTrack({_scrub_url(self.endpoint_url)})",
            scrub_message=False,
        )

        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("QuickNode MistTrack returned non-object response")
        error = data.get("error")
        if error:
            if isinstance(error, dict):
                message = error.get("message") or str(error)
            else:
                message = str(error)
            raise RuntimeError(message)

        result = data.get("result")
        if not isinstance(result, dict):
            raise TypeError("QuickNode MistTrack response missing result")

        risk_level = result.get("risk_level")
        if risk_level not in RiskLevel.values:
            raise RuntimeError(f"unknown MistTrack risk level: {risk_level}")

        score = result.get("score")
        return MistTrackRiskResult(
            risk_level=str(risk_level),
            risk_score=Decimal(str(score)) if score is not None else None,
            detail_list=(
                result.get("detail_list")
                if isinstance(result.get("detail_list"), list)
                else []
            ),
            risk_detail=_coerce_risk_detail(result.get("risk_detail")),
            risk_report_url=str(result.get("risk_report_url") or ""),
            raw_response=result,
        )


class MistTrackOpenApiClient:
    endpoint_url = "https://openapi.misttrack.io/v3/risk_score"

    def __init__(self, *, api_key: str):
        self.api_key = api_key

    def address_risk_score(self, *, coin: str, address: str) -> MistTrackRiskResult:
        response = _request_with_retry(
            "GET",
            self.endpoint_url,
            request_kwargs={
                "params": {"coin": coin, "address": address, "api_key": self.api_key},
                "timeout": _DEFAULT_TIMEOUT,
            },
            error_label="MistTrack OpenAPI",
            # api_key 走 query string，任何异常消息都必须脱敏
            scrub_message=True,
        )

        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("MistTrack OpenAPI returned non-object response")
        if not data.get("success"):
            raise RuntimeError(str(data.get("msg") or "MistTrack OpenAPI request failed"))

        result = data.get("data")
        if not isinstance(result, dict):
            raise TypeError("MistTrack OpenAPI response missing data")

        risk_level = result.get("risk_level")
        if risk_level not in RiskLevel.values:
            raise RuntimeError(f"unknown MistTrack risk level: {risk_level}")

        score = result.get("score")
        return MistTrackRiskResult(
            risk_level=str(risk_level),
            risk_score=Decimal(str(score)) if score is not None else None,
            detail_list=(
                result.get("detail_list")
                if isinstance(result.get("detail_list"), list)
                else []
            ),
            risk_detail=_coerce_risk_detail(result.get("risk_detail")),
            risk_report_url=str(result.get("risk_report_url") or ""),
            raw_response=result,
        )
