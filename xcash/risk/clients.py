from __future__ import annotations

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
    risk_detail: dict[str, Any]
    risk_report_url: str
    raw_response: dict[str, Any]


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
        response = httpx.post(self.endpoint_url, json=payload, timeout=5)
        response.raise_for_status()
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
            risk_detail=(
                result.get("risk_detail")
                if isinstance(result.get("risk_detail"), dict)
                else {}
            ),
            risk_report_url=str(result.get("risk_report_url") or ""),
            raw_response=result,
        )
