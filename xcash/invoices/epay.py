from __future__ import annotations

import hashlib
import hmac
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


EPAY_V1_SUCCESS_TEXT = "success"
EPAY_V1_TRADE_SUCCESS = "TRADE_SUCCESS"

_EPAY_V1_UNSIGNED_KEYS = {"sign", "sign_type"}
_EPAY_MONEY_QUANT = Decimal("0.01")


def format_epay_money(value: Decimal) -> str:
    return f"{value.quantize(_EPAY_MONEY_QUANT):.2f}"


def normalize_epay_value(value: object) -> str:
    if isinstance(value, Decimal):
        return format_epay_money(value)
    return str(value)


def epay_v1_signing_string(params: Mapping[str, object]) -> str:
    pairs: list[str] = []
    for key in sorted(params):
        if key in _EPAY_V1_UNSIGNED_KEYS:
            continue

        value = params[key]
        if value is None or value == "":
            continue

        pairs.append(f"{key}={normalize_epay_value(value)}")

    return "&".join(pairs)


def build_epay_v1_sign(params: Mapping[str, object], key: str) -> str:
    sign_source = f"{epay_v1_signing_string(params)}{key}"
    return hashlib.md5(sign_source.encode("utf-8"), usedforsecurity=False).hexdigest()


def verify_epay_v1_sign(params: Mapping[str, object], key: str) -> bool:
    supplied_sign = params.get("sign")
    if supplied_sign is None or supplied_sign == "":
        return False

    expected_sign = build_epay_v1_sign(params, key)
    return hmac.compare_digest(normalize_epay_value(supplied_sign), expected_sign)
