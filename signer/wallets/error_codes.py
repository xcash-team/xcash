from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from enum import unique


@dataclass(frozen=True)
class ErrorInfo:
    code: str
    message: str
    status: int


@unique
class ErrorCode(Enum):
    # signer 只保留自己真实会返回的最小错误码集合，避免继续依赖主应用完整错误域。
    PARAMETER_ERROR = ErrorInfo("1000", "参数错误", 400)
    ACCESS_DENY = ErrorInfo("1005", "无访问权限", 403)
    SIGNATURE_ERROR = ErrorInfo("1003", "签名错误", 403)
    REPLAY_ATTACK = ErrorInfo("1009", "请求重复", 400)
    RATE_LIMIT_EXCEEDED = ErrorInfo("1010", "请求过于频繁", 429)

    def __init__(self, info: ErrorInfo):
        self._info = info

    @property
    def code(self) -> str:
        return self._info.code

    @property
    def message(self) -> str:
        return self._info.message

    @property
    def status(self) -> int:
        return self._info.status

    def to_payload(self, detail: str = "") -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "detail": detail,
        }
