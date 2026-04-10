from __future__ import annotations

from base58 import b58decode_check
from base58 import b58encode_check


class TronAddressCodec:
    ADDRESS_VERSION = b"\x41"
    ADDRESS_HEX_PREFIX = "41"
    ADDRESS_HEX_LENGTH = 42
    TOPIC_HEX_LENGTH = 64

    @classmethod
    def is_valid_base58(cls, value: str) -> bool:
        try:
            cls.normalize_base58(value)
        except ValueError:
            return False
        return True

    @classmethod
    def normalize_base58(cls, value: str) -> str:
        decoded = cls._decode_base58(value)
        return cls._encode_base58(decoded)

    @classmethod
    def base58_to_hex41(cls, value: str) -> str:
        return cls._decode_base58(value).hex()

    @classmethod
    def hex41_to_base58(cls, value: str) -> str:
        normalized_hex = cls._normalize_hex(value)
        if len(normalized_hex) != cls.ADDRESS_HEX_LENGTH:
            raise ValueError(f"invalid tron hex41 address: {value}")
        if not normalized_hex.startswith(cls.ADDRESS_HEX_PREFIX):
            raise ValueError(f"invalid tron hex41 prefix: {value}")
        return cls._encode_base58(bytes.fromhex(normalized_hex))

    @classmethod
    def topic_to_base58(cls, topic: str) -> str:
        normalized_topic = cls._normalize_hex(topic)
        if len(normalized_topic) == cls.TOPIC_HEX_LENGTH:
            return cls.hex41_to_base58(
                f"{cls.ADDRESS_HEX_PREFIX}{normalized_topic[-40:]}"
            )
        if len(normalized_topic) == cls.ADDRESS_HEX_LENGTH:
            return cls.hex41_to_base58(normalized_topic)
        raise ValueError(f"invalid tron topic: {topic}")

    @classmethod
    def _decode_base58(cls, value: str) -> bytes:
        try:
            decoded = b58decode_check(value.strip())
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"invalid tron base58 address: {value}") from exc

        if len(decoded) != 21:
            raise ValueError(f"invalid tron base58 address length: {value}")
        if not decoded.startswith(cls.ADDRESS_VERSION):
            raise ValueError(f"invalid tron base58 address prefix: {value}")
        return decoded

    @staticmethod
    def _encode_base58(value: bytes) -> str:
        return b58encode_check(value).decode("ascii")

    @staticmethod
    def _normalize_hex(value: str) -> str:
        normalized = value.strip().removeprefix("0x").removeprefix("0X").lower()
        try:
            bytes.fromhex(normalized)
        except ValueError as exc:
            raise ValueError(f"invalid hex value: {value}") from exc
        return normalized
