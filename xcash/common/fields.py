from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from shortuuid import ShortUUID
from shortuuid.django_fields import ShortUUIDField
from web3 import Web3

from common.consts import LENGTH_OF_EVM_HASH
from common.consts import UPPER_ALPHABET
from common.utils.bitcoin import is_valid_bitcoin_address
from tron.codec import TronAddressCodec


class HashField(models.CharField):
    def __init__(self, *args, **kwargs):
        # 修复：默认保持唯一，但允许具体业务模型按需覆盖。
        kwargs.setdefault("unique", True)
        kwargs.setdefault("db_index", True)
        kwargs.setdefault("verbose_name", _("哈希"))
        # 覆盖当前支持链哈希最大长度：EVM=66，Bitcoin=64。
        # 统一设为 100，留有余量，不依赖调用方传入
        kwargs.setdefault("max_length", 100)

        super().__init__(*args, **kwargs)

    def pre_save(self, model_instance, add):
        value = getattr(model_instance, self.attname)

        # 当前系统仅接受真实链上交易哈希，转账明细唯一性统一由业务字段单独表达。
        if value is not None and not any(
            (
                is_valid_evm_256bit_hex_string(value),
                is_valid_bitcoin_256bit_hex_string(value),
            )
        ):
            msg = f"{value} is not a valid blockchain hash"
            raise ValueError(msg)

        return value

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        # 默认 unique=True；当业务显式覆盖为 False 时，必须把该状态写回迁移，避免反复漂移。
        if self.unique is False:
            kwargs["unique"] = False
        return name, path, args, kwargs


class EvmAddressField(models.CharField):
    def __init__(self, *args, **kwargs):
        kwargs["db_index"] = True
        # EVM 地址固定 42 字符（0x + 40 hex）
        kwargs.setdefault("max_length", 42)

        super().__init__(*args, **kwargs)

    def pre_save(self, model_instance, add):
        value = getattr(model_instance, self.attname)
        if value and not Web3.is_checksum_address(value):
            msg = f"{value} is not a valid ethereum address"
            raise ValueError(msg)

        return value


class AddressField(models.CharField):
    def __init__(self, *args, **kwargs):
        kwargs["db_index"] = True
        # 覆盖当前支持链地址最大长度：EVM=42, Bitcoin bech32/taproot 可更长。
        kwargs.setdefault("max_length", 100)

        super().__init__(*args, **kwargs)

    def pre_save(self, model_instance, add):
        value = getattr(model_instance, self.attname)
        # 依次匹配当前支持链地址格式：EVM checksum / Bitcoin。
        if value and not any(
            (
                Web3.is_checksum_address(value),
                is_valid_bitcoin_address(value),
                TronAddressCodec.is_valid_base58(value),
            )
        ):
            msg = f"{value} is not a valid address"
            raise ValueError(msg)

        return value


def is_valid_evm_256bit_hex_string(s: str) -> bool:
    if len(s) != LENGTH_OF_EVM_HASH:
        return False

    if not s.startswith("0x"):
        return False

    hex_digits = set("0123456789abcdefABCDEF")
    return all(c in hex_digits for c in s[2:])


def is_valid_bitcoin_256bit_hex_string(s: str) -> bool:
    if len(s) != 64:
        return False

    hex_digits = set("0123456789abcdefABCDEF")
    return all(c in hex_digits for c in s)


class SysNoField(ShortUUIDField):
    @staticmethod
    def get_current_date_str():
        return timezone.now().strftime("%y%m%d")

    def __init__(self, *args, **kwargs):
        kwargs["length"] = kwargs.pop("length", 8)
        kwargs["max_length"] = 32
        kwargs["unique"] = True
        kwargs["db_index"] = True
        kwargs["verbose_name"] = _("系统单号")
        kwargs["editable"] = False
        super().__init__(*args, **kwargs)

    def _generate_uuid(self) -> str:
        """Generate a short random string."""
        return (
            self.prefix
            + self.get_current_date_str()
            + ShortUUID(alphabet=UPPER_ALPHABET).random(length=self.length)
        )
