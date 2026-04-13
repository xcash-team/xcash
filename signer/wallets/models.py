from __future__ import annotations

from typing import TYPE_CHECKING

from bip_utils import Bip39Languages
from bip_utils import Bip39MnemonicGenerator
from bip_utils import Bip39MnemonicValidator
from bip_utils import Bip39SeedGenerator
from bip_utils import Bip39WordsNum
from bip_utils import Bip44
from bip_utils import Bip44Changes
from bip_utils import Bip44Coins
from bip_utils import Bip84
from bip_utils import Bip84Coins
from bitcoin_support.network import get_active_bitcoin_network

if TYPE_CHECKING:
    from bip_utils.bip.bip44_base import Bip44Base
from django.conf import settings
from django.db import models
from web3 import Web3

from wallets.crypto import AESCipher


class ChainType(models.TextChoices):
    EVM = "evm", "EVM"
    BITCOIN = "btc", "Bitcoin"


class SignerWallet(models.Model):
    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        FROZEN = "frozen", "Frozen"

    xcash_wallet_id = models.BigIntegerField(unique=True, db_index=True)
    encrypted_mnemonic = models.TextField()
    status = models.CharField(
        max_length=16,
        choices=Status,
        default=Status.ACTIVE,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("xcash_wallet_id",)
        verbose_name = "Signer 钱包"
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"SignerWallet(id={self.xcash_wallet_id}, status={self.status})"

    @staticmethod
    def _cipher() -> AESCipher:
        # 使用独立的助记词加密密钥，与 Django SECRET_KEY 解耦。
        return AESCipher(settings.SIGNER_MNEMONIC_ENCRYPTION_KEY)

    @classmethod
    def encrypt_mnemonic(cls, mnemonic: str) -> str:
        return cls._cipher().encrypt(mnemonic)

    @property
    def mnemonic(self) -> str:
        return self._cipher().decrypt(self.encrypted_mnemonic)

    @classmethod
    def validate_mnemonic(cls, mnemonic: str) -> str:
        normalized = " ".join(mnemonic.strip().split())
        validator = Bip39MnemonicValidator(Bip39Languages.ENGLISH)
        if not validator.IsValid(normalized):
            raise ValueError("助记词格式无效")
        return normalized

    @classmethod
    def generate_mnemonic(cls) -> str:
        # 使用 24 词（256 bit 熵），与行业标准（Ledger / Trezor）一致，提供最大安全边界。
        return (
            Bip39MnemonicGenerator(Bip39Languages.ENGLISH)
            .FromWordsNumber(Bip39WordsNum.WORDS_NUM_24)
            .ToStr()
        )

    @staticmethod
    def get_bip_coin_of_chain(chain_type: str) -> Bip44Coins:
        if chain_type == ChainType.EVM:
            return Bip44Coins.ETHEREUM
        if chain_type == ChainType.BITCOIN:
            return get_active_bitcoin_network().bip44_coin
        raise NotImplementedError(f"unsupported chain_type={chain_type}")

    @staticmethod
    def get_bip84_coin_of_chain(chain_type: str) -> Bip84Coins:
        if chain_type == ChainType.BITCOIN:
            return get_active_bitcoin_network().bip84_coin
        raise NotImplementedError(f"BIP84 不支持 chain_type={chain_type}")

    def _get_bip44_leaf_ctx(
        self, chain_type: str, *, bip44_account: int, address_index: int
    ) -> Bip44Base:
        """派生 HD 钱包叶子节点，根据链类型选择不同的 BIP 标准。

        - EVM：BIP44 路径 m/44'/60'/account'/0/index
        - Bitcoin：BIP84 路径 m/84'/coin'/account'/0/index（Native SegWit / P2WPKH）

        bip44_account 区分用途（0=Deposit, 1=Vault），address_index 为该用途下的地址序号。
        change 固定为 external（0），与标准 HD 钱包兼容。
        """
        seed_bytes = Bip39SeedGenerator(self.mnemonic).Generate()
        if chain_type == ChainType.BITCOIN:
            bip_obj = Bip84.FromSeed(seed_bytes, self.get_bip84_coin_of_chain(chain_type))
        else:
            bip_obj = Bip44.FromSeed(seed_bytes, self.get_bip_coin_of_chain(chain_type))
        return (
            bip_obj
            .Purpose()
            .Coin()
            .Account(bip44_account)
            .Change(Bip44Changes.CHAIN_EXT)
            .AddressIndex(address_index)
        )

    def derive_address(
        self, *, chain_type: str, bip44_account: int, address_index: int
    ) -> str:
        return (
            self._get_bip44_leaf_ctx(
                chain_type, bip44_account=bip44_account, address_index=address_index
            )
            .PublicKey()
            .ToAddress()
        )

    def private_key_hex(
        self, *, chain_type: str, bip44_account: int, address_index: int
    ) -> str:
        # 使用 .ToBytes().hex() 而非 str()，避免 __repr__ 在异常追踪中泄露私钥。
        return (
            self._get_bip44_leaf_ctx(
                chain_type, bip44_account=bip44_account, address_index=address_index
            )
            .PrivateKey()
            .Raw()
            .ToBytes()
            .hex()
        )

    def derive_key_pair(
        self, *, chain_type: str, bip44_account: int, address_index: int
    ) -> tuple[str, str]:
        """一次派生同时返回 (address, private_key_hex)，避免重复解密助记词。"""
        ctx = self._get_bip44_leaf_ctx(
            chain_type, bip44_account=bip44_account, address_index=address_index
        )
        address = ctx.PublicKey().ToAddress()
        privkey = ctx.PrivateKey().Raw().ToBytes().hex()
        return address, privkey


class SignerAddress(models.Model):
    """记录 signer 自己派生出的系统内地址，供内部地址判定使用。"""

    wallet = models.ForeignKey(
        SignerWallet,
        on_delete=models.CASCADE,
        related_name="addresses",
        verbose_name="Signer 钱包",
    )
    chain_type = models.CharField(
        max_length=16,
        choices=ChainType.choices,
        verbose_name="链类型",
    )
    # BIP44 account' 层级，区分用途（0=Deposit, 1=Vault）。
    bip44_account = models.PositiveIntegerField(verbose_name="BIP44 账户层级")
    # BIP44 address_index 层级，该用途下的地址序号。
    address_index = models.PositiveIntegerField(verbose_name="地址索引")
    address = models.CharField(max_length=128, verbose_name="地址", db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("wallet_id", "chain_type", "bip44_account", "address_index")
        constraints = [
            # (wallet, chain_type, bip44_account, address_index) 是 BIP44 派生地址的唯一身份。
            models.UniqueConstraint(
                fields=("wallet", "chain_type", "bip44_account", "address_index"),
                name="uniq_signer_addr_wallet_chain_bip44acc_addridx",
            ),
            models.UniqueConstraint(
                fields=("chain_type", "address"),
                name="uniq_signer_addr_chain_address",
            ),
        ]
        verbose_name = "Signer 地址"
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return (
            f"{self.wallet.xcash_wallet_id}:{self.chain_type}"
            f":acc{self.bip44_account}/idx{self.address_index}"
        )

    @staticmethod
    def normalize_address(*, chain_type: str, address: str) -> str:
        # EVM 地址统一存 checksum，避免大小写差异破坏内部地址判定。
        if chain_type == ChainType.EVM:
            return Web3.to_checksum_address(address)
        return address

    @classmethod
    def register_derived_address(
        cls,
        *,
        wallet: SignerWallet,
        chain_type: str,
        bip44_account: int,
        address_index: int,
        address: str,
    ) -> SignerAddress:
        normalized_address = cls.normalize_address(
            chain_type=chain_type,
            address=address,
        )
        record, created = cls.objects.get_or_create(
            wallet=wallet,
            chain_type=chain_type,
            bip44_account=bip44_account,
            address_index=address_index,
            defaults={"address": normalized_address},
        )
        # 同一派生身份必须稳定映射到同一地址，发现数据漂移时立即报错。
        if not created and record.address != normalized_address:
            raise RuntimeError("SignerAddress 与派生地址不一致，拒绝继续使用")
        return record

    @classmethod
    def is_internal_address(cls, *, chain_type: str, address: str) -> bool:
        normalized_address = cls.normalize_address(
            chain_type=chain_type,
            address=address,
        )
        return cls.objects.filter(
            chain_type=chain_type,
            address=normalized_address,
        ).exists()


class SignerRequestAudit(models.Model):
    """记录 signer 请求的最小审计轨迹。

    审计只保留请求定位、结果和错误码，不记录助记词、私钥或完整原始交易。
    """

    class Status(models.TextChoices):
        SUCCEEDED = "succeeded", "Succeeded"
        FAILED = "failed", "Failed"
        RATE_LIMITED = "rate_limited", "Rate Limited"

    request_id = models.CharField(max_length=128, unique=True, db_index=True)
    endpoint = models.CharField(max_length=64)
    wallet_id = models.BigIntegerField(blank=True, null=True, db_index=True)
    chain_type = models.CharField(
        max_length=16,
        choices=ChainType.choices,
        blank=True,
        default="",
    )
    bip44_account = models.PositiveIntegerField(blank=True, null=True)
    address_index = models.PositiveIntegerField(blank=True, null=True)
    remote_ip = models.GenericIPAddressField(blank=True, null=True)
    status = models.CharField(max_length=16, choices=Status.choices)
    error_code = models.CharField(max_length=16, blank=True, default="")
    detail = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        verbose_name = "Signer 请求审计"
        verbose_name_plural = verbose_name

    def __str__(self) -> str:
        return f"SignerRequestAudit(id={self.pk}, endpoint={self.endpoint}, status={self.status})"
