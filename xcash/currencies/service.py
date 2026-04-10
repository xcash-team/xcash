from __future__ import annotations

import typing

from chains.capabilities import ChainProductCapabilityService
from chains.service import ChainService
from currencies.models import Crypto
from currencies.models import Fiat

if typing.TYPE_CHECKING:
    from decimal import Decimal

    from django.db.models import QuerySet

    from chains.models import Chain
    from currencies.models import ChainToken
    from projects.models import Project


class CryptoService:
    """集中封装 Crypto 模型的常见读写操作。"""

    PLACEHOLDER_PREFIX = "PENDING"

    @staticmethod
    def list_all(*, active_only: bool = True) -> QuerySet[Crypto]:
        # active=False 的占位币只用于监听侧和后台治理，默认不暴露给正式业务入口。
        queryset = Crypto.objects.all()
        if active_only:
            queryset = queryset.filter(active=True)
        return queryset

    @staticmethod
    def get_by_symbol(symbol: str, *, active_only: bool = True) -> Crypto:
        # 正式业务默认只允许读取已激活资产；后台治理场景可显式放开 active_only=False。
        queryset = Crypto.objects.filter(symbol=symbol)
        if active_only:
            queryset = queryset.filter(active=True)
        return queryset.get()

    @staticmethod
    def exists(symbol: str, *, active_only: bool = True) -> bool:
        # 占位币不应被 invoice / withdrawal / deposit 地址申请等正式入口识别为可用资产。
        queryset = Crypto.objects.filter(symbol=symbol)
        if active_only:
            queryset = queryset.filter(active=True)
        return queryset.exists()

    @staticmethod
    def price(crypto: Crypto, fiat_code: str) -> Decimal:
        return crypto.price(fiat_code)

    @staticmethod
    def to_fiat(crypto: Crypto, fiat: Fiat, amount: Decimal) -> Decimal:
        return crypto.to_fiat(fiat, amount)

    @staticmethod
    def is_supported_on_chain(
        crypto: Crypto,
        *,
        chain_code: str | None = None,
        chain=None,
    ) -> bool:
        from chains.service import ChainService

        if chain is None and chain_code is None:
            raise ValueError("chain 或 chain_code 必须至少提供一个")

        target_chain = chain or ChainService.get_by_code(code=chain_code)
        return crypto.support_this_chain(target_chain)

    @staticmethod
    def allowed_methods(project: Project) -> dict[str, set[str]]:
        sanitized: dict[str, set[str]] = {}

        active_chains = list(ChainService.get_active_chains())
        # 支付方式只由已激活资产组成，防止占位币出现在商户或用户的可选项中。
        for crypto in CryptoService.list_all(active_only=True):
            available_codes = {
                chain.code
                for chain in active_chains
                if ChainProductCapabilityService.supports_invoice_method(
                    chain=chain,
                    crypto=crypto,
                )
            }
            if available_codes:
                sanitized[crypto.symbol] = available_codes

        return sanitized

    @classmethod
    def get_or_create_placeholder_chain_token(
        cls,
        *,
        chain: Chain,
        address: str,
    ) -> tuple[ChainToken, bool]:
        """为未知代币创建 inactive 占位资产，并返回其部署记录。

        设计目标：
        1. 监听层允许先接住未知代币，后续再由后台治理；
        2. 占位资产默认 inactive，不进入正式业务入口；
        3. (chain, address) 是真实身份，唯一约束负责兜底并发场景。
        """
        from django.db import IntegrityError
        from django.db import transaction

        from currencies.models import ChainToken

        existing = (
            ChainToken.objects.select_related("crypto")
            .filter(chain=chain, address=address)
            .first()
        )
        if existing is not None:
            return existing, False

        placeholder_key = f"{cls.PLACEHOLDER_PREFIX}:{chain.code}:{address.lower()}"
        placeholder_name = f"Pending {chain.code} {address.lower()}"

        crypto, _ = Crypto.objects.get_or_create(
            symbol=placeholder_key,
            defaults={
                "name": placeholder_name,
                "coingecko_id": placeholder_key,
                "active": False,
            },
        )

        try:
            with transaction.atomic():
                chain_token, created = ChainToken.objects.get_or_create(
                    crypto=crypto,
                    chain=chain,
                    defaults={"address": address},
                )
        except IntegrityError:
            # 并发下若另一个 webhook 已先写入同链同地址映射，直接复用现有部署记录即可。
            chain_token = ChainToken.objects.select_related("crypto").get(
                chain=chain,
                address=address,
            )
            return chain_token, False

        return chain_token, created


class FiatService:
    """封装法币模型的查询与转换逻辑。"""

    @staticmethod
    def list_all() -> QuerySet[Fiat]:
        return Fiat.objects.all()

    @staticmethod
    def get_by_code(code: str) -> Fiat:
        return Fiat.objects.get(code=code)

    @staticmethod
    def exists(code: str) -> bool:
        return Fiat.objects.filter(code=code).exists()

    @staticmethod
    def to_crypto(fiat: Fiat, crypto: Crypto, amount: Decimal) -> Decimal:
        return fiat.to_crypto(crypto, amount)

    @staticmethod
    def fiat_price(fiat: Fiat, target: Fiat) -> Decimal:
        return fiat.fiat_price(target)
