from __future__ import annotations

from decimal import ROUND_UP
from decimal import Decimal
from functools import cached_property

from django.core.cache import cache
from django.db import models
from django.utils.translation import gettext_lazy as _

from chains.models import Chain
from common.utils.math import round_decimal


class Crypto(models.Model):
    name = models.CharField(_("名称"), unique=True)
    symbol = models.CharField(_("代码"), help_text=_("例如:ETH、USDT"), unique=True)
    # 代币默认精度；链特定精度（如 BNB 链 USDT 使用 18）通过 ChainToken.decimals 覆盖
    decimals = models.PositiveSmallIntegerField(_("精度"), default=18)
    # M2M 关联，通过 ChainToken 中间表，保存合约地址和链特定精度等额外信息
    chains = models.ManyToManyField(
        Chain,
        through="ChainToken",
        related_name="cryptos",
        verbose_name=_("支持的链"),
        blank=True,
    )
    prices = models.JSONField(_("价格"), default=dict, blank=True)
    coingecko_id = models.CharField(unique=True, blank=True)
    active = models.BooleanField(default=True)

    class Meta:
        verbose_name = _("加密货币")
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.symbol}"

    def get_decimals(self, chain: Chain) -> int:
        """获取代币在指定链上的实际精度。

        优先使用 ChainToken 上的链特定覆盖值（解决如 BNB 链 USDT=18 的特例），
        未配置时回退到 Crypto.decimals 默认值。
        """
        try:
            ct = ChainToken.objects.get(crypto=self, chain=chain)
        except ChainToken.DoesNotExist:
            return self.decimals
        else:
            return ct.decimals if ct.decimals is not None else self.decimals

    def supported_chains(self) -> str:
        return ", ".join(self.chains.values_list("name", flat=True))

    @classmethod
    def all_methods(cls):
        # 通过 ChainToken 统一处理原生币和合约币，不再区分两种路径
        methods = {}
        for crypto in cls.objects.prefetch_related("chain_tokens__chain"):
            chain_codes = [ct.chain.code for ct in crypto.chain_tokens.all()]
            if chain_codes:
                methods[crypto.symbol] = chain_codes
        return methods

    @property
    def is_native(self):
        # 当前系统保留的原生币符号：EVM 系、Bitcoin 与 Tron。
        natives = ["ETH", "BSC", "POL", "BNB", "BTC", "TRX"]
        return self.symbol in natives

    def price(self, fiat):
        if fiat == "USD" and self.symbol in ["USDT", "USDC", "DAI"]:
            return Decimal("1")
        return Decimal(self.prices[fiat])

    def usd_amount(self, amount: Decimal) -> Decimal:
        """将代币数量换算为 USD 价值；无法获取价格时返回 0。"""
        try:
            return amount * self.price("USD")
        except (KeyError, Exception):
            return Decimal("0")

    @cached_property
    def scale(self):
        if scale := cache.get(f"{self.symbol}_scale"):
            return scale
        price_usd = self.price("USD")

        for i in range(-8, 8):
            if price_usd * Decimal("10") ** i > Decimal("0.01"):
                scale = i - 1
                cache.set(f"{self.symbol}_scale", value=scale, timeout=10)
                return scale
        raise ValueError("系统精度超出范围")

    @cached_property
    def differ_step(self):
        return Decimal("10") ** self.scale

    def to_fiat(self, fiat: Fiat, amount: Decimal) -> Decimal:
        return round_decimal(amount * self.price(fiat.code), -4)

    def gather_value(self, project: "Project"):  # noqa
        return project.gather_worth / self.price("USD") * 10**self.decimals

    def support_this_chain(self, chain: Chain) -> bool:
        # 通过 M2M chains 字段统一判断，原生币和合约币均在 ChainToken 中有记录
        return self.chains.filter(pk=chain.pk).exists()

    def address(self, chain: Chain) -> str:
        """获取代币在指定链上的合约地址；原生币的 address 为空字符串。"""
        try:
            return ChainToken.objects.get(crypto=self, chain=chain).address
        except ChainToken.DoesNotExist:
            return ""

    @property
    def icon(self):
        icons = {
            "ETH": "https://assets.coingecko.com/coins/images/279/standard/ethereum.png",
            "BNB": "https://assets.coingecko.com/coins/images/825/standard/bnb-icon2_2x.png",
            "USDC": "https://assets.coingecko.com/coins/images/6319/standard/usdc.png",
            "USDT": "https://assets.coingecko.com/coins/images/325/standard/Tether.png",
        }
        return icons.get(self.symbol, "")


class ChainToken(models.Model):
    """记录代币与链的部署关系，包含链上合约地址及可选的链特定精度覆盖。

    原生币（ETH/BTC 等）也在此建立记录，address 为空字符串，
    以使 support_this_chain / Balance 等逻辑能统一通过此表查询。
    """

    crypto = models.ForeignKey(
        Crypto,
        on_delete=models.CASCADE,
        related_name="chain_tokens",
        verbose_name=_("加密货币"),
    )
    chain = models.ForeignKey(
        Chain,
        on_delete=models.CASCADE,
        related_name="chain_tokens",
        verbose_name=_("链"),
    )
    # 合约地址；原生币为空字符串
    address = models.CharField(_("合约地址"), blank=True, default="", db_index=True)
    # 链特定精度覆盖：为 None 时使用 Crypto.decimals（如 BNB 链 USDT 设为 18 覆盖默认 6）
    decimals = models.PositiveSmallIntegerField(_("精度覆盖"), null=True, blank=True)

    class Meta:
        # 统一采用具名 UniqueConstraint，便于数据库约束报错定位和后续约束扩展。
        constraints = [
            models.UniqueConstraint(
                fields=("crypto", "chain"),
                name="uniq_chain_token_crypto_chain",
            ),
            # 同一条链上的同一个合约地址只能映射到一个资产，防止 webhook 解析歧义。
            models.UniqueConstraint(
                fields=("chain", "address"),
                name="uniq_chain_token_chain_address",
            ),
        ]
        verbose_name = _("代币部署")
        verbose_name_plural = _("代币部署")

    def __str__(self):
        return f"{self.crypto.symbol} @ {self.chain.code}"


class Fiat(models.Model):
    code = models.CharField(_("代码"), primary_key=True)

    class Meta:
        verbose_name = _("法币")
        verbose_name_plural = verbose_name

    def __str__(self):
        return f"{self.code} {self.icon}"

    def fiat_price(self, fiat: Fiat) -> Decimal:
        usdt = Crypto.objects.get(symbol="USDT")
        price0 = usdt.price(self.code)
        price1 = usdt.price(fiat.code)

        return price1 / price0

    def to_crypto(self, crypto: Crypto, amount: Decimal) -> Decimal:
        return round_decimal(
            amount / Decimal(crypto.price(self.code)),
            crypto.scale,
            rounding=ROUND_UP,
        )

    @classmethod
    def get(cls, code):
        return Fiat.objects.get(code=code)

    @property
    def icon(self):
        flags = {
            "CNY": "🇨🇳",
            "USD": "🇺🇸",
            "EUR": "🇪🇺",
            "GDB": "🇬🇧",
            "HKD": "🇭🇰",
            "JPY": "🇯🇵",
        }

        return flags.get(self.code, "")
