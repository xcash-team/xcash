from decimal import Decimal

import structlog
from django.core.exceptions import ObjectDoesNotExist
from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import Serializer

logger = structlog.get_logger()

from chains.adapters import AdapterFactory
from chains.capabilities import ChainProductCapabilityService
from chains.models import AddressUsage
from chains.service import AddressService
from chains.service import ChainService
from common.consts import APPID_HEADER
from common.error_codes import ErrorCode
from common.exceptions import APIError
from currencies.service import CryptoService
from projects.models import Project
from withdrawals.models import Withdrawal
from withdrawals.service import WithdrawalService


class CreateWithdrawalSerializer(Serializer):
    out_no = serializers.CharField(required=True, max_length=128)
    to = serializers.CharField(required=True)
    uid = serializers.CharField(required=False, max_length=32, default=None)
    crypto = serializers.CharField(required=True)
    chain = serializers.CharField(required=True)
    amount = serializers.DecimalField(
        required=True,
        max_digits=32,
        decimal_places=8,
        min_value=Decimal("0.00000001"),
        max_value=Decimal("1000000"),
    )

    def _get_project(self):
        """统一获取并缓存 project，避免同一请求中多次查询。"""
        if not hasattr(self, "_cached_project"):
            request = self.context.get("request")
            self._cached_project = Project.retrieve(
                appid=request.headers.get(APPID_HEADER)
            )
        return self._cached_project

    def _get_chain(self, code: str):
        """统一获取并缓存 chain 对象。"""
        if not hasattr(self, "_cached_chain"):
            self._cached_chain = ChainService.get_by_code(code)
        return self._cached_chain

    def _get_crypto(self, symbol: str):
        """统一获取并缓存 crypto 对象。"""
        if not hasattr(self, "_cached_crypto"):
            self._cached_crypto = CryptoService.get_by_symbol(symbol)
        return self._cached_crypto

    def validate_out_no(self, value):
        project = self._get_project()
        # APPID 无效时提前报错，避免 filter(project=None) 静默通过唯一性检查
        if project is None:
            raise ValidationError(detail=ErrorCode.INVALID_APPID.to_payload())
        if Withdrawal.objects.filter(project=project, out_no=value).exists():
            raise ValidationError(detail=ErrorCode.DUPLICATE_OUT_NO.to_payload())
        return value

    def validate_crypto(self, value):
        if not CryptoService.exists(value):
            raise ValidationError(detail=ErrorCode.INVALID_CRYPTO.to_payload())
        return value

    def validate_chain(self, value):
        try:
            self._get_chain(value)
        except ObjectDoesNotExist as exc:
            raise ValidationError(detail=ErrorCode.INVALID_CHAIN.to_payload()) from exc
        return value

    def validate(self, attrs):
        project = self._get_project()
        if project is None:
            raise APIError(ErrorCode.INVALID_APPID)

        chain = self._get_chain(attrs["chain"])
        crypto = self._get_crypto(attrs["crypto"])
        adapter = AdapterFactory.get_adapter(chain_type=chain.type)

        # 1. 链+币种组合校验（本地）
        if not CryptoService.is_supported_on_chain(crypto, chain=chain):
            raise APIError(ErrorCode.CHAIN_CRYPTO_NOT_SUPPORT)
        if not ChainProductCapabilityService.supports_withdrawal(
            chain=chain,
            crypto=crypto,
        ):
            raise APIError(ErrorCode.INVALID_CHAIN)

        # 2. 内部地址保护：禁止提币到平台自有地址（按链类型过滤，避免 EVM 跨链地址误判）
        if AddressService.find_by_address(address=attrs["to"], chain_type=chain.type):
            raise APIError(ErrorCode.INVALID_TO_ADDRESS)

        # 3. 地址合法性校验（本地 + 可选 RPC）
        if not self._is_valid_address(chain=chain, adapter=adapter, to=attrs["to"]):
            raise APIError(ErrorCode.INVALID_ADDRESS)

        # 4. 最小链上单位校验
        if int(attrs["amount"] * (10 ** crypto.get_decimals(chain))) <= 0:
            raise APIError(ErrorCode.PARAMETER_ERROR)

        # 注意：项目风控策略（限额/日限额）在 viewset 锁内由 assert_project_policy 执行，
        # serializer 层不重复校验——无锁检查对并发无保护作用，反而浪费 DB 查询。

        # 5. 余额校验（RPC，最重量级，放最后）
        vault_address = project.wallet.get_address(
            chain_type=chain.type,
            usage=AddressUsage.Vault,
        )
        if not WithdrawalService.has_sufficient_balance(
            project=project,
            chain=chain,
            crypto=crypto,
            address=vault_address.address,
            amount=attrs["amount"],
            adapter=adapter,
        ):
            raise APIError(ErrorCode.INSUFFICIENT_BALANCE)

        return attrs

    @staticmethod
    def _is_valid_address(*, chain, adapter, to: str) -> bool:
        # is_contract 需要实时调用节点 RPC，节点故障时不应阻断提币，降级为跳过合约检查
        try:
            is_contract = adapter.is_contract(chain, to)
        except Exception:
            logger.warning(
                "is_contract 检查失败，跳过合约地址验证",
                to=to,
                chain=chain.code,
            )
            is_contract = False

        return not is_contract and adapter.validate_address(to)
