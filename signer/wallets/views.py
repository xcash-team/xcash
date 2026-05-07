from __future__ import annotations

import hashlib
import hmac
import json
from datetime import timedelta

from django.conf import settings
from django.core.cache import cache
from django.db import connections
from django.db.models import Count
from django.db.models import Q
from django.http import JsonResponse
from django.utils import timezone
from rest_framework import serializers
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from wallets.models import ChainType
from wallets.models import SignerAddress
from wallets.models import SignerRequestAudit
from wallets.models import SignerWallet
from web3 import Web3

from wallets.error_codes import ErrorCode

SIGNER_REQUEST_ID_HEADER = "X-Signer-Request-Id"
SIGNER_SIGNATURE_HEADER = "X-Signer-Signature"
ERC20_TRANSFER_SELECTOR = "0xa9059cbb"


def build_signer_signature_payload(
    *,
    method: str,
    path: str,
    request_id: str,
    request_body: bytes,
) -> bytes:
    """构造 signer 请求签名材料。

    HMAC 必须同时绑定 method/path/request_id/body，避免攻击者仅替换 request_id
    就绕过“重放保护 + 请求鉴权”的组合边界。
    """

    return b"\n".join(
        [
            method.upper().encode("utf-8"),
            path.encode("utf-8"),
            request_id.encode("utf-8"),
            request_body,
        ]
    )


def _build_health_payload() -> dict:
    """healthz 与内部摘要共用同一份探针结果，避免两套健康标准逐渐漂移。

    返回的详细字段仅供内部摘要使用；对外 healthz 只暴露 ok/fail。
    """
    database_ok = False
    cache_ok = False
    auth_configured = bool(settings.SIGNER_SHARED_SECRET)

    try:
        with connections["default"].cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        database_ok = True
    except Exception:
        database_ok = False

    try:
        cache.set("signer-project:healthz", "ok", timeout=5)
        cache_ok = cache.get("signer-project:healthz") == "ok"
    except Exception:
        cache_ok = False

    healthy = database_ok and cache_ok and auth_configured
    return {
        "database": database_ok,
        "cache": cache_ok,
        "auth_configured": auth_configured,
        "healthy": healthy,
    }


def healthz(_request):
    # 对外仅暴露 ok/fail，不泄露数据库/缓存/密钥配置等基础设施状态。
    payload = _build_health_payload()
    return JsonResponse(
        {"ok": payload["healthy"]},
        status=200 if payload["healthy"] else 503,
    )


class SignerAPIError(Exception):
    def __init__(self, error_code: ErrorCode, detail: str = ""):
        self.error_code = error_code
        self.detail = detail
        super().__init__(detail)

    def to_response(self) -> JsonResponse:
        return JsonResponse(
            self.error_code.to_payload(self.detail),
            status=self.error_code.status,
        )


class SignerAPIView(APIView):
    authentication_classes = []
    permission_classes = []

    @staticmethod
    def _normalize_hex(value: str) -> str:
        return value if value.startswith("0x") else f"0x{value}"

    @staticmethod
    def _cache_key(request_id: str) -> str:
        return f"signer-project:request:{request_id}"

    @staticmethod
    def _rate_limit_key(*, remote_ip: str, endpoint: str) -> str:
        return f"signer-project:rate-limit:{endpoint}:{remote_ip}"

    @staticmethod
    def _wallet_sign_rate_limit_key(*, wallet_id: int, endpoint: str) -> str:
        return f"signer-project:wallet-sign-rate:{endpoint}:{wallet_id}"

    @staticmethod
    def _should_record_audit(*, endpoint: str, is_failure: bool = False) -> bool:
        # /internal 只读接口的成功请求不混入主签名审计流，但鉴权失败必须记录以便追溯暴力枚举。
        if endpoint.startswith("/internal/"):
            return is_failure
        return True

    @staticmethod
    def _request_json_data(request) -> dict:
        if not request.body:
            return {}
        try:
            parsed = json.loads(request.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _request_meta(self, request) -> dict:
        data = self._request_json_data(request)
        return {
            "request_id": request.headers.get(SIGNER_REQUEST_ID_HEADER, "").strip(),
            "endpoint": request.path,
            "wallet_id": data.get("wallet_id"),
            "chain_type": str(data.get("chain_type", "")),
            "bip44_account": data.get("bip44_account"),
            "address_index": data.get("address_index"),
            "remote_ip": request.META.get("REMOTE_ADDR", ""),
        }

    def _record_audit(
        self,
        *,
        request,
        status_value: SignerRequestAudit.Status,
        error_code: ErrorCode | None = None,
        detail: str = "",
    ) -> None:
        meta = self._request_meta(request)
        is_failure = status_value != SignerRequestAudit.Status.SUCCEEDED
        if not meta["request_id"] or not self._should_record_audit(
            endpoint=meta["endpoint"], is_failure=is_failure
        ):
            return
        # 审计记录只写不改，保证取证完整性；重复 request_id 静默跳过。
        if SignerRequestAudit.objects.filter(request_id=meta["request_id"]).exists():
            return
        # bip44_account / address_index 来自原始请求数据，可能非法（负数等），审计表使用 PositiveIntegerField 需做防护。
        raw_bip44_account = meta["bip44_account"]
        safe_bip44_account = (
            raw_bip44_account
            if isinstance(raw_bip44_account, int) and raw_bip44_account >= 0
            else None
        )
        raw_address_index = meta["address_index"]
        safe_address_index = (
            raw_address_index
            if isinstance(raw_address_index, int) and raw_address_index >= 0
            else None
        )
        SignerRequestAudit.objects.create(
            request_id=meta["request_id"],
            endpoint=meta["endpoint"],
            wallet_id=meta["wallet_id"],
            chain_type=meta["chain_type"],
            bip44_account=safe_bip44_account,
            address_index=safe_address_index,
            remote_ip=meta["remote_ip"],
            status=status_value,
            error_code=error_code.code if error_code else "",
            detail=detail[:255],
        )

    @staticmethod
    def _atomic_rate_check(cache_key: str, limit: int, window: int) -> bool:
        """原子化速率限制，返回是否超限。

        生产环境（django_redis）使用 Lua 脚本保证 INCR + EXPIRE 原子性；
        测试环境（LocMemCache 等）回退到 cache.add + cache.incr。
        """
        # 尝试使用 Redis Lua 脚本（原子化，无 TOCTOU 竞争）。
        try:
            redis_client = cache.client.get_client()  # type: ignore[union-attr]
            lua_script = """
            local current = redis.call('INCR', KEYS[1])
            if current == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            return current
            """
            current = redis_client.eval(lua_script, 1, cache_key, window)
            return int(current) > limit
        except AttributeError:
            # 非 django_redis 后端（如 LocMemCache），回退到非原子方式（仅用于测试）。
            if cache.add(cache_key, 1, timeout=window):
                return False
            current = cache.incr(cache_key)
            return current > limit

    def _assert_rate_limit(self, request) -> None:
        if settings.DEBUG:
            return
        remote_ip = self._request_meta(request)["remote_ip"] or "unknown"
        cache_key = self._rate_limit_key(remote_ip=remote_ip, endpoint=request.path)
        if self._atomic_rate_check(
            cache_key,
            settings.SIGNER_RATE_LIMIT_MAX_REQUESTS,
            settings.SIGNER_RATE_LIMIT_WINDOW,
        ):
            raise SignerAPIError(ErrorCode.RATE_LIMIT_EXCEEDED)

    def _assert_wallet_can_sign(self, *, wallet: SignerWallet) -> None:
        # signer 自己保留钱包冻结开关，避免主应用被绕过时某个钱包继续被无限签名。
        if wallet.status != SignerWallet.Status.ACTIVE:
            raise SignerAPIError(ErrorCode.ACCESS_DENY, "wallet 已冻结")

    def _assert_wallet_sign_rate_limit(
        self, *, wallet: SignerWallet, endpoint: str
    ) -> None:
        if settings.DEBUG:
            return
        cache_key = self._wallet_sign_rate_limit_key(
            wallet_id=wallet.xcash_wallet_id,
            endpoint=endpoint,
        )
        if self._atomic_rate_check(
            cache_key,
            settings.SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS,
            settings.SIGNER_WALLET_SIGN_RATE_LIMIT_WINDOW,
        ):
            raise SignerAPIError(
                ErrorCode.RATE_LIMIT_EXCEEDED, "wallet 签名请求过于频繁"
            )

    # ERC20 transfer 的标准 ABI 编码长度：selector(10) + address(64) + amount(64) = 138 字符。
    _ERC20_TRANSFER_DATA_LEN = 10 + 64 + 64

    @staticmethod
    def _resolve_evm_policy_recipient(*, tx_dict: dict) -> str:
        data = str(tx_dict.get("data") or "0x").lower()
        # ERC20 标准 transfer 的真实接收方不在 tx.to，而在 calldata 里；内部地址判定必须解析它。
        # 仅当 data 长度精确匹配标准 ERC20 transfer 时才解析，避免恶意构造绕过速率限制。
        if (
            data.startswith(ERC20_TRANSFER_SELECTOR)
            and len(data) == SignerAPIView._ERC20_TRANSFER_DATA_LEN
        ):
            encoded_address = data[10 : 10 + 64]
            return Web3.to_checksum_address(f"0x{encoded_address[-40:]}")
        return Web3.to_checksum_address(str(tx_dict["to"]))

    @staticmethod
    def _is_internal_destination(*, chain_type: str, address: str) -> bool:
        return SignerAddress.is_internal_address(
            chain_type=chain_type,
            address=address,
        )

    def _assert_authenticated(self, request) -> None:
        if not settings.SIGNER_SHARED_SECRET:
            raise SignerAPIError(ErrorCode.ACCESS_DENY, "signer 未配置共享密钥")

        request_id = request.headers.get(SIGNER_REQUEST_ID_HEADER, "").strip()
        signature = request.headers.get(SIGNER_SIGNATURE_HEADER, "").strip()
        if not request_id or not signature:
            raise SignerAPIError(ErrorCode.PARAMETER_ERROR, "缺少 signer 鉴权头")

        expected_signature = hmac.new(
            settings.SIGNER_SHARED_SECRET.encode("utf-8"),
            build_signer_signature_payload(
                method=request.method,
                path=request.path,
                request_id=request_id,
                request_body=request.body,
            ),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            raise SignerAPIError(ErrorCode.SIGNATURE_ERROR)

        if not cache.add(
            self._cache_key(request_id),
            "1",
            timeout=settings.SIGNER_REQUEST_TTL,
        ):
            raise SignerAPIError(ErrorCode.REPLAY_ATTACK)

    @staticmethod
    def _load_wallet(
        xcash_wallet_id: int, *, for_signing: bool = False
    ) -> SignerWallet:
        """加载钱包。签名操作使用 select_for_update 保证冻结操作立即互斥。"""
        try:
            qs = SignerWallet.objects.all()
            if for_signing:
                qs = qs.select_for_update(nowait=False)
            return qs.get(xcash_wallet_id=xcash_wallet_id)
        except SignerWallet.DoesNotExist as exc:
            raise SignerAPIError(ErrorCode.PARAMETER_ERROR, "wallet_id 无效") from exc

    def dispatch(self, request, *args, **kwargs):
        try:
            # signer 先按入口做基础速率保护，防止在线签名服务被单点请求打爆。
            self._assert_rate_limit(request)
            return super().dispatch(request, *args, **kwargs)
        except SignerAPIError as exc:
            status_value = (
                SignerRequestAudit.Status.RATE_LIMITED
                if exc.error_code == ErrorCode.RATE_LIMIT_EXCEEDED
                else SignerRequestAudit.Status.FAILED
            )
            self._record_audit(
                request=request,
                status_value=status_value,
                error_code=exc.error_code,
                detail=exc.detail,
            )
            return exc.to_response()


class WalletIdSerializer(serializers.Serializer):
    wallet_id = serializers.IntegerField(min_value=1, required=False)

    def validate(self, attrs: dict) -> dict:
        wallet_id = attrs.get("wallet_id")
        if wallet_id is None:
            raise serializers.ValidationError("缺少 wallet_id")
        return attrs


class CreateWalletSerializer(WalletIdSerializer):
    pass


class DeriveAddressSerializer(serializers.Serializer):
    wallet_id = serializers.IntegerField(min_value=1)
    chain_type = serializers.ChoiceField(choices=ChainType.choices)
    bip44_account = serializers.IntegerField(
        min_value=0, max_value=settings.SIGNER_MAX_BIP44_ACCOUNT
    )
    address_index = serializers.IntegerField(
        min_value=0, max_value=settings.SIGNER_MAX_ADDRESS_INDEX
    )


class SignEvmSerializer(serializers.Serializer):
    wallet_id = serializers.IntegerField(min_value=1)
    chain_type = serializers.ChoiceField(choices=[ChainType.EVM])
    bip44_account = serializers.IntegerField(
        min_value=0, max_value=settings.SIGNER_MAX_BIP44_ACCOUNT
    )
    address_index = serializers.IntegerField(
        min_value=0, max_value=settings.SIGNER_MAX_ADDRESS_INDEX
    )
    tx_dict = serializers.DictField()

    def validate_tx_dict(self, value: dict) -> dict:
        required_keys = {
            "chainId",
            "nonce",
            "from",
            "to",
            "value",
            "data",
            "gas",
            "gasPrice",
        }
        missing = sorted(required_keys - set(value.keys()))
        if missing:
            raise serializers.ValidationError(f"缺少字段: {', '.join(missing)}")

        try:
            value["from"] = Web3.to_checksum_address(str(value["from"]))
            value["to"] = Web3.to_checksum_address(str(value["to"]))
        except ValueError as exc:
            raise serializers.ValidationError("from/to 地址格式无效") from exc

        if not isinstance(value.get("data"), str) or not value["data"].startswith("0x"):
            raise serializers.ValidationError("data 必须是 0x 开头的十六进制字符串")
        # 限制 calldata 最大长度，防止恶意构造超大 data 消耗 signer 资源。
        if len(value["data"]) > 2 + 64 * 20:
            raise serializers.ValidationError("data 字段过长")
        return value


class InternalAdminSummaryView(SignerAPIView):
    def get(self, request):
        self._assert_authenticated(request)
        now = timezone.now()
        last_hour = now - timedelta(hours=1)
        # 内部只读摘要只返回运营观测信息，不暴露助记词、私钥或原始交易载荷。
        wallet_summary = SignerWallet.objects.aggregate(
            total=Count("id"),
            active=Count("id", filter=Q(status=SignerWallet.Status.ACTIVE)),
            frozen=Count("id", filter=Q(status=SignerWallet.Status.FROZEN)),
        )
        request_summary = SignerRequestAudit.objects.filter(
            created_at__gte=last_hour,
            endpoint__startswith="/v1/",
        ).aggregate(
            total=Count("id"),
            succeeded=Count("id", filter=Q(status=SignerRequestAudit.Status.SUCCEEDED)),
            failed=Count("id", filter=Q(status=SignerRequestAudit.Status.FAILED)),
            rate_limited=Count(
                "id",
                filter=Q(status=SignerRequestAudit.Status.RATE_LIMITED),
            ),
        )
        recent_anomalies = list(
            SignerRequestAudit.objects.filter(
                endpoint__startswith="/v1/",
                status__in=[
                    SignerRequestAudit.Status.FAILED,
                    SignerRequestAudit.Status.RATE_LIMITED,
                ],
            )
            .order_by("-created_at")[:5]
            .values(
                "request_id",
                "endpoint",
                "wallet_id",
                "chain_type",
                "bip44_account",
                "address_index",
                "status",
                "error_code",
                "detail",
                "created_at",
            )
        )
        for row in recent_anomalies:
            row["created_at"] = row["created_at"].isoformat()

        return Response(
            {
                "health": _build_health_payload(),
                "wallets": {
                    "total": int(wallet_summary["total"] or 0),
                    "active": int(wallet_summary["active"] or 0),
                    "frozen": int(wallet_summary["frozen"] or 0),
                },
                "requests_last_hour": {
                    "total": int(request_summary["total"] or 0),
                    "succeeded": int(request_summary["succeeded"] or 0),
                    "failed": int(request_summary["failed"] or 0),
                    "rate_limited": int(request_summary["rate_limited"] or 0),
                },
                "recent_anomalies": recent_anomalies,
            },
            status=status.HTTP_200_OK,
        )


class CreateWalletView(SignerAPIView):
    def post(self, request):
        self._assert_authenticated(request)
        serializer = CreateWalletSerializer(data=request.data)
        if not serializer.is_valid():
            raise SignerAPIError(ErrorCode.PARAMETER_ERROR, str(serializer.errors))

        wallet_id = serializer.validated_data["wallet_id"]
        wallet, created = SignerWallet.objects.get_or_create(
            xcash_wallet_id=wallet_id,
            defaults={
                "encrypted_mnemonic": SignerWallet.encrypt_mnemonic(
                    SignerWallet.generate_mnemonic()
                )
            },
        )
        self._record_audit(
            request=request,
            status_value=SignerRequestAudit.Status.SUCCEEDED,
        )
        return Response(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "created": created,
            },
            status=status.HTTP_200_OK,
        )


class DeriveAddressView(SignerAPIView):
    def post(self, request):
        self._assert_authenticated(request)
        serializer = DeriveAddressSerializer(data=request.data)
        if not serializer.is_valid():
            raise SignerAPIError(ErrorCode.PARAMETER_ERROR, str(serializer.errors))

        data = serializer.validated_data
        wallet = self._load_wallet(data["wallet_id"])
        address = wallet.derive_address(
            chain_type=data["chain_type"],
            bip44_account=data["bip44_account"],
            address_index=data["address_index"],
        )
        # 派生地址在 signer 侧落库，后续签名前由 signer 自己判定该地址是否属于系统内地址。
        SignerAddress.register_derived_address(
            wallet=wallet,
            chain_type=data["chain_type"],
            bip44_account=data["bip44_account"],
            address_index=data["address_index"],
            address=address,
        )
        self._record_audit(
            request=request,
            status_value=SignerRequestAudit.Status.SUCCEEDED,
        )
        return Response({"address": address}, status=status.HTTP_200_OK)


class SignEvmView(SignerAPIView):
    def post(self, request):
        self._assert_authenticated(request)
        serializer = SignEvmSerializer(data=request.data)
        if not serializer.is_valid():
            raise SignerAPIError(ErrorCode.PARAMETER_ERROR, str(serializer.errors))

        data = serializer.validated_data
        # 签名操作使用行锁，保证冻结立即互斥。
        wallet = self._load_wallet(data["wallet_id"], for_signing=True)
        self._assert_wallet_can_sign(wallet=wallet)
        # 先只派生公钥地址完成所有非密钥校验，避免拒绝路径过早把私钥放进栈帧。
        expected_from = wallet.derive_address(
            chain_type=data["chain_type"],
            bip44_account=data["bip44_account"],
            address_index=data["address_index"],
        )
        expected_from = Web3.to_checksum_address(expected_from)
        if expected_from != data["tx_dict"]["from"]:
            raise SignerAPIError(
                ErrorCode.ACCESS_DENY,
                "交易 from 地址与派生路径不匹配",
            )
        policy_recipient = self._resolve_evm_policy_recipient(tx_dict=data["tx_dict"])
        if not self._is_internal_destination(
            chain_type=data["chain_type"],
            address=policy_recipient,
        ):
            self._assert_wallet_sign_rate_limit(wallet=wallet, endpoint=request.path)

        privkey_hex = None
        try:
            privkey_hex = wallet.private_key_hex(
                chain_type=data["chain_type"],
                bip44_account=data["bip44_account"],
                address_index=data["address_index"],
            )
            signed = Web3().eth.account.sign_transaction(
                data["tx_dict"],
                privkey_hex,
            )
        except Exception:
            # 截断异常链（from None），防止 traceback frame 中的私钥泄露到日志系统。
            raise SignerAPIError(
                ErrorCode.PARAMETER_ERROR, "EVM 交易签名失败"
            ) from None
        finally:
            # Python 字符串无法原地清零；这里及时释放局部引用，尽量缩短私钥在栈帧中的生命周期。
            privkey_hex = None

        self._record_audit(
            request=request,
            status_value=SignerRequestAudit.Status.SUCCEEDED,
        )
        return Response(
            {
                "tx_hash": self._normalize_hex(signed.hash.hex()).lower(),
                "raw_transaction": self._normalize_hex(
                    signed.raw_transaction.hex()
                ).lower(),
            },
            status=status.HTTP_200_OK,
        )

