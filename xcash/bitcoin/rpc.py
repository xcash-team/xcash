from __future__ import annotations

import time
from typing import Any
from typing import TypedDict
from typing import cast

import httpx


class BitcoinRpcError(RuntimeError):
    """Bitcoin Core JSON-RPC 调用失败。"""


class BitcoinRpcErrorPayload(TypedDict, total=False):
    code: int
    message: str


class BitcoinScriptPubKey(TypedDict, total=False):
    address: str
    addresses: list[str]
    type: str


class BitcoinTxVout(TypedDict, total=False):
    n: int
    value: float | str
    scriptPubKey: BitcoinScriptPubKey


class BitcoinTxVin(TypedDict, total=False):
    txid: str
    vout: int
    coinbase: str


class BitcoinTxInfo(TypedDict, total=False):
    confirmations: int
    txid: str
    blockhash: str
    time: int
    blocktime: int
    vin: list[BitcoinTxVin]
    vout: list[BitcoinTxVout]


class BitcoinBlockInfo(TypedDict, total=False):
    hash: str
    height: int
    time: int
    tx: list[BitcoinTxInfo]


class BitcoinRpcClient:
    """Bitcoin Core JSON-RPC 客户端。

    rpc_url 格式：http://rpcuser:rpcpassword@host:port/
    例如：http://bitcoin:secret@bitcoinnode:8332/
    """

    # 网络层异常重试策略：Bitcoin Core 重启或网络瞬断时短暂不可达是常态，
    # 用指数退避重试避免扫块游标被一次抖动卡死。业务级 RPC error（JSON error
    # payload）或 JSON 解析失败不参与重试——那是响应已到达的真错误。
    _MAX_RETRY_ATTEMPTS = 3
    _RETRY_BACKOFF_BASE_SECONDS = 1.0

    def __init__(self, rpc_url: str) -> None:
        if not rpc_url:
            msg = "Bitcoin RPC URL 未配置"
            raise ValueError(msg)
        self.rpc_url = rpc_url

    def _call(
        self, method: str, params: list[Any] | dict[str, Any] | None = None
    ) -> Any:
        """执行 Bitcoin Core JSON-RPC 调用，返回 result；错误时抛出 BitcoinRpcError。"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params if params is not None else [],
        }
        resp = self._post_with_retry(method=method, payload=payload)

        try:
            data = resp.json()
        except ValueError as exc:
            msg = f"Bitcoin RPC 返回了非法 JSON（{method}）"
            raise BitcoinRpcError(msg) from exc

        error_payload = data.get("error")
        if error_payload:
            error = cast("BitcoinRpcErrorPayload", error_payload)
            error_msg = error.get("message", str(error_payload))
            msg = f"Bitcoin RPC error ({method}): {error_msg}"
            raise BitcoinRpcError(msg)

        if resp.is_error:
            # Bitcoin Core 的部分钱包 RPC（如 loadwallet 不存在）会返回 HTTP 500，
            # 但实际错误语义已在上面的 JSON error 中处理；走到这里说明服务端异常且无标准 error。
            try:
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                msg = f"Bitcoin RPC 请求失败（{method}）: {exc}"
                raise BitcoinRpcError(msg) from exc

        return data["result"]

    def _post_with_retry(
        self, *, method: str, payload: dict[str, Any]
    ) -> httpx.Response:
        """POST 请求；仅在网络/超时异常（httpx.HTTPError）时指数退避重试。

        HTTP 5xx 由调用方通过 resp 进一步检查 JSON error payload 或
        raise_for_status 处理，不在重试范围——因为 Bitcoin Core 的 500
        通常携带业务级 RPC error（如 loadwallet 不存在），重试没意义。
        """
        last_exc: httpx.HTTPError | None = None
        for attempt in range(self._MAX_RETRY_ATTEMPTS):
            try:
                return httpx.post(
                    self.rpc_url,
                    json=payload,
                    timeout=30,
                    trust_env=False,
                )
            except httpx.HTTPError as exc:
                last_exc = exc
                if attempt + 1 >= self._MAX_RETRY_ATTEMPTS:
                    break
                time.sleep(self._RETRY_BACKOFF_BASE_SECONDS * (2**attempt))
        assert last_exc is not None
        msg = f"Bitcoin RPC 请求失败（{method}）: {last_exc}"
        raise BitcoinRpcError(msg) from last_exc

    def get_block_count(self) -> int:
        return int(self._call("getblockcount"))

    def list_wallets(self) -> list[str]:
        result = self._call("listwallets")
        if not result:
            return []
        return cast("list[str]", result)

    def create_wallet(
        self,
        wallet_name: str,
        *,
        disable_private_keys: bool = False,
        blank: bool = False,
        load_on_startup: bool = True,
    ) -> dict[str, Any]:
        return cast(
            "dict[str, Any]",
            self._call(
                "createwallet",
                {
                    "wallet_name": wallet_name,
                    "disable_private_keys": disable_private_keys,
                    "blank": blank,
                    "load_on_startup": load_on_startup,
                },
            ),
        )

    def load_wallet(
        self, wallet_name: str, *, load_on_startup: bool = True
    ) -> dict[str, Any]:
        return cast(
            "dict[str, Any]",
            self._call(
                "loadwallet",
                {"filename": wallet_name, "load_on_startup": load_on_startup},
            ),
        )

    def unload_wallet(self, wallet_name: str) -> dict[str, Any]:
        return cast("dict[str, Any]", self._call("unloadwallet", [wallet_name]))

    def get_wallet_info(self) -> dict[str, Any]:
        return cast("dict[str, Any]", self._call("getwalletinfo"))

    def get_new_address(self, label: str = "", address_type: str = "legacy") -> str:
        return cast("str", self._call("getnewaddress", [label, address_type]))

    def generate_to_address(self, block_count: int, address: str) -> list[str]:
        return cast(
            "list[str]", self._call("generatetoaddress", [block_count, address])
        )

    def import_address(
        self,
        address: str,
        *,
        label: str = "",
        rescan: bool = False,
    ) -> None:
        self._call("importaddress", [address, label, rescan])

    def import_descriptor(
        self,
        *,
        descriptor: str,
        label: str = "",
        timestamp: str | int = "now",
    ) -> list[dict[str, Any]]:
        # importdescriptors 要求带 checksum 的 descriptor；先走 getdescriptorinfo 统一规范化。
        descriptor_info = self.get_descriptor_info(descriptor)
        request = {
            "desc": descriptor_info["descriptor"],
            "timestamp": timestamp,
            "label": label,
        }
        result = cast(
            "list[dict[str, Any]]", self._call("importdescriptors", [[request]])
        )
        first_result = result[0] if result else {}
        if not first_result.get("success", False):
            error_payload = first_result.get("error", {})
            error_message = error_payload.get("message", "unknown error")
            msg = f"Bitcoin RPC error (importdescriptors): {error_message}"
            raise BitcoinRpcError(msg)
        return result

    def get_descriptor_info(self, descriptor: str) -> dict[str, Any]:
        return cast("dict[str, Any]", self._call("getdescriptorinfo", [descriptor]))

    def send_to_address(self, address: str, amount_btc: float | str) -> str:
        # Bitcoin Core 接受字符串格式金额；避免 float() 导致精度丢失。
        return cast("str", self._call("sendtoaddress", [address, str(amount_btc)]))

    def get_block_hash(self, height: int) -> str:
        return cast("str", self._call("getblockhash", [height]))

    def get_block(self, block_hash: str, verbosity: int = 2) -> BitcoinBlockInfo:
        return cast("BitcoinBlockInfo", self._call("getblock", [block_hash, verbosity]))

    def get_transaction(self, txid: str) -> BitcoinTxInfo | None:
        try:
            return cast("BitcoinTxInfo", self._call("gettransaction", [txid, True]))
        except BitcoinRpcError as exc:
            error_message = str(exc)
            if (
                "Invalid or non-wallet transaction id" in error_message
                or "Requested wallet does not exist or is not loaded" in error_message
            ):
                return None
            raise

    def get_raw_transaction(self, txid: str) -> BitcoinTxInfo | None:
        try:
            return cast("BitcoinTxInfo", self._call("getrawtransaction", [txid, True]))
        except BitcoinRpcError as exc:
            if "No such mempool or blockchain transaction" in str(exc):
                return None
            raise

