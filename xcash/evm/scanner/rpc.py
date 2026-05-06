from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from web3 import Web3
from web3.exceptions import ExtraDataLengthError
from web3.middleware import ExtraDataToPOAMiddleware

if TYPE_CHECKING:
    from chains.models import Chain


class EvmScannerRpcError(RuntimeError):
    """统一包装 EVM 自扫描涉及的 RPC 异常。"""


class EvmScannerRpcClient:
    """对扫描器暴露最小 RPC 面，隔离 Web3 原始异常细节。"""

    def __init__(self, *, chain: Chain):
        self.chain = chain

    def get_latest_block_number(self) -> int:
        try:
            return int(self.chain.get_latest_block_number)
        except Exception as exc:  # noqa: BLE001
            raise EvmScannerRpcError(
                self._format_rpc_error(
                    "获取最新区块失败",
                    method="eth_blockNumber",
                    exc=exc,
                )
            ) from exc

    def get_transfer_logs(
        self,
        *,
        from_block: int,
        to_block: int,
        token_addresses: list[str],
        topic0: str,
    ) -> list[dict[str, Any]]:
        if from_block > to_block or not token_addresses:
            return []

        max_block_range = max(1, int(getattr(self.chain, "evm_log_max_block_range", 10)))
        logs: list[dict[str, Any]] = []
        chunk_from = from_block

        while chunk_from <= to_block:
            chunk_to = min(to_block, chunk_from + max_block_range - 1)
            logs.extend(
                self._get_transfer_logs_chunk(
                    from_block=chunk_from,
                    to_block=chunk_to,
                    token_addresses=token_addresses,
                    topic0=topic0,
                )
            )
            chunk_from = chunk_to + 1

        return logs

    def _get_transfer_logs_chunk(
        self,
        *,
        from_block: int,
        to_block: int,
        token_addresses: list[str],
        topic0: str,
    ) -> list[dict[str, Any]]:
        try:
            return list(
                self.chain.w3.eth.get_logs(  # noqa: SLF001
                    {
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "address": token_addresses,
                        "topics": [topic0],
                    }
                )
            )
        except Exception as exc:  # noqa: BLE001
            raise EvmScannerRpcError(
                self._format_rpc_error(
                    "获取 ERC20 日志失败",
                    method="eth_getLogs",
                    exc=exc,
                    context=f"from={from_block} to={to_block}",
                )
            ) from exc

    def get_block_timestamp(self, *, block_number: int) -> int:
        try:
            block = self._get_block_with_poa_retry(
                block_number=block_number,
                full_transactions=False,
            )
            return int(block["timestamp"])
        except Exception as exc:  # noqa: BLE001
            raise EvmScannerRpcError(
                self._format_rpc_error(
                    "获取区块时间失败",
                    method="eth_getBlockByNumber",
                    exc=exc,
                    context=f"block={block_number}",
                )
            ) from exc

    def get_full_block(self, *, block_number: int) -> dict[str, Any]:
        try:
            return dict(
                self._get_block_with_poa_retry(
                    block_number=block_number,
                    full_transactions=True,
                )
            )
        except Exception as exc:  # noqa: BLE001
            raise EvmScannerRpcError(
                self._format_rpc_error(
                    "获取完整区块失败",
                    method="eth_getBlockByNumber",
                    exc=exc,
                    context=f"block={block_number}",
                )
            ) from exc

    def get_transaction_receipt_status(self, *, tx_hash: str) -> int | None:
        try:
            receipt = self.chain.w3.eth.get_transaction_receipt(tx_hash)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            raise EvmScannerRpcError(
                self._format_rpc_error(
                    "获取交易回执失败",
                    method="eth_getTransactionReceipt",
                    exc=exc,
                    context=f"tx_hash={tx_hash}",
                )
            ) from exc

        if receipt is None:
            return None
        status = receipt.get("status")
        return int(status) if status in (0, 1) else None

    def _get_block_with_poa_retry(
        self,
        *,
        block_number: int,
        full_transactions: bool,
    ) -> Any:
        try:
            return self.chain.w3.eth.get_block(
                block_number,
                full_transactions=full_transactions,
            )  # noqa: SLF001
        except ExtraDataLengthError:
            self._mark_chain_as_poa()
            retry_w3 = self._build_poa_retry_w3()
            return retry_w3.eth.get_block(
                block_number,
                full_transactions=full_transactions,
            )

    def _build_poa_retry_w3(self) -> Web3:
        w3 = Web3(Web3.HTTPProvider(self.chain.rpc, request_kwargs={"timeout": 8}))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.chain.__dict__["w3"] = w3
        return w3

    def _mark_chain_as_poa(self) -> None:
        self.chain.__class__.objects.filter(pk=self.chain.pk).update(is_poa=True)
        self.chain.is_poa = True

    def _format_rpc_error(
        self,
        summary: str,
        *,
        method: str,
        exc: Exception,
        context: str = "",
    ) -> str:
        raw_error = self._format_raw_exception(exc)
        parts = [
            f"{summary}: rpc={method}",
            f"error={exc.__class__.__name__}: {raw_error}",
            f"chain={self.chain.code}",
        ]
        if context:
            parts.append(context)
        return " ".join(parts)

    @staticmethod
    def _format_raw_exception(exc: Exception) -> str:
        raw_error = " ".join(str(exc).split())
        if not raw_error:
            raw_error = repr(exc)
        return raw_error
