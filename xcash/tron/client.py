from __future__ import annotations

import httpx
from django.conf import settings


class TronClientError(RuntimeError):
    """Tron HTTP 客户端异常。"""


class TronHttpClient:
    def __init__(self, *, chain):
        self.chain = chain
        self.base_url = chain.rpc.rstrip("/")
        self.timeout = settings.TRON_RPC_TIMEOUT

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if settings.TRON_API_KEY:
            headers["TRON-PRO-API-KEY"] = settings.TRON_API_KEY
        return headers

    def list_confirmed_trc20_history(
        self,
        *,
        address: str,
        contract_address: str,
        fingerprint: str | None = None,
        limit: int = 200,
    ) -> dict:
        params = {
            "limit": limit,
            "only_confirmed": "true",
            "contract_address": contract_address,
        }
        if fingerprint:
            params["fingerprint"] = fingerprint

        try:
            response = httpx.get(
                f"{self.base_url}/v1/accounts/{address}/transactions/trc20",
                params=params,
                headers=self._headers(),
                timeout=self.timeout,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise TronClientError(
                f"failed to fetch confirmed TRC20 history from {self.chain.code}"
            ) from exc

        return response.json()
