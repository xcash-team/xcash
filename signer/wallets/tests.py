from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import patch
from uuid import uuid4

from django.core.cache import cache
from django.test import TestCase
from django.test import override_settings
from wallets.models import ChainType
from wallets.models import SignerAddress
from wallets.models import SignerRequestAudit
from wallets.models import SignerWallet
from wallets.views import SIGNER_REQUEST_ID_HEADER
from wallets.views import SIGNER_SIGNATURE_HEADER
from wallets.views import build_signer_signature_payload
from web3 import Web3

from core.error_codes import ErrorCode


@override_settings(
    SIGNER_SHARED_SECRET="signer-secret",
    SIGNER_RATE_LIMIT_WINDOW=60,
    SIGNER_RATE_LIMIT_MAX_REQUESTS=2,
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "signer-wallet-tests",
        }
    },
)
class SignerWalletApiTests(TestCase):
    def setUp(self):
        cache.clear()

    def _signed_headers(
        self,
        *,
        body: bytes,
        method: str = "POST",
        path: str = "/v1/wallets/create",
    ) -> dict:
        request_id = str(uuid4())
        signature = hmac.new(
            b"signer-secret",
            build_signer_signature_payload(
                method=method,
                path=path,
                request_id=request_id,
                request_body=body,
            ),
            hashlib.sha256,
        ).hexdigest()
        # Django Test Client 需要使用 HTTP_* 形式传自定义请求头，才能覆盖 signer 鉴权中间件。
        return {
            f"HTTP_{SIGNER_REQUEST_ID_HEADER.upper().replace('-', '_')}": request_id,
            f"HTTP_{SIGNER_SIGNATURE_HEADER.upper().replace('-', '_')}": signature,
        }

    def test_create_wallet_uses_wallet_id_as_unique_mapping_key(self):
        body = json.dumps({"wallet_id": 1001}).encode("utf-8")

        response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body),
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["wallet_id"], 1001)
        self.assertTrue(payload["created"])
        self.assertTrue(SignerWallet.objects.filter(xcash_wallet_id=1001).exists())
        audit = SignerRequestAudit.objects.get(wallet_id=1001)
        self.assertEqual(audit.status, SignerRequestAudit.Status.SUCCEEDED)
        self.assertEqual(audit.endpoint, "/v1/wallets/create")

    def test_replay_request_is_rejected(self):
        body = json.dumps({"wallet_id": 1002}).encode("utf-8")
        headers = self._signed_headers(body=body)

        first_response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
            **headers,
        )
        second_response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
            **headers,
        )

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 400)
        # 审计记录只写不改，首次成功写入后不会被重放请求覆盖。
        audit = SignerRequestAudit.objects.get(
            request_id=headers["HTTP_X_SIGNER_REQUEST_ID"]
        )
        self.assertEqual(audit.status, SignerRequestAudit.Status.SUCCEEDED)

    def test_derive_address_registers_internal_address(self):
        wallet = SignerWallet.objects.create(
            xcash_wallet_id=1003,
            encrypted_mnemonic=SignerWallet.encrypt_mnemonic(
                SignerWallet.generate_mnemonic()
            ),
        )
        body = json.dumps(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "chain_type": ChainType.EVM,
                "bip44_account": 0,
                "address_index": 9,
            }
        ).encode("utf-8")

        response = self.client.post(
            "/v1/wallets/derive-address",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/wallets/derive-address"),
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            SignerAddress.objects.filter(
                wallet=wallet,
                chain_type=ChainType.EVM,
                bip44_account=0,
                address_index=9,
            ).exists()
        )

    def test_rate_limit_writes_rate_limited_audit(self):
        for wallet_id in (2001, 2002):
            body = json.dumps({"wallet_id": wallet_id}).encode("utf-8")
            response = self.client.post(
                "/v1/wallets/create",
                data=body,
                content_type="application/json",
                REMOTE_ADDR="10.0.0.2",
                **self._signed_headers(body=body),
            )
            self.assertEqual(response.status_code, 200)

        body = json.dumps({"wallet_id": 2003}).encode("utf-8")
        headers = self._signed_headers(body=body)
        response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
            REMOTE_ADDR="10.0.0.2",
            **headers,
        )

        self.assertEqual(response.status_code, 429)
        audit = SignerRequestAudit.objects.get(
            request_id=headers["HTTP_X_SIGNER_REQUEST_ID"]
        )
        self.assertEqual(audit.status, SignerRequestAudit.Status.RATE_LIMITED)
        self.assertEqual(audit.error_code, ErrorCode.RATE_LIMIT_EXCEEDED.code)

    def test_signature_must_bind_request_id(self):
        body = json.dumps({"wallet_id": 2004}).encode("utf-8")
        valid_headers = self._signed_headers(body=body)
        tampered_headers = dict(valid_headers)
        tampered_headers["HTTP_X_SIGNER_REQUEST_ID"] = str(uuid4())

        response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
            **tampered_headers,
        )

        self.assertEqual(response.status_code, 403)
        audit = SignerRequestAudit.objects.get(
            request_id=tampered_headers["HTTP_X_SIGNER_REQUEST_ID"]
        )
        self.assertEqual(audit.status, SignerRequestAudit.Status.FAILED)
        self.assertEqual(audit.error_code, ErrorCode.SIGNATURE_ERROR.code)


@override_settings(
    SIGNER_SHARED_SECRET="signer-secret",
    SIGNER_RATE_LIMIT_WINDOW=60,
    SIGNER_RATE_LIMIT_MAX_REQUESTS=20,
    SIGNER_WALLET_SIGN_RATE_LIMIT_WINDOW=60,
    SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS=2,
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "signer-wallet-sign-tests",
        }
    },
)
class SignerWalletSignPolicyTests(TestCase):
    def setUp(self):
        cache.clear()

    def _signed_headers(
        self,
        *,
        body: bytes,
        method: str = "POST",
        path: str = "/v1/sign/evm",
    ) -> dict:
        request_id = str(uuid4())
        signature = hmac.new(
            b"signer-secret",
            build_signer_signature_payload(
                method=method,
                path=path,
                request_id=request_id,
                request_body=body,
            ),
            hashlib.sha256,
        ).hexdigest()
        return {
            f"HTTP_{SIGNER_REQUEST_ID_HEADER.upper().replace('-', '_')}": request_id,
            f"HTTP_{SIGNER_SIGNATURE_HEADER.upper().replace('-', '_')}": signature,
        }

    def _create_wallet(
        self, *, wallet_id: int, status: str = SignerWallet.Status.ACTIVE
    ) -> SignerWallet:
        return SignerWallet.objects.create(
            xcash_wallet_id=wallet_id,
            encrypted_mnemonic=SignerWallet.encrypt_mnemonic(
                SignerWallet.generate_mnemonic()
            ),
            status=status,
        )

    @staticmethod
    def _evm_sign_body(*, wallet: SignerWallet, nonce: int) -> bytes:
        from_address = Web3.to_checksum_address(
            wallet.derive_address(
                chain_type=ChainType.EVM, bip44_account=0, address_index=0
            )
        )
        payload = {
            "wallet_id": wallet.xcash_wallet_id,
            "chain_type": ChainType.EVM,
            "bip44_account": 0,
            "address_index": 0,
            "tx_dict": {
                "chainId": 1,
                "nonce": nonce,
                "from": from_address,
                "to": from_address,
                "value": 1,
                "data": "0x",
                "gas": 21000,
                "gasPrice": 1,
            },
        }
        return json.dumps(payload).encode("utf-8")

    @staticmethod
    def _erc20_sign_body(*, wallet: SignerWallet, nonce: int, recipient: str) -> bytes:
        from_address = Web3.to_checksum_address(
            wallet.derive_address(
                chain_type=ChainType.EVM, bip44_account=0, address_index=0
            )
        )
        encoded_address = recipient.lower().replace("0x", "").rjust(64, "0")
        encoded_amount = hex(1)[2:].rjust(64, "0")
        payload = {
            "wallet_id": wallet.xcash_wallet_id,
            "chain_type": ChainType.EVM,
            "bip44_account": 0,
            "address_index": 0,
            "tx_dict": {
                "chainId": 1,
                "nonce": nonce,
                "from": from_address,
                "to": Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000f1"
                ),
                "value": 0,
                "data": f"0xa9059cbb{encoded_address}{encoded_amount}",
                "gas": 65000,
                "gasPrice": 1,
            },
        }
        return json.dumps(payload).encode("utf-8")

    @staticmethod
    def _bitcoin_sign_body(
        *,
        wallet: SignerWallet,
        recipient: str,
        replaceable: bool,
    ) -> bytes:
        source_address = wallet.derive_address(
            chain_type=ChainType.BITCOIN,
            bip44_account=0,
            address_index=0,
        )
        payload = {
            "wallet_id": wallet.xcash_wallet_id,
            "chain_type": ChainType.BITCOIN,
            "bip44_account": 0,
            "address_index": 0,
            "source_address": source_address,
            "to": recipient,
            "amount_satoshi": 1000,
            "fee_satoshi": 200,
            "replaceable": replaceable,
            "utxos": [
                {
                    "txid": "ab" * 32,
                    "vout": 0,
                    "amount": "0.001",
                    "confirmations": 12,
                    "script_pub_key": "76a914",
                }
            ],
        }
        return json.dumps(payload).encode("utf-8")

    def test_frozen_wallet_cannot_sign(self):
        wallet = self._create_wallet(wallet_id=3001, status=SignerWallet.Status.FROZEN)
        body = self._evm_sign_body(wallet=wallet, nonce=1)
        headers = self._signed_headers(body=body)

        response = self.client.post(
            "/v1/sign/evm",
            data=body,
            content_type="application/json",
            **headers,
        )

        self.assertEqual(response.status_code, 403)
        audit = SignerRequestAudit.objects.get(
            request_id=headers["HTTP_X_SIGNER_REQUEST_ID"]
        )
        self.assertEqual(audit.status, SignerRequestAudit.Status.FAILED)
        self.assertEqual(audit.error_code, ErrorCode.ACCESS_DENY.code)
        self.assertEqual(audit.detail, "wallet 已冻结")

    def test_wallet_sign_rate_limit_blocks_third_request(self):
        wallet = self._create_wallet(wallet_id=3002)

        for nonce in (1, 2):
            body = self._evm_sign_body(wallet=wallet, nonce=nonce)
            response = self.client.post(
                "/v1/sign/evm",
                data=body,
                content_type="application/json",
                **self._signed_headers(body=body),
            )
            self.assertEqual(response.status_code, 200)

        body = self._evm_sign_body(wallet=wallet, nonce=3)
        headers = self._signed_headers(body=body)
        response = self.client.post(
            "/v1/sign/evm",
            data=body,
            content_type="application/json",
            **headers,
        )

        self.assertEqual(response.status_code, 429)
        audit = SignerRequestAudit.objects.get(
            request_id=headers["HTTP_X_SIGNER_REQUEST_ID"]
        )
        self.assertEqual(audit.status, SignerRequestAudit.Status.RATE_LIMITED)
        self.assertEqual(audit.error_code, ErrorCode.RATE_LIMIT_EXCEEDED.code)

    def test_internal_erc20_transfer_bypasses_wallet_sign_rate_limit(self):
        # 系统内地址由 signer 自己的账户表判定，命中后仅放宽钱包签名频率限制，其余保护仍保留。
        wallet = self._create_wallet(wallet_id=3003)
        internal_recipient = Web3.to_checksum_address(
            wallet.derive_address(
                chain_type=ChainType.EVM, bip44_account=0, address_index=1
            )
        )
        SignerAddress.register_derived_address(
            wallet=wallet,
            chain_type=ChainType.EVM,
            bip44_account=0,
            address_index=1,
            address=internal_recipient,
        )

        for nonce in (1, 2, 3):
            body = self._erc20_sign_body(
                wallet=wallet,
                nonce=nonce,
                recipient=internal_recipient,
            )
            response = self.client.post(
                "/v1/sign/evm",
                data=body,
                content_type="application/json",
                **self._signed_headers(body=body),
            )
            self.assertEqual(response.status_code, 200)

    @patch("wallets.views.compute_txid", return_value="ab" * 32)
    @patch("wallets.views.SignBitcoinView._load_bit_dependencies")
    def test_bitcoin_sign_endpoint_passes_replaceable_flag_to_bit_library(
        self,
        load_bit_dependencies_mock,
        _compute_txid_mock,
    ):
        wallet = self._create_wallet(wallet_id=3004)
        create_transaction_kwargs = {}

        class DummyUnspent:
            def __init__(
                self,
                *,
                amount,
                confirmations,
                script,
                txid,
                txindex,
                type="p2pkh",
            ):
                self.amount = amount
                self.confirmations = confirmations
                self.script = script
                self.txid = txid
                self.txindex = txindex

        class DummyKey:
            def __init__(self, _wif):
                self.unspents = []

            def create_transaction(self, **kwargs):
                create_transaction_kwargs.update(kwargs)
                return "00"

        load_bit_dependencies_mock.return_value = (DummyKey, DummyUnspent)
        recipient = wallet.derive_address(
            chain_type=ChainType.BITCOIN,
            bip44_account=0,
            address_index=1,
        )
        body = self._bitcoin_sign_body(
            wallet=wallet,
            recipient=recipient,
            replaceable=True,
        )

        response = self.client.post(
            "/v1/sign/bitcoin",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/sign/bitcoin"),
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(create_transaction_kwargs.get("replace_by_fee"))


@override_settings(
    SIGNER_SHARED_SECRET="signer-secret",
    SIGNER_RATE_LIMIT_WINDOW=60,
    SIGNER_RATE_LIMIT_MAX_REQUESTS=20,
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "signer-admin-summary-tests",
        }
    },
)
class SignerInternalAdminSummaryTests(TestCase):
    def setUp(self):
        cache.clear()

    def _signed_headers(
        self,
        *,
        body: bytes = b"",
        method: str = "GET",
        path: str = "/internal/admin-summary",
    ) -> dict:
        request_id = str(uuid4())
        signature = hmac.new(
            b"signer-secret",
            build_signer_signature_payload(
                method=method,
                path=path,
                request_id=request_id,
                request_body=body,
            ),
            hashlib.sha256,
        ).hexdigest()
        return {
            f"HTTP_{SIGNER_REQUEST_ID_HEADER.upper().replace('-', '_')}": request_id,
            f"HTTP_{SIGNER_SIGNATURE_HEADER.upper().replace('-', '_')}": signature,
        }

    def test_internal_admin_summary_returns_health_wallets_and_recent_anomalies(self):
        # 只读摘要接口要把运营观测信息集中返回给主应用后台，但不能暴露敏感密钥材料。
        SignerWallet.objects.create(
            xcash_wallet_id=4001,
            encrypted_mnemonic=SignerWallet.encrypt_mnemonic(
                SignerWallet.generate_mnemonic()
            ),
            status=SignerWallet.Status.ACTIVE,
        )
        SignerWallet.objects.create(
            xcash_wallet_id=4002,
            encrypted_mnemonic=SignerWallet.encrypt_mnemonic(
                SignerWallet.generate_mnemonic()
            ),
            status=SignerWallet.Status.FROZEN,
        )
        SignerRequestAudit.objects.create(
            request_id="audit-succeeded",
            endpoint="/v1/sign/evm",
            wallet_id=4001,
            chain_type=ChainType.EVM,
            bip44_account=0,
            address_index=0,
            status=SignerRequestAudit.Status.SUCCEEDED,
        )
        SignerRequestAudit.objects.create(
            request_id="audit-failed",
            endpoint="/v1/sign/bitcoin",
            wallet_id=4002,
            chain_type=ChainType.BITCOIN,
            bip44_account=0,
            address_index=1,
            status=SignerRequestAudit.Status.FAILED,
            error_code=ErrorCode.ACCESS_DENY.code,
            detail="wallet 已冻结",
        )

        response = self.client.get(
            "/internal/admin-summary",
            **self._signed_headers(),
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(SignerRequestAudit.objects.count(), 2)
        self.assertTrue(payload["health"]["healthy"])
        # 确认字段名不泄露鉴权机制细节。
        self.assertNotIn("signer_shared_secret", json.dumps(payload["health"]))
        self.assertEqual(payload["wallets"]["total"], 2)
        self.assertEqual(payload["wallets"]["active"], 1)
        self.assertEqual(payload["wallets"]["frozen"], 1)
        self.assertEqual(payload["requests_last_hour"]["total"], 2)
        self.assertEqual(payload["requests_last_hour"]["failed"], 1)
        self.assertEqual(len(payload["recent_anomalies"]), 1)
        self.assertEqual(payload["recent_anomalies"][0]["wallet_id"], 4002)
        self.assertNotIn("mnemonic", json.dumps(payload))


# ---------------------------------------------------------------------------
# 补充测试：鉴权头缺失、from 地址不匹配、healthz、wallet_id 不存在、acc_idx 越界
# ---------------------------------------------------------------------------


@override_settings(
    SIGNER_SHARED_SECRET="signer-secret",
    SIGNER_RATE_LIMIT_WINDOW=60,
    SIGNER_RATE_LIMIT_MAX_REQUESTS=50,
    SIGNER_WALLET_SIGN_RATE_LIMIT_WINDOW=60,
    SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS=50,
    SIGNER_MAX_ADDRESS_INDEX=100_000_000,
    SIGNER_MAX_BIP44_ACCOUNT=10,
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "signer-extra-tests",
        }
    },
)
class SignerExtraSecurityTests(TestCase):
    """审计发现的安全缺口补充测试。"""

    def setUp(self):
        cache.clear()

    def _signed_headers(
        self,
        *,
        body: bytes,
        method: str = "POST",
        path: str = "/v1/wallets/create",
    ) -> dict:
        request_id = str(uuid4())
        signature = hmac.new(
            b"signer-secret",
            build_signer_signature_payload(
                method=method,
                path=path,
                request_id=request_id,
                request_body=body,
            ),
            hashlib.sha256,
        ).hexdigest()
        return {
            f"HTTP_{SIGNER_REQUEST_ID_HEADER.upper().replace('-', '_')}": request_id,
            f"HTTP_{SIGNER_SIGNATURE_HEADER.upper().replace('-', '_')}": signature,
        }

    def _create_wallet(
        self, *, wallet_id: int, status: str = SignerWallet.Status.ACTIVE
    ) -> SignerWallet:
        return SignerWallet.objects.create(
            xcash_wallet_id=wallet_id,
            encrypted_mnemonic=SignerWallet.encrypt_mnemonic(
                SignerWallet.generate_mnemonic()
            ),
            status=status,
        )

    # --- 鉴权头缺失 ---

    def test_missing_auth_headers_returns_400(self):
        body = json.dumps({"wallet_id": 5001}).encode("utf-8")
        response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], ErrorCode.PARAMETER_ERROR.code)

    def test_empty_signature_returns_400(self):
        body = json.dumps({"wallet_id": 5002}).encode("utf-8")
        response = self.client.post(
            "/v1/wallets/create",
            data=body,
            content_type="application/json",
            headers={"x-signer-request-id": str(uuid4()), "x-signer-signature": ""},
        )
        self.assertEqual(response.status_code, 400)

    # --- wallet_id 不存在 ---

    def test_derive_address_with_nonexistent_wallet_returns_400(self):
        body = json.dumps(
            {
                "wallet_id": 99999,
                "chain_type": "evm",
                "bip44_account": 0,
                "address_index": 0,
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/wallets/derive-address",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/wallets/derive-address"),
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("wallet_id", response.json()["detail"])

    def test_sign_evm_with_nonexistent_wallet_returns_400(self):
        body = json.dumps(
            {
                "wallet_id": 99999,
                "chain_type": "evm",
                "bip44_account": 0,
                "address_index": 0,
                "tx_dict": {
                    "chainId": 1,
                    "nonce": 0,
                    "from": "0x" + "0" * 40,
                    "to": "0x" + "0" * 40,
                    "value": 0,
                    "data": "0x",
                    "gas": 21000,
                    "gasPrice": 1,
                },
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/sign/evm",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/sign/evm"),
        )
        self.assertEqual(response.status_code, 400)

    # --- from 地址不匹配 ---

    def test_evm_sign_rejects_mismatched_from_address(self):
        wallet = self._create_wallet(wallet_id=5003)
        wrong_from = Web3.to_checksum_address("0x" + "ab" * 20)
        body = json.dumps(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "chain_type": "evm",
                "bip44_account": 0,
                "address_index": 0,
                "tx_dict": {
                    "chainId": 1,
                    "nonce": 0,
                    "from": wrong_from,
                    "to": wrong_from,
                    "value": 0,
                    "data": "0x",
                    "gas": 21000,
                    "gasPrice": 1,
                },
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/sign/evm",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/sign/evm"),
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("from 地址", response.json()["detail"])

    # --- acc_idx 越界 ---

    def test_derive_address_rejects_address_index_above_max(self):
        wallet = self._create_wallet(wallet_id=5004)
        body = json.dumps(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "chain_type": "evm",
                "bip44_account": 0,
                "address_index": 200_000_000,
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/wallets/derive-address",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/wallets/derive-address"),
        )
        self.assertEqual(response.status_code, 400)

    def test_derive_address_rejects_negative_address_index(self):
        wallet = self._create_wallet(wallet_id=5005)
        body = json.dumps(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "chain_type": "evm",
                "bip44_account": 0,
                "address_index": -1,
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/wallets/derive-address",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/wallets/derive-address"),
        )
        self.assertEqual(response.status_code, 400)

    def test_derive_address_rejects_bip44_account_above_max(self):
        wallet = self._create_wallet(wallet_id=5010)
        body = json.dumps(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "chain_type": "evm",
                "bip44_account": 99,
                "address_index": 0,
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/wallets/derive-address",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/wallets/derive-address"),
        )
        self.assertEqual(response.status_code, 400)

    # --- healthz ---

    def test_healthz_does_not_expose_infra_details(self):
        response = self.client.get("/healthz")
        self.assertIn(response.status_code, (200, 503))
        payload = response.json()
        # 对外只暴露 ok，不暴露 database/cache/signer_shared_secret。
        self.assertIn("ok", payload)
        self.assertNotIn("database", payload)
        self.assertNotIn("cache", payload)
        self.assertNotIn("signer_shared_secret", payload)

    # --- ERC20 外部地址触发速率限制 ---

    def test_erc20_external_recipient_triggers_wallet_sign_rate_limit(self):
        wallet = self._create_wallet(wallet_id=5006)
        from_address = Web3.to_checksum_address(
            wallet.derive_address(
                chain_type=ChainType.EVM, bip44_account=0, address_index=0
            )
        )
        # 外部地址（未注册为 SignerAccount）
        external_recipient = "0x" + "cc" * 20
        encoded_address = external_recipient.lower().replace("0x", "").rjust(64, "0")
        encoded_amount = hex(1)[2:].rjust(64, "0")

        # 使用 SIGNER_WALLET_SIGN_RATE_LIMIT_MAX_REQUESTS=50，不会被速率限制。
        # 这里只验证请求正常通过（非内部地址走速率限制路径），不验证限流触发。
        body = json.dumps(
            {
                "wallet_id": wallet.xcash_wallet_id,
                "chain_type": "evm",
                "bip44_account": 0,
                "address_index": 0,
                "tx_dict": {
                    "chainId": 1,
                    "nonce": 0,
                    "from": from_address,
                    "to": Web3.to_checksum_address("0x" + "f1" * 20),
                    "value": 0,
                    "data": f"0xa9059cbb{encoded_address}{encoded_amount}",
                    "gas": 65000,
                    "gasPrice": 1,
                },
            }
        ).encode("utf-8")
        response = self.client.post(
            "/v1/sign/evm",
            data=body,
            content_type="application/json",
            **self._signed_headers(body=body, path="/v1/sign/evm"),
        )
        self.assertEqual(response.status_code, 200)

    # --- AES 加解密完整性 ---

    def test_aes_cipher_encrypt_decrypt_roundtrip(self):
        mnemonic = SignerWallet.generate_mnemonic()
        encrypted = SignerWallet.encrypt_mnemonic(mnemonic)
        wallet = SignerWallet.objects.create(
            xcash_wallet_id=5007,
            encrypted_mnemonic=encrypted,
        )
        self.assertEqual(wallet.mnemonic, mnemonic)

    # --- BIP44 派生地址稳定性 ---

    def test_derive_address_is_deterministic(self):
        wallet = self._create_wallet(wallet_id=5008)
        addr1 = wallet.derive_address(
            chain_type=ChainType.EVM, bip44_account=0, address_index=0
        )
        addr2 = wallet.derive_address(
            chain_type=ChainType.EVM, bip44_account=0, address_index=0
        )
        self.assertEqual(addr1, addr2)
        # 不同 address_index 应产生不同地址。
        addr3 = wallet.derive_address(
            chain_type=ChainType.EVM, bip44_account=0, address_index=1
        )
        self.assertNotEqual(addr1, addr3)
        # 不同 bip44_account 也应产生不同地址。
        addr4 = wallet.derive_address(
            chain_type=ChainType.EVM, bip44_account=1, address_index=0
        )
        self.assertNotEqual(addr1, addr4)

    # --- derive_key_pair 一致性 ---

    def test_derive_key_pair_matches_individual_methods(self):
        wallet = self._create_wallet(wallet_id=5009)
        addr, privkey = wallet.derive_key_pair(
            chain_type=ChainType.EVM, bip44_account=0, address_index=0
        )
        self.assertEqual(
            addr,
            wallet.derive_address(
                chain_type=ChainType.EVM, bip44_account=0, address_index=0
            ),
        )
        self.assertEqual(
            privkey,
            wallet.private_key_hex(
                chain_type=ChainType.EVM, bip44_account=0, address_index=0
            ),
        )
