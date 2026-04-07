from __future__ import annotations

from uuid import uuid4

from bip_utils import Bip39SeedGenerator
from bip_utils import Bip44
from bip_utils import Bip44Changes
from bip_utils import Bip44Coins
from bip_utils import Bip84
from bip_utils import Bip84Coins
from web3 import Web3

from bitcoin.constants import BTC_P2WPKH_DUST_LIMIT
from bitcoin.network import get_active_bitcoin_network
from bitcoin.utils import btc_to_satoshi
from chains.models import ChainType
from chains.signer import BitcoinSignedPayload
from chains.signer import EvmSignedPayload
from chains.signer import SignerAdminSummary

# 测试环境使用固定助记词构造“进程内远端 signer”，
# 既保留 signer 作为唯一持钥方的调用语义，又避免单元测试依赖额外 HTTP 容器。
TEST_SIGNER_MNEMONIC = (
    "abandon abandon abandon abandon abandon abandon "
    "abandon abandon abandon abandon abandon abandon "
    "abandon abandon abandon abandon abandon agent"
)


class TestRemoteSignerBackend:
    # 这是测试辅助 fake signer，不是 pytest 要收集的测试类。
    __test__ = False

    def __init__(self) -> None:
        # 测试进程内的 fake signer 需要持有独立钱包映射，
        # 否则数据库 flush 后 wallet_id 复用会把不同测试错误地映射到同一地址空间。
        self._wallet_slots: dict[int, int] = {}
        self._next_wallet_slot = 1
        self._run_salt = uuid4().hex

    @staticmethod
    def _normalize_hex(value: str) -> str:
        return value if value.startswith("0x") else f"0x{value}"

    @staticmethod
    def fetch_admin_summary() -> SignerAdminSummary:
        return SignerAdminSummary(
            health={
                "healthy": True,
                "database": True,
                "cache": True,
                "signer_shared_secret": True,
            },
            wallets={"total": 0, "active": 0, "frozen": 0},
            requests_last_hour={
                "total": 0,
                "succeeded": 0,
                "failed": 0,
                "rate_limited": 0,
            },
            recent_anomalies=[],
        )

    def create_wallet(self, *, wallet_id: int) -> int:
        self._wallet_slots[wallet_id] = self._next_wallet_slot
        self._next_wallet_slot += 1
        return wallet_id

    @staticmethod
    def _coin(chain_type: str) -> Bip44Coins:
        if chain_type == ChainType.EVM:
            return Bip44Coins.ETHEREUM
        if chain_type == ChainType.BITCOIN:
            return get_active_bitcoin_network().bip44_coin
        raise NotImplementedError(f"unsupported chain_type={chain_type}")

    @staticmethod
    def _bip84_coin(chain_type: str) -> Bip84Coins:
        if chain_type == ChainType.BITCOIN:
            return get_active_bitcoin_network().bip84_coin
        raise NotImplementedError(f"BIP84 不支持 chain_type={chain_type}")

    def _wallet_passphrase(self, *, wallet_id: int) -> str:
        # 真实 signer 是“每个钱包一份独立密钥材料”；测试假体用 run salt + wallet slot 模拟这个边界，
        # 既避免同一测试进程内 wallet_id 复用，也避免多次跑测试时复用历史链上 nonce。
        wallet_slot = self._wallet_slots.get(wallet_id, wallet_id)
        return f"xcash-test-signer:{self._run_salt}:{wallet_slot}"

    def _account_ctx(
        self, *, wallet_id: int, chain_type: str, bip44_account: int, address_index: int
    ):
        """派生 HD 钱包完整叶子节点，与 signer 保持一致。

        EVM: BIP44 路径 m/44'/coin'/bip44_account'/0/address_index
        Bitcoin: BIP84 路径 m/84'/coin'/bip44_account'/0/address_index
        """
        seed_bytes = Bip39SeedGenerator(TEST_SIGNER_MNEMONIC).Generate(
            self._wallet_passphrase(wallet_id=wallet_id)
        )

        if chain_type == ChainType.BITCOIN:
            return (
                Bip84.FromSeed(seed_bytes, self._bip84_coin(chain_type))
                .Purpose()
                .Coin()
                .Account(bip44_account)
                .Change(Bip44Changes.CHAIN_EXT)
                .AddressIndex(address_index)
            )

        return (
            Bip44.FromSeed(seed_bytes, self._coin(chain_type))
            .Purpose()
            .Coin()
            .Account(bip44_account)
            .Change(Bip44Changes.CHAIN_EXT)
            .AddressIndex(address_index)
        )

    def derive_address(
        self, *, wallet, chain_type: str, bip44_account: int, address_index: int
    ) -> str:
        return (
            self._account_ctx(
                wallet_id=wallet.pk,
                chain_type=chain_type,
                bip44_account=bip44_account,
                address_index=address_index,
            )
            .PublicKey()
            .ToAddress()
        )

    def _private_key_hex(
        self, *, wallet_id: int, chain_type: str, bip44_account: int, address_index: int
    ) -> str:
        return (
            self._account_ctx(
                wallet_id=wallet_id,
                chain_type=chain_type,
                bip44_account=bip44_account,
                address_index=address_index,
            )
            .PrivateKey()
            .Raw()
            .ToBytes()
            .hex()
        )

    def sign_evm_transaction(
        self, *, address, chain, tx_dict: dict
    ) -> EvmSignedPayload:
        signed = Web3().eth.account.sign_transaction(
            tx_dict,
            self._private_key_hex(
                wallet_id=address.wallet_id,
                chain_type=address.chain_type,
                bip44_account=address.bip44_account,
                address_index=address.address_index,
            ),
        )
        return EvmSignedPayload(
            tx_hash=self._normalize_hex(signed.hash.hex()).lower(),
            raw_transaction=self._normalize_hex(signed.raw_transaction.hex()).lower(),
        )

    def sign_bitcoin_transaction(
        self,
        *,
        address,
        chain,
        source_address: str,
        to: str,
        amount_satoshi: int,
        fee_satoshi: int,
        replaceable: bool,
        utxos: list[dict],
    ) -> BitcoinSignedPayload:
        from bitcoinutils.keys import P2pkhAddress
        from bitcoinutils.keys import P2shAddress
        from bitcoinutils.keys import P2wpkhAddress
        from bitcoinutils.keys import PrivateKey
        from bitcoinutils.transactions import Transaction
        from bitcoinutils.transactions import TxInput
        from bitcoinutils.transactions import TxOutput
        from bitcoinutils.transactions import TxWitnessInput
        from common.utils.bitcoin import classify_bitcoin_address

        # 构造输入
        sequence = b"\xfd\xff\xff\xff" if replaceable else b"\xfe\xff\xff\xff"
        inputs = [
            TxInput(utxo["txid"], int(utxo["vout"]), sequence=sequence)
            for utxo in utxos
        ]

        # 构造目标输出
        addr_type = classify_bitcoin_address(to)
        if addr_type == "p2wpkh":
            target_script = P2wpkhAddress(to).to_script_pub_key()
        elif addr_type == "p2sh":
            target_script = P2shAddress(to).to_script_pub_key()
        else:
            target_script = P2pkhAddress(to).to_script_pub_key()
        outputs = [TxOutput(amount_satoshi, target_script)]

        # 找零
        total_input = sum(btc_to_satoshi(utxo["amount"]) for utxo in utxos)
        change = total_input - amount_satoshi - fee_satoshi
        if change > BTC_P2WPKH_DUST_LIMIT:
            change_script = P2wpkhAddress(source_address).to_script_pub_key()
            outputs.append(TxOutput(change, change_script))

        # 构建交易
        tx = Transaction(inputs, outputs, has_segwit=True)

        # 签名
        privkey_bytes = bytes.fromhex(
            self._private_key_hex(
                wallet_id=address.wallet_id,
                chain_type=address.chain_type,
                bip44_account=address.bip44_account,
                address_index=address.address_index,
            )
        )
        secret_exponent = int.from_bytes(privkey_bytes, byteorder="big")
        key = PrivateKey(secret_exponent=secret_exponent)
        pub = key.get_public_key()
        script_code = pub.get_address().to_script_pub_key()

        for i, utxo in enumerate(utxos):
            utxo_amount = btc_to_satoshi(utxo["amount"])
            sig = key.sign_segwit_input(tx, i, script_code, utxo_amount)
            tx.witnesses.append(TxWitnessInput([sig, pub.to_hex()]))

        return BitcoinSignedPayload(
            txid=tx.get_txid(),
            signed_payload=tx.serialize(),
        )


def build_test_remote_signer_backend() -> TestRemoteSignerBackend:
    return TestRemoteSignerBackend()
