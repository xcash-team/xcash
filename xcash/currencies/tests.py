import threading
from decimal import Decimal
from unittest.mock import patch

from django.core.cache import cache
from django.db import close_old_connections
from django.db import connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.utils import timezone

from chains.models import Address
from chains.models import AddressUsage
from chains.models import Balance
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.models import Wallet
from common.utils.math import format_decimal_stripped
from currencies.models import ChainToken
from currencies.models import Crypto
from currencies.service import CryptoService


class ChainNativeCryptoMappingTests(TestCase):
    def test_creating_chain_auto_creates_native_crypto_mapping(self):
        native_coin = Crypto.objects.create(
            name="Ethereum",
            symbol="ETH",
            coingecko_id="ethereum",
        )

        chain = Chain.objects.create(
            name="Ethereum Mainnet",
            code="eth-mainnet",
            type=ChainType.EVM,
            native_coin=native_coin,
            chain_id=1,
            rpc="http://localhost:8545",
            active=True,
        )

        native_mapping = ChainToken.objects.get(crypto=native_coin, chain=chain)
        self.assertEqual(native_mapping.address, "")
        self.assertIsNone(native_mapping.decimals)


class CryptoServiceAllowedMethodsTests(TestCase):
    def setUp(self):
        cache.clear()

    def test_allowed_methods_reuses_chaintoken_relation_and_filters_chain_codes(self):
        native = Crypto.objects.create(
            name="Ethereum Allowed Methods Native",
            symbol="ETH-AM",
            coingecko_id="ethereum-allowed-methods-native",
        )
        token = Crypto.objects.create(
            name="Allowed Methods Token",
            symbol="AMT",
            coingecko_id="allowed-methods-token",
        )
        included_chain = Chain.objects.create(
            name="Allowed Methods Included",
            code="am-included",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=8801,
            rpc="http://localhost:8545",
            active=True,
        )
        excluded_chain = Chain.objects.create(
            name="Allowed Methods Excluded",
            code="am-excluded",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=8802,
            rpc="http://localhost:8546",
            active=True,
        )
        ChainToken.objects.create(
            crypto=token,
            chain=included_chain,
            address="0x0000000000000000000000000000000000008801",
        )
        ChainToken.objects.create(
            crypto=token,
            chain=excluded_chain,
            address="0x0000000000000000000000000000000000008802",
        )

        with patch.object(
            Crypto,
            "support_this_chain",
            side_effect=AssertionError("ChainToken row already proves support"),
        ):
            methods = CryptoService.allowed_methods(chain_codes={included_chain.code})

        self.assertEqual(methods, {token.symbol: {included_chain.code}})


class ChainTokenRemapTests(TestCase):
    @patch("chains.tasks.process_transfer.apply_async")
    @patch("chains.tasks.process_transfer.delay")
    def test_remap_chain_mapping_updates_transfers_and_triggers_rematch(
        self,
        process_transfer_delay_mock,
        _process_transfer_apply_async_mock,
    ):
        # 修改 ChainToken.crypto 后，历史 OnchainTransfer 应自动切到新币种，并触发一次业务重归类。
        native_coin = Crypto.objects.create(
            name="Ethereum",
            symbol="ETH",
            coingecko_id="ethereum",
        )
        placeholder = Crypto.objects.create(
            name="Pending eth usdt",
            symbol="PENDING:eth:0x00000000000000000000000000000000000000aa",
            coingecko_id="PENDING:eth:0x00000000000000000000000000000000000000aa",
            active=False,
        )
        real_crypto = Crypto.objects.create(
            name="Tether",
            symbol="USDT",
            coingecko_id="tether",
        )
        chain = Chain.objects.create(
            name="Ethereum",
            code="eth",
            type=ChainType.EVM,
            native_coin=native_coin,
            chain_id=1,
            rpc="http://localhost:8545",
            active=True,
        )
        chain_token = ChainToken.objects.create(
            crypto=placeholder,
            chain=chain,
            address="0x00000000000000000000000000000000000000AA",
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "1" * 64,
            event_id="erc20:0",
            crypto=placeholder,
            from_address="0x0000000000000000000000000000000000000002",
            to_address="0x0000000000000000000000000000000000000003",
            value="1",
            amount="1",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
            processed_at=timezone.now(),
        )

        # save() 内部通过 on_commit 调度重归类任务；TestCase 事务里要显式执行回调。
        with self.captureOnCommitCallbacks(execute=True):
            chain_token.crypto = real_crypto
            chain_token.save(update_fields=["crypto"])

        transfer.refresh_from_db()
        self.assertEqual(transfer.crypto_id, real_crypto.id)
        self.assertIsNone(transfer.processed_at)
        process_transfer_delay_mock.assert_called_once_with(transfer.pk)


class BalanceRawValueTests(TestCase):
    def test_update_from_transfer_uses_raw_value_as_authority(self):
        # Balance 必须按 OnchainTransfer.value 记账，不能继续依赖已被 8 位展示精度截断的 amount。
        native_coin = Crypto.objects.create(
            name="Ethereum",
            symbol="ETH",
            coingecko_id="ethereum",
            decimals=18,
        )
        token = Crypto.objects.create(
            name="Precise Token",
            symbol="P18",
            coingecko_id="precise-token",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Ethereum",
            code="eth",
            type=ChainType.EVM,
            native_coin=native_coin,
            chain_id=1,
            rpc="http://localhost:8545",
            active=True,
        )
        chain_token = ChainToken.objects.create(
            crypto=token,
            chain=chain,
            address="0x00000000000000000000000000000000000000AA",
        )
        wallet = Wallet.objects.create()
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x0000000000000000000000000000000000000001",
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "3" * 64,
            event_id="erc20:1",
            crypto=token,
            from_address="0x0000000000000000000000000000000000000002",
            to_address=addr.address,
            value="123456789",
            amount="0.00000000",
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
        )

        Balance.update_from_transfer(transfer)

        balance = Balance.objects.get(address=addr, chain_token=chain_token)
        self.assertEqual(balance.value, Decimal("123456789"))
        self.assertEqual(
            format_decimal_stripped(balance.amount), "0.000000000123456789"
        )


class BalanceConcurrencyTests(TransactionTestCase):
    def test_adjust_is_safe_under_concurrent_first_insert(self):
        # 两个并发入账同时打到空余额时，必须汇总成一条记录且数值正确。
        native_coin = Crypto.objects.create(
            name="Ethereum Balance",
            symbol="ETH-BAL",
            coingecko_id="ethereum-balance-concurrency",
            decimals=18,
        )
        token = Crypto.objects.create(
            name="Precise Balance Token",
            symbol="P18-BAL",
            coingecko_id="precise-balance-token-concurrency",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Ethereum Balance Chain",
            code="eth-balance",
            type=ChainType.EVM,
            native_coin=native_coin,
            chain_id=2888,
            rpc="http://localhost:8545",
            active=True,
        )
        chain_token = ChainToken.objects.create(
            crypto=token,
            chain=chain,
            address="0x00000000000000000000000000000000000000BA",
        )
        wallet = Wallet.objects.create()
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x00000000000000000000000000000000000000B1",
        )
        barrier = threading.Barrier(2)
        errors: list[Exception] = []

        def adjust(delta_value: Decimal) -> None:
            close_old_connections()
            try:
                barrier.wait()
                Balance.adjust(
                    address=addr,
                    chain_token=chain_token,
                    delta_value=delta_value,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                # 线程内连接若不主动关闭，TransactionTestCase flush 阶段可能持锁死锁。
                connections.close_all()

        threads = [
            threading.Thread(target=adjust, args=(Decimal("1000000000000000000"),)),
            threading.Thread(target=adjust, args=(Decimal("2000000000000000000"),)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertFalse(errors)
        self.assertEqual(Balance.objects.count(), 1)
        balance = Balance.objects.get(address=addr, chain_token=chain_token)
        self.assertEqual(balance.value, Decimal("3000000000000000000"))
        self.assertEqual(format_decimal_stripped(balance.amount), "3")
