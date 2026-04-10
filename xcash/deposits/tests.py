import unittest
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.test import SimpleTestCase
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone
from rest_framework.test import APIRequestFactory
from rest_framework.test import force_authenticate
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.models import TransferType
from chains.models import Wallet
from currencies.models import Crypto
from currencies.models import ChainToken
from deposits.models import Deposit
from deposits.models import DepositAddress
from deposits.models import DepositCollection
from deposits.models import DepositStatus
from deposits.service import DepositService
from deposits.tasks import gather_deposits
from evm.models import EvmBroadcastTask
from projects.models import Project
from projects.models import RecipientAddress
from users.models import Customer
from users.models import User
from common.error_codes import ErrorCode
from deposits.viewsets import DepositViewSet


class DepositServiceCoreTests(TestCase):
    """DepositService 核心逻辑的单元测试。"""

    # -- 状态机幂等性 --

    @patch("deposits.service.Deposit.objects")
    def test_confirm_deposit_idempotent_when_already_completed(
        self, deposit_objects_mock
    ):
        # 已完成的 deposit 重复 confirm 不应抛异常，也不应重复发 webhook。
        deposit = SimpleNamespace(
            pk=1, status=DepositStatus.COMPLETED, refresh_from_db=Mock()
        )
        # 不抛异常即通过
        DepositService.confirm_deposit(deposit)

    @patch("deposits.service.Deposit.objects")
    def test_drop_deposit_idempotent_when_already_deleted(self, deposit_objects_mock):
        # 已删除的 deposit 重复 drop 不应抛异常。
        deposit_objects_mock.select_for_update.return_value.filter.return_value.exists.return_value = (
            False
        )
        deposit = SimpleNamespace(pk=1)
        DepositService.drop_deposit(deposit)

    @patch("deposits.service.Deposit.objects")
    def test_drop_deposit_rejects_non_confirming_status(self, deposit_objects_mock):
        # 非 CONFIRMING 状态（如 COMPLETED）调用 drop 应抛异常。
        from deposits.exceptions import DepositStatusError

        deposit = SimpleNamespace(pk=1, status=DepositStatus.COMPLETED)
        deposit.refresh_from_db = Mock()
        deposit.delete = Mock()
        deposit_objects_mock.select_for_update.return_value.filter.return_value.exists.return_value = (
            True
        )
        with self.assertRaises(DepositStatusError):
            DepositService.drop_deposit(deposit)

    # -- _should_collect 阈值判断 --

    def test_should_collect_triggers_by_time_deadline(self):
        # 金额低于门槛但超过 gather_period 时间的充币应触发归集。
        chain = SimpleNamespace(type=ChainType.EVM, code="eth")
        crypto = SimpleNamespace(
            symbol="USDT",
            get_decimals=Mock(return_value=6),
            price=Mock(return_value=Decimal("1")),
        )
        project = SimpleNamespace(gather_worth=Decimal("100"), gather_period=3)
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
        deposit = SimpleNamespace(
            customer=customer,
            transfer=transfer,
            # 4 天前创建，超过 gather_period=3
            created_at=timezone.now() - timedelta(days=4),
        )

        # 0.5 USDT，远低于 100 USD 门槛，但时间已过期
        should = DepositService._should_collect(deposit, Decimal("0.5"))
        self.assertTrue(should)

    def test_should_collect_fallback_on_missing_price(self):
        # 缺少价格时 worth 回退到 gather_worth，强制触发归集。
        chain = SimpleNamespace(type=ChainType.EVM, code="eth")
        crypto = SimpleNamespace(
            symbol="UNKNOWN",
            get_decimals=Mock(return_value=18),
            price=Mock(side_effect=KeyError("USD")),
        )
        project = SimpleNamespace(gather_worth=Decimal("10"), gather_period=365)
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
        deposit = SimpleNamespace(
            customer=customer,
            transfer=transfer,
            created_at=timezone.now(),
        )

        should = DepositService._should_collect(deposit, Decimal("1"))
        self.assertTrue(should)

    def test_should_collect_above_threshold_immediately(self):
        # 金额达到门槛时应立即触发归集，无需等待 deadline。
        crypto = SimpleNamespace(
            symbol="USDT", price=Mock(return_value=Decimal("1")),
        )
        project = SimpleNamespace(gather_worth=Decimal("100"), gather_period=30)
        deposit = SimpleNamespace(
            customer=SimpleNamespace(project=project),
            transfer=SimpleNamespace(crypto=crypto),
            created_at=timezone.now(),  # 刚创建，远未到 deadline
        )
        # $100 恰好 == 门槛 → 归集
        self.assertTrue(DepositService._should_collect(deposit, Decimal("100")))
        # $100.01 略高于门槛 → 归集
        self.assertTrue(DepositService._should_collect(deposit, Decimal("100.01")))

    def test_should_collect_below_threshold_and_not_expired_skips(self):
        # 金额低于门槛且未到 deadline → 跳过归集。
        crypto = SimpleNamespace(
            symbol="USDT", price=Mock(return_value=Decimal("1")),
        )
        project = SimpleNamespace(gather_worth=Decimal("100"), gather_period=30)
        deposit = SimpleNamespace(
            customer=SimpleNamespace(project=project),
            transfer=SimpleNamespace(crypto=crypto),
            created_at=timezone.now(),  # 刚创建
        )
        # $99.99 略低于 $100 门槛 → 不归集
        self.assertFalse(DepositService._should_collect(deposit, Decimal("99.99")))
        # $0.01 极低 → 不归集
        self.assertFalse(DepositService._should_collect(deposit, Decimal("0.01")))

    def test_should_collect_multi_deposit_sum_crosses_threshold(self):
        # 单笔低于门槛，但多笔合并总额超过门槛时应触发归集。
        # 模拟 _calculate_collection_amount 返回的合并金额。
        crypto = SimpleNamespace(
            symbol="ETH", price=Mock(return_value=Decimal("2000")),
        )
        project = SimpleNamespace(gather_worth=Decimal("100"), gather_period=30)
        deposit = SimpleNamespace(
            customer=SimpleNamespace(project=project),
            transfer=SimpleNamespace(crypto=crypto),
            created_at=timezone.now(),
        )
        # 单笔 0.04 ETH = $80 < $100 → 不归集
        self.assertFalse(DepositService._should_collect(deposit, Decimal("0.04")))
        # 合并 0.04 + 0.02 = 0.06 ETH = $120 > $100 → 归集
        self.assertTrue(DepositService._should_collect(deposit, Decimal("0.06")))

    # -- _ensure_gas_and_check 异常容错 --

    @patch.object(DepositService, "_get_gas_price", return_value=10)
    def test_ensure_gas_and_check_returns_false_on_send_failure(self, _get_gas_price_mock):
        # Gas 补充交易失败时应返回 False，允许后续归集跳过。
        native_coin = SimpleNamespace(
            symbol="ETH",
            get_decimals=Mock(return_value=18),
        )
        vault_addr = SimpleNamespace(
            send_crypto=Mock(side_effect=RuntimeError("vault RPC timeout")),
        )
        wallet = SimpleNamespace(get_address=Mock(return_value=vault_addr))
        project = SimpleNamespace(wallet=wallet)
        customer = SimpleNamespace(project=project)
        chain = SimpleNamespace(
            type=ChainType.EVM, code="eth", native_coin=native_coin,
            base_transfer_gas=50_000, erc20_transfer_gas=100_000,
        )
        crypto = SimpleNamespace(symbol="USDT", is_native=False)
        deposit = SimpleNamespace(
            id=1,
            customer=customer,
            transfer=SimpleNamespace(chain=chain, crypto=crypto),
        )
        deposit_address = SimpleNamespace(address="0xdeposit")
        adapter = SimpleNamespace(get_balance=Mock(return_value=0))

        result = DepositService._ensure_gas_and_check(
            deposit=deposit,
            deposit_address=deposit_address,
            adapter=adapter,
            collection_amount=Decimal("100"),
        )
        self.assertFalse(result)
        vault_addr.send_crypto.assert_called_once()

    # -- collect_deposit 防御分支 --

    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    @patch.object(DepositService, "_lock_collectible_group")
    def test_collect_deposit_returns_false_when_no_recipient(
        self,
        lock_group_mock,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
    ):
        # 项目未配置归集收款地址时应直接返回 False。
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            None
        )

        chain = SimpleNamespace(type=ChainType.EVM, code="eth")
        crypto = SimpleNamespace(
            symbol="USDT",
            is_native=False,
            get_decimals=Mock(return_value=6),
        )
        project = SimpleNamespace(id=1)
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
        deposit = SimpleNamespace(
            id=1,
            pk=1,
            status=DepositStatus.COMPLETED,
            collection_id=None,
            customer=customer,
            customer_id=1,
            transfer=transfer,
        )
        lock_group_mock.return_value = [deposit]

        collected = DepositService.collect_deposit(deposit)
        self.assertFalse(collected)
        adapter_factory_mock.assert_not_called()

    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    @patch.object(DepositService, "_lock_collectible_group")
    def test_collect_deposit_returns_false_when_zero_balance(
        self,
        lock_group_mock,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
    ):
        # 链上余额为 0 时应直接返回 False。
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(address="0xrecipient")
        )
        chain = SimpleNamespace(type=ChainType.EVM, code="eth")
        crypto = SimpleNamespace(
            symbol="USDT",
            is_native=False,
            get_decimals=Mock(return_value=6),
        )
        project = SimpleNamespace(id=1)
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
        deposit = SimpleNamespace(
            id=1,
            pk=1,
            status=DepositStatus.COMPLETED,
            collection_id=None,
            customer=customer,
            customer_id=1,
            transfer=transfer,
        )
        lock_group_mock.return_value = [deposit]
        deposit_address_get_mock.return_value = SimpleNamespace(
            address=SimpleNamespace(address="0xdeposit")
        )
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=0)
        )

        collected = DepositService.collect_deposit(deposit)
        self.assertFalse(collected)

    # -- content property null 保护 --

    def test_content_property_handles_null_customer(self):
        # customer 为 None 时 content 不应抛 AttributeError。
        transfer = SimpleNamespace(
            chain=SimpleNamespace(code="eth"),
            block=100,
            hash="0x" + "a" * 64,
            crypto=SimpleNamespace(symbol="USDT"),
            amount=Decimal("1.5"),
        )

        # 直接调用 Deposit.content.fget 绕过 Django 描述符
        fake_deposit = SimpleNamespace(customer=None, transfer=transfer)
        content = Deposit.content.fget(fake_deposit)

        self.assertIsNone(content["data"]["uid"])
        self.assertEqual(content["data"]["chain"], "eth")



class DepositServiceDecimalsTests(SimpleTestCase):
    def test_inactive_placeholder_transfer_does_not_create_deposit(self):
        # inactive 占位币允许进入余额统计，但不能进入商户充值业务流。
        transfer = SimpleNamespace(
            chain=SimpleNamespace(type=ChainType.EVM),
            crypto=SimpleNamespace(active=False),
        )

        with patch(
            "deposits.service.DepositAddress.objects.get"
        ) as deposit_address_get_mock:
            created = DepositService.try_create_deposit(transfer)

        self.assertFalse(created)
        deposit_address_get_mock.assert_not_called()

    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    @patch.object(DepositService, "_ensure_gas_and_check", return_value=True)
    @patch.object(DepositService, "_lock_collectible_group")
    @patch("deposits.service.DepositCollection.objects")
    @patch("deposits.service.Deposit.objects.filter")
    @patch("evm.models.EvmBroadcastTask.schedule_transfer")
    def test_collect_deposit_uses_chain_specific_crypto_decimals(
        self,
        schedule_transfer_mock,
        deposit_filter_mock,
        collection_objects_mock,
        lock_group_mock,
        ensure_gas_mock,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
    ):
        # 覆盖精度场景下，归集发送金额必须按链特定精度换算，而不是 Crypto 默认精度。
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(
                address=Web3.to_checksum_address(
                    "0x00000000000000000000000000000000000000aa"
                )
            )
        )
        # mock 占位 collection 创建和 deposit 批量更新
        collection_objects_mock.create.return_value = SimpleNamespace(pk=999)
        deposit_filter_mock.return_value.update = Mock()
        collection_objects_mock.filter.return_value.update = Mock()
        schedule_transfer_mock.return_value = SimpleNamespace(base_task=Mock())

        chain = SimpleNamespace(
            type=ChainType.EVM,
            code="bsc",
            native_coin=SimpleNamespace(symbol="BNB"),
        )
        crypto = SimpleNamespace(
            symbol="USDT",
            decimals=6,
            is_native=False,
            get_decimals=Mock(return_value=18),
            price=Mock(return_value=Decimal("1")),
        )
        project = SimpleNamespace(id=1, gather_worth=Decimal("0.1"))
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto, amount=Decimal("1"))
        deposit = SimpleNamespace(
            id=1,
            pk=1,
            status="completed",
            collection_id=None,
            customer=customer,
            customer_id=1,
            transfer=transfer,
            created_at=timezone.now(),
            save=Mock(),
        )
        lock_group_mock.return_value = [deposit]

        fake_addr = SimpleNamespace(
            address="0xdeposit",
            send_crypto=Mock(return_value="0x" + "a" * 64),
        )
        deposit_address_get_mock.return_value = SimpleNamespace(address=fake_addr)

        adapter = SimpleNamespace(get_balance=Mock(return_value=10**18))
        adapter_factory_mock.return_value = adapter

        collected = DepositService.collect_deposit(deposit)

        self.assertTrue(collected)
        ensure_gas_mock.assert_called_once()
        schedule_transfer_mock.assert_called_once_with(
            crypto=crypto,
            chain=chain,
            address=fake_addr,
            to=Web3.to_checksum_address("0x00000000000000000000000000000000000000aa"),
            value_raw=10**18,
            transfer_type=TransferType.DepositCollection,
        )

    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    @patch.object(DepositService, "_ensure_gas_and_check", return_value=True)
    @patch.object(DepositService, "_lock_collectible_group")
    @patch("deposits.service.DepositCollection.objects")
    @patch("deposits.service.Deposit.objects.filter")
    @patch.object(DepositService, "_cleanup_placeholder_collection")
    def test_collect_deposit_failure_does_not_persist_collection_hash(
        self,
        cleanup_mock,
        deposit_filter_mock,
        collection_objects_mock,
        lock_group_mock,
        ensure_gas_mock,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
    ):
        # 归集发送失败时占位 collection 应被清理，deposit 可被下次重试。
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(address="0xrecipient")
        )
        # mock 占位 collection 创建和 deposit 批量更新
        collection_objects_mock.create.return_value = SimpleNamespace(pk=999)
        deposit_filter_mock.return_value.update = Mock()

        chain = SimpleNamespace(
            type=ChainType.EVM,
            code="eth",
            native_coin=SimpleNamespace(symbol="ETH"),
        )
        crypto = SimpleNamespace(
            symbol="USDT",
            is_native=False,
            get_decimals=Mock(return_value=6),
            price=Mock(return_value=Decimal("1")),
        )
        project = SimpleNamespace(id=1, gather_worth=Decimal("0.1"))
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto, amount=Decimal("1"))
        deposit = SimpleNamespace(
            id=2,
            pk=2,
            status=DepositStatus.COMPLETED,
            collection_id=None,
            customer=customer,
            customer_id=1,
            transfer=transfer,
            created_at=timezone.now(),
            save=Mock(),
        )
        lock_group_mock.return_value = [deposit]
        fake_addr = SimpleNamespace(
            address="0xdeposit",
            send_crypto=Mock(side_effect=RuntimeError("broadcast failed")),
        )
        deposit_address_get_mock.return_value = SimpleNamespace(address=fake_addr)
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=10**6)
        )

        collected = DepositService.collect_deposit(deposit)

        self.assertFalse(collected)
        # 广播失败后占位 collection 应被清理
        cleanup_mock.assert_called_once()
        ensure_gas_mock.assert_called_once()

    def test_should_collect_uses_chain_specific_crypto_decimals(self):
        # 链特定精度为 18、默认精度为 6 时，0.5 个代币不应被误判成巨额资产。
        chain = SimpleNamespace(type=ChainType.EVM, code="bsc")
        crypto = SimpleNamespace(
            symbol="USDT",
            decimals=6,
            get_decimals=Mock(return_value=18),
            price=Mock(return_value=Decimal("1")),
        )
        project = SimpleNamespace(gather_worth=Decimal("1"), gather_period=7)
        customer = SimpleNamespace(project=project)
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
        deposit = SimpleNamespace(
            customer=customer,
            transfer=transfer,
            created_at=timezone.now() - timedelta(days=1),
        )

        should_collect = DepositService._should_collect(deposit, Decimal("0.5"))

        self.assertFalse(should_collect)

    @patch.object(DepositService, "_get_gas_price", return_value=20_000_000_000)
    def test_ensure_gas_and_check_uses_correct_recharge_formula(
        self, get_gas_price_mock
    ):
        # Gas 补充金额必须按 min(5*erc20_gas_cost, 10*native_gas_cost) 公式换算。
        native_coin = SimpleNamespace(
            symbol="BNB",
            get_decimals=Mock(return_value=18),
        )
        vault_addr = SimpleNamespace(send_crypto=Mock())
        wallet = SimpleNamespace(
            get_address=Mock(return_value=vault_addr),
        )
        project = SimpleNamespace(wallet=wallet)
        customer = SimpleNamespace(project=project)
        chain = SimpleNamespace(
            type=ChainType.EVM, code="bsc", native_coin=native_coin,
            base_transfer_gas=50_000, erc20_transfer_gas=100_000,
        )
        crypto = SimpleNamespace(symbol="USDT", is_native=False)
        deposit = SimpleNamespace(
            customer=customer,
            transfer=SimpleNamespace(chain=chain, crypto=crypto),
        )
        deposit_address = SimpleNamespace(address="0xdeposit")
        adapter = SimpleNamespace(get_balance=Mock(return_value=0))

        result = DepositService._ensure_gas_and_check(
            deposit=deposit,
            deposit_address=deposit_address,
            adapter=adapter,
            collection_amount=Decimal("100"),
        )

        self.assertFalse(result)
        vault_addr.send_crypto.assert_called_once_with(
            crypto=native_coin,
            chain=chain,
            to="0xdeposit",
            amount=Decimal("0.01"),
            transfer_type=TransferType.GasRecharge,
        )


class EnsureGasAndCheckTests(SimpleTestCase):
    """_ensure_gas_and_check 的独立单元测试。"""

    def _make_fixtures(self, *, crypto_is_native, gas_price=10, balance=0, native_balance=0):
        """构造通用测试 fixtures。"""
        native_coin = SimpleNamespace(
            symbol="ETH", get_decimals=Mock(return_value=18), is_native=True
        )
        chain = SimpleNamespace(
            type=ChainType.EVM, code="eth",
            native_coin=native_coin,
            base_transfer_gas=50_000,
            erc20_transfer_gas=100_000,
        )
        if crypto_is_native:
            crypto = native_coin
        else:
            crypto = SimpleNamespace(
                symbol="USDT", is_native=False, get_decimals=Mock(return_value=6)
            )

        vault_addr = SimpleNamespace(send_crypto=Mock())
        wallet = SimpleNamespace(get_address=Mock(return_value=vault_addr))
        project = SimpleNamespace(wallet=wallet)
        customer = SimpleNamespace(project=project)
        deposit = SimpleNamespace(
            id=1, customer=customer,
            transfer=SimpleNamespace(chain=chain, crypto=crypto),
        )
        deposit_address = SimpleNamespace(address="0xdeposit")

        def mock_get_balance(addr, ch, cr):
            if cr == native_coin or getattr(cr, 'is_native', False):
                return balance if crypto_is_native else native_balance
            return balance

        adapter = SimpleNamespace(get_balance=Mock(side_effect=mock_get_balance))
        return deposit, deposit_address, adapter, vault_addr, chain

    @patch.object(DepositService, "_get_gas_price", return_value=10)
    def test_native_sufficient_returns_true(self, _gp):
        # 原生币余额充足（>= 归集金额 + 2 * gas）应直接返回 True。
        # gas = 10 * 50_000 = 500_000; need 10**18 + 1_000_000
        deposit, addr, adapter, vault, _ = self._make_fixtures(
            crypto_is_native=True, balance=10**18 + 10**6
        )
        result = DepositService._ensure_gas_and_check(
            deposit=deposit, deposit_address=addr, adapter=adapter,
            collection_amount=Decimal("1"),
        )
        self.assertTrue(result)
        vault.send_crypto.assert_not_called()

    @patch.object(DepositService, "_get_gas_price", return_value=10)
    def test_native_insufficient_recharges_and_returns_false(self, _gp):
        # 原生币余额恰好等于归集金额（无多余 gas），应补充 gas 并跳过。
        deposit, addr, adapter, vault, _ = self._make_fixtures(
            crypto_is_native=True, balance=10**18
        )
        result = DepositService._ensure_gas_and_check(
            deposit=deposit, deposit_address=addr, adapter=adapter,
            collection_amount=Decimal("1"),
        )
        self.assertFalse(result)
        vault.send_crypto.assert_called_once()

    @patch.object(DepositService, "_get_gas_price", return_value=10)
    def test_token_sufficient_returns_true(self, _gp):
        # 代币归集时原生币余额 >= erc20 gas 应返回 True。
        # erc20_gas = 10 * 100_000 = 1_000_000
        deposit, addr, adapter, vault, _ = self._make_fixtures(
            crypto_is_native=False, native_balance=10**6
        )
        result = DepositService._ensure_gas_and_check(
            deposit=deposit, deposit_address=addr, adapter=adapter,
            collection_amount=Decimal("100"),
        )
        self.assertTrue(result)
        vault.send_crypto.assert_not_called()

    @patch.object(DepositService, "_get_gas_price", return_value=10)
    def test_token_insufficient_recharges_and_returns_false(self, _gp):
        # 代币归集时原生币余额不足，应补充并跳过。
        deposit, addr, adapter, vault, _ = self._make_fixtures(
            crypto_is_native=False, native_balance=0
        )
        result = DepositService._ensure_gas_and_check(
            deposit=deposit, deposit_address=addr, adapter=adapter,
            collection_amount=Decimal("100"),
        )
        self.assertFalse(result)
        vault.send_crypto.assert_called_once()

    def test_collection_amount_is_sum_of_deposits(self):
        # 归集金额 = 分组内充值金额之和。
        deposits = [
            SimpleNamespace(transfer=SimpleNamespace(amount=Decimal("1.5"))),
            SimpleNamespace(transfer=SimpleNamespace(amount=Decimal("2.3"))),
            SimpleNamespace(transfer=SimpleNamespace(amount=Decimal("0.7"))),
        ]
        total = DepositService._calculate_collection_amount(deposits)
        self.assertEqual(total, Decimal("4.5"))

    @patch.object(DepositService, "_get_gas_price", return_value=20_000_000_000)
    def test_gas_recharge_uses_min_formula(self, _gp):
        # Gas 补充金额 = min(5*erc20_gas, 10*native_gas)。
        # native: 20G * 50k = 10^15; erc20: 20G * 100k = 2*10^15
        # 10 * 10^15 = 10^16; 5 * 2*10^15 = 10^16 → min = 10^16 → Decimal("0.01")
        deposit, addr, adapter, vault, _ = self._make_fixtures(
            crypto_is_native=False, native_balance=0, gas_price=20_000_000_000
        )
        DepositService._ensure_gas_and_check(
            deposit=deposit, deposit_address=addr, adapter=adapter,
            collection_amount=Decimal("100"),
        )
        vault.send_crypto.assert_called_once()
        call_kwargs = vault.send_crypto.call_args[1]
        self.assertEqual(call_kwargs["amount"], Decimal("0.01"))
        self.assertEqual(call_kwargs["transfer_type"], TransferType.GasRecharge)


class DepositTransferRematchTests(TestCase):
    @patch("deposits.service.WebhookService.create_event")
    def test_confirm_deposit_emits_completed_webhook(self, create_event_mock):
        # Deposit 显式确认后必须直接发完成通知，不再依赖 post_save signal。
        project = Project.objects.create(
            name="DemoConfirm",
            wallet=Wallet.objects.create(),
            pre_notify=True,
        )
        customer = Customer.objects.create(project=project, uid="customer-confirm")
        chain = Chain.objects.create(
            name="EthereumConfirm",
            code="eth-confirm",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="Ethereum Confirm",
                symbol="ETHC",
                coingecko_id="ethereum",
            ),
            chain_id=101,
            rpc="http://localhost:8545",
            active=True,
        )
        addr = Address.objects.create(
            wallet=project.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x0000000000000000000000000000000000000011",
        )
        DepositAddress.objects.create(
            customer=customer,
            chain_type=chain.type,
            address=addr,
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "4" * 64,
            event_id="erc20:4",
            crypto=Crypto.objects.create(
                name="Tether Confirm",
                symbol="USDTC",
                coingecko_id="tether",
            ),
            from_address="0x0000000000000000000000000000000000000002",
            to_address=addr.address,
            value="1",
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        created = DepositService.try_create_deposit(transfer)
        self.assertTrue(created)
        create_event_mock.reset_mock()

        DepositService.confirm_deposit(transfer.deposit)

        create_event_mock.assert_called_once()

    @patch("evm.models.EvmBroadcastTask.schedule_transfer")
    @patch.object(DepositService, "_ensure_gas_and_check", return_value=True)
    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.RecipientAddress.objects.filter")
    def test_collect_deposit_marks_same_group_records_with_one_collection_hash(
        self,
        recipient_filter_mock,
        adapter_factory_mock,
        ensure_gas_mock,
        schedule_transfer_mock,
    ):
        # 同一客户在同链同币下多笔完成充币应共享一笔归集交易，不能重复发起第二笔归集。
        project = Project.objects.create(
            name="DemoGroupCollect",
            wallet=Wallet.objects.create(),
            gather_worth=Decimal("0.1"),
        )
        customer = Customer.objects.create(
            project=project, uid="customer-group-collect"
        )
        native = Crypto.objects.create(
            name="Ethereum Collect Native",
            symbol="ETHGC",
            coingecko_id="ethereum-group-collect-native",
        )
        crypto = Crypto.objects.create(
            name="Tether Group Collect",
            symbol="USDTGC",
            prices={"USD": "1"},
            coingecko_id="tether-group-collect",
            decimals=6,
        )
        chain = Chain.objects.create(
            name="Ethereum Group Collect",
            code="eth-group-collect",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=201,
            rpc="http://localhost:8545",
            active=True,
        )
        addr = Address.objects.create(
            wallet=project.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x00000000000000000000000000000000000000C1",
        )
        DepositAddress.objects.create(
            customer=customer,
            chain_type=chain.type,
            address=addr,
        )
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(address="0x00000000000000000000000000000000000000D1")
        )
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=10**6)
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.DepositCollection,
            crypto=crypto,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000000d1"
            ),
            amount=Decimal("3"),
        )
        schedule_transfer_mock.return_value = SimpleNamespace(base_task=base_task)
        fake_addr = SimpleNamespace(
            address=addr.address,
            send_crypto=Mock(return_value="0x" + "c" * 64),
        )

        with patch(
            "deposits.service.DepositAddress.objects.get",
            return_value=SimpleNamespace(address=fake_addr),
        ):
            transfer1 = OnchainTransfer.objects.create(
                chain=chain,
                block=1,
                hash="0x" + "6" * 64,
                event_id="erc20:6",
                crypto=crypto,
                from_address="0x0000000000000000000000000000000000000101",
                to_address=addr.address,
                value="1",
                amount=Decimal("1"),
                timestamp=1,
                datetime=timezone.now(),
                status=TransferStatus.CONFIRMED,
                type=TransferType.Deposit,
            )
            transfer2 = OnchainTransfer.objects.create(
                chain=chain,
                block=2,
                hash="0x" + "7" * 64,
                event_id="erc20:7",
                crypto=crypto,
                from_address="0x0000000000000000000000000000000000000102",
                to_address=addr.address,
                value="2",
                amount=Decimal("2"),
                timestamp=2,
                datetime=timezone.now(),
                status=TransferStatus.CONFIRMED,
                type=TransferType.Deposit,
            )
            deposit1 = Deposit.objects.create(
                customer=customer,
                transfer=transfer1,
                status=DepositStatus.COMPLETED,
            )
            deposit2 = Deposit.objects.create(
                customer=customer,
                transfer=transfer2,
                status=DepositStatus.COMPLETED,
            )

            collected = DepositService.collect_deposit(deposit1)
            duplicate = DepositService.collect_deposit(deposit2)

        self.assertTrue(collected)
        self.assertFalse(duplicate)
        deposit1.refresh_from_db()
        deposit2.refresh_from_db()
        # 同组 Deposit 应指向同一个 DepositCollection，共享归集哈希
        self.assertIsNotNone(deposit1.collection_id)
        self.assertEqual(deposit1.collection_id, deposit2.collection_id)
        self.assertIsNone(deposit1.collection.collection_hash)
        self.assertEqual(deposit1.collection.broadcast_task, base_task)
        schedule_transfer_mock.assert_called_once()
        ensure_gas_mock.assert_called_once()

    @patch("evm.models.EvmBroadcastTask.schedule_transfer")
    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    @patch.object(Address, "send_crypto", return_value="0x" + "0" * 64)
    @patch.object(Wallet, "get_address")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_multi_deposit_gas_recharge_then_collect(
        self,
        chain_w3_mock,
        wallet_get_address_mock,
        send_crypto_mock,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
        schedule_transfer_mock,
    ):
        """
        新客户连续充值多笔，首轮 gas 不足触发补充并跳过，
        二轮 gas 到账后合并归集，归集金额 = 充值总额。
        """
        project = Project.objects.create(
            name="DemoGasRechargeGroup",
            wallet=Wallet.objects.create(),
            gather_worth=Decimal("0.1"),
        )
        customer = Customer.objects.create(
            project=project, uid="customer-gas-recharge-group"
        )
        native = Crypto.objects.create(
            name="Ethereum GasRechargeGroup Native",
            symbol="ETHGRG",
            prices={"USD": "2000"},
            coingecko_id="ethereum-gas-recharge-group-native",
        )
        chain = Chain.objects.create(
            name="Ethereum GasRechargeGroup",
            code="eth-gas-recharge-group",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=301,
            rpc="http://localhost:8545",
            active=True,
        )
        # gas_price=10, base_transfer_gas=50000 → native_gas=500_000
        # 2 * native_gas = 1_000_000
        chain_w3_mock.return_value = SimpleNamespace(
            eth=SimpleNamespace(gas_price=10, send_raw_transaction=Mock())
        )
        addr = Address.objects.create(
            wallet=project.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x00000000000000000000000000000000000005C1",
        )
        # Wallet.get_address 已被 mock，返回 deposit addr（send_crypto 也已被类级 mock）
        wallet_get_address_mock.return_value = addr
        DepositAddress.objects.create(
            customer=customer, chain_type=chain.type, address=addr,
        )
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(address="0x00000000000000000000000000000000000005D1")
        )
        deposit_address_get_mock.return_value = SimpleNamespace(
            address=SimpleNamespace(address=addr.address)
        )

        # 两笔充值：1 ETH + 2 ETH = 总计 3 ETH
        transfer1 = OnchainTransfer.objects.create(
            chain=chain, block=1, hash="0x" + "f1" * 32, event_id="native:grg1",
            crypto=native,
            from_address="0x0000000000000000000000000000000000000501",
            to_address=addr.address,
            value="1", amount=Decimal("1"), timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED, type=TransferType.Deposit,
        )
        transfer2 = OnchainTransfer.objects.create(
            chain=chain, block=2, hash="0x" + "f2" * 32, event_id="native:grg2",
            crypto=native,
            from_address="0x0000000000000000000000000000000000000502",
            to_address=addr.address,
            value="2", amount=Decimal("2"), timestamp=2,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED, type=TransferType.Deposit,
        )
        deposit1 = Deposit.objects.create(
            customer=customer, transfer=transfer1, status=DepositStatus.COMPLETED,
        )
        deposit2 = Deposit.objects.create(
            customer=customer, transfer=transfer2, status=DepositStatus.COMPLETED,
        )

        # --- 第一轮：余额 = 3 ETH（恰好等于充值总额，无多余 gas）→ 补充 gas 并跳过 ---
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=3 * 10**18)
        )
        collected_round1 = DepositService.collect_deposit(deposit1)
        self.assertFalse(collected_round1)
        # 补充了 gas（send_crypto 被 vault 调用），但未创建 collection
        deposit1.refresh_from_db()
        deposit2.refresh_from_db()
        self.assertIsNone(deposit1.collection_id)
        self.assertIsNone(deposit2.collection_id)
        schedule_transfer_mock.assert_not_called()

        # --- 第二轮：gas 到账，余额 = 3 ETH + 足够 gas → 合并归集 ---
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=3 * 10**18 + 10**7)
        )
        base_task = BroadcastTask.objects.create(
            chain=chain, address=addr,
            transfer_type=TransferType.DepositCollection,
            crypto=native,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000005d1"
            ),
            amount=Decimal("3"),
        )
        schedule_transfer_mock.return_value = SimpleNamespace(base_task=base_task)

        collected_round2 = DepositService.collect_deposit(deposit1)
        self.assertTrue(collected_round2)

        deposit1.refresh_from_db()
        deposit2.refresh_from_db()
        # 两笔充值共享同一个 DepositCollection
        self.assertIsNotNone(deposit1.collection_id)
        self.assertEqual(deposit1.collection_id, deposit2.collection_id)
        # 归集金额 = 1 + 2 = 3 ETH（非余额），value_raw = 3 * 10^18
        schedule_transfer_mock.assert_called_once()
        call_kwargs = schedule_transfer_mock.call_args[1]
        self.assertEqual(call_kwargs["value_raw"], 3 * 10**18)

    def test_confirm_collection_marks_same_hash_group_completed(self):
        # 同一归集哈希命中的多条充币记录在确认后要一起写入 collected_at。
        project = Project.objects.create(
            name="DemoGroupConfirm",
            wallet=Wallet.objects.create(),
        )
        customer = Customer.objects.create(
            project=project, uid="customer-group-confirm"
        )
        native = Crypto.objects.create(
            name="Ethereum Confirm Native",
            symbol="ETHGCC",
            coingecko_id="ethereum-group-confirm-native",
        )
        crypto = Crypto.objects.create(
            name="Tether Group Confirm",
            symbol="USDTGCC",
            coingecko_id="tether-group-confirm",
        )
        chain = Chain.objects.create(
            name="Ethereum Group Confirm",
            code="eth-group-confirm",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=202,
            rpc="http://localhost:8545",
            active=True,
        )
        transfer1 = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "8" * 64,
            event_id="erc20:8",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000201",
            to_address="0x0000000000000000000000000000000000000211",
            value="1",
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        transfer2 = OnchainTransfer.objects.create(
            chain=chain,
            block=2,
            hash="0x" + "9" * 64,
            event_id="erc20:9",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000202",
            to_address="0x0000000000000000000000000000000000000211",
            value="2",
            amount=Decimal("2"),
            timestamp=2,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        collection = DepositCollection.objects.create(collection_hash="0x" + "d" * 64)
        deposit1 = Deposit.objects.create(
            customer=customer,
            transfer=transfer1,
            status=DepositStatus.COMPLETED,
            collection=collection,
        )
        deposit2 = Deposit.objects.create(
            customer=customer,
            transfer=transfer2,
            status=DepositStatus.COMPLETED,
            collection=collection,
        )

        DepositService.confirm_collection(collection)

        collection.refresh_from_db()
        self.assertIsNotNone(collection.collected_at)
        # 同一 DepositCollection 下的所有充币记录均通过 collection.collected_at 反映归集完成
        deposit1.refresh_from_db()
        deposit2.refresh_from_db()
        self.assertEqual(deposit1.collection_id, collection.pk)
        self.assertEqual(deposit2.collection_id, collection.pk)

    def test_drop_collection_clears_hash_for_retry(self):
        # 归集失效后应清空 collection_hash 和 collection_transfer，使充币重新进入待归集队列。
        project = Project.objects.create(
            name="DemoDropCollection",
            wallet=Wallet.objects.create(),
        )
        customer = Customer.objects.create(
            project=project, uid="customer-drop-collection"
        )
        native = Crypto.objects.create(
            name="Ethereum Drop Collection Native",
            symbol="ETHDC",
            coingecko_id="ethereum-drop-collection-native",
        )
        crypto = Crypto.objects.create(
            name="Tether Drop Collection",
            symbol="USDTDC",
            coingecko_id="tether-drop-collection",
        )
        chain = Chain.objects.create(
            name="Ethereum Drop Collection",
            code="eth-drop-collection",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=204,
            rpc="http://localhost:8545",
            active=True,
        )
        collection_hash = "0x" + "e" * 64
        transfer1 = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "d1" * 32,
            event_id="erc20:d1",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000401",
            to_address="0x0000000000000000000000000000000000000411",
            value="1",
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        transfer2 = OnchainTransfer.objects.create(
            chain=chain,
            block=2,
            hash="0x" + "d2" * 32,
            event_id="erc20:d2",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000402",
            to_address="0x0000000000000000000000000000000000000411",
            value="2",
            amount=Decimal("2"),
            timestamp=2,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        collection = DepositCollection.objects.create(
            collection_hash=collection_hash,
        )
        deposit1 = Deposit.objects.create(
            customer=customer,
            transfer=transfer1,
            status=DepositStatus.COMPLETED,
            collection=collection,
        )
        deposit2 = Deposit.objects.create(
            customer=customer,
            transfer=transfer2,
            status=DepositStatus.COMPLETED,
            collection=collection,
        )

        DepositService.drop_collection(collection)

        deposit1.refresh_from_db()
        deposit2.refresh_from_db()
        self.assertIsNone(deposit1.collection_id)
        self.assertIsNone(deposit2.collection_id)

    @patch("evm.models.EvmBroadcastTask.schedule_transfer")
    @patch.object(DepositService, "_ensure_gas_and_check", return_value=True)
    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    def test_gather_task_only_sends_once_for_same_collect_group(
        self,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
        ensure_gas_mock,
        schedule_transfer_mock,
    ):
        # 定时归集任务即使一次捞到同组两条 completed deposit，也只能真正发出一笔归集交易。
        project = Project.objects.create(
            name="DemoGroupTask",
            wallet=Wallet.objects.create(),
            gather_worth=Decimal("0.1"),
        )
        customer = Customer.objects.create(project=project, uid="customer-group-task")
        native = Crypto.objects.create(
            name="Ethereum Task Native",
            symbol="ETHGCT",
            coingecko_id="ethereum-group-task-native",
        )
        crypto = Crypto.objects.create(
            name="Tether Group Task",
            symbol="USDTGCT",
            prices={"USD": "1"},
            coingecko_id="tether-group-task",
            decimals=6,
        )
        chain = Chain.objects.create(
            name="Ethereum Group Task",
            code="eth-group-task",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=203,
            rpc="http://localhost:8545",
            active=True,
        )
        addr = Address.objects.create(
            wallet=project.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address="0x00000000000000000000000000000000000003C1",
        )
        DepositAddress.objects.create(
            customer=customer,
            chain_type=chain.type,
            address=addr,
        )
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(address="0x00000000000000000000000000000000000003D1")
        )
        fake_addr = SimpleNamespace(
            address=addr.address,
            send_crypto=Mock(return_value="0x" + "f" * 64),
        )
        deposit_address_get_mock.return_value = SimpleNamespace(address=fake_addr)
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=10**6)
        )
        base_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.DepositCollection,
            crypto=crypto,
            recipient=Web3.to_checksum_address(
                "0x00000000000000000000000000000000000003d1"
            ),
            amount=Decimal("3"),
        )
        schedule_transfer_mock.return_value = SimpleNamespace(base_task=base_task)

        transfer1 = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "a" * 64,
            event_id="erc20:10",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000301",
            to_address=addr.address,
            value="1",
            amount=Decimal("1"),
            timestamp=10,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        transfer2 = OnchainTransfer.objects.create(
            chain=chain,
            block=2,
            hash="0x" + "b" * 64,
            event_id="erc20:11",
            crypto=crypto,
            from_address="0x0000000000000000000000000000000000000302",
            to_address=addr.address,
            value="2",
            amount=Decimal("2"),
            timestamp=11,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        deposit1 = Deposit.objects.create(
            customer=customer,
            transfer=transfer1,
            status=DepositStatus.COMPLETED,
        )
        deposit2 = Deposit.objects.create(
            customer=customer,
            transfer=transfer2,
            status=DepositStatus.COMPLETED,
        )

        gather_deposits.run()

        deposit1.refresh_from_db()
        deposit2.refresh_from_db()
        # 同组 Deposit 应指向同一个 DepositCollection，且只发出一笔归集交易
        self.assertIsNotNone(deposit1.collection_id)
        self.assertEqual(deposit1.collection_id, deposit2.collection_id)
        self.assertIsNone(deposit1.collection.collection_hash)
        self.assertEqual(deposit1.collection.broadcast_task, base_task)
        schedule_transfer_mock.assert_called_once()
        ensure_gas_mock.assert_called_once()


class DepositAddressApiGuardTests(TestCase):
    def test_address_endpoint_rejects_bitcoin_chain_without_allocating_deposit_address(
        self,
    ):
        project = Project.objects.create(
            name="Bitcoin Deposit Guard Project",
            wallet=Wallet.objects.create(),
            ip_white_list="*",
            webhook="https://example.com/webhook",
        )
        btc = Crypto.objects.create(
            name="Bitcoin Native",
            symbol="BTC-DEPOSIT-GUARD",
            coingecko_id="bitcoin-native-guard",
            decimals=8,
        )
        bitcoin_chain = Chain.objects.create(
            name="Bitcoin Mainnet Guard",
            code="btc-guard",
            type=ChainType.BITCOIN,
            native_coin=btc,
            rpc="http://bitcoin.invalid",
            active=True,
            latest_block_number=321,
        )
        request = APIRequestFactory().get(
            "/v1/deposit/address",
            {"uid": "btc-user", "chain": bitcoin_chain.code, "crypto": btc.symbol},
            HTTP_XC_APPID=project.appid,
        )
        force_authenticate(
            request,
            user=User.objects.create(username="deposit-api-btc"),
        )

        with patch("deposits.viewsets.DepositAddress.get_address") as get_address_mock:
            response = DepositViewSet.as_view({"get": "address"})(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["code"], ErrorCode.INVALID_CHAIN.code)
        get_address_mock.assert_not_called()

    def test_address_endpoint_uses_capability_service_to_reject_tron_usdt(
        self,
    ):
        project = Project.objects.create(
            name="Tron Deposit Guard Project",
            wallet=Wallet.objects.create(),
            ip_white_list="*",
            webhook="https://example.com/webhook",
        )
        trx = Crypto.objects.create(
            name="Tron Native",
            symbol="TRX",
            coingecko_id="tron-native-guard",
        )
        usdt = Crypto.objects.create(
            name="Tether on Tron",
            symbol="USDT",
            coingecko_id="tether-tron-guard",
            decimals=6,
        )
        tron_chain = Chain.objects.create(
            name="Tron Mainnet Guard",
            code="tron-guard",
            type=ChainType.TRON,
            native_coin=trx,
            rpc="http://tron.invalid",
            active=True,
            latest_block_number=321,
        )
        ChainToken.objects.create(
            crypto=usdt,
            chain=tron_chain,
            address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            decimals=6,
        )
        request = APIRequestFactory().get(
            "/v1/deposit/address",
            {"uid": "tron-user", "chain": tron_chain.code, "crypto": usdt.symbol},
            HTTP_XC_APPID=project.appid,
        )
        force_authenticate(request, user=User.objects.create(username="deposit-api"))

        with (
            patch(
                "deposits.viewsets.ChainProductCapabilityService.supports_deposit_address",
                return_value=False,
            ) as supports_deposit_address_mock,
            patch("deposits.viewsets.DepositAddress.get_address") as get_address_mock,
        ):
            response = DepositViewSet.as_view({"get": "address"})(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data["code"], ErrorCode.INVALID_CHAIN.code)
        supports_deposit_address_mock.assert_called_once_with(
            chain=tron_chain,
            crypto=usdt,
        )
        get_address_mock.assert_not_called()


@override_settings(
    SIGNER_BACKEND="remote",
    SIGNER_BASE_URL="http://signer.internal",
    SIGNER_SHARED_SECRET="secret",
)
class DepositRemoteSignerFlowTests(TestCase):
    @patch("chains.signer.get_signer_backend")
    def test_deposit_address_allocation_uses_remote_signer_without_local_mnemonic(
        self,
        get_signer_backend_mock,
    ):
        # remote signer 模式下，充币地址分配必须只走远端派生，不能再读取本地助记词。
        signer_backend = Mock()
        signer_backend.derive_address.return_value = Web3.to_checksum_address(
            "0x000000000000000000000000000000000000d001"
        )
        get_signer_backend_mock.return_value = signer_backend
        wallet = Wallet.objects.create()
        with patch("projects.signals.Wallet.generate", return_value=wallet):
            project = Project.objects.create(
                name="RemoteDepositAddressProject",
                wallet=wallet,
            )
        customer = Customer.objects.create(
            project=project, uid="customer-remote-deposit-address"
        )
        chain = Chain.objects.create(
            name="Ethereum Remote Deposit Address",
            code="eth-remote-deposit-address",
            type=ChainType.EVM,
            native_coin=Crypto.objects.create(
                name="Ethereum Remote Deposit Address Native",
                symbol="ETHRDA",
                coingecko_id="ethereum-remote-deposit-address-native",
            ),
            chain_id=401,
            rpc="http://localhost:8545",
            active=True,
        )

        with patch("projects.signals.Wallet.generate", return_value=wallet):
            address = DepositAddress.get_address(chain=chain, customer=customer)

        deposit_addr = DepositAddress.objects.get(
            customer=customer, chain_type=chain.type
        )
        self.assertEqual(
            address,
            Web3.to_checksum_address("0x000000000000000000000000000000000000d001"),
        )
        self.assertEqual(deposit_addr.address.address, address)
        signer_backend.derive_address.assert_called_once()

    @patch("evm.models.get_signer_backend")
    @patch.object(EvmBroadcastTask, "_next_nonce", return_value=0)
    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.RecipientAddress.objects.filter")
    @patch.object(Chain, "w3", new_callable=PropertyMock)
    def test_collect_deposit_uses_remote_signer_without_local_mnemonic(
        self,
        chain_w3_mock,
        recipient_filter_mock,
        adapter_factory_mock,
        _next_nonce_mock,
        get_signer_backend_mock,
    ):
        # remote signer 模式下，归集链路应直接使用远端签名，不允许回退到主应用本地持钥。
        signer_backend = Mock()
        signer_backend.sign_evm_transaction.return_value = SimpleNamespace(
            tx_hash="0x" + "e" * 64,
            raw_transaction="0xdeadbeef",
        )
        get_signer_backend_mock.return_value = signer_backend
        wallet = Wallet.objects.create()
        with patch("projects.signals.Wallet.generate", return_value=wallet):
            project = Project.objects.create(
                name="RemoteDepositCollectProject",
                wallet=wallet,
                gather_worth=Decimal("0.1"),
            )
        customer = Customer.objects.create(
            project=project, uid="customer-remote-deposit-collect"
        )
        native = Crypto.objects.create(
            name="Ethereum Remote Deposit Collect Native",
            symbol="ETHRDC",
            prices={"USD": "1"},
            coingecko_id="ethereum-remote-deposit-collect-native",
        )
        chain = Chain.objects.create(
            name="Ethereum Remote Deposit Collect",
            code="eth-remote-deposit-collect",
            type=ChainType.EVM,
            native_coin=native,
            chain_id=402,
            rpc="http://localhost:8545",
            active=True,
        )
        chain_w3_mock.return_value = SimpleNamespace(
            eth=SimpleNamespace(gas_price=5, send_raw_transaction=Mock())
        )
        addr = Address.objects.create(
            wallet=wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit,
            bip44_account=0,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000d002"
            ),
        )
        DepositAddress.objects.create(
            customer=customer,
            chain_type=chain.type,
            address=addr,
        )
        recipient_filter_mock.return_value.order_by.return_value.first.return_value = (
            SimpleNamespace(
                address=Web3.to_checksum_address(
                    "0x000000000000000000000000000000000000d003"
                )
            )
        )
        adapter_factory_mock.return_value = SimpleNamespace(
            get_balance=Mock(return_value=10**18 + 10**6)
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash="0x" + "1" * 64,
            event_id="native:remote-collect",
            crypto=native,
            from_address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000d010"
            ),
            to_address=addr.address,
            value="1",
            amount=Decimal("1"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        deposit = Deposit.objects.create(
            customer=customer,
            transfer=transfer,
            status=DepositStatus.COMPLETED,
        )

        collected = DepositService.collect_deposit(deposit)

        self.assertTrue(collected)
        deposit.refresh_from_db()
        self.assertIsNotNone(deposit.collection_id)
        self.assertIsNotNone(deposit.collection.broadcast_task_id)
        self.assertIsNone(deposit.collection.collection_hash)
        signer_backend.sign_evm_transaction.assert_not_called()


# ---------------------------------------------------------------------------
# Anvil 集成测试：充币归集完整链路
# 依赖本地 anvil (docker compose -f docker-compose.dev.yml up ethereum)
# ---------------------------------------------------------------------------

ANVIL_RPC = "http://127.0.0.1:8545"

_anvil_available = False
try:
    _w3_probe = Web3(Web3.HTTPProvider(ANVIL_RPC, request_kwargs={"timeout": 2}))
    _anvil_available = _w3_probe.is_connected()
except Exception:  # noqa: BLE001
    pass


@unittest.skipUnless(_anvil_available, "需要本地 anvil")
class DepositCollectionAnvilTests(TestCase):
    """
    依赖本地 anvil 的充币归集完整链路集成测试。

    仅 mock 两处：
      1. Wallet.get_address — 跳过 remote signer
      2. EvmBroadcastTask.schedule_transfer — 通过 anvil impersonation 做真实链上转账
    其余（余额查询、gas price、归集逻辑）全部走真实链路。
    """

    DEPOSIT_ADDR = Web3.to_checksum_address(
        "0x1111111111111111111111111111111111111111"
    )
    VAULT_ADDR = Web3.to_checksum_address(
        "0x2222222222222222222222222222222222222222"
    )
    RECIPIENT_ADDR = Web3.to_checksum_address(
        "0x3333333333333333333333333333333333333333"
    )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.w3 = Web3(Web3.HTTPProvider(ANVIL_RPC, request_kwargs={"timeout": 8}))

    def setUp(self):
        # anvil 快照，每个测试独立
        self._snapshot = self.w3.provider.make_request("evm_snapshot", [])["result"]

        # -- DB fixtures --
        self.native = Crypto.objects.create(
            name="ETH Anvil", symbol="ETH_ANV",
            prices={"USD": "2000"}, coingecko_id="eth-anvil",
        )
        self.chain = Chain.objects.create(
            name="Anvil", code="anvil",
            type=ChainType.EVM, native_coin=self.native,
            chain_id=31337, rpc=ANVIL_RPC, active=True,
        )
        self.wallet = Wallet.objects.create()
        self.project = Project.objects.create(
            name="AnvilProject", wallet=self.wallet,
            gather_worth=Decimal("0.001"), gather_period=7,
        )
        self.customer = Customer.objects.create(
            project=self.project, uid="anvil-customer",
        )
        self.deposit_addr_obj = Address.objects.create(
            wallet=self.wallet, chain_type=ChainType.EVM,
            usage=AddressUsage.Deposit, bip44_account=0,
            address_index=0, address=self.DEPOSIT_ADDR,
        )
        self.vault_addr_obj = Address.objects.create(
            wallet=self.wallet, chain_type=ChainType.EVM,
            usage=AddressUsage.Vault, bip44_account=100_000_000,
            address_index=0, address=self.VAULT_ADDR,
        )
        DepositAddress.objects.create(
            customer=self.customer, chain_type=ChainType.EVM,
            address=self.deposit_addr_obj,
        )
        RecipientAddress.objects.create(
            project=self.project, chain_type=ChainType.EVM,
            address=self.RECIPIENT_ADDR, name="vault",
            used_for_invoice=False, used_for_deposit=True,
        )

        # 为 vault 预充 10 ETH（作为 gas 补充资金池）
        self._set_balance(self.VAULT_ADDR, 10 * 10**18)
        self._set_balance(self.RECIPIENT_ADDR, 0)

        # -- Mocks --
        # 1. Wallet.get_address → 直接返回 vault Address（跳过 signer）
        patcher_wallet = patch.object(Wallet, "get_address", return_value=self.vault_addr_obj)
        self.wallet_get_addr_mock = patcher_wallet.start()
        self.addCleanup(patcher_wallet.stop)

        # 2. EvmBroadcastTask.schedule_transfer → anvil impersonation 真实转账
        patcher_schedule = patch(
            "evm.models.EvmBroadcastTask.schedule_transfer",
            side_effect=self._anvil_schedule_transfer,
        )
        self.schedule_mock = patcher_schedule.start()
        self.addCleanup(patcher_schedule.stop)

    def tearDown(self):
        self.w3.provider.make_request("evm_revert", [self._snapshot])

    # ---- helpers ----

    def _set_balance(self, addr: str, amount_wei: int):
        self.w3.provider.make_request("anvil_setBalance", [addr, hex(amount_wei)])

    def _on_chain_balance(self, addr: str) -> int:
        return self.w3.eth.get_balance(Web3.to_checksum_address(addr))

    def _impersonate_send_eth(self, from_addr: str, to_addr: str, value_wei: int) -> str:
        self.w3.provider.make_request("anvil_impersonateAccount", [from_addr])
        tx = self.w3.eth.send_transaction({
            "from": from_addr, "to": to_addr,
            "value": value_wei, "gas": 21_000,
            "gasPrice": self.w3.eth.gas_price,
        })
        receipt = self.w3.eth.wait_for_transaction_receipt(tx)
        self.w3.provider.make_request("anvil_stopImpersonatingAccount", [from_addr])
        return "0x" + receipt.transactionHash.hex()

    def _anvil_schedule_transfer(self, *, address, crypto, chain, to, value_raw, transfer_type):
        """EvmBroadcastTask.schedule_transfer 的替代：在 anvil 上真实转账。"""
        from_addr = address.address if hasattr(address, "address") else address
        tx_hash = self._impersonate_send_eth(from_addr, to, value_raw)
        decimals = crypto.get_decimals(chain)
        base_task = BroadcastTask.objects.create(
            chain=chain, address=address if isinstance(address, Address) else self.deposit_addr_obj,
            transfer_type=transfer_type, crypto=crypto,
            recipient=to,
            amount=Decimal(value_raw) / Decimal(10**decimals),
            tx_hash=tx_hash,
        )
        return SimpleNamespace(base_task=base_task)

    def _create_deposit(self, amount: Decimal, *, seq: int = 1) -> Deposit:
        """创建 Deposit 记录并在 anvil 上设置对应余额。"""
        decimals = self.native.get_decimals(self.chain)
        amount_raw = int(amount * Decimal(10**decimals))

        # 在链上为充值地址添加余额（模拟用户充币）
        current = self._on_chain_balance(self.DEPOSIT_ADDR)
        self._set_balance(self.DEPOSIT_ADDR, current + amount_raw)

        transfer = OnchainTransfer.objects.create(
            chain=self.chain, block=seq,
            hash="0x" + f"{seq:064x}",
            event_id=f"native:anvil:{seq}",
            crypto=self.native,
            from_address="0x0000000000000000000000000000000000000999",
            to_address=self.DEPOSIT_ADDR,
            value=str(amount_raw), amount=amount,
            timestamp=seq, datetime=timezone.now(),
            status=TransferStatus.CONFIRMED,
            type=TransferType.Deposit,
        )
        return Deposit.objects.create(
            customer=self.customer, transfer=transfer,
            status=DepositStatus.COMPLETED,
        )

    def _collect_until_success(self, deposit: Deposit, max_rounds: int = 3) -> int:
        """
        重复 collect_deposit 直到成功，返回实际尝试次数。
        模拟 gather_deposits 任务的多轮重试行为。
        """
        for attempt in range(1, max_rounds + 1):
            if DepositService.collect_deposit(deposit):
                return attempt
        raise AssertionError(f"归集在 {max_rounds} 轮内未成功")

    # ---- 场景 1: 单笔原生币充值 → gas 不足 → 补充 → 归集 ----

    def test_single_native_deposit_gas_recharge_then_collect(self):
        deposit = self._create_deposit(Decimal("1"), seq=1)

        # 第一轮：余额 = 1 ETH（无多余 gas）→ 应补充 gas 并跳过
        self.assertFalse(DepositService.collect_deposit(deposit))
        deposit.refresh_from_db()
        self.assertIsNone(deposit.collection_id)

        # 验证 deposit 地址收到了 gas 补充（余额 > 1 ETH）
        balance_after_recharge = self._on_chain_balance(self.DEPOSIT_ADDR)
        self.assertGreater(balance_after_recharge, 10**18)

        # 第二轮：gas 已到账 → 归集成功
        self.assertTrue(DepositService.collect_deposit(deposit))
        deposit.refresh_from_db()
        self.assertIsNotNone(deposit.collection_id)

        # 验证 recipient 收到恰好 1 ETH
        recipient_balance = self._on_chain_balance(self.RECIPIENT_ADDR)
        self.assertEqual(recipient_balance, 10**18)

    # ---- 场景 2: 多笔充值合并归集 ----

    def test_multi_deposit_merged_collection(self):
        d1 = self._create_deposit(Decimal("0.5"), seq=1)
        d2 = self._create_deposit(Decimal("1.5"), seq=2)
        d3 = self._create_deposit(Decimal("1"), seq=3)

        # 第一轮 gas 不足
        self.assertFalse(DepositService.collect_deposit(d1))

        # 第二轮归集成功
        self.assertTrue(DepositService.collect_deposit(d1))

        d1.refresh_from_db()
        d2.refresh_from_db()
        d3.refresh_from_db()

        # 三笔充值共享同一 DepositCollection
        self.assertIsNotNone(d1.collection_id)
        self.assertEqual(d1.collection_id, d2.collection_id)
        self.assertEqual(d1.collection_id, d3.collection_id)

        # recipient 收到精确 3 ETH
        self.assertEqual(self._on_chain_balance(self.RECIPIENT_ADDR), 3 * 10**18)

    # ---- 场景 3: gas 充足，无需补充直接归集 ----

    def test_sufficient_gas_no_recharge_needed(self):
        deposit = self._create_deposit(Decimal("1"), seq=1)

        # 额外给 deposit 地址充足 gas（0.01 ETH）
        current = self._on_chain_balance(self.DEPOSIT_ADDR)
        self._set_balance(self.DEPOSIT_ADDR, current + 10**16)

        # 一轮即成功
        self.assertTrue(DepositService.collect_deposit(deposit))

        # 没有 GasRecharge 类型的 BroadcastTask
        gas_tasks = BroadcastTask.objects.filter(
            transfer_type=TransferType.GasRecharge
        )
        self.assertEqual(gas_tasks.count(), 0)

        self.assertEqual(self._on_chain_balance(self.RECIPIENT_ADDR), 10**18)

    # ---- 场景 4: 两轮充值归集，第二轮 gas 耗尽需重新补充 ----

    def test_two_rounds_gas_exhausted_recharge_again(self):
        # -- 第一轮：充 1 ETH --
        d1 = self._create_deposit(Decimal("1"), seq=1)
        rounds = self._collect_until_success(d1)
        self.assertEqual(rounds, 2)  # 第一次补 gas，第二次归集

        d1.refresh_from_db()
        self.assertIsNotNone(d1.collection_id)
        self.assertEqual(self._on_chain_balance(self.RECIPIENT_ADDR), 10**18)

        # -- 清空 deposit 地址残余 gas，模拟 gas 完全耗尽 --
        self._set_balance(self.DEPOSIT_ADDR, 0)

        # -- 第二轮：充 2 ETH --
        d2 = self._create_deposit(Decimal("2"), seq=2)
        # deposit 余额 = 2 ETH（无 gas）→ 需重新补充
        self.assertFalse(DepositService.collect_deposit(d2))
        # gas 补充后再次归集
        self.assertTrue(DepositService.collect_deposit(d2))

        d2.refresh_from_db()
        self.assertIsNotNone(d2.collection_id)
        # 两轮独立 DepositCollection
        self.assertNotEqual(d1.collection_id, d2.collection_id)
        # recipient 累计收到 1 + 2 = 3 ETH
        self.assertEqual(self._on_chain_balance(self.RECIPIENT_ADDR), 3 * 10**18)

    # ---- 场景 5: 连续 3 轮递增充值，每轮独立对账 ----

    def test_three_rounds_progressive_deposits(self):
        amounts = [Decimal("0.1"), Decimal("0.5"), Decimal("2")]
        collection_ids = []
        expected_recipient = 0

        for i, amount in enumerate(amounts, start=1):
            # 每轮清空 gas 强制补充
            self._set_balance(self.DEPOSIT_ADDR, 0)

            deposit = self._create_deposit(amount, seq=i)
            rounds = self._collect_until_success(deposit)
            self.assertGreaterEqual(rounds, 1)

            deposit.refresh_from_db()
            self.assertIsNotNone(deposit.collection_id)
            collection_ids.append(deposit.collection_id)

            # 归集金额精确等于充值金额
            decimals = self.native.get_decimals(self.chain)
            expected_recipient += int(amount * Decimal(10**decimals))
            self.assertEqual(
                self._on_chain_balance(self.RECIPIENT_ADDR), expected_recipient
            )

        # 3 轮 DepositCollection 各自独立
        self.assertEqual(len(set(collection_ids)), 3)

    # ---- 场景 6: 小额低于阈值，deadline 后才归集 ----

    def test_deposit_below_threshold_collected_after_deadline(self):
        # 抬高归集阈值到 $5000，使 0.001 ETH ($2) 低于门槛
        self.project.gather_worth = Decimal("5000")
        self.project.save(update_fields=["gather_worth"])

        deposit = self._create_deposit(Decimal("0.001"), seq=1)

        # 额外给充足 gas，排除 gas 因素
        current = self._on_chain_balance(self.DEPOSIT_ADDR)
        self._set_balance(self.DEPOSIT_ADDR, current + 10**16)

        # worth = 0.001 ETH * $2000 = $2 < gather_worth=$5000 且未过期 → 跳过
        self.assertFalse(DepositService.collect_deposit(deposit))
        deposit.refresh_from_db()
        self.assertIsNone(deposit.collection_id)

        # 修改创建时间到 gather_period 天前 → deadline 过期触发归集
        Deposit.objects.filter(pk=deposit.pk).update(
            created_at=timezone.now() - timedelta(days=self.project.gather_period + 1)
        )
        deposit.refresh_from_db()

        self.assertTrue(DepositService.collect_deposit(deposit))
        deposit.refresh_from_db()
        self.assertIsNotNone(deposit.collection_id)
        self.assertEqual(self._on_chain_balance(self.RECIPIENT_ADDR), 10**15)
