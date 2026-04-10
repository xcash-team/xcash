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
        should = DepositService._should_collect(deposit, 5 * 10**5)
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

        should = DepositService._should_collect(deposit, 10**18)
        self.assertTrue(should)

    # -- _ensure_native_buffer 异常容错 --

    @patch.object(DepositService, "_estimate_native_fee", return_value=10**18)
    def test_ensure_native_buffer_continues_on_send_failure(self, _estimate_fee_mock):
        # Gas 补充交易失败时不应抛异常，允许后续归集继续尝试。
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
        chain = SimpleNamespace(type=ChainType.EVM, code="eth", native_coin=native_coin)
        crypto = SimpleNamespace(symbol="USDT", is_native=False)
        deposit = SimpleNamespace(
            id=1,
            customer=customer,
            transfer=SimpleNamespace(chain=chain, crypto=crypto),
        )
        deposit_address = SimpleNamespace(address="0xdeposit")
        adapter = SimpleNamespace(get_balance=Mock(return_value=0))

        # 不抛异常即通过
        DepositService._ensure_native_buffer(
            deposit=deposit,
            deposit_address=deposit_address,
            adapter=adapter,
        )
        vault_addr.send_crypto.assert_called_once()

    def test_estimate_native_fee_unknown_chain_returns_zero(self):
        # 未知链类型返回 0（安全兜底）。
        chain = SimpleNamespace(type="unknown", code="x")
        crypto = SimpleNamespace(symbol="X", is_native=True)

        fee = DepositService._estimate_native_fee(chain, crypto)
        self.assertEqual(fee, 0)

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
    @patch.object(DepositService, "_ensure_native_buffer")
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
        ensure_native_buffer_mock,
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
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
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
        ensure_native_buffer_mock.assert_called_once()
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
    @patch.object(DepositService, "_ensure_native_buffer")
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
        ensure_native_buffer_mock,
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
        transfer = SimpleNamespace(chain=chain, crypto=crypto)
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
        ensure_native_buffer_mock.assert_called_once()

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

        should_collect = DepositService._should_collect(deposit, 5 * 10**17)

        self.assertFalse(should_collect)

    @patch.object(DepositService, "_estimate_native_fee", return_value=10**18)
    def test_ensure_native_buffer_uses_chain_specific_native_decimals(
        self, estimate_fee_mock
    ):
        # 原生币补 gas 时也必须按链特定精度换算，避免把 1 个币错算成 10^12 个。
        native_coin = SimpleNamespace(
            symbol="BNB",
            decimals=6,
            get_decimals=Mock(return_value=18),
        )
        vault_addr = SimpleNamespace(send_crypto=Mock())
        wallet = SimpleNamespace(
            get_address=Mock(return_value=vault_addr),
        )
        project = SimpleNamespace(wallet=wallet)
        customer = SimpleNamespace(project=project)
        chain = SimpleNamespace(type=ChainType.EVM, code="bsc", native_coin=native_coin)
        crypto = SimpleNamespace(symbol="USDT", is_native=False)
        deposit = SimpleNamespace(
            customer=customer,
            transfer=SimpleNamespace(chain=chain, crypto=crypto),
        )
        deposit_address = SimpleNamespace(address="0xdeposit")
        adapter = SimpleNamespace(get_balance=Mock(return_value=0))

        DepositService._ensure_native_buffer(
            deposit=deposit,
            deposit_address=deposit_address,
            adapter=adapter,
        )

        vault_addr.send_crypto.assert_called_once_with(
            crypto=native_coin,
            chain=chain,
            to="0xdeposit",
            amount=Decimal("1.2"),
            transfer_type=TransferType.GasRecharge,
        )
        estimate_fee_mock.assert_called_once_with(chain, crypto)


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
    @patch.object(DepositService, "_ensure_native_buffer")
    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.RecipientAddress.objects.filter")
    def test_collect_deposit_marks_same_group_records_with_one_collection_hash(
        self,
        recipient_filter_mock,
        adapter_factory_mock,
        ensure_native_buffer_mock,
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
        ensure_native_buffer_mock.assert_called_once()

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
    @patch.object(DepositService, "_ensure_native_buffer")
    @patch("deposits.service.AdapterFactory.get_adapter")
    @patch("deposits.service.DepositAddress.objects.get")
    @patch("deposits.service.RecipientAddress.objects.filter")
    def test_gather_task_only_sends_once_for_same_collect_group(
        self,
        recipient_filter_mock,
        deposit_address_get_mock,
        adapter_factory_mock,
        ensure_native_buffer_mock,
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
        ensure_native_buffer_mock.assert_called_once()


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
            get_balance=Mock(return_value=10**18)
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
