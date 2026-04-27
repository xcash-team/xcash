import threading
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.contrib.admin.sites import AdminSite
from django.core.cache import cache
from django.db import connections
from django.db import close_old_connections
from django.test import TestCase
from django.test import TransactionTestCase
from django.test import override_settings
from django.utils import timezone
from web3 import Web3

from chains.models import Address
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskFailureReason
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferType
from chains.models import TxHash
from chains.models import Wallet
from chains.service import ObservedTransferPayload
from chains.service import TransferService
from common.consts import ERC20_TRANSFER_GAS
from currencies.models import ChainToken
from currencies.models import Crypto
from evm.admin import EvmScanCursorAdmin
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.erc20 import EvmErc20ScanResult
from evm.scanner.erc20 import EvmErc20TransferScanner
from evm.scanner.native import EvmNativeDirectScanner
from evm.scanner.native import EvmNativeScanResult
from evm.scanner.rpc import EvmScannerRpcError
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage



class EvmNonceConcurrencyTests(TransactionTestCase):
    """多线程并发创建 EvmBroadcastTask，验证 nonce 分配的严格递增和互斥性。

    EVM 的 schedule_native → _create_broadcast_task 只做 nonce 分配和 DB 写入，
    不涉及 signer 或 RPC，因此无需 mock 外部依赖。
    """

    THREAD_COUNT = 5

    def setUp(self):
        self.native = Crypto.objects.create(
            name="Ethereum Concurrency",
            symbol="ETHCC",
            coingecko_id="ethereum-concurrency",
        )
        self.chain = Chain.objects.create(
            code="eth-concurrency",
            name="Ethereum Concurrency",
            type=ChainType.EVM,
            chain_id=99901,
            rpc="http://localhost:8545",
            native_coin=self.native,
            active=True,
            base_transfer_gas=21_000,
        )
        self.wallet = Wallet.objects.create()
        self.address = Address.objects.create(
            wallet=self.wallet,
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
            bip44_account=1,
            address_index=0,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000CC01"
            ),
        )

    def test_concurrent_schedule_native_assigns_unique_sequential_nonces(self):
        """同一 (address, chain) 上 N 个线程同时 schedule_native，nonce 必须为 {0..N-1}。"""
        barrier = threading.Barrier(self.THREAD_COUNT)
        results: list[int] = []
        errors: list[Exception] = []
        recipient = Web3.to_checksum_address(
            "0x000000000000000000000000000000000000CC02"
        )

        def schedule(thread_idx: int) -> None:
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                task = EvmBroadcastTask.schedule_native(
                    address=self.address,
                    chain=self.chain,
                    to=recipient,
                    value=thread_idx + 1,
                    transfer_type=TransferType.Withdrawal,
                )
                results.append(task.nonce)
            except Exception as exc:
                errors.append(exc)
            finally:
                connections.close_all()

        threads = [
            threading.Thread(target=schedule, args=(i,))
            for i in range(self.THREAD_COUNT)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        self.assertFalse(errors, f"线程异常: {errors}")
        self.assertEqual(len(results), self.THREAD_COUNT)
        self.assertEqual(sorted(results), list(range(self.THREAD_COUNT)))

        from chains.models import AddressChainState

        state = AddressChainState.objects.get(
            address=self.address, chain=self.chain
        )
        self.assertEqual(state.next_nonce, self.THREAD_COUNT)
        self.assertEqual(
            EvmBroadcastTask.objects.filter(
                address=self.address, chain=self.chain
            ).count(),
            self.THREAD_COUNT,
        )

    def test_concurrent_schedule_native_across_addresses_are_independent(self):
        """不同地址并发 schedule，各自 nonce 独立从 0 开始。"""
        addresses = []
        for i in range(self.THREAD_COUNT):
            addr = Address.objects.create(
                wallet=self.wallet,
                chain_type=ChainType.EVM,
                usage=AddressUsage.Vault,
                bip44_account=1,
                address_index=100 + i,
                address=Web3.to_checksum_address(
                    f"0x000000000000000000000000000000000000D{i:03d}"
                ),
            )
            addresses.append(addr)

        barrier = threading.Barrier(self.THREAD_COUNT)
        results: list[tuple[str, int]] = []
        errors: list[Exception] = []
        recipient = Web3.to_checksum_address(
            "0x000000000000000000000000000000000000CC02"
        )

        def schedule(addr: Address) -> None:
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                task = EvmBroadcastTask.schedule_native(
                    address=addr,
                    chain=self.chain,
                    to=recipient,
                    value=1,
                    transfer_type=TransferType.Withdrawal,
                )
                results.append((str(addr.address), task.nonce))
            except Exception as exc:
                errors.append(exc)
            finally:
                connections.close_all()

        threads = [
            threading.Thread(target=schedule, args=(addr,))
            for addr in addresses
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        self.assertFalse(errors, f"线程异常: {errors}")
        self.assertEqual(len(results), self.THREAD_COUNT)
        for _addr_str, nonce in results:
            self.assertEqual(nonce, 0)

    def test_concurrent_1000_tasks_nonce_sequential_0_to_999(self):
        """1000 个线程并发创建任务，验证 nonce 严格从 0 递增到 999。

        同时验证数据库触发器 trg_evm_broadcast_task_nonce_sequential
        在高并发下与 AddressChainState 行锁协同工作，不会出现跳跃或重复。
        """
        task_count = 1000
        # 分批启动线程，每批 50 个，避免一次性开启过多连接
        batch_size = 50
        results: list[int] = []
        errors: list[Exception] = []
        lock = threading.Lock()
        recipient = Web3.to_checksum_address(
            "0x000000000000000000000000000000000000CC02"
        )

        def schedule(batch_barrier: threading.Barrier) -> None:
            close_old_connections()
            try:
                batch_barrier.wait(timeout=10)
                task = EvmBroadcastTask.schedule_native(
                    address=self.address,
                    chain=self.chain,
                    to=recipient,
                    value=1,
                    transfer_type=TransferType.Withdrawal,
                )
                with lock:
                    results.append(task.nonce)
            except Exception as exc:
                with lock:
                    errors.append(exc)
            finally:
                connections.close_all()

        # 分批执行，每批线程同时起跑
        for batch_start in range(0, task_count, batch_size):
            current_batch = min(batch_size, task_count - batch_start)
            barrier = threading.Barrier(current_batch)
            threads = [
                threading.Thread(target=schedule, args=(barrier,))
                for _ in range(current_batch)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=60)

        self.assertFalse(errors, f"线程异常（共 {len(errors)} 个）: {errors[:5]}")
        self.assertEqual(len(results), task_count)

        # 核心断言：nonce 恰好是 {0, 1, 2, ..., 999}
        self.assertEqual(sorted(results), list(range(task_count)))

        # 数据库记录数一致
        db_count = EvmBroadcastTask.objects.filter(
            address=self.address, chain=self.chain
        ).count()
        self.assertEqual(db_count, task_count)

        # AddressChainState.next_nonce 正确推进
        from chains.models import AddressChainState

        state = AddressChainState.objects.get(
            address=self.address, chain=self.chain
        )
        self.assertEqual(state.next_nonce, task_count)

        # 数据库层面无空洞：max(nonce) == count - 1
        from django.db.models import Max

        max_nonce = EvmBroadcastTask.objects.filter(
            address=self.address, chain=self.chain
        ).aggregate(m=Max("nonce"))["m"]
        self.assertEqual(max_nonce, task_count - 1)
