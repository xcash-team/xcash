from __future__ import annotations

import shutil
import subprocess
from datetime import timedelta
from decimal import Decimal
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock
from unittest.mock import patch

from django.core.cache import cache
from django.core.cache import cache as _cache
from django.core.management import call_command
from django.test import TestCase
from django.test import override_settings
from django.utils import timezone
from web3 import Web3

from bitcoin.rpc import BitcoinRpcClient
from bitcoin.rpc import BitcoinRpcError
from chains.adapters import AdapterFactory
from chains.models import AddressUsage
from chains.models import BroadcastTask
from chains.models import BroadcastTaskResult
from chains.models import BroadcastTaskStage
from chains.models import Chain
from chains.models import ChainType
from chains.models import OnchainTransfer
from chains.models import TransferStatus
from chains.models import TransferType
from chains.models import Wallet
from chains.tasks import block_number_updated
from chains.tasks import confirm_transfer
from chains.tasks import update_the_latest_block
from chains.test_signer import build_test_remote_signer_backend
from core.default_data import ensure_base_currencies
from core.default_data import ensure_local_chains
from core.models import PLATFORM_SETTINGS_CACHE_KEY
from core.models import PlatformSettings
from core.runtime_settings import get_admin_sensitive_action_otp_max_age_seconds
from core.runtime_settings import get_alerts_repeat_interval_minutes
from core.runtime_settings import get_webhook_delivery_breaker_threshold
from core.runtime_settings import get_webhook_delivery_max_backoff_seconds
from core.runtime_settings import get_webhook_delivery_max_retries
from currencies.models import ChainToken
from currencies.models import Crypto
from deposits.models import DepositAddress
from deposits.models import DepositStatus
from deposits.service import DepositService
from evm.local_erc20 import LOCAL_EVM_ERC20_ABI
from evm.local_erc20 import LOCAL_EVM_ERC20_BYTECODE
from evm.models import EvmBroadcastTask
from evm.models import EvmScanCursor
from evm.models import EvmScanCursorType
from evm.scanner.constants import ERC20_TRANSFER_TOPIC0
from evm.scanner.service import EvmChainScannerService
from invoices.models import Invoice
from invoices.models import InvoiceStatus
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage
from users.models import Customer
from withdrawals.models import Withdrawal
from withdrawals.models import WithdrawalStatus

_CORE_TEST_PATCHERS = []


def setUpModule():
    # core 真实链路会用到账户锁；每轮开始前清掉测试 Redis，避免前序 run 遗留锁串扰。
    _cache.clear()
    backend = build_test_remote_signer_backend()
    # core 联调测试需要真实地址派生与签名，但不应额外依赖外部 signer 进程。
    for target in (
        "chains.signer.get_signer_backend",
        "evm.models.get_signer_backend",
    ):
        patcher = patch(target, return_value=backend)
        patcher.start()
        _CORE_TEST_PATCHERS.append(patcher)


def tearDownModule():
    while _CORE_TEST_PATCHERS:
        _CORE_TEST_PATCHERS.pop().stop()
    _cache.clear()


@override_settings(
    ADMIN_SENSITIVE_ACTION_OTP_MAX_AGE_SECONDS=900,
    ALERTS_REPEAT_INTERVAL_MINUTES=30,
)
class PlatformSettingsRuntimeTests(TestCase):
    def tearDown(self):
        cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
        super().tearDown()

    def test_runtime_settings_use_database_override_before_settings_fallback(self):
        # 平台运行参数中心存在记录时，业务读取应优先采用数据库值，而不是继续回退到 settings 常量。
        PlatformSettings.objects.create(
            admin_sensitive_action_otp_max_age_seconds=480,
            alerts_repeat_interval_minutes=7,
            webhook_delivery_breaker_threshold=12,
            webhook_delivery_max_retries=9,
            webhook_delivery_max_backoff_seconds=45,
        )

        self.assertEqual(get_admin_sensitive_action_otp_max_age_seconds(), 480)
        self.assertEqual(get_alerts_repeat_interval_minutes(), 7)
        self.assertEqual(get_webhook_delivery_breaker_threshold(), 12)
        self.assertEqual(get_webhook_delivery_max_retries(), 9)
        self.assertEqual(get_webhook_delivery_max_backoff_seconds(), 45)


class LocalChainBootstrapCommandTests(TestCase):
    def _require_local_evm(self) -> Web3:
        w3 = Web3(
            Web3.HTTPProvider(
                "http://127.0.0.1:8545",
                request_kwargs={"timeout": 5},
            )
        )
        if not w3.is_connected():
            self.skipTest("本地 anvil 未启动，跳过本地链初始化部署测试")
        return w3

    @patch.dict(
        environ,
        {
            "BITCOIN_NETWORK": "regtest",
            "LOCAL_EVM_CHAIN_CODE": "ethereum-local",
            "LOCAL_EVM_CHAIN_NAME": "Ethereum Local",
            "LOCAL_EVM_RPC": "http://127.0.0.1:8545",
            "LOCAL_EVM_CHAIN_ID": "31337",
            "LOCAL_BTC_CHAIN_CODE": "bitcoin-local",
            "LOCAL_BTC_CHAIN_NAME": "Bitcoin Local",
            "LOCAL_BTC_RPC": "http://xcash:xcash@127.0.0.1:18443/wallet/xcash",
        },
        clear=False,
    )
    def test_init_local_chains_creates_local_chain_records(self):
        # 本地链初始化必须独立于生产 init，直接生成本地 Ethereum / Bitcoin 配置与原生币映射。
        call_command("init_local_chains")

        evm_chain = Chain.objects.get(code="ethereum-local")
        btc_chain = Chain.objects.get(code="bitcoin-local")

        self.assertEqual(evm_chain.type, ChainType.EVM)
        self.assertEqual(evm_chain.chain_id, 31337)
        self.assertEqual(evm_chain.confirm_block_count, 1)
        self.assertEqual(btc_chain.type, ChainType.BITCOIN)
        self.assertEqual(btc_chain.confirm_block_count, 1)
        self.assertTrue(
            ChainToken.objects.filter(
                chain=evm_chain,
                crypto__symbol="ETH",
                address="",
            ).exists()
        )
        self.assertTrue(
            ChainToken.objects.filter(
                chain=btc_chain,
                crypto__symbol="BTC",
                address="",
            ).exists()
        )

    @patch.dict(
        environ,
        {
            "BITCOIN_NETWORK": "regtest",
            "LOCAL_EVM_CHAIN_CODE": "ethereum-local",
            "LOCAL_EVM_CHAIN_NAME": "Ethereum Local",
            "LOCAL_EVM_RPC": "http://127.0.0.1:8545",
            "LOCAL_EVM_CHAIN_ID": "31337",
            "LOCAL_BTC_CHAIN_CODE": "bitcoin-local",
            "LOCAL_BTC_CHAIN_NAME": "Bitcoin Local",
            "LOCAL_BTC_RPC": "http://xcash:xcash@127.0.0.1:18443/wallet/xcash",
            "LOCAL_EVM_USDT_ADDRESS": "",
        },
        clear=False,
    )
    def test_init_local_chains_deploys_local_usdt_and_creates_chain_token(self):
        w3 = self._require_local_evm()

        call_command("init_local_chains")

        evm_chain = Chain.objects.get(code="ethereum-local")
        usdt_mapping = ChainToken.objects.get(
            chain=evm_chain,
            crypto__symbol="USDT",
        )

        self.assertTrue(Web3.is_address(usdt_mapping.address))
        self.assertEqual(usdt_mapping.decimals, 6)
        self.assertGreater(len(w3.eth.get_code(usdt_mapping.address)), 0)

    @patch.dict(
        environ,
        {
            "BITCOIN_NETWORK": "regtest",
            "LOCAL_EVM_CHAIN_CODE": "ethereum-local",
            "LOCAL_EVM_CHAIN_NAME": "Ethereum Local",
            "LOCAL_EVM_RPC": "http://127.0.0.1:8545",
            "LOCAL_EVM_CHAIN_ID": "31337",
            "LOCAL_BTC_CHAIN_CODE": "bitcoin-local",
            "LOCAL_BTC_CHAIN_NAME": "Bitcoin Local",
            "LOCAL_BTC_RPC": "http://xcash:xcash@127.0.0.1:18443/wallet/xcash",
            "LOCAL_EVM_USDT_ADDRESS": "",
        },
        clear=False,
    )
    def test_init_local_chains_deploys_standard_erc20_usdt(self):
        w3 = self._require_local_evm()

        call_command("init_local_chains")

        usdt_mapping = ChainToken.objects.get(
            chain__code="ethereum-local",
            crypto__symbol="USDT",
        )
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(usdt_mapping.address),
            abi=LOCAL_EVM_ERC20_ABI,
        )
        mint_hash = contract.functions.mint(
            w3.eth.accounts[0],
            1_000_000,
        ).transact({"from": w3.eth.accounts[0]})
        w3.eth.wait_for_transaction_receipt(mint_hash)
        transfer_hash = contract.functions.transfer(
            w3.eth.accounts[1],
            250_000,
        ).transact({"from": w3.eth.accounts[0]})
        receipt = w3.eth.wait_for_transaction_receipt(transfer_hash)

        self.assertGreaterEqual(len(receipt["logs"]), 1)
        self.assertEqual(
            Web3.to_hex(receipt["logs"][0]["topics"][0]),
            ERC20_TRANSFER_TOPIC0,
        )

    @patch.dict(
        environ,
        {
            "BITCOIN_NETWORK": "regtest",
            "LOCAL_EVM_CHAIN_CODE": "ethereum-local",
            "LOCAL_EVM_CHAIN_NAME": "Ethereum Local",
            "LOCAL_EVM_RPC": "http://127.0.0.1:8545",
            "LOCAL_EVM_CHAIN_ID": "31337",
            "LOCAL_BTC_CHAIN_CODE": "bitcoin-local",
            "LOCAL_BTC_CHAIN_NAME": "Bitcoin Local",
            "LOCAL_BTC_RPC": "http://xcash:xcash@127.0.0.1:18443/wallet/xcash",
        },
        clear=False,
    )
    @patch("core.default_data.ensure_local_evm_usdt_contract_address")
    def test_init_local_chains_rolls_back_db_when_local_usdt_deploy_fails(
        self,
        ensure_local_usdt_contract_address,
    ):
        ensure_local_usdt_contract_address.side_effect = RuntimeError("deploy failed")
        Chain.objects.filter(code__in=("ethereum-local", "bitcoin-local")).delete()

        ensure_base_currencies()
        with self.assertRaisesMessage(RuntimeError, "deploy failed"):
            ensure_local_chains()

        self.assertFalse(Chain.objects.filter(code="ethereum-local").exists())
        self.assertFalse(Chain.objects.filter(code="bitcoin-local").exists())
        self.assertFalse(
            ChainToken.objects.filter(chain__code="ethereum-local").exists()
        )
        self.assertFalse(
            ChainToken.objects.filter(chain__code="bitcoin-local").exists()
        )

    @patch.dict(
        environ,
        {
            "BITCOIN_NETWORK": "regtest",
            "LOCAL_BTC_RPC_USER": "xcash",
            "LOCAL_BTC_RPC_PASSWORD": "xcash",
            "LOCAL_BTC_RPC_HOST": "127.0.0.1",
            "LOCAL_BTC_RPC_PORT": "18443",
        },
        clear=False,
    )
    @patch("core.management.commands.prepare_local_bitcoin.BitcoinRpcClient")
    def test_prepare_local_bitcoin_creates_wallet_mines_blocks_and_imports_addresses(
        self,
        bitcoin_client_cls,
    ):
        # 本地 regtest 准备命令要能一次性完成钱包准备、预挖区块和 watch-only 导入。
        wallet = Wallet.generate()
        addr = wallet.get_address(
            chain_type=ChainType.BITCOIN,
            usage=AddressUsage.Deposit,
            address_index=0,
        )
        project = Project.objects.create(
            name="Demo",
            wallet=Wallet.objects.create(),
        )
        RecipientAddress.objects.create(
            name="BTC 收款地址",
            project=project,
            chain_type=ChainType.BITCOIN,
            address="mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn",
            usage=RecipientAddressUsage.INVOICE,
        )

        root_client = Mock()
        wallet_client = Mock()
        miner_client = Mock()
        root_client.list_wallets.return_value = []
        root_client.load_wallet.side_effect = BitcoinRpcError("Wallet file not found")
        miner_client.get_new_address.return_value = (
            "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn"
        )
        # prepare_local_bitcoin 创建 3 个 client：root, wallet(watch-only), miner
        bitcoin_client_cls.side_effect = [root_client, wallet_client, miner_client]

        call_command("prepare_local_bitcoin", "--wallet-name=xcash", "--mine-blocks=2")

        # 主钱包（watch-only）和矿工钱包分别创建
        root_client.create_wallet.assert_any_call("xcash", disable_private_keys=True)
        root_client.create_wallet.assert_any_call("xcash-miner", disable_private_keys=False)
        miner_client.generate_to_address.assert_called_once_with(
            2,
            "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn",
        )
        wallet_client.import_address.assert_any_call(
            addr.address,
            label="xcash-watch-only",
            rescan=False,
        )
        wallet_client.import_address.assert_any_call(
            "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn",
            label="xcash-watch-only",
            rescan=False,
        )


class InitEnvScriptTests(TestCase):
    def test_init_env_creates_env_from_example_and_replaces_placeholders(self):
        repo_root = Path(__file__).resolve().parents[2]
        script_path = repo_root / "scripts" / "init_env.sh"

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            example_path = tmp_path / ".env.example"
            scripts_dir = tmp_path / "scripts"
            scripts_dir.mkdir()
            copied_script_path = scripts_dir / "init_env.sh"
            example_path.write_text(
                "DJANGO_SECRET_KEY=change-me-with-a-64-char-random-string\n"
                "POSTGRES_PASSWORD=change-me-main-db-password\n"
                "STATIC_VALUE=keep-me\n",
                encoding="utf-8",
            )
            shutil.copy2(script_path, copied_script_path)
            copied_script_path.chmod(0o755)

            result = subprocess.run(  # noqa: S603
                [str(copied_script_path)],
                cwd=tmp_path,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(result.returncode, 0)
            env_path = tmp_path / ".env"
            self.assertTrue(env_path.exists())
            env_content = env_path.read_text(encoding="utf-8")
            self.assertIn("STATIC_VALUE=keep-me", env_content)
            self.assertNotIn("change-me-with-a-64-char-random-string", env_content)
            self.assertNotIn("change-me-main-db-password", env_content)

            env_values = dict(
                line.split("=", maxsplit=1)
                for line in env_content.splitlines()
                if line and not line.startswith("#") and "=" in line
            )
            self.assertEqual(len(env_values["DJANGO_SECRET_KEY"]), 64)
            self.assertEqual(len(env_values["POSTGRES_PASSWORD"]), 32)
            self.assertRegex(env_values["DJANGO_SECRET_KEY"], r"^[A-Za-z0-9]+$")
            self.assertRegex(env_values["POSTGRES_PASSWORD"], r"^[A-Za-z0-9]+$")

    def test_init_env_does_not_overwrite_existing_env(self):
        repo_root = Path(__file__).resolve().parents[2]
        script_path = repo_root / "scripts" / "init_env.sh"

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (tmp_path / ".env.example").write_text(
                "DJANGO_SECRET_KEY=change-me-with-a-64-char-random-string\n",
                encoding="utf-8",
            )
            scripts_dir = tmp_path / "scripts"
            scripts_dir.mkdir()
            copied_script_path = scripts_dir / "init_env.sh"
            shutil.copy2(script_path, copied_script_path)
            copied_script_path.chmod(0o755)

            env_path = tmp_path / ".env"
            env_path.write_text("DJANGO_SECRET_KEY=existing-secret\n", encoding="utf-8")

            result = subprocess.run(  # noqa: S603
                [str(copied_script_path)],
                cwd=tmp_path,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(result.returncode, 0)
            self.assertIn(".env 已存在", result.stdout)
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                "DJANGO_SECRET_KEY=existing-secret\n",
            )


class LocalChainIntegrationMixin:
    EVM_RPC = "http://127.0.0.1:8545"
    BTC_RPC = "http://xcash:xcash@127.0.0.1:18443/wallet/xcash"
    BTC_MINER_RPC = "http://xcash:xcash@127.0.0.1:18443/wallet/xcash-miner"

    def _require_anvil(self) -> Web3:
        w3 = Web3(Web3.HTTPProvider(self.EVM_RPC, request_kwargs={"timeout": 5}))
        if not w3.is_connected():
            self.skipTest("本地 anvil 未启动，跳过真实 EVM 联调测试")
        return w3

    def _require_bitcoin(self) -> BitcoinRpcClient:
        client = BitcoinRpcClient(self.BTC_RPC)
        try:
            client.get_block_count()
        except Exception as exc:  # noqa: BLE001
            self.skipTest(f"本地 bitcoind regtest 不可用，跳过真实 BTC 联调测试: {exc}")
        return client

    def _require_bitcoin_miner(self) -> BitcoinRpcClient:
        """返回带私钥的矿工钱包客户端，用于 regtest 打款和挖矿。"""
        client = BitcoinRpcClient(self.BTC_MINER_RPC)
        try:
            client.get_block_count()
        except Exception as exc:  # noqa: BLE001
            self.skipTest(f"本地 bitcoind regtest 矿工钱包不可用: {exc}")
        return client

    def _deploy_test_erc20(self, w3: Web3, *, supply_raw: int):
        token_factory = w3.eth.contract(
            abi=LOCAL_EVM_ERC20_ABI,
            bytecode=LOCAL_EVM_ERC20_BYTECODE,
        )
        deployer = w3.eth.accounts[0]
        tx_hash = token_factory.constructor().transact({"from": deployer})
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        token = w3.eth.contract(
            address=receipt.contractAddress,
            abi=LOCAL_EVM_ERC20_ABI,
        )
        if supply_raw > 0:
            mint_hash = token.functions.mint(deployer, supply_raw).transact(
                {"from": deployer}
            )
            w3.eth.wait_for_transaction_receipt(mint_hash)
        return token

    def _scan_evm_chain_and_get_transfer(
        self,
        *,
        chain: Chain,
        tx_hash,
        expected_scanner: str,
    ) -> OnchainTransfer:
        """使用真实自扫描器从链上抓取交易，再返回命中的 OnchainTransfer。

        expected_scanner:
        - native: 预期由原生币直转扫描器命中
        - erc20: 预期由 ERC20 OnchainTransfer 扫描器命中
        """
        summary = EvmChainScannerService.scan_chain(chain=chain)
        normalized_tx_hash = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
        if not normalized_tx_hash.startswith("0x"):
            normalized_tx_hash = f"0x{normalized_tx_hash}"
        transfer = OnchainTransfer.objects.filter(
            chain=chain,
            hash=normalized_tx_hash.lower(),
        ).first()
        if transfer is None:
            raise RuntimeError(
                f"EVM scanner did not capture transfer: {normalized_tx_hash}"
            )

        # 自扫描任务跑完后，链级两个游标都必须存在，说明扫描编排链路已经走通。
        self.assertTrue(
            EvmScanCursor.objects.filter(
                chain=chain,
                scanner_type=EvmScanCursorType.NATIVE_DIRECT,
            ).exists()
        )
        self.assertTrue(
            EvmScanCursor.objects.filter(
                chain=chain,
                scanner_type=EvmScanCursorType.ERC20_TRANSFER,
            ).exists()
        )
        native_cursor = EvmScanCursor.objects.get(
            chain=chain,
            scanner_type=EvmScanCursorType.NATIVE_DIRECT,
        )
        erc20_cursor = EvmScanCursor.objects.get(
            chain=chain,
            scanner_type=EvmScanCursorType.ERC20_TRANSFER,
        )
        chain.refresh_from_db(fields=("latest_block_number",))
        # 每轮真实自扫描后，两个游标都必须刷新且不能残留错误。
        self.assertEqual(native_cursor.last_error, "")
        self.assertEqual(erc20_cursor.last_error, "")
        self.assertLessEqual(
            native_cursor.last_scanned_block, chain.latest_block_number
        )
        self.assertLessEqual(erc20_cursor.last_scanned_block, chain.latest_block_number)

        if expected_scanner == "native":
            self.assertGreaterEqual(summary.native.observed_transfers, 1)
            self.assertGreaterEqual(summary.native.created_transfers, 1)
            self.assertEqual(summary.erc20.created_transfers, 0)
            self.assertEqual(transfer.event_id, "native:tx")
        elif expected_scanner == "erc20":
            self.assertGreaterEqual(summary.erc20.observed_logs, 1)
            self.assertGreaterEqual(summary.erc20.created_transfers, 1)
            self.assertTrue(transfer.event_id.startswith("erc20:"))
        else:
            raise ValueError(f"Unsupported expected_scanner: {expected_scanner}")

        return transfer

    @staticmethod
    def _prime_evm_scan_cursors(*, chain: Chain) -> None:
        """把 EVM 自扫描游标预热到当前链头附近，模拟生产中的持续轮询状态。

        真实 Anvil 在开发机上通常已经运行了很久；若测试链记录是刚创建的，而游标从 0 开始，
        单次扫描只会先扫最前面的少量区块，无法覆盖刚刚产生的最新交易。
        """
        latest_block = chain.get_latest_block_number
        safe_block = max(0, latest_block - chain.confirm_block_count)

        for scanner_type in (
            EvmScanCursorType.NATIVE_DIRECT,
            EvmScanCursorType.ERC20_TRANSFER,
        ):
            EvmScanCursor.objects.update_or_create(
                chain=chain,
                scanner_type=scanner_type,
                defaults={
                    "last_scanned_block": latest_block,
                    "last_safe_block": safe_block,
                    "enabled": True,
                    "last_error": "",
                    "last_error_at": None,
                },
            )

    def _mine_evm_block(self, w3: Web3) -> None:
        """通过发送一笔极小原生币转账推进 anvil 块高，供 FULL 确认链路使用。"""
        tx_hash = w3.eth.send_transaction(
            {
                "from": w3.eth.accounts[0],
                "to": w3.eth.accounts[1],
                "value": 1,
            }
        )
        w3.eth.wait_for_transaction_receipt(tx_hash)

    def _run_local_confirm_pipeline(self, *, chain: Chain) -> None:
        """同步执行“刷新块高 -> 调度确认 -> 执行确认”链路，覆盖真实任务推进逻辑。"""
        with (
            patch(
                "chains.tasks.confirm_transfer.delay",
                side_effect=confirm_transfer.run,
            ),
            patch(
                "chains.tasks.block_number_updated.delay",
                side_effect=block_number_updated.run,
            ),
        ):
            update_the_latest_block.run(chain.pk)


class LocalEvmContractCompatibilityTests(LocalChainIntegrationMixin, TestCase):
    def test_deploy_test_erc20_emits_standard_transfer_event(self):
        # 联调 helper 部署出的测试 ERC20 必须兼容标准 Transfer 事件，否则扫描器无法观测到日志。
        w3 = self._require_anvil()
        token_contract = self._deploy_test_erc20(w3, supply_raw=1_000_000)

        receipt = w3.eth.wait_for_transaction_receipt(
            token_contract.functions.transfer(w3.eth.accounts[1], 250_000).transact(
                {"from": w3.eth.accounts[0]}
            )
        )

        self.assertGreaterEqual(len(receipt["logs"]), 1)
        self.assertEqual(
            Web3.to_hex(receipt["logs"][0]["topics"][0]),
            ERC20_TRANSFER_TOPIC0,
        )


class LocalEvmScannerIntegrationTests(LocalChainIntegrationMixin, TestCase):
    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_withdrawal_can_broadcast_and_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # 真实本地链联调：从签名建单到 anvil 广播，再到链上观察与业务完成必须能闭环。
        w3 = self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Local",
            symbol="ETHL",
            coingecko_id="ethereum-local",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil Integration",
            code="anvil-integration",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        wallet = Wallet.generate()
        project = Project.objects.create(
            name="Local EVM Project",
            wallet=wallet,
        )
        vault_address = wallet.get_address(
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
        )
        recipient = Web3.to_checksum_address(w3.eth.accounts[1])
        amount = Decimal("0.01")

        fund_tx_hash = w3.eth.send_transaction(
            {
                "from": w3.eth.accounts[0],
                "to": vault_address.address,
                "value": int(Decimal("0.2") * Decimal(10**18)),
            }
        )
        w3.eth.wait_for_transaction_receipt(fund_tx_hash)
        self._prime_evm_scan_cursors(chain=chain)

        evm_task = EvmBroadcastTask.schedule_transfer(
            address=vault_address,
            crypto=crypto,
            chain=chain,
            to=recipient,
            value_raw=int(amount * Decimal(10**18)),
            transfer_type=TransferType.Withdrawal,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="local-evm-order",
            chain=chain,
            crypto=crypto,
            amount=amount,
            to=recipient,
            hash=evm_task.base_task.tx_hash,
            broadcast_task=evm_task.base_task,
            status=WithdrawalStatus.PENDING,
        )
        evm_task.broadcast()
        _receipt = w3.eth.wait_for_transaction_receipt(evm_task.base_task.tx_hash)
        adapter = AdapterFactory.get_adapter(chain.type)
        self.assertEqual(
            adapter.tx_result(chain=chain, tx_hash=evm_task.base_task.tx_hash),
            TransferStatus.CONFIRMED,
        )

        transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=evm_task.base_task.tx_hash,
            expected_scanner="native",
        )
        transfer.process()
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)

        transfer.confirm()
        withdrawal.refresh_from_db()
        evm_task.base_task.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)
        self.assertEqual(evm_task.base_task.result, BroadcastTaskResult.SUCCESS)

    @patch("deposits.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_deposit_can_create_and_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # 真实本地链联调：anvil 上的入账转账必须能生成 Deposit 并完成确认。
        w3 = self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Deposit Local",
            symbol="ETHD",
            coingecko_id="ethereum-deposit-local",
            decimals=18,
            prices={"USD": "2000"},
        )
        chain = Chain.objects.create(
            name="Anvil Deposit",
            code="anvil-deposit",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        project = Project.objects.create(
            name="Local EVM Deposit Project",
            wallet=Wallet.generate(),
            pre_notify=True,
        )
        customer = Customer.objects.create(project=project, uid="evm-customer-1")
        # L2：DepositAddress.get_address 现在要求 project 已配 DEPOSIT_COLLECTION recipient。
        RecipientAddress.objects.create(
            project=project,
            chain_type=chain.type,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000beef"
            ),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        deposit_address = DepositAddress.get_address(chain, customer)
        amount = Decimal("0.03")
        self._prime_evm_scan_cursors(chain=chain)

        tx_hash = w3.eth.send_transaction(
            {
                "from": w3.eth.accounts[0],
                "to": deposit_address,
                "value": int(amount * Decimal(10**18)),
            }
        )
        _receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

        transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=tx_hash,
            expected_scanner="native",
        )
        transfer.process()
        deposit = transfer.deposit
        self.assertEqual(deposit.status, DepositStatus.CONFIRMING)

        transfer.confirm()
        deposit.refresh_from_db()
        self.assertEqual(deposit.status, DepositStatus.COMPLETED)

    @patch("deposits.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_native_collection_can_broadcast_and_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # 原生币归集要验证完整闭环：客户入账 -> Deposit 完成 -> 归集广播 -> 归集 OnchainTransfer 完成。
        w3 = self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Collection Local",
            symbol="ETHC2",
            coingecko_id="ethereum-collection-local",
            decimals=18,
            prices={"USD": "2000"},
        )
        chain = Chain.objects.create(
            name="Anvil Collection",
            code="anvil-collection",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        project = Project.objects.create(
            name="Local EVM Collection Project",
            wallet=Wallet.generate(),
            pre_notify=True,
            gather_worth=Decimal("10"),
            gather_period=1,
        )
        RecipientAddress.objects.create(
            name="归集地址",
            project=project,
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(w3.eth.accounts[2]),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        customer = Customer.objects.create(project=project, uid="evm-collector-1")
        deposit_address = DepositAddress.get_address(chain, customer)
        deposit_amount = Decimal("0.05")
        self._prime_evm_scan_cursors(chain=chain)

        incoming_tx_hash = w3.eth.send_transaction(
            {
                "from": w3.eth.accounts[0],
                "to": deposit_address,
                "value": int(deposit_amount * Decimal(10**18)),
            }
        )
        _incoming_receipt = w3.eth.wait_for_transaction_receipt(incoming_tx_hash)
        incoming_transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=incoming_tx_hash,
            expected_scanner="native",
        )
        incoming_transfer.process()
        deposit = incoming_transfer.deposit
        incoming_transfer.confirm()
        deposit.refresh_from_db()
        self.assertEqual(deposit.status, DepositStatus.COMPLETED)

        # --- 第一轮：prepare 不再关心 gas，直接创建 collection 任务；gas 判定由 broadcast 层兜底 ---
        collected = DepositService.collect_deposit(deposit)
        self.assertTrue(collected)
        deposit.refresh_from_db()
        self.assertIsNotNone(deposit.collection_id)

        collection_task = EvmBroadcastTask.objects.get(
            base_task=deposit.collection.broadcast_task
        )

        # --- 第二轮：broadcast pre-flight 发现 balance < value + 2×erc20_gas → 请 Vault 补 gas，保持 QUEUED ---
        collection_task.broadcast()
        collection_task.refresh_from_db()
        self.assertEqual(
            collection_task.base_task.stage, BroadcastTaskStage.QUEUED
        )
        self.assertIsNone(collection_task.last_attempt_at)

        gas_task = EvmBroadcastTask.objects.filter(
            base_task__chain=chain,
            base_task__transfer_type=TransferType.GasRecharge,
        ).latest("created_at")
        # 为 vault 充值以便 gas recharge 可以广播
        w3.eth.wait_for_transaction_receipt(
            w3.eth.send_transaction(
                {
                    "from": w3.eth.accounts[0],
                    "to": gas_task.address.address,
                    "value": int(Decimal("1") * Decimal(10**18)),
                }
            )
        )
        gas_task.broadcast()
        w3.eth.wait_for_transaction_receipt(gas_task.base_task.tx_hash)

        # --- 第三轮：gas 已到账，再次广播归集任务，pre-flight 阈值通过 → 上链 ---
        collection_task.broadcast()
        deposit.collection.refresh_from_db()
        self.assertIsNone(deposit.collection.collection_hash)
        collection_hash = collection_task.base_task.tx_hash
        w3.eth.wait_for_transaction_receipt(collection_hash)
        collection_transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=collection_hash,
            expected_scanner="native",
        )
        collection_transfer.process()
        deposit.refresh_from_db()
        self.assertEqual(collection_transfer.type, TransferType.DepositCollection)
        self.assertEqual(deposit.collection.transfer_id, collection_transfer.id)

        collection_transfer.confirm()
        deposit.collection.refresh_from_db()
        self.assertIsNotNone(deposit.collection.collected_at)

    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_erc20_withdrawal_can_broadcast_and_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # ERC20 提币必须验证真实合约调用、OnchainTransfer 事件观测和业务完成，而不是只测 calldata 构造。
        w3 = self._require_anvil()
        native_crypto = Crypto.objects.create(
            name="Ethereum ERC20 Withdrawal Native",
            symbol="ETHW2",
            coingecko_id="ethereum-erc20-withdrawal-native",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil ERC20 Withdrawal",
            code="anvil-erc20-withdrawal",
            type=ChainType.EVM,
            native_coin=native_crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        token_contract = self._deploy_test_erc20(w3, supply_raw=1_000_000_000)
        token_crypto = Crypto.objects.create(
            name="Test Token Withdrawal",
            symbol="TTW",
            coingecko_id="test-token-withdrawal",
            decimals=6,
        )
        ChainToken.objects.create(
            crypto=token_crypto,
            chain=chain,
            address=token_contract.address,
        )
        wallet = Wallet.generate()
        project = Project.objects.create(
            name="Local ERC20 Withdrawal Project",
            wallet=wallet,
        )
        # 归集补 gas 走的是 project.wallet 取金库账户，测试也复用同一入口，避免实例缓存差异。
        vault_address = project.wallet.get_address(
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
        )
        recipient = Web3.to_checksum_address(w3.eth.accounts[3])
        transfer_amount_raw = 5_000_000
        transfer_amount = Decimal(transfer_amount_raw).scaleb(-6)

        # 金库提币既要有 token 余额，也要有原生币支付 gas。
        w3.eth.wait_for_transaction_receipt(
            w3.eth.send_transaction(
                {
                    "from": w3.eth.accounts[0],
                    "to": vault_address.address,
                    "value": int(Decimal("0.2") * Decimal(10**18)),
                }
            )
        )
        w3.eth.wait_for_transaction_receipt(
            token_contract.functions.transfer(
                vault_address.address, 30_000_000
            ).transact({"from": w3.eth.accounts[0]})
        )
        self._prime_evm_scan_cursors(chain=chain)

        evm_task = EvmBroadcastTask.schedule_transfer(
            address=vault_address,
            chain=chain,
            crypto=token_crypto,
            to=recipient,
            value_raw=transfer_amount_raw,
            transfer_type=TransferType.Withdrawal,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="local-erc20-withdraw-order",
            chain=chain,
            crypto=token_crypto,
            amount=transfer_amount,
            to=recipient,
            hash=evm_task.base_task.tx_hash,
            broadcast_task=evm_task.base_task,
            status=WithdrawalStatus.PENDING,
        )

        evm_task.broadcast()
        _receipt = w3.eth.wait_for_transaction_receipt(evm_task.base_task.tx_hash)
        transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=evm_task.base_task.tx_hash,
            expected_scanner="erc20",
        )
        transfer.process()
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)

        transfer.confirm()
        withdrawal.refresh_from_db()
        evm_task.base_task.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)
        self.assertEqual(evm_task.base_task.result, BroadcastTaskResult.SUCCESS)

    @patch("deposits.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_erc20_deposit_can_create_and_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # ERC20 充币要验证真实 token 合约事件能正确命中 Deposit，而不是只依赖 webhook 伪造数据。
        w3 = self._require_anvil()
        native_crypto = Crypto.objects.create(
            name="Ethereum ERC20 Deposit Native",
            symbol="ETHD2",
            coingecko_id="ethereum-erc20-deposit-native",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil ERC20 Deposit",
            code="anvil-erc20-deposit",
            type=ChainType.EVM,
            native_coin=native_crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        token_contract = self._deploy_test_erc20(w3, supply_raw=1_000_000_000)
        token_crypto = Crypto.objects.create(
            name="Test Token Deposit",
            symbol="TTD",
            coingecko_id="test-token-deposit",
            decimals=6,
            prices={"USD": "1"},
        )
        ChainToken.objects.create(
            crypto=token_crypto,
            chain=chain,
            address=token_contract.address,
        )
        project = Project.objects.create(
            name="Local ERC20 Deposit Project",
            wallet=Wallet.generate(),
            pre_notify=True,
        )
        customer = Customer.objects.create(project=project, uid="erc20-customer-1")
        # L2：DepositAddress.get_address 现在要求 project 已配 DEPOSIT_COLLECTION recipient。
        RecipientAddress.objects.create(
            project=project,
            chain_type=chain.type,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000beef"
            ),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        deposit_address = DepositAddress.get_address(chain, customer)
        self._prime_evm_scan_cursors(chain=chain)

        receipt = w3.eth.wait_for_transaction_receipt(
            token_contract.functions.transfer(deposit_address, 7_500_000).transact(
                {"from": w3.eth.accounts[0]}
            )
        )
        transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=receipt.transactionHash,
            expected_scanner="erc20",
        )
        transfer.process()
        deposit = transfer.deposit
        self.assertEqual(deposit.status, DepositStatus.CONFIRMING)

        transfer.confirm()
        deposit.refresh_from_db()
        self.assertEqual(deposit.status, DepositStatus.COMPLETED)

    @patch("deposits.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_erc20_collection_can_broadcast_and_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # ERC20 归集必须覆盖 gas 补充和 token 归集两笔真实链上交易，否则归集链路只测到一半。
        w3 = self._require_anvil()
        native_crypto = Crypto.objects.create(
            name="Ethereum ERC20 Collection Native",
            symbol="ETHC3",
            coingecko_id="ethereum-erc20-collection-native",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil ERC20 Collection",
            code="anvil-erc20-collection",
            type=ChainType.EVM,
            native_coin=native_crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        token_contract = self._deploy_test_erc20(w3, supply_raw=1_000_000_000)
        token_crypto = Crypto.objects.create(
            name="Test Token Collection",
            symbol="TTC",
            coingecko_id="test-token-collection",
            decimals=6,
            prices={"USD": "1"},
        )
        ChainToken.objects.create(
            crypto=token_crypto,
            chain=chain,
            address=token_contract.address,
        )
        wallet = Wallet.generate()
        project = Project.objects.create(
            name="Local ERC20 Collection Project",
            wallet=wallet,
            pre_notify=True,
            gather_worth=Decimal("10"),
            gather_period=1,
        )
        RecipientAddress.objects.create(
            name="ERC20归集地址",
            project=project,
            chain_type=ChainType.EVM,
            address=Web3.to_checksum_address(w3.eth.accounts[4]),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        customer = Customer.objects.create(project=project, uid="erc20-collector-1")
        deposit_address = DepositAddress.get_address(chain, customer)
        _deposit_addr = DepositAddress.objects.get(
            customer=customer, chain_type=chain.type
        ).address
        _vault_address = wallet.get_address(
            chain_type=ChainType.EVM, usage=AddressUsage.Vault
        )
        self._prime_evm_scan_cursors(chain=chain)

        incoming_receipt = w3.eth.wait_for_transaction_receipt(
            token_contract.functions.transfer(deposit_address, 25_000_000).transact(
                {"from": w3.eth.accounts[0]}
            )
        )
        incoming_transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=incoming_receipt.transactionHash,
            expected_scanner="erc20",
        )
        incoming_transfer.process()
        deposit = incoming_transfer.deposit
        incoming_transfer.confirm()
        deposit.refresh_from_db()
        self.assertEqual(deposit.status, DepositStatus.COMPLETED)

        # --- 第一轮：prepare 不再关心 gas，直接创建 collection 任务；gas 判定由 broadcast 层兜底 ---
        collected = DepositService.collect_deposit(deposit)
        self.assertTrue(collected)
        deposit.refresh_from_db()
        self.assertIsNotNone(deposit.collection_id)

        collection_task = EvmBroadcastTask.objects.get(
            base_task=deposit.collection.broadcast_task
        )

        # --- 第二轮：broadcast pre-flight 发现 native < 2×erc20_gas → 请 Vault 补 gas，保持 QUEUED ---
        collection_task.broadcast()
        collection_task.refresh_from_db()
        self.assertEqual(
            collection_task.base_task.stage, BroadcastTaskStage.QUEUED
        )
        self.assertIsNone(collection_task.last_attempt_at)

        gas_task = EvmBroadcastTask.objects.filter(
            base_task__chain=chain,
            base_task__transfer_type=TransferType.GasRecharge,
        ).latest("created_at")
        # gas recharge 任务以系统最终选中的金库账户为准；这里再真实补足原生币，确保后续广播闭环。
        w3.eth.wait_for_transaction_receipt(
            w3.eth.send_transaction(
                {
                    "from": w3.eth.accounts[0],
                    "to": gas_task.address.address,
                    "value": int(Decimal("100") * Decimal(10**18)),
                }
            )
        )
        self.assertGreater(
            w3.eth.get_balance(gas_task.address.address), int(gas_task.value)
        )
        gas_task.broadcast()
        w3.eth.wait_for_transaction_receipt(gas_task.base_task.tx_hash)

        # --- 第三轮：gas 已到账，再次广播归集任务，pre-flight 阈值通过 → 上链 ---
        collection_task.broadcast()
        deposit.collection.refresh_from_db()
        self.assertIsNone(deposit.collection.collection_hash)
        collection_hash = collection_task.base_task.tx_hash
        w3.eth.wait_for_transaction_receipt(collection_hash)
        collection_transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=collection_hash,
            expected_scanner="erc20",
        )
        collection_transfer.process()
        deposit.refresh_from_db()
        self.assertEqual(collection_transfer.type, TransferType.DepositCollection)
        self.assertEqual(deposit.collection.transfer_id, collection_transfer.id)

        collection_transfer.confirm()
        deposit.collection.refresh_from_db()
        self.assertIsNotNone(deposit.collection.collected_at)

    @patch("deposits.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_deposit_can_complete_via_confirm_task_pipeline(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # 真实链路不应只靠手工 confirm()；FULL 确认应能经由块高刷新任务推进到业务完成。
        w3 = self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Deposit Task Local",
            symbol="ETHDT2",
            coingecko_id="ethereum-deposit-task-local",
            decimals=18,
            prices={"USD": "2000"},
        )
        chain = Chain.objects.create(
            name="Anvil Deposit Task",
            code="anvil-deposit-task",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        project = Project.objects.create(
            name="Local EVM Deposit Task Project",
            wallet=Wallet.generate(),
            pre_notify=True,
        )
        customer = Customer.objects.create(project=project, uid="evm-customer-task-1")
        # L2：DepositAddress.get_address 现在要求 project 已配 DEPOSIT_COLLECTION recipient。
        RecipientAddress.objects.create(
            project=project,
            chain_type=chain.type,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000beef"
            ),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        deposit_address = DepositAddress.get_address(chain, customer)
        self._prime_evm_scan_cursors(chain=chain)

        tx_hash = w3.eth.send_transaction(
            {
                "from": w3.eth.accounts[0],
                "to": deposit_address,
                "value": int(Decimal("0.02") * Decimal(10**18)),
            }
        )
        w3.eth.wait_for_transaction_receipt(tx_hash)
        transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=tx_hash,
            expected_scanner="native",
        )
        transfer.process()
        deposit = transfer.deposit
        self.assertEqual(transfer.status, TransferStatus.CONFIRMING)
        self.assertEqual(deposit.status, DepositStatus.CONFIRMING)

        # FULL 确认只会处理“足够老且已达到确认数”的转账，因此这里同时推进块高并回填创建时间。
        matured_created_at = timezone.now() - timedelta(seconds=12)
        OnchainTransfer.objects.filter(pk=transfer.pk).update(
            created_at=matured_created_at
        )
        self._mine_evm_block(w3)
        self._run_local_confirm_pipeline(chain=chain)

        transfer.refresh_from_db()
        deposit.refresh_from_db()
        self.assertEqual(transfer.status, TransferStatus.CONFIRMED)
        self.assertEqual(deposit.status, DepositStatus.COMPLETED)

    @patch("withdrawals.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_withdrawal_can_complete_via_confirm_task_pipeline(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # 提币确认也必须经过统一块高任务链路，而不是测试里直接手工推状态。
        w3 = self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Withdrawal Task Local",
            symbol="ETHWT2",
            coingecko_id="ethereum-withdrawal-task-local",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil Withdrawal Task",
            code="anvil-withdrawal-task",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        wallet = Wallet.generate()
        project = Project.objects.create(
            name="Local EVM Withdrawal Task Project",
            wallet=wallet,
        )
        vault_address = wallet.get_address(
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
        )
        recipient = Web3.to_checksum_address(w3.eth.accounts[5])
        self._prime_evm_scan_cursors(chain=chain)
        w3.eth.wait_for_transaction_receipt(
            w3.eth.send_transaction(
                {
                    "from": w3.eth.accounts[0],
                    "to": vault_address.address,
                    "value": int(Decimal("0.2") * Decimal(10**18)),
                }
            )
        )

        evm_task = EvmBroadcastTask.schedule_transfer(
            address=vault_address,
            crypto=crypto,
            chain=chain,
            to=recipient,
            value_raw=int(Decimal("0.01") * Decimal(10**18)),
            transfer_type=TransferType.Withdrawal,
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="local-evm-withdraw-task-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("0.01"),
            to=recipient,
            hash=evm_task.base_task.tx_hash,
            broadcast_task=evm_task.base_task,
            status=WithdrawalStatus.PENDING,
        )

        evm_task.broadcast()
        w3.eth.wait_for_transaction_receipt(evm_task.base_task.tx_hash)
        transfer = self._scan_evm_chain_and_get_transfer(
            chain=chain,
            tx_hash=evm_task.base_task.tx_hash,
            expected_scanner="native",
        )
        transfer.process()
        withdrawal.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.CONFIRMING)

        matured_created_at = timezone.now() - timedelta(seconds=12)
        OnchainTransfer.objects.filter(pk=transfer.pk).update(
            created_at=matured_created_at
        )
        self._mine_evm_block(w3)
        self._run_local_confirm_pipeline(chain=chain)

        transfer.refresh_from_db()
        withdrawal.refresh_from_db()
        evm_task.base_task.refresh_from_db()
        self.assertEqual(transfer.status, TransferStatus.CONFIRMED)
        self.assertEqual(withdrawal.status, WithdrawalStatus.COMPLETED)
        self.assertEqual(evm_task.base_task.result, BroadcastTaskResult.SUCCESS)

    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_native_scan_replay_keeps_transfer_idempotent(
        self,
        _process_transfer_mock,
    ):
        # 真实链上重复扫描同一原生币转账时，只允许首轮创建 OnchainTransfer，后续必须走幂等重放。
        w3 = self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Native Replay Local",
            symbol="ETHR2",
            coingecko_id="ethereum-native-replay-local",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil Native Replay",
            code="anvil-native-replay",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        project = Project.objects.create(
            name="Local EVM Native Replay Project",
            wallet=Wallet.generate(),
        )
        customer = Customer.objects.create(
            project=project, uid="evm-native-replay-customer"
        )
        # L2：DepositAddress.get_address 现在要求 project 已配 DEPOSIT_COLLECTION recipient。
        RecipientAddress.objects.create(
            project=project,
            chain_type=chain.type,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000beef"
            ),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        deposit_address = DepositAddress.get_address(chain, customer)
        self._prime_evm_scan_cursors(chain=chain)

        tx_hash = w3.eth.send_transaction(
            {
                "from": w3.eth.accounts[0],
                "to": deposit_address,
                "value": int(Decimal("0.015") * Decimal(10**18)),
            }
        )
        w3.eth.wait_for_transaction_receipt(tx_hash)

        first_summary = EvmChainScannerService.scan_chain(chain=chain)
        second_summary = EvmChainScannerService.scan_chain(chain=chain)

        self.assertGreaterEqual(first_summary.native.created_transfers, 1)
        self.assertEqual(second_summary.native.created_transfers, 0)
        normalized_tx_hash = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
        if not normalized_tx_hash.startswith("0x"):
            normalized_tx_hash = f"0x{normalized_tx_hash}"
        self.assertEqual(
            OnchainTransfer.objects.filter(
                chain=chain, hash=normalized_tx_hash.lower()
            ).count(),
            1,
        )

    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_evm_erc20_scan_replay_keeps_transfer_idempotent(
        self,
        _process_transfer_mock,
    ):
        # ERC20 真实链回放同样必须依赖唯一键保持幂等，不能因为尾部重扫重复生成业务转账。
        w3 = self._require_anvil()
        native_crypto = Crypto.objects.create(
            name="Ethereum ERC20 Replay Native",
            symbol="ETHER2",
            coingecko_id="ethereum-erc20-replay-native",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil ERC20 Replay",
            code="anvil-erc20-replay",
            type=ChainType.EVM,
            native_coin=native_crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        token_contract = self._deploy_test_erc20(w3, supply_raw=1_000_000_000)
        token_crypto = Crypto.objects.create(
            name="Test Token Replay",
            symbol="TTR",
            coingecko_id="test-token-replay",
            decimals=6,
            prices={"USD": "1"},
        )
        ChainToken.objects.create(
            crypto=token_crypto,
            chain=chain,
            address=token_contract.address,
        )
        project = Project.objects.create(
            name="Local ERC20 Replay Project",
            wallet=Wallet.generate(),
        )
        customer = Customer.objects.create(project=project, uid="erc20-replay-customer")
        # L2：DepositAddress.get_address 现在要求 project 已配 DEPOSIT_COLLECTION recipient。
        RecipientAddress.objects.create(
            project=project,
            chain_type=chain.type,
            address=Web3.to_checksum_address(
                "0x000000000000000000000000000000000000beef"
            ),
            usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
        )
        deposit_address = DepositAddress.get_address(chain, customer)
        self._prime_evm_scan_cursors(chain=chain)

        receipt = w3.eth.wait_for_transaction_receipt(
            token_contract.functions.transfer(deposit_address, 6_000_000).transact(
                {"from": w3.eth.accounts[0]}
            )
        )

        first_summary = EvmChainScannerService.scan_chain(chain=chain)
        second_summary = EvmChainScannerService.scan_chain(chain=chain)

        self.assertGreaterEqual(first_summary.erc20.created_transfers, 1)
        self.assertEqual(second_summary.erc20.created_transfers, 0)
        normalized_tx_hash = receipt.transactionHash.hex()
        if not normalized_tx_hash.startswith("0x"):
            normalized_tx_hash = f"0x{normalized_tx_hash}"
        self.assertEqual(
            OnchainTransfer.objects.filter(
                chain=chain, hash=normalized_tx_hash.lower()
            ).count(),
            1,
        )

    def test_local_evm_missing_tx_is_dropped_and_reverts_withdrawal(self):
        # 节点查不到 hash 时，OnchainTransfer 被 drop，提币回退到 PENDING 等待重新匹配。
        self._require_anvil()
        crypto = Crypto.objects.create(
            name="Ethereum Missing Tx Local",
            symbol="ETHMX",
            coingecko_id="ethereum-missing-tx-local",
            decimals=18,
        )
        chain = Chain.objects.create(
            name="Anvil Missing Tx",
            code="anvil-missing-tx",
            type=ChainType.EVM,
            native_coin=crypto,
            chain_id=31337,
            rpc=self.EVM_RPC,
            active=True,
            confirm_block_count=1,
        )
        wallet = Wallet.generate()
        project = Project.objects.create(
            name="Local EVM Missing Tx Project",
            wallet=wallet,
        )
        addr = wallet.get_address(
            chain_type=ChainType.EVM,
            usage=AddressUsage.Vault,
        )
        broadcast_task = BroadcastTask.objects.create(
            chain=chain,
            address=addr,
            transfer_type=TransferType.Withdrawal,
            crypto=crypto,
            recipient=Web3.to_checksum_address(
                "0x0000000000000000000000000000000000000009"
            ),
            amount=Decimal("0.01"),
            tx_hash="0x" + "9" * 64,
            stage=BroadcastTaskStage.PENDING_CONFIRM,
            result=BroadcastTaskResult.UNKNOWN,
        )
        transfer = OnchainTransfer.objects.create(
            chain=chain,
            block=1,
            hash=broadcast_task.tx_hash,
            event_id="native:tx",
            crypto=crypto,
            from_address=addr.address,
            to_address=broadcast_task.recipient,
            value=Decimal("1"),
            amount=Decimal("0.01"),
            timestamp=1,
            datetime=timezone.now(),
            status=TransferStatus.CONFIRMING,
            type=TransferType.Withdrawal,
            processed_at=timezone.now(),
        )
        withdrawal = Withdrawal.objects.create(
            project=project,
            out_no="local-missing-tx-order",
            chain=chain,
            crypto=crypto,
            amount=Decimal("0.01"),
            to=broadcast_task.recipient,
            hash=broadcast_task.tx_hash,
            broadcast_task=broadcast_task,
            transfer=transfer,
            status=WithdrawalStatus.CONFIRMING,
        )

        with self.captureOnCommitCallbacks(execute=True):
            confirm_transfer.run(transfer.pk)

        # OnchainTransfer 被 drop 后直接删除，释放唯一约束以允许 reorg 后重建
        self.assertFalse(OnchainTransfer.objects.filter(pk=transfer.pk).exists())
        withdrawal.refresh_from_db()
        broadcast_task.refresh_from_db()
        self.assertEqual(withdrawal.status, WithdrawalStatus.PENDING)
        self.assertIsNone(withdrawal.transfer_id)
        self.assertEqual(broadcast_task.stage, BroadcastTaskStage.PENDING_CHAIN)
        self.assertEqual(broadcast_task.result, BroadcastTaskResult.UNKNOWN)


class LocalBitcoinIntegrationTests(LocalChainIntegrationMixin, TestCase):
    @patch.dict(environ, {"BITCOIN_NETWORK": "regtest"}, clear=False)
    @patch("invoices.service.WebhookService.create_event")
    @patch("chains.tasks.process_transfer.apply_async")
    def test_local_bitcoin_invoice_payment_can_complete(
        self,
        _process_transfer_mock,
        _create_event_mock,
    ):
        # 真实 regtest 联调：项目 BTC 收款地址收到付款后，
        # 扫描、Invoice 命中、确认和 Completed 终局都必须打通。
        self._require_bitcoin()
        # prepare_local_bitcoin 负责创建 xcash / xcash-miner 钱包并预挖区块
        call_command(
            "prepare_local_bitcoin", "--wallet-name=xcash", "--mine-blocks=101"
        )
        wallet_client = self._require_bitcoin_miner()
        crypto = Crypto.objects.create(
            name="Bitcoin Invoice Local",
            symbol="BTCI",
            coingecko_id="bitcoin-invoice-local",
            decimals=8,
            prices={"USD": "65000"},
        )
        chain = Chain.objects.create(
            name="Bitcoin Local Invoice",
            code="bitcoin-local-invoice",
            type=ChainType.BITCOIN,
            native_coin=crypto,
            rpc=self.BTC_RPC,
            active=True,
            confirm_block_count=1,
        )
        project = Project.objects.create(
            name="Local BTC Invoice Project",
            wallet=Wallet.generate(),
        )
        ensure_base_currencies()
        recipient = RecipientAddress.objects.create(
            name="BTC Invoice Recipient",
            project=project,
            chain_type=ChainType.BITCOIN,
            address=wallet_client.get_new_address(
                label="btc-invoice-recipient",
                address_type="legacy",
            ),
            usage=RecipientAddressUsage.INVOICE,
        )
        invoice = Invoice.objects.create(
            project=project,
            out_no="local-btc-invoice-order",
            title="Local BTC Invoice",
            currency=crypto.symbol,
            amount=Decimal("0.012"),
            methods={crypto.symbol: [chain.code]},
            expires_at=timezone.now() + timedelta(minutes=10),
        )
        invoice.select_method(crypto, chain)
        invoice.refresh_from_db()
        self.assertEqual(invoice.pay_address, recipient.address)
        self.assertEqual(invoice.pay_amount, Decimal("0.012"))
        tx_hash = wallet_client.send_to_address(invoice.pay_address, invoice.pay_amount)
        mining_address = wallet_client.get_new_address(
            label="btc-invoice-miner",
            address_type="legacy",
        )
        wallet_client.generate_to_address(1, mining_address)

        from bitcoin.tasks import scan_bitcoin_receipts

        scan_bitcoin_receipts.run()

        transfer = OnchainTransfer.objects.get(
            chain=chain,
            hash=tx_hash,
            to_address=invoice.pay_address,
        )
        transfer.process()
        invoice.refresh_from_db()
        self.assertEqual(transfer.type, TransferType.Invoice)
        self.assertEqual(invoice.status, InvoiceStatus.CONFIRMING)
        self.assertEqual(invoice.transfer_id, transfer.pk)

        transfer.confirm()
        invoice.refresh_from_db()
        self.assertEqual(invoice.status, InvoiceStatus.COMPLETED)
