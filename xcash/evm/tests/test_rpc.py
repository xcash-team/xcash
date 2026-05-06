from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase
from django.test import TestCase
from web3 import Web3
from web3.exceptions import ExtraDataLengthError

from chains.models import Chain
from chains.models import ChainType
from currencies.models import Crypto
from evm.scanner.rpc import EvmScannerRpcClient
from evm.scanner.rpc import EvmScannerRpcError


class EvmScannerRpcErrorMessageTests(SimpleTestCase):
    def test_get_transfer_logs_error_includes_rpc_method_and_raw_reason(self):
        # 游标 last_error 直接使用此异常文本；必须带上具体 RPC 方法和节点原始报错，
        # 否则后台只能看到失败区块范围，无法判断是套餐限流、超时还是节点内部错误。
        chain = SimpleNamespace(
            code="bsc-mainnet",
            evm_log_max_block_range=10,
            w3=SimpleNamespace(
                eth=SimpleNamespace(
                    get_logs=Mock(
                        side_effect=ValueError(
                            {"code": -32005, "message": "limit exceeded: 5000 results"}
                        )
                    )
                )
            ),
        )

        with self.assertRaises(EvmScannerRpcError) as caught:
            EvmScannerRpcClient(chain=chain).get_transfer_logs(
                from_block=100,
                to_block=109,
                token_addresses=[
                    Web3.to_checksum_address(
                        "0x00000000000000000000000000000000000000aa"
                    )
                ],
                topic0=Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)")),
            )

        message = str(caught.exception)
        self.assertIn("获取 ERC20 日志失败", message)
        self.assertIn("rpc=eth_getLogs", message)
        self.assertIn("limit exceeded: 5000 results", message)
        self.assertLess(message.index("rpc=eth_getLogs"), message.index("from=100"))
        self.assertIn("rpc=eth_getLogs", message[:60])

    def test_get_full_block_error_includes_rpc_method_and_raw_reason(self):
        chain = SimpleNamespace(
            code="bsc-mainnet",
            w3=SimpleNamespace(
                eth=SimpleNamespace(
                    get_block=Mock(side_effect=TimeoutError("read timeout"))
                )
            ),
        )

        with self.assertRaises(EvmScannerRpcError) as caught:
            EvmScannerRpcClient(chain=chain).get_full_block(block_number=9_586_911)

        message = str(caught.exception)
        self.assertIn("获取完整区块失败", message)
        self.assertIn("rpc=eth_getBlockByNumber", message)
        self.assertIn("read timeout", message)
        self.assertLess(
            message.index("rpc=eth_getBlockByNumber"),
            message.index("block=9586911"),
        )
        self.assertIn("rpc=eth_getBlockByNumber", message[:60])

    def test_get_transfer_logs_error_preserves_full_raw_reason(self):
        # 节点错误文本会直接进入扫描游标；长错误不能被提前截断，否则后台无法看到
        # 供应商返回的完整限制参数、建议区间或请求上下文。
        raw_reason = "limit exceeded: " + "x" * 360
        chain = SimpleNamespace(
            code="arbitrum-mainnet",
            evm_log_max_block_range=10,
            w3=SimpleNamespace(
                eth=SimpleNamespace(
                    get_logs=Mock(
                        side_effect=ValueError(
                            {
                                "code": -32005,
                                "message": raw_reason,
                            }
                        )
                    )
                )
            ),
        )

        with self.assertRaises(EvmScannerRpcError) as caught:
            EvmScannerRpcClient(chain=chain).get_transfer_logs(
                from_block=45_996_974,
                to_block=45_996_983,
                token_addresses=[
                    Web3.to_checksum_address(
                        "0x00000000000000000000000000000000000000aa"
                    )
                ],
                topic0=Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)")),
            )

        message = str(caught.exception)
        self.assertIn(raw_reason, message)
        self.assertIn("from=45996974 to=45996983", message)


class EvmScannerRpcClientTests(TestCase):
    def setUp(self):
        self.native = Crypto.objects.create(
            name="BNB RPC",
            symbol="BNBR",
            coingecko_id="binancecoin-rpc",
        )
        self.chain = Chain.objects.create(
            code="bsc-rpc-test",
            name="BSC RPC Test",
            type=ChainType.EVM,
            chain_id=56_001,
            rpc="http://bsc.rpc.local",
            native_coin=self.native,
            active=True,
        )

    def test_get_transfer_logs_splits_request_by_chain_max_block_range(self):
        # RPC 供应商限制 eth_getLogs 区块跨度时，应按链配置切片并聚合结果。
        Chain.objects.filter(pk=self.chain.pk).update(evm_log_max_block_range=10)
        self.chain.refresh_from_db()
        requested_ranges: list[tuple[int, int]] = []

        def fake_get_logs(filter_params: dict) -> list[dict]:
            requested_ranges.append(
                (filter_params["fromBlock"], filter_params["toBlock"])
            )
            return [
                {
                    "blockNumber": filter_params["fromBlock"],
                    "logIndex": 0,
                    "transactionHash": bytes.fromhex("ab" * 32),
                }
            ]

        self.chain.__dict__["w3"] = SimpleNamespace(
            eth=SimpleNamespace(get_logs=Mock(side_effect=fake_get_logs))
        )

        logs = EvmScannerRpcClient(chain=self.chain).get_transfer_logs(
            from_block=100,
            to_block=124,
            token_addresses=[
                Web3.to_checksum_address("0x00000000000000000000000000000000000000aa")
            ],
            topic0=Web3.to_hex(Web3.keccak(text="Transfer(address,address,uint256)")),
        )

        self.assertEqual(requested_ranges, [(100, 109), (110, 119), (120, 124)])
        self.assertEqual(len(logs), 3)

    @patch("evm.scanner.rpc.EvmScannerRpcClient._build_poa_retry_w3")
    def test_get_block_timestamp_retries_with_poa_when_extradata_is_too_long(
        self,
        build_poa_retry_w3_mock,
    ):
        # BSC 等 POA 链若因 is_poa 配置失真未注入 middleware，应能自动重试并自愈。
        self.chain.is_poa = False
        failing_w3 = SimpleNamespace(
            eth=SimpleNamespace(
                get_block=Mock(
                    side_effect=ExtraDataLengthError(
                        "poa extraData too long",
                    )
                )
            )
        )
        retry_w3 = SimpleNamespace(
            eth=SimpleNamespace(get_block=Mock(return_value={"timestamp": 1_776_734_136}))
        )
        self.chain.__dict__["w3"] = failing_w3
        build_poa_retry_w3_mock.return_value = retry_w3

        timestamp = EvmScannerRpcClient(chain=self.chain).get_block_timestamp(
            block_number=93_739_122
        )

        self.assertEqual(timestamp, 1_776_734_136)
        self.chain.refresh_from_db()
        self.assertTrue(self.chain.is_poa)
        build_poa_retry_w3_mock.assert_called_once()

    @patch("evm.scanner.rpc.EvmScannerRpcClient._build_poa_retry_w3")
    def test_get_full_block_retries_with_poa_when_extradata_is_too_long(
        self,
        build_poa_retry_w3_mock,
    ):
        self.chain.is_poa = False
        failing_w3 = SimpleNamespace(
            eth=SimpleNamespace(
                get_block=Mock(
                    side_effect=ExtraDataLengthError(
                        "poa extraData too long",
                    )
                )
            )
        )
        retry_w3 = SimpleNamespace(
            eth=SimpleNamespace(
                get_block=Mock(
                    return_value={"number": 93_739_122, "transactions": []}
                )
            )
        )
        self.chain.__dict__["w3"] = failing_w3
        build_poa_retry_w3_mock.return_value = retry_w3

        block = EvmScannerRpcClient(chain=self.chain).get_full_block(
            block_number=93_739_122
        )

        self.assertEqual(block["number"], 93_739_122)
        self.chain.refresh_from_db()
        self.assertTrue(self.chain.is_poa)
        build_poa_retry_w3_mock.assert_called_once()
