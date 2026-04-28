# xcash/stress/service.py
import hashlib
import hmac
import json
import random
import re
import time
from datetime import timedelta
from decimal import Decimal

import httpx
import structlog
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from common.consts import APPID_HEADER
from common.consts import NONCE_HEADER
from common.consts import SIGNATURE_HEADER
from common.consts import TIMESTAMP_HEADER
from invoices.models import Invoice
from projects.models import Project
from projects.models import RecipientAddress
from projects.models import RecipientAddressUsage

from .models import DepositStressCase
from .models import InvoiceStressCase
from .models import StressRun
from .models import StressRunStatus
from .models import WithdrawalStressCase

logger = structlog.get_logger()

# 压测链路只打本地测试链，避免误选到其他环境链导致支付不可达。
STRESS_FIXED_METHODS = {
    "BTC": ["bitcoin-local"],
    "ETH": ["ethereum-local"],
    "USDT": ["ethereum-local"],
}
STRESS_FIXED_METHOD_CHOICES = (
    ("BTC", "bitcoin-local"),
    ("ETH", "ethereum-local"),
    ("USDT", "ethereum-local"),
)
# 提币只支持 EVM 链，BTC 充提已砍掉。
STRESS_WITHDRAWAL_METHOD_CHOICES = (
    ("ETH", "ethereum-local"),
    ("USDT", "ethereum-local"),
)
STRESS_SAAS_PERMISSION_CACHE_TTL = 24 * 60 * 60


class StressService:

    @staticmethod
    def prepare(stress: StressRun) -> None:
        """创建专用 Project 和批量测试 Case。由 Celery 任务异步调用。"""
        if stress.status != StressRunStatus.PREPARING:
            return

        with transaction.atomic():
            _ensure_stress_crypto_prices()
            _cleanup_orphan_stress_project(stress)
            project = _create_stress_project(stress)
            _setup_recipient_addresses(project)

            # 提币或充币测试需要 Wallet + Vault 地址
            if stress.withdrawal_count > 0 or stress.deposit_count > 0:
                _setup_wallet_for_withdrawal(project)

            # 充币压测：到期即归集，并把首次窗口压到 1 分钟，避免验证阶段久等。
            if stress.deposit_count > 0:
                project.gather_worth = Decimal("0")
                project.gather_period = 1
                project.save(update_fields=["gather_worth", "gather_period"])

            stress.project = project
            stress.error = ""
            stress.finished_at = None
            stress.save(update_fields=["project", "error", "finished_at"])

            cases = _build_stress_cases(stress)
            InvoiceStressCase.objects.bulk_create(cases)

            if stress.withdrawal_count > 0:
                wd_cases = _build_withdrawal_cases(stress)
                WithdrawalStressCase.objects.bulk_create(wd_cases)

            if stress.deposit_count > 0:
                dep_cases = _build_deposit_cases(stress)
                DepositStressCase.objects.bulk_create(dep_cases)

            stress.status = StressRunStatus.READY
            stress.save(update_fields=["status"])

        # 事务提交后立刻把新建的 BTC invoice RecipientAddress 同步到节点
        # watch-only 视图，避免 sync_bitcoin_watch_addresses 默认 300s 周期
        # 让 BTC 长尾拖到 12 分钟（实测 100 笔压测里 BTC P95 会从 760s 降到
        # ~30s 量级）。EVM 收款地址不需要这一步，xcash 直接按 RecipientAddress
        # 表扫，没有节点 watch-only 中间层。
        if stress.count > 0:
            _sync_stress_btc_watch_only()

        # Vault 注资在事务提交后执行，确保数据库记录已落库
        if stress.withdrawal_count > 0 or stress.deposit_count > 0:
            _fund_vault_for_withdrawal(stress.project)

        logger.info(
            "stress.prepared",
            stress_id=stress.pk,
            count=stress.count,
            withdrawal_count=stress.withdrawal_count,
            deposit_count=stress.deposit_count,
        )

    @staticmethod
    def start(stress: StressRun) -> None:
        """触发测试执行。"""
        from .evm import sync_chain_clock
        from .tasks import execute_deposit_case
        from .tasks import execute_stress_case
        from .tasks import execute_withdrawal_case
        from .tasks import finalize_stress_timeout
        from .tasks import verify_deposit_collection

        # 每轮压测启动时把 Anvil 链时钟拉齐到系统时间，避免 block.timestamp
        # 漂移导致 try_match_invoice 的时间窗口过滤失败。详见 evm.sync_chain_clock。
        sync_chain_clock()

        stress.status = StressRunStatus.RUNNING
        stress.started_at = timezone.now()
        stress.save(update_fields=["status", "started_at"])

        max_offset = 0.0
        for case in stress.cases.all().only("id", "scheduled_offset"):
            eta = stress.started_at + timedelta(seconds=case.scheduled_offset)
            execute_stress_case.apply_async(args=[case.pk], eta=eta)
            max_offset = max(max_offset, case.scheduled_offset)

        for case in stress.withdrawal_cases.all().only("id", "scheduled_offset"):
            eta = stress.started_at + timedelta(seconds=case.scheduled_offset)
            execute_withdrawal_case.apply_async(args=[case.pk], eta=eta)
            max_offset = max(max_offset, case.scheduled_offset)

        for case in stress.deposit_cases.all().only("id", "scheduled_offset"):
            eta = stress.started_at + timedelta(seconds=case.scheduled_offset)
            execute_deposit_case.apply_async(args=[case.pk], eta=eta)
            max_offset = max(max_offset, case.scheduled_offset)

        # 充币归集验证兜底：最大调度偏移 + 20 分钟
        # webhook 触发是主路径，这里是安全网，verify_deposit_collection 本身幂等。
        if stress.deposit_count > 0:
            collection_eta = stress.started_at + timedelta(
                seconds=max_offset + 20 * 60
            )
            verify_deposit_collection.apply_async(
                args=[stress.pk], eta=collection_eta
            )

        # 兜底超时：最大调度偏移 + 30 分钟
        timeout_seconds = max_offset + 30 * 60
        finalize_stress_timeout.apply_async(
            args=[stress.pk],
            eta=stress.started_at + timedelta(seconds=timeout_seconds),
        )

        logger.info("stress.started", stress_id=stress.pk)

    @staticmethod
    def on_case_finished(case) -> None:
        """InvoiceStressCase 或 WithdrawalStressCase 进入终态后，更新 StressRun 统计并检查是否全部完成。

        使用 select_for_update 锁定 StressRun 行，在同一事务内完成
        计数递增、终态判定和状态更新，防止并发 worker 竞态。
        """
        with transaction.atomic():
            stress_run = StressRun.objects.select_for_update().get(
                pk=case.stress_run_id
            )

            if case.status == "failed":
                stress_run.failed += 1
            elif case.status == "succeeded":
                stress_run.succeeded += 1
            elif case.status == "skipped":
                stress_run.skipped += 1

            update_fields = ["succeeded", "failed", "skipped"]

            total_expected = (
                stress_run.count
                + stress_run.withdrawal_count
                + stress_run.deposit_count
            )
            if stress_run.total_finished >= total_expected:
                stress_run.status = StressRunStatus.COMPLETED
                stress_run.finished_at = timezone.now()
                update_fields += ["status", "finished_at"]
                logger.info(
                    "stress.completed",
                    stress_run_id=stress_run.pk,
                    succeeded=stress_run.succeeded,
                    failed=stress_run.failed,
                )

            stress_run.save(update_fields=update_fields)

    @staticmethod
    def _build_hmac_headers(project: Project, body_str: str) -> dict[str, str]:
        """构造带 HMAC 签名的 API 请求头。"""
        nonce = hashlib.md5(  # noqa: S324
            f"{time.time()}{random.random()}".encode()  # noqa: S311
        ).hexdigest()
        timestamp = str(int(timezone.now().timestamp()))
        message = f"{nonce}{timestamp}{body_str}"
        signature = hmac.new(
            project.hmac_key.encode(), message.encode(), hashlib.sha256
        ).hexdigest()

        return {
            "Content-Type": "application/json",
            APPID_HEADER: project.appid,
            NONCE_HEADER: nonce,
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: signature,
        }

    @staticmethod
    def create_invoice(case: InvoiceStressCase) -> dict:
        """调用 API 创建 Invoice，仅使用 Stress Project 已准备好的本地链 methods。"""
        stress_run = case.stress_run
        project = stress_run.project
        methods = _require_stress_methods_ready(project)

        # 下限 1 USD：BTC ~6 万 USD/枚下，0.1-0.2 USD 折算 ≈ 170-340 sat，
        # 加上 invoice differ_amount 微调后会落进 bech32 dust 区间(~294 sat)，
        # 触发 sendtoaddress "Transaction amount too small"。提到 1 USD ≈
        # 1700 sat，对任何地址类型 dust threshold 都有充足余量。
        amount = str(round(random.uniform(1, 10), 2))  # noqa: S311
        out_no = f"STRESS-{stress_run.pk}-{case.sequence}"

        body = json.dumps(
            {
                "out_no": out_no,
                "title": f"Stress Test #{case.sequence}",
                "currency": "USD",
                "amount": amount,
                "duration": 30,
                "methods": methods,
            }
        )

        headers = StressService._build_hmac_headers(project, body)
        base_url = settings.STRESS_WEBHOOK_BASE_URL
        url = f"{base_url}/v1/invoice"

        resp = httpx.post(url, content=body, headers=headers, timeout=10)
        if resp.status_code >= 400:
            detail = _extract_error_detail(resp)
            raise RuntimeError(f"创建 Invoice API {resp.status_code}: {detail}")
        return resp.json()

    @staticmethod
    def select_method(case: InvoiceStressCase) -> dict:
        """调用 API 选择支付方式，只在压测固定的本地链组合中选择。"""
        stress_run = case.stress_run
        project = stress_run.project
        base_url = settings.STRESS_WEBHOOK_BASE_URL

        crypto_symbol, chain_code = random.choice(  # noqa: S311
            STRESS_FIXED_METHOD_CHOICES
        )

        body = json.dumps({"crypto": crypto_symbol, "chain": chain_code})
        url = f"{base_url}/v1/invoice/{case.invoice_sys_no}/select-method"

        resp = httpx.post(
            url,
            content=body,
            headers={
                "Content-Type": "application/json",
                APPID_HEADER: project.appid,
            },
            timeout=10,
        )
        if resp.status_code >= 400:
            detail = _extract_error_detail(resp)
            raise RuntimeError(f"选择支付方式 API {resp.status_code}: {detail}")
        return resp.json()

    @staticmethod
    def get_deposit_address(case: DepositStressCase) -> str:
        """调用 API 获取客户充值地址。"""
        project = case.stress_run.project
        base_url = settings.STRESS_WEBHOOK_BASE_URL
        url = f"{base_url}/v1/deposit/address"

        # GET 请求 body 为空，HMAC 签名仍需携带
        headers = StressService._build_hmac_headers(project, "")
        resp = httpx.get(
            url,
            params={
                "uid": case.customer_uid,
                "chain": case.chain,
                "crypto": case.crypto,
            },
            headers=headers,
            timeout=10,
        )
        if resp.status_code >= 400:
            detail = _extract_error_detail(resp)
            raise RuntimeError(f"获取充值地址 API {resp.status_code}: {detail}")
        return resp.json()["deposit_address"]

    @staticmethod
    def create_withdrawal(case: WithdrawalStressCase) -> dict:
        """调用提币 API，使用 HMAC 签名。"""
        stress_run = case.stress_run
        project = stress_run.project
        out_no = f"STRESS-WD-{stress_run.pk}-{case.sequence}"

        body = json.dumps(
            {
                "out_no": out_no,
                "to": case.to_address,
                "crypto": case.crypto,
                "chain": case.chain,
                "amount": str(case.amount.normalize()),
            }
        )

        headers = StressService._build_hmac_headers(project, body)
        base_url = settings.STRESS_WEBHOOK_BASE_URL
        url = f"{base_url}/v1/withdrawal"

        resp = httpx.post(url, content=body, headers=headers, timeout=30)
        if resp.status_code >= 400:
            detail = _extract_error_detail(resp)
            logger.error(
                "stress.create_withdrawal.failed",
                status=resp.status_code,
                detail=detail,
                request_body=body,
            )
            raise RuntimeError(
                f"提币 API {resp.status_code}: {detail}"
            )
        return resp.json()


# Anvil 默认助记词派生的账户地址（索引 5-9，避免与付款账户 0 冲突）
_ANVIL_RECIPIENT_ADDRESSES = [
    "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",  # index 5
    "0x976EA74026E726554dB657fA54763abd0C3a0aa9",  # index 6
]


def _setup_recipient_addresses(project: Project) -> None:
    """为 Stress Project 配置收币地址。

    压测链路固定跑本地测试链，不再依赖"系统里已有其他项目模板地址"。
    - EVM: 直接使用 Anvil 预置账户地址
    - BTC: 直接向 regtest 节点申请新地址
    - RecipientAddress 全局仍保持"单记录单用途"约束，因此 stress 专用
      Project 通过同链两条地址记录分别承接 invoice / deposit。
    """
    from chains.models import ChainType

    def upsert_recipient(
        *,
        name: str,
        chain_type: str,
        address: str,
        usage: str,
    ) -> None:
        RecipientAddress.objects.update_or_create(
            chain_type=chain_type,
            address=address,
            defaults={
                "name": name,
                "project": project,
                "usage": usage,
            },
        )

    upsert_recipient(
        name=f"Stress-{project.pk}-evm-invoice",
        chain_type=ChainType.EVM,
        address=_ANVIL_RECIPIENT_ADDRESSES[0],
        usage=RecipientAddressUsage.INVOICE,
    )
    upsert_recipient(
        name=f"Stress-{project.pk}-evm-deposit",
        chain_type=ChainType.EVM,
        address=_ANVIL_RECIPIENT_ADDRESSES[1],
        usage=RecipientAddressUsage.DEPOSIT_COLLECTION,
    )

    try:
        from .bitcoin import BitcoinStressClient
        from .bitcoin import _mine_blocks

        btc_client = BitcoinStressClient()
        btc_invoice_address = btc_client.get_new_address()
        upsert_recipient(
            name=f"Stress-{project.pk}-btc-invoice",
            chain_type=ChainType.BITCOIN,
            address=btc_invoice_address,
            usage=RecipientAddressUsage.INVOICE,
        )
        # BTC 充币已砍掉，只保留 Invoice 收款地址。

        # 预挖矿：确保 root wallet 有充足余额供后续 BTC 支付 case 使用，
        # 避免并发 case 同时耗尽余额导致 Insufficient funds。
        _mine_blocks(btc_client.root_wallet_client, count=110)
        logger.info("stress.btc.pre_mined", blocks=110)
    except Exception as exc:
        logger.warning("stress.setup_btc_recipient_failed", exc_info=True)
        raise RuntimeError("创建 BTC 收款地址失败，Stress Project 未准备完成") from exc


# 压测用的法币价格兜底值，仅在 prices 为空时回填，避免依赖外部行情源。
_STRESS_FALLBACK_PRICES = {
    "BTC": {"USD": 60000},
    "ETH": {"USD": 3000},
}


def _ensure_stress_crypto_prices() -> None:
    """确保压测币种有可用的 USD 价格，避免 select_method 法币换算时 KeyError。"""
    from currencies.models import Crypto

    for symbol, fallback in _STRESS_FALLBACK_PRICES.items():
        try:
            crypto = Crypto.objects.get(symbol=symbol)
        except Crypto.DoesNotExist:
            continue
        if "USD" not in crypto.prices:
            crypto.prices.update(fallback)
            crypto.save(update_fields=["prices"])
            logger.info("stress.backfill_price", symbol=symbol, prices=crypto.prices)


def _cleanup_orphan_stress_project(stress: StressRun) -> None:
    """清理 prepare 历史失败后遗留的未绑定专用 Project。"""
    if stress.project_id is not None:
        return
    Project.objects.filter(
        name=f"Stress-{stress.pk}",
        stressrun__isnull=True,
    ).delete()


def _create_stress_project(stress: StressRun) -> Project:
    """创建当前 StressRun 专用 Project。"""
    webhook_url = f"{settings.STRESS_WEBHOOK_BASE_URL}/stress/webhook"
    project = Project.objects.create(
        name=f"Stress-{stress.pk}",
        webhook=webhook_url,
        ip_white_list="*",
        active=True,
        # 设置极大阈值使所有 Invoice 都走 QUICK 确认模式，
        # 因为 Anvil 本地测试链不会自动产生新区块，FULL 模式无法完成确认。
        fast_confirm_threshold=Decimal("99999999"),
        withdrawal_review_required=False,
    )
    transaction.on_commit(lambda: _seed_stress_saas_permission_cache(project))
    return project


def _seed_stress_saas_permission_cache(project: Project) -> None:
    """为压测专用 Project 预置 SaaS 权限缓存，避免业务压测打到 SaaS 权限服务。"""
    perm = {
        "appid": project.appid,
        "frozen": False,
        "enable_deposit": True,
        "enable_withdrawal": True,
    }
    cache_key = f"saas:permission:{project.appid}"
    cache.set(f"{cache_key}:stale", perm, STRESS_SAAS_PERMISSION_CACHE_TTL)
    cache.set(cache_key, perm, STRESS_SAAS_PERMISSION_CACHE_TTL)


def _setup_wallet_for_withdrawal(project: Project) -> None:
    """为 Stress Project 创建 Wallet 并预派生 Vault 地址。"""
    from chains.models import AddressUsage
    from chains.models import ChainType
    from chains.models import Wallet

    wallet = Wallet.generate()
    project.wallet = wallet
    project.save(update_fields=["wallet"])

    # 预派生 EVM Vault 地址（BTC 提币已砍掉，无需派生 BTC Vault）
    wallet.get_address(chain_type=ChainType.EVM, usage=AddressUsage.Vault)


def _build_stress_cases(stress: StressRun) -> list[InvoiceStressCase]:
    """构建本轮待执行的 InvoiceStressCase 列表。"""
    total_seconds = stress.count / 10.0
    mu = total_seconds / 2
    sigma = total_seconds / 6

    cases = []
    for i in range(1, stress.count + 1):
        offset = max(0.0, min(total_seconds, random.gauss(mu, sigma)))
        cases.append(
            InvoiceStressCase(
                stress_run=stress,
                sequence=i,
                scheduled_offset=offset,
            )
        )

    random.shuffle(cases)
    for idx, case in enumerate(cases, 1):
        case.sequence = idx
    return cases


def _build_withdrawal_cases(stress: StressRun) -> list[WithdrawalStressCase]:
    """构建本轮待执行的 WithdrawalStressCase 列表。

    每个 case 预分配 crypto/chain、金额和目标地址，
    执行时直接调用提币 API。提币仅支持 EVM 链。
    """
    from web3 import Web3

    total_seconds = stress.withdrawal_count / 10.0
    mu = total_seconds / 2
    sigma = total_seconds / 6

    amount_ranges = {
        "ETH": (Decimal("0.0001"), Decimal("0.001")),
        "USDT": (Decimal("0.01"), Decimal("0.1")),
    }

    cases = []
    for i in range(1, stress.withdrawal_count + 1):
        offset = max(0.0, min(total_seconds, random.gauss(mu, sigma)))
        crypto_symbol, chain_code = random.choice(STRESS_WITHDRAWAL_METHOD_CHOICES)  # noqa: S311

        lo, hi = amount_ranges[crypto_symbol]
        amount = Decimal(str(round(random.uniform(float(lo), float(hi)), 8)))  # noqa: S311

        to_address = Web3().eth.account.create().address

        cases.append(
            WithdrawalStressCase(
                stress_run=stress,
                sequence=i,
                scheduled_offset=offset,
                crypto=crypto_symbol,
                chain=chain_code,
                to_address=to_address,
                amount=amount,
            )
        )

    random.shuffle(cases)
    for idx, case in enumerate(cases, 1):
        case.sequence = idx
    return cases


STRESS_DEPOSIT_METHOD_CHOICES = (
    ("ETH", "ethereum-local"),
    ("USDT", "ethereum-local"),
)

_DEPOSIT_AMOUNT_RANGES = {
    "ETH": (Decimal("0.001"), Decimal("0.05")),
    "USDT": (Decimal("1"), Decimal("100")),
}


def _sample_decimal_amount(
    *, lo: Decimal, hi: Decimal, decimal_places: int = 8
) -> Decimal:
    """按固定小数精度采样金额，避免 float/uniform 路径引入尾差。"""
    scale = 10**decimal_places
    lo_units = int(lo * scale)
    hi_units = int(hi * scale)
    sampled_units = random.randint(lo_units, hi_units)  # noqa: S311
    return Decimal(sampled_units).scaleb(-decimal_places)


def _build_deposit_cases(stress: StressRun) -> list[DepositStressCase]:
    """构建本轮待执行的 DepositStressCase 列表。

    将 deposit_count 均匀分配到 deposit_customer_count 个客户，
    每个 case 随机选择 ETH 或 USDT on ethereum-local。
    """
    customer_count = stress.deposit_customer_count
    deposit_count = stress.deposit_count

    # 生成客户 UID 列表
    customer_uids = [f"STRESS-{stress.pk}-C{i}" for i in range(customer_count)]

    # 均匀分配充值次数到各客户
    base_per_customer = deposit_count // customer_count
    remainder = deposit_count % customer_count
    customer_deposits = []
    for i in range(customer_count):
        n = base_per_customer + (1 if i < remainder else 0)
        customer_deposits.extend([customer_uids[i]] * n)

    total_seconds = deposit_count / 10.0
    mu = total_seconds / 2
    sigma = total_seconds / 6

    cases = []
    for i, uid in enumerate(customer_deposits, 1):
        offset = max(0.0, min(total_seconds, random.gauss(mu, sigma)))
        crypto_symbol, chain_code = random.choice(STRESS_DEPOSIT_METHOD_CHOICES)  # noqa: S311
        lo, hi = _DEPOSIT_AMOUNT_RANGES[crypto_symbol]
        amount = _sample_decimal_amount(lo=lo, hi=hi, decimal_places=8)

        cases.append(
            DepositStressCase(
                stress_run=stress,
                sequence=i,
                scheduled_offset=offset,
                customer_uid=uid,
                crypto=crypto_symbol,
                chain=chain_code,
                amount=amount,
            )
        )

    random.shuffle(cases)
    for idx, case in enumerate(cases, 1):
        case.sequence = idx
    return cases


def _require_stress_methods_ready(project: Project) -> dict[str, list[str]]:
    """校验 Stress Project 的本地链 methods 已完整可用。"""
    methods = Invoice.available_methods(project)
    if methods != STRESS_FIXED_METHODS:
        raise RuntimeError(
            "Stress Project 收款地址未准备完整，必须同时支持 BTC/ETH/USDT 本地链支付"
        )
    return methods


def _extract_error_detail(resp: httpx.Response) -> str:
    """从 API 错误响应中提取关键信息。

    JSON 响应直接返回；Django HTML 调试页提取异常类型和 traceback 摘要。
    """
    text = resp.text
    try:
        return json.dumps(resp.json(), ensure_ascii=False)[:2000]
    except Exception:
        pass
    # Django debug HTML: 提取 <title> 和 <pre class="exception_value">
    title = re.search(r"<title>(.*?)</title>", text, re.DOTALL)
    exc_value = re.search(
        r'class="exception_value">(.*?)</pre>', text, re.DOTALL
    )
    # 兜底提取所有 <pre> 块中的 traceback 片段
    tracebacks = re.findall(r"<pre[^>]*>(.*?)</pre>", text, re.DOTALL)
    parts = []
    if title:
        parts.append(title.group(1).strip())
    if exc_value:
        parts.append(exc_value.group(1).strip())
    if not exc_value and tracebacks:
        # 取最后一个 <pre> 往往是 traceback 摘要
        parts.append(tracebacks[-1].strip()[:1000])
    return " | ".join(parts)[:2000] if parts else text[:2000]


def _fund_vault_for_withdrawal(project: Project) -> None:
    """为 Stress Project 的 EVM Vault 地址注入测试币。

    在 prepare 事务提交后调用，确保 Wallet 和 Address 记录已落库。
    """
    _fund_evm_vault(project)


def _sync_stress_btc_watch_only() -> None:
    """主动触发一次 BTC watch-only 同步，把 Stress Project 新建的 BTC 收款
    地址立即 import 到 xcash 节点钱包视图。

    背景：sync_bitcoin_watch_addresses 默认每 300s 跑一次；新创建的
    RecipientAddress 要等到下一个 sync 周期 + 一次扫块才能被 xcash 的
    BitcoinChainScannerService 看到。100 笔级别压测里这会让 BTC 部分的
    paid_to_webhook 长尾从 ~30s 拖到 ~12 分钟（实测）。

    同步阻塞调用，但只 import 1-2 个新地址，耗时 100ms 量级，可接受。
    失败仅记录 warning，不阻塞 prepare —— 5 分钟周期的全量 sync 仍会兜底。
    """
    from bitcoin.watch_sync import BitcoinWatchSyncService
    from chains.models import Chain
    from chains.models import ChainType

    btc_chain = Chain.objects.filter(active=True, type=ChainType.BITCOIN).first()
    if btc_chain is None:
        return

    try:
        imported = BitcoinWatchSyncService.sync_chain(btc_chain)
        logger.info("stress.btc.watch_sync_done", imported=imported)
    except Exception:  # noqa: BLE001
        logger.warning("stress.btc.watch_sync_failed", exc_info=True)


def _fund_evm_vault(project: Project) -> None:
    """EVM Vault 注资：ETH 用 anvil_setBalance，USDT 用 ERC20 mint。"""
    from web3 import Web3

    from chains.models import AddressUsage
    from chains.models import Chain
    from chains.models import ChainType
    from currencies.models import Crypto
    from evm.local_erc20 import LOCAL_EVM_ERC20_ABI

    from .evm import _get_w3
    from .evm import _require_contract
    from .evm import _set_balance

    vault_address = project.wallet.get_address(
        chain_type=ChainType.EVM,
        usage=AddressUsage.Vault,
    ).address

    w3 = _get_w3()

    # 1. 注入 10000 ETH（覆盖大量提币 + gas）
    eth_amount_wei = 10000 * 10**18
    _set_balance(w3, vault_address, eth_amount_wei)
    logger.info("stress.vault.evm_eth_funded", vault=vault_address, amount_eth=10000)

    # 2. 为 Vault 铸造 USDT
    evm_chain = Chain.objects.get(code="ethereum-local")
    usdt = Crypto.objects.get(symbol="USDT")
    usdt_contract_address = usdt.address(evm_chain)
    if not usdt_contract_address:
        raise RuntimeError("USDT 在 ethereum-local 上没有合约地址，无法为 Vault 铸币")

    checksum_token = _require_contract(w3, usdt_contract_address)
    contract = w3.eth.contract(address=checksum_token, abi=LOCAL_EVM_ERC20_ABI)

    usdt_decimals = usdt.get_decimals(evm_chain)
    mint_amount = int(Decimal("10000000") * Decimal(10**usdt_decimals))

    # 用临时账户签名 mint 交易（本地测试合约的 mint 是公开方法）
    minter = w3.eth.account.create()
    _set_balance(w3, minter.address, 10**17)  # 0.1 ETH gas

    mint_tx = contract.functions.mint(
        Web3.to_checksum_address(vault_address), mint_amount
    ).build_transaction(
        {
            "from": minter.address,
            "nonce": w3.eth.get_transaction_count(minter.address, "pending"),
            "gasPrice": w3.eth.gas_price,
            "chainId": w3.eth.chain_id,
        }
    )
    signed = minter.sign_transaction(mint_tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash)
    logger.info(
        "stress.vault.evm_usdt_funded",
        vault=vault_address,
        amount="10000000",
        tx_hash=tx_hash.hex(),
    )
