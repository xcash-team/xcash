"""让 pytest 与 manage.py 共享同一套应用导入路径。"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent
APPS_DIR = PROJECT_ROOT / "xcash"

# 项目 app 实际位于内层 xcash 目录；pytest 不会像 manage.py 那样自动补这段路径。
if str(APPS_DIR) not in sys.path:
    sys.path.append(str(APPS_DIR))


def pytest_ignore_collect(collection_path, config):
    # signer 是独立 Django 项目，根仓库 pytest 使用主应用 settings 时必须跳过它，避免两套项目在同一进程里混跑。
    try:
        relative_path = Path(collection_path).resolve().relative_to(PROJECT_ROOT)
    except ValueError:
        return False
    return bool(relative_path.parts) and relative_path.parts[0] == "signer"


@pytest.fixture(autouse=True, scope="session")
def _short_circuit_chain_rpc_probe(django_db_setup):
    # Chain.save() 默认会同步调用 _detect_chain_id / _detect_poa 走 web3 RPC；
    # 测试里 chain.rpc 多为伪 URL（如 http://bsc.local），mac DNS 解析需要 ~5s 才超时，
    # 累计到完整测试里能占去 50s+。测试链默认不声明 POA，短路为 False 与常规失败兜底等价。
    from chains.models import Chain  # noqa: PLC0415

    real_detect_poa = Chain._detect_poa

    def _fake_detect_chain_id(self):
        return self.chain_id

    with (
        patch(
            "chains.models.Chain._detect_poa",
            autospec=True,
            return_value=False,
        ) as detect_poa_mock,
        patch(
            "chains.models.Chain._detect_chain_id",
            autospec=True,
            side_effect=_fake_detect_chain_id,
        ),
    ):
        detect_poa_mock.real_implementation = real_detect_poa
        yield


@pytest.fixture(autouse=True)
def _reset_platform_settings_cache():
    # PlatformSettings 单例走 Redis 缓存 timeout=None；TestCase 事务回滚后缓存里仍残留旧对象，
    # 会让下一个用例读到上一个测试创建的 open_native_scanner 等开关，造成跨用例污染。
    from django.core.cache import cache  # noqa: PLC0415
    from core.models import PLATFORM_SETTINGS_CACHE_KEY  # noqa: PLC0415

    cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
    yield
    cache.delete(PLATFORM_SETTINGS_CACHE_KEY)
