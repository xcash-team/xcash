import os
import shlex
import sys
from dataclasses import dataclass

from django.core.exceptions import ImproperlyConfigured

PROFILE_ENV = "PERFORMANCE"


@dataclass(frozen=True)
class PerformanceProfile:
    django_workers: int
    django_threads: int
    signer_workers: int
    celery_worker_concurrency: int
    evm_scan_seconds: int
    tron_scan_seconds: int
    bitcoin_scan_seconds: int
    bitcoin_watch_sync_seconds: int


PROFILES = {
    "low": PerformanceProfile(
        django_workers=1,
        django_threads=2,
        signer_workers=1,
        celery_worker_concurrency=2,
        evm_scan_seconds=16,
        tron_scan_seconds=16,
        bitcoin_scan_seconds=60,
        bitcoin_watch_sync_seconds=600,
    ),
    "medium": PerformanceProfile(
        django_workers=4,
        django_threads=4,
        signer_workers=2,
        celery_worker_concurrency=8,
        evm_scan_seconds=10,
        tron_scan_seconds=10,
        bitcoin_scan_seconds=45,
        bitcoin_watch_sync_seconds=300,
    ),
    "high": PerformanceProfile(
        django_workers=8,
        django_threads=8,
        signer_workers=2,
        celery_worker_concurrency=16,
        evm_scan_seconds=6,
        tron_scan_seconds=6,
        bitcoin_scan_seconds=30,
        bitcoin_watch_sync_seconds=180,
    ),
}

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def profile_name() -> str:
    name = os.environ.get(PROFILE_ENV, "low").strip().lower()
    if name in PROFILES:
        return name
    valid = ", ".join(sorted(PROFILES))
    raise ImproperlyConfigured(f"{PROFILE_ENV} must be one of: {valid}")


def active_profile() -> PerformanceProfile:
    return PROFILES[profile_name()]


def get_int(env_name: str, field_name: str) -> int:
    if env_name in os.environ:
        return int(os.environ[env_name])
    return int(getattr(active_profile(), field_name))


def get_int_default(env_name: str, default: int) -> int:
    if env_name in os.environ:
        return int(os.environ[env_name])
    return default


def get_bool_default(env_name: str, *, default: bool) -> bool:
    if env_name not in os.environ:
        return default
    return _parse_bool(env_name, os.environ[env_name])


def _parse_bool(env_name: str, raw_value: str) -> bool:
    value = raw_value.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise ImproperlyConfigured(f"{env_name} must be a boolean value")


def shell_env(target: str) -> dict[str, int]:
    match target:
        case "web":
            return {
                "GUNICORN_WORKERS": get_int("GUNICORN_WORKERS", "django_workers"),
                "GUNICORN_THREADS": get_int("GUNICORN_THREADS", "django_threads"),
            }
        case "worker":
            return {
                "CELERY_WORKER_CONCURRENCY": get_int(
                    "CELERY_WORKER_CONCURRENCY",
                    "celery_worker_concurrency",
                ),
            }
        case "signer":
            return {
                "SIGNER_GUNICORN_WORKERS": get_int(
                    "SIGNER_GUNICORN_WORKERS",
                    "signer_workers",
                ),
            }
    raise ImproperlyConfigured("shell-env target must be one of: web, worker, signer")


def print_shell_env(target: str) -> None:
    for key, value in shell_env(target).items():
        sys.stdout.write(f"export {key}={shlex.quote(str(value))}\n")


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) == 2 and args[0] == "shell-env":
        print_shell_env(args[1])
        return 0
    sys.stderr.write(
        "Usage: python config/performance.py shell-env <web|worker|signer>\n"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
