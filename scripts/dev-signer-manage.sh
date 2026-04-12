#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

ENV_FILE="${ENV_FILE:-.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # 独立 signer 也统一走同一套环境加载入口，避免宿主机 shell 差异导致配置漂移。
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

# 独立 signer 进程必须强制绑定 signer 自己的 settings，并使用 signer 独立依赖集。
export DJANGO_SETTINGS_MODULE="config.settings"
export SIGNER_POSTGRES_HOST="${SIGNER_POSTGRES_HOST:-127.0.0.1}"
export SIGNER_POSTGRES_PORT="${SIGNER_POSTGRES_PORT:-5433}"
export SIGNER_REDIS_HOST="${SIGNER_REDIS_HOST:-127.0.0.1}"
export SIGNER_REDIS_PORT="${SIGNER_REDIS_PORT:-6379}"

cd signer

exec uv run --project . python manage.py "$@"
