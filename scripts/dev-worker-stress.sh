#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

ENV_FILE="${ENV_FILE:-.env}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

export DJANGO_SETTINGS_MODULE="${DJANGO_SETTINGS_MODULE:-config.settings.dev}"
export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
export CELERY_STRESS_WORKER_CONCURRENCY="${CELERY_STRESS_WORKER_CONCURRENCY:-8}"

# 压测任务独占队列，避免一次性调度海量 case 时把普通业务 worker 撑满。
exec uv run watchfiles --filter python celery.__main__.main --args "-A config.celery worker -l INFO --pool=threads --concurrency=${CELERY_STRESS_WORKER_CONCURRENCY} -Q stress"
