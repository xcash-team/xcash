#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

ENV_FILE="${ENV_FILE:-.env}"
DC="docker compose --env-file ${ENV_FILE} -f docker-compose.dev.yml"

if [[ -f "${ENV_FILE}" ]]; then
  # 真实联调依赖主应用和 signer 的同一份环境变量快照，避免手工切换时地址不一致。
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

# 端到端联调需要主库、缓存、本地链和独立 signer 全部就绪。
${DC} up -d django-db redis signer-db signer anvil bitcoin

# signer 独立数据库先完成 schema 初始化，再执行主应用对独立 signer 的依赖检查。
ENV_FILE="${ENV_FILE}" /Users/void/PycharmProjects/xcash/scripts/dev-signer-manage.sh migrate
ENV_FILE="${ENV_FILE}" /Users/void/PycharmProjects/xcash/scripts/dev-manage.sh check_signer_service
