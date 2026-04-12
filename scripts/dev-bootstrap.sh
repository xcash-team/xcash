#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

ENV_FILE="${ENV_FILE:-.env}"

# 新项目初始化必须先让独立 signer 就绪，再初始化主库和本地链配置。
ENV_FILE="${ENV_FILE}" /Users/void/PycharmProjects/xcash/scripts/dev-signer-e2e.sh
ENV_FILE="${ENV_FILE}" /Users/void/PycharmProjects/xcash/scripts/dev-manage.sh migrate
ENV_FILE="${ENV_FILE}" /Users/void/PycharmProjects/xcash/scripts/dev-manage.sh init_local_chains
ENV_FILE="${ENV_FILE}" /Users/void/PycharmProjects/xcash/scripts/dev-manage.sh prepare_local_bitcoin
