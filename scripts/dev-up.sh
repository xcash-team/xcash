#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

ENV_FILE="${ENV_FILE:-.env}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
child_pids=()

cleanup() {
  local exit_code=$?

  trap - EXIT INT TERM

  if ((${#child_pids[@]} > 0)); then
    for pid in "${child_pids[@]}"; do
      if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}" 2>/dev/null || true
      fi
    done

    wait "${child_pids[@]}" 2>/dev/null || true
  fi

  exit "${exit_code}"
}

wait_for_first_exit() {
  local pid

  while true; do
    for pid in "${child_pids[@]}"; do
      if ! kill -0 "${pid}" 2>/dev/null; then
        wait "${pid}"
        return $?
      fi
    done

    sleep 1
  done
}

trap cleanup EXIT INT TERM

ENV_FILE="${ENV_FILE}" "${SCRIPT_DIR}/dev-web.sh" &
child_pids+=("$!")

ENV_FILE="${ENV_FILE}" "${SCRIPT_DIR}/dev-worker.sh" &
child_pids+=("$!")

ENV_FILE="${ENV_FILE}" "${SCRIPT_DIR}/dev-worker-stress.sh" &
child_pids+=("$!")

ENV_FILE="${ENV_FILE}" "${SCRIPT_DIR}/dev-worker-scan.sh" &
child_pids+=("$!")

ENV_FILE="${ENV_FILE}" "${SCRIPT_DIR}/dev-beat.sh" &
child_pids+=("$!")

wait_for_first_exit
