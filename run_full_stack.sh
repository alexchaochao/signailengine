#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${ROOT_DIR}/.run/full-stack"
PID_DIR="${LOG_DIR}/pids"

mkdir -p "${LOG_DIR}" "${PID_DIR}"

cd "${ROOT_DIR}"
. .venv/bin/activate
set -a
. ./.env
set +a

start_service() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  local pid_file="${PID_DIR}/${name}.pid"

  if [[ -f "${pid_file}" ]]; then
    local existing_pid
    existing_pid="$(cat "${pid_file}")"
    if kill -0 "${existing_pid}" 2>/dev/null; then
      echo "${name} already running with pid ${existing_pid}"
      return 0
    fi
    rm -f "${pid_file}"
  fi

  nohup "$@" >>"${log_file}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pid_file}"
  echo "started ${name} pid=${pid} log=${log_file}"
}

start_service worker python -m core.worker --group signal-workers --consumer worker-full
start_service onchain-feature python -m core.worker --onchain-feature-live
start_service launch-alpha python -m core.worker --launch-alpha-live
start_service catalyst-alpha python -m core.worker --catalyst-alpha-live
start_service flow-alpha python -m core.worker --flow-alpha-live
start_service telegram-publisher python -m core.worker --telegram-publisher-live
start_service wallet-intelligence python -m core.worker --wallet-intelligence-sync

echo "logs: ${LOG_DIR}"