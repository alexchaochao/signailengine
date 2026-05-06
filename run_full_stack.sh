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
  local metrics_port="$2"
  shift
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

  nohup env SIGNALENGINE_OBSERVABILITY__METRICS_PORT="${metrics_port}" "$@" >>"${log_file}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pid_file}"
  echo "started ${name} pid=${pid} log=${log_file}"
}

start_service worker 9000 python -m core.worker --group signal-workers --consumer worker-full
start_service onchain-feature 9001 python -m core.worker --onchain-feature-live
start_service launch-alpha 9002 python -m core.worker --launch-alpha-live
start_service catalyst-alpha 9003 python -m core.worker --catalyst-alpha-live
start_service flow-measurement 9004 python -m core.worker --flow-measurement-live
start_service social-live 9005 python -m core.worker --social-live
start_service social-confirmation 9006 python -m core.worker --social-confirmation-live --group social-confirmation --consumer social-confirmation-1
start_service telegram-publisher 9007 python -m core.worker --telegram-publisher-live
start_service wallet-intelligence 9008 python -m core.worker --wallet-intelligence-sync
start_service measurement-bridge 9009 python -m core.worker --measurement-bridge

echo "logs: ${LOG_DIR}"