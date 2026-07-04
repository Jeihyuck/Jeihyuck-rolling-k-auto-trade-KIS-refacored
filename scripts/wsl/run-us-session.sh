#!/usr/bin/env bash
set -euo pipefail

SESSION_NAME="${1:-}"
case "${SESSION_NAME}" in
  prep|am|afternoon|close) ;;
  *) echo "[$(date -Is)] [US_LAUNCHER][ERROR] invalid_session=${SESSION_NAME:-missing}" >&2; exit 2 ;;
esac

APP="${APP:-/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored}"
LOG_DIR="${APP}/runtime/cron"
LOG_FILE="${LOG_DIR}/us-${SESSION_NAME}.log"

fail() {
  local msg="$1"
  mkdir -p "${LOG_DIR}" 2>/dev/null || true
  echo "[$(date -Is)] [US_LAUNCHER][ERROR] session=${SESSION_NAME} ${msg}" | tee -a "${LOG_FILE}" >&2
  exit 1
}

[[ -d "${APP}" ]] || fail "repo_root_missing path=${APP}"
cd "${APP}" || fail "repo_root_cd_failed path=${APP}"
[[ -x ".venv/bin/python" ]] || fail "venv_python_missing path=${APP}/.venv/bin/python"
[[ -f ".env" ]] || fail "env_file_missing path=${APP}/.env"
[[ -x "scripts/wsl/run-us-${SESSION_NAME}.sh" ]] || fail "session_script_missing path=${APP}/scripts/wsl/run-us-${SESSION_NAME}.sh"
mkdir -p "${LOG_DIR}" || fail "log_dir_create_failed path=${LOG_DIR}"
[[ -w "${LOG_DIR}" ]] || fail "log_dir_not_writable path=${LOG_DIR}"

echo "[$(date -Is)] [US_LAUNCHER][START] session=${SESSION_NAME}" >> "${LOG_FILE}"
exec "${APP}/scripts/wsl/run-us-${SESSION_NAME}.sh"
