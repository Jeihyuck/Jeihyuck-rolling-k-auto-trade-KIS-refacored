#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
repo_dir="$APP_DIR"
mkdir -p runtime runtime/locks

# Dispatcher wrapper for WSL. Prefer calling the session-specific scripts from
# Windows Task Scheduler; this auto mode is a convenience fallback.
SESSION_NAME="trader"
LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"
LOG_FILE="runtime/wsl-us-${SESSION_NAME}.log"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[$(date -Is)] [US_WSL_LOCK][SKIP_DUPLICATE] session=${SESSION_NAME} lock=${LOCK_FILE}" >> "${LOG_FILE}"
  exit 0
fi
echo "[$(date -Is)] [US_WSL_LOCK][ACQUIRED] session=${SESSION_NAME} lock=${LOCK_FILE}" >> "${LOG_FILE}"
cleanup() {
  exit_code=$?
  echo "[$(date -Is)] [US_WSL_LOCK][RELEASED] session=${SESSION_NAME} lock=${LOCK_FILE} exit_code=${exit_code}" >> "${LOG_FILE}"
}
trap cleanup EXIT

session="${1:-auto}"
if [[ "${session}" == "auto" ]]; then
  et_hhmm="$(TZ=America/New_York date +%H%M)"
  case "${et_hhmm}" in
    05*|06*|07*) session="prep" ;;
    09*|10*|11*|12[0-2]*) session="am" ;;
    12[3-5]*|13*|14*|15[0-4]*) session="afternoon" ;;
    15[5-9]*|16*) session="close" ;;
    *)
      echo "[WSL_US_TRADER][SKIP] no US session for et_hhmm=${et_hhmm}" >> "$LOG_FILE"
      exit 0
      ;;
  esac
fi

echo "[WSL_US_TRADER][DISPATCH] session=${session}" >> "$LOG_FILE"
case "${session}" in
  prep)
    "${repo_dir}/scripts/wsl/run-us-prep.sh"
    exit $?
    ;;
  am|trade-am|session-am)
    "${repo_dir}/scripts/wsl/run-us-am.sh"
    exit $?
    ;;
  afternoon|trade-afternoon|session-afternoon)
    "${repo_dir}/scripts/wsl/run-us-afternoon.sh"
    exit $?
    ;;
  close|trade-close)
    "${repo_dir}/scripts/wsl/run-us-close.sh"
    exit $?
    ;;
  *)
    echo "[WSL_US_TRADER][ERROR] unknown session=${session}" >> "$LOG_FILE"
    exit 2
    ;;
esac
