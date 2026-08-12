#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
export WSL_RUN_MARKET="US"
export MARKET="US"
export WSL_RUN_SESSION="dispatcher"
export PB1_SESSION="dispatcher"
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
NULLIM_TRADE_DATE="${NULLIM_TRADE_DATE:-${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}}"
source scripts/wsl/session-lock.sh
set +e
nullim_session_lock_acquire "$LOCK_FILE" US "$SESSION_NAME" "$NULLIM_TRADE_DATE" "$LOG_FILE"
lock_rc=$?
set -e
case "$lock_rc" in
  0) ;;
  75|76)
    export NULLIM_SESSION_FINAL_STATUS=SKIP_DUPLICATE NULLIM_SESSION_FINAL_REASON=SESSION_LOCK_HELD
    exit 0
    ;;
  *)
    echo "[LOCK][ACQUIRE][FAIL] rc=$lock_rc lock=${LOCK_FILE:-${lock_file:-unknown}}" >&2
    exit "$lock_rc"
    ;;
esac
cleanup() {
  exit_code=$?
  trap - EXIT INT TERM
  nullim_session_lock_release || true
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
