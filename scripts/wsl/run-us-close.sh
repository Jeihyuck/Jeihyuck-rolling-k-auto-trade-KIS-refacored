#!/usr/bin/env bash
set -euo pipefail
# Set scope before any Python helper import; US jobs must never load KR providers.
export MARKET_SCOPE="us"
export TRADING_MARKET="us"
export DISABLE_KR_IMPORTS_IN_US="1"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log US close "close" "${BASH_SOURCE[0]}"
set +e
nullim_require_trading_day us "$NULLIM_TRADE_DATE"
trading_day_rc=$?
set -e
[[ "$trading_day_rc" == 10 ]] && exit 0
[[ "$trading_day_rc" == 0 ]] || exit "$trading_day_rc"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
export WSL_RUN_MARKET="US"
export MARKET="US"
export WSL_RUN_SESSION="close"
export PB1_SESSION="close"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime runtime/locks

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi
nullim_reassert_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"

# .env may carry Korean defaults; pin the process back to US before lock helpers run.
export MARKET_SCOPE="us"
export TRADING_MARKET="us"
export DISABLE_KR_IMPORTS_IN_US="1"

SESSION_NAME="close"
LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"
LOG_FILE="$NULLIM_SESSION_LOG"
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
  nullim_finish_session_log "$exit_code" || true
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export MARKET="US"
export EXCHANGE="US"
export PB1_MARKET_SCOPE="US"
export WSL_RUN_SOURCE="local-wsl"
export US_RUN_SOURCE="${US_RUN_SOURCE:-WINDOWS_SCHEDULE}"
export WSL_RUN_MARKET="US"
export WSL_RUN_SESSION="close"

export TRADING_REGION="${TRADING_REGION:-US}"
export US_AGENT_ENABLED="${US_AGENT_ENABLED:-1}"
export US_PAPER_TRADING_ENABLED="${US_PAPER_TRADING_ENABLED:-1}"
export LIVE_TRADING_ENABLED="${LIVE_TRADING_ENABLED:-1}"
export US_LIVE_TRADING_ENABLED="${US_LIVE_TRADING_ENABLED:-1}"
export US_ORDER_ARMED="${US_ORDER_ARMED:-1}"
export DRY_RUN="${DRY_RUN:-0}"
export DISABLE_LIVE_TRADING="${DISABLE_LIVE_TRADING:-0}"
export DISABLE_REAL_TRADING="${DISABLE_REAL_TRADING:-0}"
export ALLOW_REAL_ORDER="${ALLOW_REAL_ORDER:-0}"
export US_STRATEGY_ENGINE="${US_STRATEGY_ENGINE:-pb1}"
export US_ENTRY_ENABLED="${US_ENTRY_ENABLED:-1}"
export US_EXIT_ENABLED="${US_EXIT_ENABLED:-1}"
export US_DEFAULT_ENTRY_BOOK="${US_DEFAULT_ENTRY_BOOK:-SWING_BOOK}"
export US_DEFAULT_ENTRY_HORIZON="${US_DEFAULT_ENTRY_HORIZON:-SWING_CARRY}"
export US_DEFAULT_EXIT_POLICY="${US_DEFAULT_EXIT_POLICY:-US_SWING_DEFAULT}"
export US_MAX_ORDER_USD="${US_MAX_ORDER_USD:-2500}"
export US_MAX_DAILY_NOTIONAL_USD="${US_MAX_DAILY_NOTIONAL_USD:-10000}"
export US_MAX_POSITIONS="${US_MAX_POSITIONS:-30}"
export US_TREAT_MAX_POSITIONS_AS_OK="${US_TREAT_MAX_POSITIONS_AS_OK:-1}"
export US_EARLY_BLOCK_ENTRY_WHEN_MAX_POSITIONS="${US_EARLY_BLOCK_ENTRY_WHEN_MAX_POSITIONS:-0}"
export US_SESSION_INTERVAL_SEC="${US_SESSION_INTERVAL_SEC:-300}"
export US_WATCHLIST_LOAD_TIMEOUT_SEC="${US_WATCHLIST_LOAD_TIMEOUT_SEC:-20}"
export US_ENTRY_EVAL_TIMEOUT_SEC="${US_ENTRY_EVAL_TIMEOUT_SEC:-120}"
export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-240}"
export US_TICK_TIMEOUT_MIN_SEC="${US_TICK_TIMEOUT_MIN_SEC:-240}"

export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"
export KIS_BALANCE_SNAPSHOT_TTL_SEC="${KIS_BALANCE_SNAPSHOT_TTL_SEC:-180}"
export KIS_BALANCE_BREAKER_COOLDOWN_SEC="${KIS_BALANCE_BREAKER_COOLDOWN_SEC:-30}"
export KIS_BALANCE_MAX_RETRIES="${KIS_BALANCE_MAX_RETRIES:-2}"
export KIS_BALANCE_TIMEOUT_SEC="${KIS_BALANCE_TIMEOUT_SEC:-5}"

PYTHON_BIN="python"
if [[ -x .venv/bin/python ]]; then PYTHON_BIN=.venv/bin/python; fi
cmd=("${PYTHON_BIN}" -m trader.us.runner.dispatcher --mode trade-close --env practice)
if [[ -n "${US_FORCE_NOW:-}" ]]; then
  cmd+=(--force-now "${US_FORCE_NOW}")
fi
if [[ "${US_OFFLINE:-0}" == "1" ]]; then
  cmd+=(--offline)
fi

"${cmd[@]}" >> "$LOG_FILE" 2>&1
