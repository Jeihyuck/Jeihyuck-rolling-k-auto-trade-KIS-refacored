#!/usr/bin/env bash
set -euo pipefail
# Set scope before any Python helper import; US jobs must never load KR providers.
export MARKET_SCOPE="us"
export TRADING_MARKET="us"
export DISABLE_KR_IMPORTS_IN_US="1"
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
export WSL_RUN_MARKET="US"
export MARKET="US"
export WSL_RUN_SESSION="afternoon"
export PB1_SESSION="afternoon"
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

SESSION_NAME="afternoon"
LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"
LOG_FILE="runtime/wsl-us-${SESSION_NAME}.log"
TRADE_DATE="${US_TRADE_DATE:-$(date -u +%F)}"
PYTHON_BIN="${PYTHON_BIN:-python}"
if [[ -x .venv/bin/python ]]; then PYTHON_BIN=.venv/bin/python; fi
LOCK_RESULT="$(${PYTHON_BIN} -m trader.us.session_lock --market us --session "${SESSION_NAME}" --trade-date "${TRADE_DATE}" --min-interval-sec 60 2>/dev/null)" || lock_rc=$?
lock_rc="${lock_rc:-0}"
if [[ "${lock_rc}" == "10" ]]; then
  echo "[$(date -Is)] [US_SCHEDULER][DUPLICATE_BLOCKED] session=${SESSION_NAME} trade_date=${TRADE_DATE} result=${LOCK_RESULT}" >> "${LOG_FILE}"
  exit 0
elif [[ "${lock_rc}" != "0" ]]; then
  echo "[$(date -Is)] [US_SCHEDULER][LOCK_HELPER_WARN] session=${SESSION_NAME} trade_date=${TRADE_DATE} rc=${lock_rc}" >> "${LOG_FILE}"
fi
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[$(date -Is)] [US_SCHEDULER][DUPLICATE_BLOCKED] reason=already_running [US_WSL_LOCK][SKIP_DUPLICATE] session=${SESSION_NAME} lock=${LOCK_FILE}" >> "${LOG_FILE}"
  exit 0
fi
echo "[$(date -Is)] [US_WSL_LOCK][ACQUIRED] session=${SESSION_NAME} lock=${LOCK_FILE}" >> "${LOG_FILE}"
cleanup() {
  exit_code=$?
  echo "[$(date -Is)] [US_WSL_LOCK][RELEASED] session=${SESSION_NAME} lock=${LOCK_FILE} exit_code=${exit_code}" >> "${LOG_FILE}"
}
trap cleanup EXIT

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export MARKET="US"
export EXCHANGE="US"
export PB1_MARKET_SCOPE="US"
export WSL_RUN_SOURCE="local-wsl"
export US_RUN_SOURCE="${US_RUN_SOURCE:-WINDOWS_SCHEDULE}"
export WSL_RUN_MARKET="US"
export WSL_RUN_SESSION="afternoon"

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
export US_MAX_ORDER_USD="${US_MAX_ORDER_USD:-3500}"
export US_MAX_DAILY_NOTIONAL_USD="${US_MAX_DAILY_NOTIONAL_USD:-30000}"
export US_MAX_POSITIONS="${US_MAX_POSITIONS:-35}"
export US_TREAT_MAX_POSITIONS_AS_OK="${US_TREAT_MAX_POSITIONS_AS_OK:-1}"
export US_EARLY_BLOCK_ENTRY_WHEN_MAX_POSITIONS="${US_EARLY_BLOCK_ENTRY_WHEN_MAX_POSITIONS:-0}"
export US_SESSION_INTERVAL_SEC="${US_SESSION_INTERVAL_SEC:-300}"
export US_WATCHLIST_LOAD_TIMEOUT_SEC="${US_WATCHLIST_LOAD_TIMEOUT_SEC:-20}"
export US_ENTRY_EVAL_TIMEOUT_SEC="${US_ENTRY_EVAL_TIMEOUT_SEC:-120}"
export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-150}"

# US account capital / capital deployment
export US_PAPER_MAX_CAPITAL_KRW="${US_PAPER_MAX_CAPITAL_KRW:-300000000}"
export US_EXPECTED_PRACTICE_CAPITAL_KRW="${US_EXPECTED_PRACTICE_CAPITAL_KRW:-300000000}"
export US_BUDGET_FX_KRW_PER_USD="${US_BUDGET_FX_KRW_PER_USD:-1450}"
export US_TARGET_POSITION_WEIGHT="${US_TARGET_POSITION_WEIGHT:-0.025}"
export US_MAX_POSITION_WEIGHT="${US_MAX_POSITION_WEIGHT:-0.05}"
export US_TARGET_EXPOSURE_PCT="${US_TARGET_EXPOSURE_PCT:-0.70}"
export US_MAX_EXPOSURE_PCT="${US_MAX_EXPOSURE_PCT:-0.85}"
export US_MIN_CASH_BUFFER_PCT="${US_MIN_CASH_BUFFER_PCT:-0.15}"
export US_CAPITAL_DEPLOYMENT_MODE="${US_CAPITAL_DEPLOYMENT_MODE:-TARGET_WEIGHT_TOPUP}"
export US_ALLOW_ADD_TO_EXISTING="${US_ALLOW_ADD_TO_EXISTING:-1}"
export US_ALLOW_AVERAGING_DOWN="${US_ALLOW_AVERAGING_DOWN:-0}"
export US_ADD_MIN_PNL_PCT="${US_ADD_MIN_PNL_PCT:-0.02}"
export US_MAX_ADD_COUNT_PER_SYMBOL="${US_MAX_ADD_COUNT_PER_SYMBOL:-2}"
export US_FULL_POSITION_ALLOW_ADD_TO_EXISTING="${US_FULL_POSITION_ALLOW_ADD_TO_EXISTING:-1}"
export US_FULL_POSITION_BLOCK_NEW_SYMBOLS="${US_FULL_POSITION_BLOCK_NEW_SYMBOLS:-1}"

export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"
export KIS_BALANCE_SNAPSHOT_TTL_SEC="${KIS_BALANCE_SNAPSHOT_TTL_SEC:-180}"
export KIS_BALANCE_BREAKER_COOLDOWN_SEC="${KIS_BALANCE_BREAKER_COOLDOWN_SEC:-30}"
export KIS_BALANCE_MAX_RETRIES="${KIS_BALANCE_MAX_RETRIES:-2}"
export KIS_BALANCE_TIMEOUT_SEC="${KIS_BALANCE_TIMEOUT_SEC:-5}"

max_minutes="${US_AFTERNOON_MAX_MINUTES:-185}"
interval_sec="${US_SESSION_INTERVAL_SEC:-300}"
run_mode="${US_RUN_MODE:-TRADE}"
cmd=("${PYTHON_BIN}" -m trader.us.runner.trade_session_runner --session afternoon --env practice --max-minutes "${max_minutes}" --interval-sec "${interval_sec}" --run-mode "${run_mode}")
if [[ "${US_SIGNAL_ONLY:-0}" == "1" ]]; then
  cmd+=(--signal-only)
fi
if [[ -n "${US_FORCE_NOW:-}" ]]; then
  cmd+=(--force-now "${US_FORCE_NOW}")
fi
if [[ -n "${US_MAX_TICKS:-}" && "${US_MAX_TICKS}" != "0" ]]; then
  cmd+=(--max-ticks "${US_MAX_TICKS}")
fi
if [[ "${US_OFFLINE:-0}" == "1" ]]; then
  cmd+=(--offline)
fi

"${cmd[@]}" >> "$LOG_FILE" 2>&1
