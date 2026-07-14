#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime runtime/locks

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

SESSION_NAME="prep"
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

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export MARKET="US"
export EXCHANGE="US"
export PB1_MARKET_SCOPE="US"
export WSL_RUN_SOURCE="local-wsl"
export WSL_RUN_MARKET="US"
export WSL_RUN_SESSION="prep"

export TRADING_REGION="${TRADING_REGION:-US}"
export US_AGENT_ENABLED="${US_AGENT_ENABLED:-1}"
export US_PAPER_TRADING_ENABLED="${US_PAPER_TRADING_ENABLED:-1}"
export US_LIVE_TRADING_ENABLED="${US_LIVE_TRADING_ENABLED:-0}"
export DISABLE_REAL_TRADING="${DISABLE_REAL_TRADING:-1}"
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
export US_SESSION_INTERVAL_SEC="${US_SESSION_INTERVAL_SEC:-300}"
export US_WATCHLIST_LOAD_TIMEOUT_SEC="${US_WATCHLIST_LOAD_TIMEOUT_SEC:-20}"
export US_ENTRY_EVAL_TIMEOUT_SEC="${US_ENTRY_EVAL_TIMEOUT_SEC:-120}"
export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-150}"

export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"

PYTHON_BIN="python"
if [[ -x .venv/bin/python ]]; then PYTHON_BIN=.venv/bin/python; fi
cmd=("${PYTHON_BIN}" -m trader.us.runner.dispatcher --mode prep --env practice)
if [[ -n "${US_FORCE_NOW:-}" ]]; then
  cmd+=(--force-now "${US_FORCE_NOW}")
fi
if [[ "${US_OFFLINE:-0}" == "1" ]]; then
  cmd+=(--offline)
fi

"${cmd[@]}" >> "$LOG_FILE" 2>&1
