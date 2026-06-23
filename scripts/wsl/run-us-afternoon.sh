#!/usr/bin/env bash
set -euo pipefail

cd /home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
mkdir -p runtime runtime/locks

set -a
source .env
set +a

SESSION_NAME="afternoon"
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
export US_MAX_ORDER_USD="${US_MAX_ORDER_USD:-2500}"
export US_MAX_DAILY_NOTIONAL_USD="${US_MAX_DAILY_NOTIONAL_USD:-10000}"
export US_MAX_POSITIONS="${US_MAX_POSITIONS:-30}"
export US_TREAT_MAX_POSITIONS_AS_OK="${US_TREAT_MAX_POSITIONS_AS_OK:-1}"
export US_EARLY_BLOCK_ENTRY_WHEN_MAX_POSITIONS="${US_EARLY_BLOCK_ENTRY_WHEN_MAX_POSITIONS:-0}"
export US_SESSION_INTERVAL_SEC="${US_SESSION_INTERVAL_SEC:-300}"
export US_WATCHLIST_LOAD_TIMEOUT_SEC="${US_WATCHLIST_LOAD_TIMEOUT_SEC:-20}"
export US_ENTRY_EVAL_TIMEOUT_SEC="${US_ENTRY_EVAL_TIMEOUT_SEC:-120}"
export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-150}"

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
cmd=(.venv/bin/python -m trader.us.runner.trade_session_runner --session afternoon --env practice --max-minutes "${max_minutes}" --interval-sec "${interval_sec}" --run-mode "${run_mode}")
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
