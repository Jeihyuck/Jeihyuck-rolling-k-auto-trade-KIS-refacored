#!/usr/bin/env bash
set -euo pipefail

cd /home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
mkdir -p runtime

set -a
source .env
set +a

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export MARKET="US"
export EXCHANGE="US"
export PB1_MARKET_SCOPE="US"
export WSL_RUN_SOURCE="local-wsl"
export WSL_RUN_MARKET="US"
export WSL_RUN_SESSION="am"

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

max_minutes="${US_AM_MAX_MINUTES:-180}"
interval_sec="${US_SESSION_INTERVAL_SEC:-300}"
run_mode="${US_RUN_MODE:-TRADE}"
cmd=(.venv/bin/python -m trader.us.runner.trade_session_runner --session am --env practice --max-minutes "${max_minutes}" --interval-sec "${interval_sec}" --run-mode "${run_mode}")
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

"${cmd[@]}" >> runtime/wsl-us-am.log 2>&1
