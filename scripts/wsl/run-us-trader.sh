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

export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"

.venv/bin/python -m trader.trader --window auto --phase auto >> runtime/wsl-us-trader.log 2>&1
