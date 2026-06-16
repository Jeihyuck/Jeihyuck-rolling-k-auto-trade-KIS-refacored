#!/usr/bin/env bash
set -euo pipefail

echo "[KR_TRADER][DEPRECATED] this script is not valid for PB1 prep/am/afternoon/close scheduler"
if [ "${ALLOW_LEGACY_KR_TRADER:-0}" != "1" ]; then
  echo "[KR_TRADER][BLOCKED] reason=LEGACY_SCRIPT_NOT_ALLOWED_FOR_SCHEDULER"
  exit 2
fi

echo "[KR_TRADER][WARNING] PB1 scheduler forbidden; legacy execution manually allowed"
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"

mkdir -p runtime

set -a
source .env
set +a

export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"

export MARKET="KR"
export EXCHANGE="KRX"
export PB1_MARKET_SCOPE="KRX"
export WSL_RUN_SOURCE="local-wsl"
export WSL_RUN_MARKET="KR"

export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"

legacy_module="trader.trader"
cmd=(.venv/bin/python -m "$legacy_module" --window auto --phase auto)
"${cmd[@]}" >> runtime/wsl-kr-trader.log 2>&1
