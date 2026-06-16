#!/usr/bin/env bash
set -euo pipefail
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"; mkdir -p runtime
if [[ -f .env ]]; then set -a; source .env; set +a; fi
if [[ -f .venv/bin/activate ]]; then source .venv/bin/activate; fi
export MARKET=KR REGION=KR TRADING_REGION=KR EXCHANGE=KRX PB1_MARKET_SCOPE=KRX
export STRATEGY_ENV="${STRATEGY_ENV:-practice}" KIS_ENV="${KIS_ENV:-practice}" TZ=Asia/Seoul PYTHONUNBUFFERED=1
export WSL_RUN_SOURCE="local-wsl" WSL_RUN_MARKET="KR"
export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}" DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}" DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"

export PB1_SESSION=prep WSL_RUN_SESSION=prep STRATEGY_MODE=PREP DRY_RUN=1 DISABLE_LIVE_TRADING=1 LIVE_TRADING_ENABLED=0
LOG=runtime/wsl-kr-prep.log
{
NOW_HM=$(TZ=Asia/Seoul date +%H:%M)
if [[ "$NOW_HM" < "06:30" || "$NOW_HM" > "08:50" ]]; then
  echo "[KR_PREP][SCHEDULE_GUARD] now=$NOW_HM allowed=0 reason=OUTSIDE_PREP_WINDOW"
  if [[ "${ALLOW_KR_PREP_OUTSIDE_WINDOW:-0}" != "1" ]]; then
    echo "[KR_PREP][BLOCKED] reason=OUTSIDE_PREP_WINDOW"
    exit 2
  fi
  echo "[KR_PREP][SCHEDULE_GUARD] now=$NOW_HM allowed=1 reason=OUTSIDE_PREP_WINDOW_ALLOWED"
else
  echo "[KR_PREP][SCHEDULE_GUARD] now=$NOW_HM allowed=1 reason=PREP_WINDOW_OK"
fi

  echo "[KR_PREP][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV session=$PB1_SESSION"
  set +e
  python -m trader.kr.runner.trade_session_runner --session prep --env "$STRATEGY_ENV"
  rc=$?
  set -e
  echo "[KR_PREP][EXIT] ts=$(date -Is) exit_code=$rc"
  exit $rc
} >> "$LOG" 2>&1
