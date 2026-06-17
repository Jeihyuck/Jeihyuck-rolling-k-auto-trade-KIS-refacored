#!/usr/bin/env bash
set -euo pipefail
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"; mkdir -p runtime
if [[ -f .env ]]; then set -a; source .env; set +a; fi
if [[ -f .venv/bin/activate ]]; then source .venv/bin/activate; fi
export TZ=Asia/Seoul
export PYTHONUNBUFFERED=1
export MARKET=KR
export REGION=KR
export TRADING_REGION=KR
export EXCHANGE=KRX
export PB1_MARKET_SCOPE=KRX
export STRATEGY_ENV="${STRATEGY_ENV:-practice}"
export KIS_ENV="${KIS_ENV:-practice}"
export KR_ARTIFACT_STRICT="${KR_ARTIFACT_STRICT:-1}"
export KR_BLOCK_LEGACY_ARTIFACT="${KR_BLOCK_LEGACY_ARTIFACT:-1}"
export KR_QUARANTINE_STALE_ARTIFACT="${KR_QUARANTINE_STALE_ARTIFACT:-1}"
export KR_REQUIRE_CANONICAL_PREP_CONTRACT="${KR_REQUIRE_CANONICAL_PREP_CONTRACT:-1}"
export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
export PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT="${PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT:-1}"
export KIS_BALANCE_TIMEOUT_SEC="${KIS_BALANCE_TIMEOUT_SEC:-5}"
export KIS_BALANCE_MAX_RETRIES="${KIS_BALANCE_MAX_RETRIES:-2}"
export KIS_BALANCE_BREAKER_COOLDOWN_SEC="${KIS_BALANCE_BREAKER_COOLDOWN_SEC:-30}"
export KR_BALANCE_CACHE_MAX_AGE_SEC="${KR_BALANCE_CACHE_MAX_AGE_SEC:-180}"
export KR_ALLOW_BALANCE_CACHE_FOR_ENTRY="${KR_ALLOW_BALANCE_CACHE_FOR_ENTRY:-0}"
export KR_ALLOW_BALANCE_CACHE_FOR_EXIT="${KR_ALLOW_BALANCE_CACHE_FOR_EXIT:-1}"
export KR_ALLOW_BALANCE_CACHE_FOR_CLOSE="${KR_ALLOW_BALANCE_CACHE_FOR_CLOSE:-1}"
export WSL_RUN_SOURCE="local-wsl"
export WSL_RUN_MARKET="KR"

export PB1_SESSION=prep WSL_RUN_SESSION=prep STRATEGY_MODE=PREP DRY_RUN=1 DISABLE_LIVE_TRADING=1 LIVE_TRADING_ENABLED=0
TODAY_KST="$(TZ=Asia/Seoul date +%F)"
LOG_DIR="runtime/logs/kr/${TODAY_KST}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/wsl-kr-prep.log"
LATEST_LINK="runtime/logs/kr/wsl-kr-prep.latest.log"
ln -sfn "${TODAY_KST}/wsl-kr-prep.log" "$LATEST_LINK"
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
} >> "$LOG_FILE" 2>&1
