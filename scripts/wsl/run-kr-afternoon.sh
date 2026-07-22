#!/usr/bin/env bash
set -euo pipefail
APP_DIR="${NULLIM_APP_DIR:-/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored}"
cd "$APP_DIR"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"; mkdir -p runtime runtime/locks
export WSL_RUN_SOURCE="${WSL_RUN_SOURCE:-local-wsl}"
export WSL_RUN_MARKET="KR"
source scripts/wsl/kr-session-lock.sh
lock_file="runtime/locks/kr-afternoon.lock"
if [[ "${LOCK_DELEGATED:-0}" == "1" ]]; then
  echo "[KR_AFTERNOON][LOCK_DELEGATED] external caller owns duplicate prevention lock=${lock_file}"
else
  exec 9>"${lock_file}"
  if ! flock -n 9; then
    kr_duplicate_result "${lock_file:-$LOCK_FILE}" "afternoon"
    exit 0
  fi
  kr_lock_owner "$lock_file" "afternoon"
  echo "[KR_AFTERNOON][LOCK_ACQUIRED] lock=${lock_file}"
fi
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
export PB1_FINAL30_STRATEGY_KEY="${PB1_FINAL30_STRATEGY_KEY:-pb1_watchlist_final_scored}"

export KR_PB1_VOL_MAX="${KR_PB1_VOL_MAX:-1.25}"
export KR_PB1_VOLU_MAX="${KR_PB1_VOLU_MAX:-1.25}"
export KR_PB1_VOLU_MAX_INTRADAY="${KR_PB1_VOLU_MAX_INTRADAY:-1.25}"
export PB1_VOL_MAX="${PB1_VOL_MAX:-$KR_PB1_VOL_MAX}"
export PB1_VOLU_MAX="${PB1_VOLU_MAX:-$KR_PB1_VOLU_MAX}"
export PB1_VOLU_MAX_INTRADAY="${PB1_VOLU_MAX_INTRADAY:-$KR_PB1_VOLU_MAX_INTRADAY}"
export PB1_UNIVERSE_STRATEGY_KEY="${PB1_UNIVERSE_STRATEGY_KEY:-best_k_meta}"
export PB1_CANDIDATE_POOL_STRATEGY_KEY="${PB1_CANDIDATE_POOL_STRATEGY_KEY:-pb1_candidate_pool}"
export KR_INJECT_CANONICAL_FINAL30="${KR_INJECT_CANONICAL_FINAL30:-1}"
export KIS_BALANCE_SNAPSHOT_TTL_SEC="${KIS_BALANCE_SNAPSHOT_TTL_SEC:-180}"
export KIS_BALANCE_TIMEOUT_SEC="${KIS_BALANCE_TIMEOUT_SEC:-5}"
export KIS_BALANCE_MAX_RETRIES="${KIS_BALANCE_MAX_RETRIES:-2}"
export KIS_BALANCE_BREAKER_COOLDOWN_SEC="${KIS_BALANCE_BREAKER_COOLDOWN_SEC:-30}"
export KR_BALANCE_CACHE_MAX_AGE_SEC="${KR_BALANCE_CACHE_MAX_AGE_SEC:-180}"
export KR_ALLOW_BALANCE_CACHE_FOR_ENTRY="${KR_ALLOW_BALANCE_CACHE_FOR_ENTRY:-1}"
export KR_ALLOW_BALANCE_CACHE_FOR_EXIT="${KR_ALLOW_BALANCE_CACHE_FOR_EXIT:-1}"
export KR_ALLOW_BALANCE_CACHE_FOR_CLOSE="${KR_ALLOW_BALANCE_CACHE_FOR_CLOSE:-1}"
export PB1_ENTRY_PLAN_FAIL_OPEN="${PB1_ENTRY_PLAN_FAIL_OPEN:-1}"
export PB1_PRACTICE_FORCE_MIN_TRADE="${PB1_PRACTICE_FORCE_MIN_TRADE:-0}"
export PB1_FORCE_MIN_ONE_ORDER_PRACTICE="${PB1_FORCE_MIN_ONE_ORDER_PRACTICE:-0}"
export PB1_PRACTICE_ATR_MAX_PCT_FALLBACK="${PB1_PRACTICE_ATR_MAX_PCT_FALLBACK:-14}"
export PB1_PRICE_FALLBACK_TO_FINAL30_CLOSE="${PB1_PRICE_FALLBACK_TO_FINAL30_CLOSE:-1}"


export PB1_LOOP_ENABLED="${PB1_LOOP_ENABLED:-1}"
export PB1_RUN_LOOP="${PB1_RUN_LOOP:-1}"
export PB1_LOOP_INTERVAL_SEC="${PB1_LOOP_INTERVAL_SEC:-60}"
export PM_TARGET_START_TIME="${PM_TARGET_START_TIME:-13:00}"
export PM_SESSION_END="${PM_SESSION_END:-15:10}"
export PB1_SESSION_EXIT_GRACE_SEC="${PB1_SESSION_EXIT_GRACE_SEC:-15}"
export PB1_TICK_HARD_TIMEOUT_SEC="${PB1_TICK_HARD_TIMEOUT_SEC:-900}"
export KR_AFTERNOON_TIMEOUT_SEC="${KR_AFTERNOON_TIMEOUT_SEC:-9000}"
echo "[KR_AFTERNOON][PB1 loop enabled] PB1_LOOP_ENABLED=${PB1_LOOP_ENABLED} PB1_RUN_LOOP=${PB1_RUN_LOOP} PB1_LOOP_INTERVAL_SEC=${PB1_LOOP_INTERVAL_SEC} PM_TARGET_START_TIME=${PM_TARGET_START_TIME} PM_SESSION_END=${PM_SESSION_END} PB1_SESSION_EXIT_GRACE_SEC=${PB1_SESSION_EXIT_GRACE_SEC} PB1_TICK_HARD_TIMEOUT_SEC=${PB1_TICK_HARD_TIMEOUT_SEC} KR_AFTERNOON_TIMEOUT_SEC=${KR_AFTERNOON_TIMEOUT_SEC}"

export PB1_SESSION=afternoon WSL_RUN_SESSION=afternoon STRATEGY_MODE=LIVE DRY_RUN=0 DISABLE_LIVE_TRADING=0 LIVE_TRADING_ENABLED=1 KR_LIVE_TRADING_ENABLED=1 KR_ORDER_ARMED=1
echo "[KR_FORCE_TRADE_ENV] fail_open=${PB1_ENTRY_PLAN_FAIL_OPEN} force_min_trade=${PB1_PRACTICE_FORCE_MIN_TRADE} atr_fallback=${PB1_PRACTICE_ATR_MAX_PCT_FALLBACK} dry_run=${DRY_RUN} live=${LIVE_TRADING_ENABLED} armed=${KR_ORDER_ARMED}"
if [[ "${DRY_RUN:-0}" == "1" || "${KR_ORDER_ARMED:-0}" == "0" ]]; then echo "[KR_FORCE_TRADE_ENV][BLOCK] reason=DRY_RUN_OR_NOT_ARMED"; fi
TODAY_KST="$(TZ=Asia/Seoul date +%F)"
LOG_DIR="runtime/logs/kr/${TODAY_KST}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/wsl-kr-afternoon.log"
LATEST_LINK="runtime/logs/kr/wsl-kr-afternoon.latest.log"
ln -sfn "${TODAY_KST}/wsl-kr-afternoon.log" "$LATEST_LINK"
{
  echo "[KR_AFTERNOON][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV session=$PB1_SESSION"
  KR_AFTERNOON_TIMEOUT_SEC="${KR_AFTERNOON_TIMEOUT_SEC:-7200}"
  echo "[KR_AFTERNOON][EFFECTIVE_ENV] timeout_sec=${KR_AFTERNOON_TIMEOUT_SEC}"
  set +e
  timeout --kill-after=30s "${KR_AFTERNOON_TIMEOUT_SEC}" python -m trader.kr.runner.trade_session_runner --session afternoon --env "$STRATEGY_ENV"
  rc=$?
  set -e
  if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
    LAST_STAGE_FILE="runtime/state/kr/session_last_stage_afternoon.json"
    LAST_STAGE=""
    if [[ -f "$LAST_STAGE_FILE" ]]; then LAST_STAGE=$(python -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8")).get("stage", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true); fi
    echo "[KR_AFTERNOON][TIMEOUT] timeout_sec=${KR_AFTERNOON_TIMEOUT_SEC} last_stage=${LAST_STAGE}"
    echo "[RUN_SUMMARY][RESULT] market=KR session=afternoon status=FAIL reason=SESSION_TIMEOUT orders_intent=0 orders_ack=0 blocked=0"
  fi
  echo "[KR_AFTERNOON][EXIT] ts=$(date -Is) exit_code=$rc"
  exit $rc
} >> "$LOG_FILE" 2>&1
