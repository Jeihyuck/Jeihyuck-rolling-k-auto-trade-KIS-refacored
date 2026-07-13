#!/usr/bin/env bash
set -euo pipefail
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"; mkdir -p runtime
lock_file="/tmp/nullim-kr-close.lock"
compat_lock_file="runtime/locks/kr-close.lock"
if [[ -f "$compat_lock_file" ]]; then
  exec 8>"$compat_lock_file"
  if ! flock -n 8; then
    echo "[KR_CLOSE][LOCK_SKIP] another instance is already running lock=${compat_lock_file}"
    exit 0
  fi
fi
if [[ "${LOCK_DELEGATED:-0}" == "1" ]]; then
  echo "[KR_CLOSE][LOCK_DELEGATED] external caller owns duplicate prevention lock=${lock_file}"
else
  exec 9>"${lock_file}"
  if ! flock -n 9; then
    echo "[KR_CLOSE][LOCK_SKIP] another instance is already running lock=${lock_file}"
    exit 0
  fi
  echo "[KR_CLOSE][LOCK_ACQUIRED] lock=${lock_file}"
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
export PB1_UNIVERSE_STRATEGY_KEY="${PB1_UNIVERSE_STRATEGY_KEY:-best_k_meta}"
export PB1_CANDIDATE_POOL_STRATEGY_KEY="${PB1_CANDIDATE_POOL_STRATEGY_KEY:-pb1_candidate_pool}"
export KR_INJECT_CANONICAL_FINAL30="${KR_INJECT_CANONICAL_FINAL30:-1}"
export KIS_BALANCE_SNAPSHOT_TTL_SEC="${KIS_BALANCE_SNAPSHOT_TTL_SEC:-180}"
export KIS_BALANCE_TIMEOUT_SEC="${KIS_BALANCE_TIMEOUT_SEC:-5}"
export KIS_BALANCE_MAX_RETRIES="${KIS_BALANCE_MAX_RETRIES:-2}"
export KIS_BALANCE_BREAKER_COOLDOWN_SEC="${KIS_BALANCE_BREAKER_COOLDOWN_SEC:-30}"
export KR_BALANCE_CACHE_MAX_AGE_SEC="${KR_BALANCE_CACHE_MAX_AGE_SEC:-180}"
export KR_ALLOW_BALANCE_CACHE_FOR_ENTRY="${KR_ALLOW_BALANCE_CACHE_FOR_ENTRY:-0}"
export KR_ALLOW_BALANCE_CACHE_FOR_EXIT="${KR_ALLOW_BALANCE_CACHE_FOR_EXIT:-1}"
export KR_ALLOW_BALANCE_CACHE_FOR_CLOSE="${KR_ALLOW_BALANCE_CACHE_FOR_CLOSE:-1}"
export WSL_RUN_SOURCE="local-wsl"
export WSL_RUN_MARKET="KR"

export PB1_SESSION=close WSL_RUN_SESSION=close STRATEGY_MODE=LIVE DRY_RUN=0 DISABLE_LIVE_TRADING=0 LIVE_TRADING_ENABLED=1 KR_LIVE_TRADING_ENABLED=1 KR_ORDER_ARMED=1
export FORCE_PB1_PHASE=close
export FORCE_MARKET_WINDOW=close
export PB1_ENTRY_ENABLED=0
export PB1_EXIT_ENABLED=1
export PB1_CLOSE_ENABLED=1
export PB1_CLOSE_LIQUIDATION_ENABLED=1
export KR_CLOSE_SESSION=1
TODAY_KST="$(TZ=Asia/Seoul date +%F)"
LOG_DIR="runtime/logs/kr/${TODAY_KST}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/wsl-kr-close.log"
LATEST_LINK="runtime/logs/kr/wsl-kr-close.latest.log"
ln -sfn "${TODAY_KST}/wsl-kr-close.log" "$LATEST_LINK"
{
  echo "[KR_CLOSE][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV session=$PB1_SESSION"
  KR_CLOSE_SESSION_TIMEOUT_SEC="${KR_CLOSE_SESSION_TIMEOUT_SEC:-1800}"
  echo "[KR_CLOSE][EFFECTIVE_ENV] timeout_sec=${KR_CLOSE_SESSION_TIMEOUT_SEC}"
  set +e
  timeout --kill-after=30s "${KR_CLOSE_SESSION_TIMEOUT_SEC}" python -m trader.kr.runner.trade_session_runner --session close --env "$STRATEGY_ENV"
  rc=$?
  set -e
  if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
    LAST_STAGE_FILE="runtime/state/kr/session_last_stage_close.json"
    LAST_STAGE=""
    if [[ -f "$LAST_STAGE_FILE" ]]; then LAST_STAGE=$(python -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8")).get("stage", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true); fi
    echo "[KR_CLOSE][TIMEOUT] timeout_sec=${KR_CLOSE_SESSION_TIMEOUT_SEC} last_stage=${LAST_STAGE}"
    echo "[RUN_SUMMARY][RESULT] market=KR session=close status=FAIL reason=SESSION_TIMEOUT orders_intent=0 orders_ack=0 blocked=0"
  elif [[ "$rc" -ne 0 ]] && tail -n 300 "$LOG_FILE" 2>/dev/null | grep -Eiq 'Kis(Auth|Temporary|TokenRateLimit)Error|EGW00133|1분당 1회|tokenP|AUTH_REFRESH|HTTP 403'; then
    HEALTH_DIR="runtime/health"
    mkdir -p "$HEALTH_DIR"
    printf '{"status":"RETRYABLE_DEGRADED","market":"KR","session":"close","date":"%s","reason":"kis_auth_or_token_temporary","exit_code":%s,"ts":"%s"}\n' "${TODAY_KST}" "$rc" "$(date -Is)" > "${HEALTH_DIR}/kr-close-${TODAY_KST}.json"
    echo "[KR_CLOSE][DEGRADED] reason=kis_auth_or_token_temporary original_exit_code=${rc} marker=${HEALTH_DIR}/kr-close-${TODAY_KST}.json"
    echo "[RUN_SUMMARY][RESULT] market=KR session=close status=RETRYABLE_DEGRADED reason=kis_auth_or_token_temporary orders_intent=0 orders_ack=0 blocked=0"
    rc=0
  fi
  echo "[KR_CLOSE][EXIT] ts=$(date -Is) exit_code=$rc"
  exit $rc
} >> "$LOG_FILE" 2>&1
