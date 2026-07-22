#!/usr/bin/env bash
set -euo pipefail
APP_DIR="${NULLIM_APP_DIR:-/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored}"
cd "$APP_DIR"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
export KR_PB1_VOL_MAX="${KR_PB1_VOL_MAX:-1.25}"
export KR_PB1_VOLU_MAX="${KR_PB1_VOLU_MAX:-1.25}"
export KR_PB1_VOLU_MAX_INTRADAY="${KR_PB1_VOLU_MAX_INTRADAY:-1.25}"
export PB1_VOL_MAX="${PB1_VOL_MAX:-$KR_PB1_VOL_MAX}"
export PB1_VOLU_MAX="${PB1_VOLU_MAX:-$KR_PB1_VOLU_MAX}"
export PB1_VOLU_MAX_INTRADAY="${PB1_VOLU_MAX_INTRADAY:-$KR_PB1_VOLU_MAX_INTRADAY}"
export KR_PREP_AUX_WATCHLIST_TIMEOUT_SEC="${KR_PREP_AUX_WATCHLIST_TIMEOUT_SEC:-20}"
export KR_PREP_AUX_REPORT_TIMEOUT_SEC="${KR_PREP_AUX_REPORT_TIMEOUT_SEC:-30}"
export KR_PREP_AUX_DEFAULT_TIMEOUT_SEC="${KR_PREP_AUX_DEFAULT_TIMEOUT_SEC:-20}"
REPO="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
if [[ ! -d "$REPO" ]]; then REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; fi
cd "$REPO"; mkdir -p runtime runtime/locks
export WSL_RUN_SOURCE="${WSL_RUN_SOURCE:-local-wsl}"
export WSL_RUN_MARKET="KR"
source scripts/wsl/kr-session-lock.sh
LOCK_FILE="runtime/locks/kr-prep.lock"
if [[ "${LOCK_DELEGATED:-0}" == "1" ]]; then
  echo "[KR_PREP][LOCK_DELEGATED] external caller owns duplicate prevention lock=${LOCK_FILE}"
else
  exec 9>"${LOCK_FILE}"
  if ! flock -n 9; then
    kr_duplicate_result "${lock_file:-$LOCK_FILE}" "prep"
    exit 0
  fi
  kr_lock_owner "$LOCK_FILE" "prep"
  echo "[KR_PREP][LOCK_ACQUIRED] lock=${LOCK_FILE}"
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
export KIS_BALANCE_TIMEOUT_SEC="${KIS_BALANCE_TIMEOUT_SEC:-5}"
export KIS_BALANCE_MAX_RETRIES="${KIS_BALANCE_MAX_RETRIES:-2}"
export KIS_BALANCE_BREAKER_COOLDOWN_SEC="${KIS_BALANCE_BREAKER_COOLDOWN_SEC:-30}"
export KR_BALANCE_CACHE_MAX_AGE_SEC="${KR_BALANCE_CACHE_MAX_AGE_SEC:-180}"
export KR_ALLOW_BALANCE_CACHE_FOR_ENTRY="${KR_ALLOW_BALANCE_CACHE_FOR_ENTRY:-0}"
export KR_ALLOW_BALANCE_CACHE_FOR_EXIT="${KR_ALLOW_BALANCE_CACHE_FOR_EXIT:-1}"
export KR_ALLOW_BALANCE_CACHE_FOR_CLOSE="${KR_ALLOW_BALANCE_CACHE_FOR_CLOSE:-1}"

export PB1_SESSION=prep WSL_RUN_SESSION=prep STRATEGY_MODE=PREP DRY_RUN=1 DISABLE_LIVE_TRADING=1 LIVE_TRADING_ENABLED=0
TODAY_KST="$(TZ=Asia/Seoul date +%F)"
LOG_DIR="runtime/logs/kr/${TODAY_KST}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/wsl-kr-prep.log"
LATEST_LINK="runtime/logs/kr/wsl-kr-prep.latest.log"
ln -sfn "${TODAY_KST}/wsl-kr-prep.log" "$LATEST_LINK"
{
NOW_HM="${NOW_HM:-$(TZ=Asia/Seoul date +%H:%M)}"
if [[ "$NOW_HM" < "06:30" || "$NOW_HM" > "08:50" ]]; then
  echo "[KR_PREP][SCHEDULE_GUARD][SHELL] now=$NOW_HM allowed=0 reason=OUTSIDE_PREP_WINDOW window=06:30-08:50"
  if [[ "${ALLOW_KR_PREP_OUTSIDE_WINDOW:-0}" != "1" ]]; then
    echo "[KR_PREP][BLOCKED] reason=OUTSIDE_PREP_WINDOW action=shell_block_no_python"
    exit 2
  fi
  echo "[KR_PREP][SCHEDULE_GUARD][SHELL] now=$NOW_HM allowed=1 reason=OUTSIDE_PREP_WINDOW_ALLOWED window=06:30-08:50"
else
  echo "[KR_PREP][SCHEDULE_GUARD][SHELL] now=$NOW_HM allowed=1 reason=PREP_WINDOW_OK window=06:30-08:50"
fi

  echo "[KR_PREP][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV session=$PB1_SESSION"
  KR_PREP_TIMEOUT_SEC="${KR_PREP_TIMEOUT_SEC:-10800}"
  echo "[KR_PREP][EFFECTIVE_ENV] timeout_sec=${KR_PREP_TIMEOUT_SEC} allow_outside_window=${ALLOW_KR_PREP_OUTSIDE_WINDOW:-0} artifact_strict=${KR_ARTIFACT_STRICT:-1} require_contract=${KR_REQUIRE_CANONICAL_PREP_CONTRACT:-1}"
  set +e
  (
    # The parent shell keeps the session lock. Close the lock fd before exec'ing
    # timeout/python so a hung child cannot keep runtime/locks/kr-prep.lock busy
    # after the wrapper exits or is killed.
    exec 9>&-
    timeout --kill-after=60s "${KR_PREP_TIMEOUT_SEC}" \
      python -m trader.kr.runner.trade_session_runner --session prep --env "$STRATEGY_ENV"
  )
  rc=$?
  set -e
  if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
    LAST_STAGE_FILE="runtime/state/kr/prep_last_stage.json"
    LAST_STAGE=""; LAST_STAGE_TS=""; LAST_STAGE_ASOF=""; LAST_STAGE_ROWS=""; LAST_STAGE_PID=""; LAST_STAGE_ELAPSED=""; CORE_DONE="0"; ARTIFACT_SAVED="0"
    if [[ -f "$LAST_STAGE_FILE" ]]; then
      LAST_STAGE=$(python -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p,encoding="utf-8")); print(d.get("stage", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true)
      LAST_STAGE_TS=$(python -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p,encoding="utf-8")); print(d.get("updated_at", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true)
      LAST_STAGE_ASOF=$(python -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p,encoding="utf-8")); print(d.get("expected_as_of", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true)
      LAST_STAGE_ROWS=$(python -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p,encoding="utf-8")); print(d.get("final30_rows", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true)
      LAST_STAGE_PID=$(python -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p,encoding="utf-8")); print(d.get("pid", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true)
      LAST_STAGE_ELAPSED=$(python -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p,encoding="utf-8")); print(d.get("elapsed_sec_from_start", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true)
      [[ "$LAST_STAGE" == "core_done" ]] && CORE_DONE="1" || true
      [[ "$LAST_STAGE" == "core_artifact_write_saved" || "$LAST_STAGE" == "core_artifact_files_verify_done" || "$LAST_STAGE" == "core_done" ]] && ARTIFACT_SAVED="1" || true
    fi
    PREP_DONE_EXISTS=0; LATEST_CONTRACT_EXISTS=0; RUNTIME_CONTRACT_EXISTS=0
    [[ -f "runtime/kr/watchlist/${TODAY_KST}/prep_done.json" ]] && PREP_DONE_EXISTS=1 || true
    [[ -f "signals/kr/latest_prep_contract.json" ]] && LATEST_CONTRACT_EXISTS=1 || true
    [[ -f "runtime/kr/watchlist/${TODAY_KST}/prep_contract.json" ]] && RUNTIME_CONTRACT_EXISTS=1 || true
    echo "[KR_PREP][TIMEOUT] timeout_sec=${KR_PREP_TIMEOUT_SEC} last_stage=${LAST_STAGE} last_stage_ts=${LAST_STAGE_TS} expected_as_of=${LAST_STAGE_ASOF} final30_rows=${LAST_STAGE_ROWS} pid=${LAST_STAGE_PID} elapsed_sec=${LAST_STAGE_ELAPSED} core_done=${CORE_DONE} artifact_saved=${ARTIFACT_SAVED} prep_done_exists=${PREP_DONE_EXISTS} latest_contract_exists=${LATEST_CONTRACT_EXISTS}"
    echo "[KR_PREP][TIMEOUT][ARTIFACT_STATE] runtime_contract=${RUNTIME_CONTRACT_EXISTS} latest_contract=${LATEST_CONTRACT_EXISTS} prep_done=${PREP_DONE_EXISTS} db_final30_hint=${LAST_STAGE_ROWS}"
  fi
  echo "[KR_PREP][EXIT] ts=$(date -Is) exit_code=$rc"
  exit $rc
} >> "$LOG_FILE" 2>&1
