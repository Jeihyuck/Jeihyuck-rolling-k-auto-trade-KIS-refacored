#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
source "$SCRIPT_DIR/kr-infinite-sidecar.sh"
source "$SCRIPT_DIR/kr-close-failure-classifier.sh"
nullim_init_session_log KR close "close" "${BASH_SOURCE[0]}"
# The KR calendar is advisory only; Windows Task Scheduler owns execution timing.
set +e
nullim_require_trading_day kr "$NULLIM_TRADE_DATE"
trading_day_rc=$?
set -e
if [[ "$trading_day_rc" != 0 ]]; then
  echo "[KR_SESSION][CALENDAR_WARN] rc=$trading_day_rc action=continue"
fi
unset NULLIM_SESSION_FINAL_STATUS NULLIM_SESSION_FINAL_REASON
source "$SCRIPT_DIR/nullim-repo-root.sh"
nullim_resolve_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
NULLIM_WRAPPER="${BASH_SOURCE[0]}"
export WSL_RUN_MARKET="KR"
export MARKET="KR"
export WSL_RUN_SESSION="close"
export PB1_SESSION="close"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
REPO="$APP_DIR"
cd "$REPO"; mkdir -p runtime runtime/locks
export WSL_RUN_SOURCE="${WSL_RUN_SOURCE:-local-wsl}"
export WSL_RUN_MARKET="KR"
source scripts/wsl/kr-session-lock.sh
lock_file="runtime/locks/kr-close.lock"
if [[ "${LOCK_DELEGATED:-0}" == "1" ]]; then
  echo "[KR_CLOSE][LOCK_DELEGATED] external caller owns duplicate prevention lock=${lock_file}"
else
  source scripts/wsl/session-lock.sh
  set +e
  nullim_session_lock_acquire "${lock_file}" KR "close" "$NULLIM_TRADE_DATE" "$NULLIM_SESSION_LOG"
  lock_rc=$?
  set -e
  case "$lock_rc" in
    0) ;;
    75|76)
      export NULLIM_SESSION_FINAL_STATUS=SKIP_DUPLICATE NULLIM_SESSION_FINAL_REASON=SESSION_LOCK_HELD
      exit 0
      ;;
    *)
      echo "[LOCK][ACQUIRE][FAIL] rc=$lock_rc lock=${LOCK_FILE:-${lock_file:-unknown}}" >&2
      exit "$lock_rc"
      ;;
  esac
fi
if [[ -f .env ]]; then set -a; source .env; set +a; fi
nullim_reassert_repo_root "${BASH_SOURCE[0]}"
APP_DIR="$NULLIM_RESOLVED_REPO_ROOT"
cd "$APP_DIR"
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
# Keep the close runner on the same dedicated PB1 lock policy as AM/afternoon.
export DB_LOCK_CONN_LOCK_TIMEOUT_MS="${DB_LOCK_CONN_LOCK_TIMEOUT_MS:-5000}"
export DB_LOCK_CONN_STATEMENT_TIMEOUT_MS="${DB_LOCK_CONN_STATEMENT_TIMEOUT_MS:-15000}"
export DB_LOCK_CONN_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_LOCK_CONN_IDLE_IN_TX_SESSION_TIMEOUT_MS:-30000}"
# PB1 holds its xact lock while persistence uses separate connections, so the
# general 30s idle timeout must never release this dedicated lock owner.
export DB_XACT_LOCK_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_XACT_LOCK_IDLE_IN_TX_SESSION_TIMEOUT_MS:-0}"
export KR_LOCK_STALE_XACT_SEC="${KR_LOCK_STALE_XACT_SEC:-300}"
export KR_LOCK_TERMINATE_STALE_HOLDER="${KR_LOCK_TERMINATE_STALE_HOLDER:-0}"
export PB1_LOCK_LOG_OWNER_ON_FAIL="${PB1_LOCK_LOG_OWNER_ON_FAIL:-1}"
export LOCK_ACQUIRE_RETRIES="${LOCK_ACQUIRE_RETRIES:-3}"
export LOCK_ACQUIRE_SLEEP_SEC="${LOCK_ACQUIRE_SLEEP_SEC:-0.5}"
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

export PB1_SESSION=close WSL_RUN_SESSION=close STRATEGY_MODE=LIVE DRY_RUN=0 DISABLE_LIVE_TRADING=0 LIVE_TRADING_ENABLED=1 KR_LIVE_TRADING_ENABLED=1 KR_ORDER_ARMED=1
export FORCE_PB1_PHASE=close
export FORCE_MARKET_WINDOW=close
export PB1_ENTRY_ENABLED=0
export PB1_EXIT_ENABLED=1
export PB1_CLOSE_ENABLED=1
export PB1_CLOSE_LIQUIDATION_ENABLED="${PB1_CLOSE_LIQUIDATION_ENABLED:-0}"
export PB1_CLOSE_EXIT_SAFETY_ENGINE="${PB1_CLOSE_EXIT_SAFETY_ENGINE:-1}"
export PB1_EXIT_ONLY_MODE=1
export KR_CLOSE_SESSION=1
TODAY_KST="$(TZ=Asia/Seoul date +%F)"
LOG_DIR="runtime/logs/kr/${TODAY_KST}"
mkdir -p "$LOG_DIR"
LOG_FILE="$NULLIM_SESSION_LOG"
LATEST_LINK="runtime/logs/kr/wsl-kr-close.latest.log"
ln -sfn "$(realpath --relative-to="$(dirname "$LATEST_LINK")" "$NULLIM_SESSION_LOG")" "$LATEST_LINK"
{
  echo "[KR_CLOSE][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV session=$PB1_SESSION"
  KR_CLOSE_SESSION_TIMEOUT_SEC="${KR_CLOSE_SESSION_TIMEOUT_SEC:-1800}"
  echo "[KR_CLOSE][EFFECTIVE_ENV] timeout_sec=${KR_CLOSE_SESSION_TIMEOUT_SEC}"
  nullim_start_kr_infinite_sidecar "$WSL_RUN_SESSION" "$STRATEGY_ENV"
  set +e
  timeout --kill-after=30s "${KR_CLOSE_SESSION_TIMEOUT_SEC}" python -m trader.kr.runner.trade_session_runner --session close --env "$STRATEGY_ENV"
  rc=$?
  set -e
  if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
    LAST_STAGE_FILE="runtime/state/kr/session_last_stage_close.json"
    LAST_STAGE=""
    if [[ -f "$LAST_STAGE_FILE" ]]; then LAST_STAGE=$(python -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8")).get("stage", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true); fi
    export NULLIM_SESSION_FINAL_STATUS=TIMEOUT NULLIM_SESSION_FINAL_REASON=SESSION_TIMEOUT
    echo "[KR_CLOSE][TIMEOUT] timeout_sec=${KR_CLOSE_SESSION_TIMEOUT_SEC} last_stage=${LAST_STAGE}"
    echo "[RUN_SUMMARY][RESULT] market=KR session=close status=FAIL reason=SESSION_TIMEOUT orders_intent=unknown orders_ack=unknown blocked=0"
  elif [[ "$rc" -ne 0 ]]; then
    HEALTH_DIR="runtime/health"
    mkdir -p "$HEALTH_DIR"
    recent_log="$(tail -n 300 "$LOG_FILE" 2>/dev/null || true)"
    original_rc="$rc"
    IFS=$'\t' read -r reason is_retryable_kis classified_rc classified_status < <(classify_kr_close_failure "$recent_log" "$original_rc")
    if [[ "$is_retryable_kis" -eq 1 ]]; then
      printf '{"status":"RETRYABLE_DEGRADED","completed":0,"retryable":1,"market":"KR","session":"close","date":"%s","reason":"%s","original_exit_code":%s,"exit_code":75,"ts":"%s"}\n' "${TODAY_KST}" "$reason" "$original_rc" "$(date -Is)" > "${HEALTH_DIR}/kr-close-${TODAY_KST}.json"
      echo "[SESSION][TERMINAL] status=RETRYABLE_DEGRADED completed=0 retryable=1 reason=${reason}"
      echo "[RUN_SUMMARY][RESULT] market=KR session=close status=RETRYABLE_DEGRADED reason=${reason} orders_intent=0 orders_ack=0 blocked=0"
      rc="$classified_rc"
    else
      reason="NON_KIS_RUNTIME_FAILURE"
      printf '{"status":"FAIL","completed":0,"retryable":0,"market":"KR","session":"close","date":"%s","reason":"%s","exit_code":%s,"ts":"%s"}\n' "${TODAY_KST}" "$reason" "$original_rc" "$(date -Is)" > "${HEALTH_DIR}/kr-close-${TODAY_KST}.json"
      echo "[SESSION][TERMINAL] status=FAIL completed=0 retryable=0 reason=${reason}"
      echo "[RUN_SUMMARY][RESULT] market=KR session=close status=FAIL reason=${reason} orders_intent=0 orders_ack=0 blocked=0"
      rc="$original_rc"
    fi
  fi
  nullim_wait_kr_infinite_sidecar
  echo "[KR_CLOSE][EXIT] ts=$(date -Is) exit_code=$rc"
  exit $rc
} >> "$LOG_FILE" 2>&1
