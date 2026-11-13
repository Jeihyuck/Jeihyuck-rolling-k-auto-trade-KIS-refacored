#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log KR am "am" "${BASH_SOURCE[0]}"
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
export WSL_RUN_SESSION="am"
export PB1_SESSION="am"
source scripts/wsl/deploy-preflight.sh
deploy_preflight
if [[ "${NULLIM_PREFLIGHT_ONLY:-0}" == "1" ]]; then exit 0; fi
REPO="$APP_DIR"
cd "$REPO"; mkdir -p runtime runtime/locks
export WSL_RUN_SOURCE="${WSL_RUN_SOURCE:-local-wsl}"
export WSL_RUN_MARKET="KR"
source scripts/wsl/kr-session-lock.sh
lock_file="runtime/locks/kr-am.lock"
if [[ "${LOCK_DELEGATED:-0}" == "1" ]]; then
  echo "[KR_AM][LOCK_DELEGATED] external caller owns duplicate prevention lock=${lock_file}"
else
  source scripts/wsl/session-lock.sh
  set +e
  nullim_session_lock_acquire "${lock_file}" KR "am" "$NULLIM_TRADE_DATE" "$NULLIM_SESSION_LOG"
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
# KIS practice capital is explicitly 100m KRW. .env is loaded again after
# deploy preflight; reassert the KR-only contract here so stale 50m .env
# values cannot override preflight. Real account capital remains unchanged.
if [[ "$STRATEGY_ENV" == "practice" && "$KIS_ENV" == "practice" ]]; then
  export PAPER_MAX_CAPITAL_KRW=100000000
  export EXPECTED_PRACTICE_CAPITAL_KRW=100000000
  # 2026-11-11 explicit user-approved KR PB1 profit activation.
  # .env is loaded above: override stale SHADOW/LEGACY only for KIS practice.
  # Real account remains governed by its existing LEGACY policy, unchanged.
  export KR_SWING_US_PROFIT_MODE=ACTIVE_PRACTICE
  export KR_SAMSUNG_EA_AMENDMENT_ENABLED=1
  echo "[KR_PROFIT_PARITY][EFFECTIVE] mode=${KR_SWING_US_PROFIT_MODE} samsung_ea_amendment=${KR_SAMSUNG_EA_AMENDMENT_ENABLED} env=practice"
  echo "[KR_CAPITAL][FINAL] expected=100000000 cap=100000000 session=${WSL_RUN_SESSION:-unknown}"
fi
export KR_ARTIFACT_STRICT="${KR_ARTIFACT_STRICT:-1}"
export KR_BLOCK_LEGACY_ARTIFACT="${KR_BLOCK_LEGACY_ARTIFACT:-1}"
export KR_QUARANTINE_STALE_ARTIFACT="${KR_QUARANTINE_STALE_ARTIFACT:-1}"
export KR_REQUIRE_CANONICAL_PREP_CONTRACT="${KR_REQUIRE_CANONICAL_PREP_CONTRACT:-1}"
export DB_LOCK_TIMEOUT_MS="${DB_LOCK_TIMEOUT_MS:-5000}"
export DB_STATEMENT_TIMEOUT_MS="${DB_STATEMENT_TIMEOUT_MS:-15000}"
export DB_IDLE_IN_TX_SESSION_TIMEOUT_MS="${DB_IDLE_IN_TX_SESSION_TIMEOUT_MS:-15000}"
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
export PB1_ENTRY_PLAN_FAIL_OPEN="${PB1_ENTRY_PLAN_FAIL_OPEN:-0}"
export PB1_PRACTICE_FORCE_MIN_TRADE="${PB1_PRACTICE_FORCE_MIN_TRADE:-0}"
export PB1_FORCE_MIN_ONE_ORDER_PRACTICE="${PB1_FORCE_MIN_ONE_ORDER_PRACTICE:-0}"
export PB1_PRACTICE_ATR_MAX_PCT_FALLBACK="${PB1_PRACTICE_ATR_MAX_PCT_FALLBACK:-14}"
export PB1_PRICE_FALLBACK_TO_FINAL30_CLOSE="${PB1_PRICE_FALLBACK_TO_FINAL30_CLOSE:-1}"


export PB1_LOOP_ENABLED="${PB1_LOOP_ENABLED:-1}"
export PB1_RUN_LOOP="${PB1_RUN_LOOP:-1}"
export PB1_RUN_LOOP_MINUTES="${PB1_RUN_LOOP_MINUTES:-235}"
export PB1_LOOP_INTERVAL_SEC="${PB1_LOOP_INTERVAL_SEC:-60}"
export PB1_AM_SESSION_END="12:55"
# AM-only effective windows. Explicit legacy .env 10:30 values cannot truncate a live AM.
# Do NOT override PB1_AM_EXPECTED_END: it controls start/recovery classification.
export PB1_MORNING_WINDOW_END="$PB1_AM_SESSION_END"
export MORNING_WINDOW_END="$PB1_AM_SESSION_END"
export PB1_AFTERNOON_WINDOW_START="13:00"
export AFTERNOON_WINDOW_START="13:00"
export PB1_SESSION_EXIT_GRACE_SEC="${PB1_SESSION_EXIT_GRACE_SEC:-15}"
export PB1_TICK_HARD_TIMEOUT_SEC="${PB1_TICK_HARD_TIMEOUT_SEC:-360}"
export PB1_MIN_TICK_BUDGET_SEC="${PB1_MIN_TICK_BUDGET_SEC:-380}"
export KR_AM_SESSION_TIMEOUT_SEC="${KR_AM_SESSION_TIMEOUT_SEC:-15000}"
echo "[KR_AM][PB1 loop enabled] PB1_LOOP_ENABLED=${PB1_LOOP_ENABLED} PB1_RUN_LOOP=${PB1_RUN_LOOP} PB1_RUN_LOOP_MINUTES=${PB1_RUN_LOOP_MINUTES} PB1_LOOP_INTERVAL_SEC=${PB1_LOOP_INTERVAL_SEC} PB1_AM_SESSION_END=${PB1_AM_SESSION_END} PB1_MORNING_WINDOW_END=${PB1_MORNING_WINDOW_END} MORNING_WINDOW_END=${MORNING_WINDOW_END} PB1_AFTERNOON_WINDOW_START=${PB1_AFTERNOON_WINDOW_START} PB1_SESSION_EXIT_GRACE_SEC=${PB1_SESSION_EXIT_GRACE_SEC} PB1_TICK_HARD_TIMEOUT_SEC=${PB1_TICK_HARD_TIMEOUT_SEC} PB1_MIN_TICK_BUDGET_SEC=${PB1_MIN_TICK_BUDGET_SEC} KR_AM_SESSION_TIMEOUT_SEC=${KR_AM_SESSION_TIMEOUT_SEC}"

export PB1_SESSION=am WSL_RUN_SESSION=am STRATEGY_MODE=LIVE DRY_RUN=0 DISABLE_LIVE_TRADING=0 LIVE_TRADING_ENABLED=1 KR_LIVE_TRADING_ENABLED=1 KR_ORDER_ARMED=1
source scripts/wsl/kr-entry-timing-activation.sh
kr_pb1_entry_timing_defaults
echo "[KR_FORCE_TRADE_ENV] fail_open=${PB1_ENTRY_PLAN_FAIL_OPEN} force_min_trade=${PB1_PRACTICE_FORCE_MIN_TRADE} atr_fallback=${PB1_PRACTICE_ATR_MAX_PCT_FALLBACK} dry_run=${DRY_RUN} live=${LIVE_TRADING_ENABLED} armed=${KR_ORDER_ARMED}"
if [[ "${DRY_RUN:-0}" == "1" || "${KR_ORDER_ARMED:-0}" == "0" ]]; then echo "[KR_FORCE_TRADE_ENV][BLOCK] reason=DRY_RUN_OR_NOT_ARMED"; fi
TODAY_KST="$(TZ=Asia/Seoul date +%F)"
LOG_DIR="runtime/logs/kr/${TODAY_KST}"
mkdir -p "$LOG_DIR"
LOG_FILE="$NULLIM_SESSION_LOG"
LATEST_LINK="runtime/logs/kr/wsl-kr-am.latest.log"
ln -sfn "$(realpath --relative-to="$(dirname "$LATEST_LINK")" "$NULLIM_SESSION_LOG")" "$LATEST_LINK"
{
  echo "[KR_AM][START] ts=$(date -Is) env=$STRATEGY_ENV kis_env=$KIS_ENV session=$PB1_SESSION"
  # Read-only last CLOSE debt summary before AM. No historical reconcile here.
  # A prior-date snapshot is explicitly labelled: not today's live proof.
  if [[ "${KR_CONTRACT_AUDIT_AM_SUMMARY:-1}" == "1" ]]; then
    python - "${TODAY_KST}" <<'PY_KR_AM_DEBT'
import json, sys
from pathlib import Path
today = sys.argv[1]
path = next((p for p in sorted(Path("runtime/health").glob(
    "kr-contract-audit-????-??-??.json"), reverse=True)
    if p.stem[-10:] < today), None)
if path is None:
    print("[KR_CONTRACT_AUDIT][AM_PRIOR] status=NOT_AVAILABLE source=NONE")
else:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        summary = report.get("recurrence") or {}
        status = report.get("audit_status") or report.get("status") or "UNKNOWN"
        print("[KR_CONTRACT_AUDIT][AM_PRIOR] status=%s source=%s "
              "new_issues=%s existing_issues=%s generated=%s data_is_prior_day=1" % (
                  status, path.name, summary.get("new_unresolved_issue_count", "UNKNOWN"),
                  summary.get("existing_unresolved_issue_count", "UNKNOWN"),
                  report.get("generated_utc") or "UNKNOWN"))
    except (OSError, ValueError, TypeError):
        print("[KR_CONTRACT_AUDIT][AM_PRIOR] status=AUDIT_FILE_UNREADABLE source=%s" % path.name)
PY_KR_AM_DEBT
  fi
  kr_pb1_opening_observer_start "$LOG_DIR" "$TODAY_KST"
  echo "[KR_AM][HISTORICAL_RECOVERY][CONFIG] flag=${KR_HISTORICAL_RECOVERY_ONCE_ENABLED-<default-on>} effective=${KR_HISTORICAL_RECOVERY_ONCE_ENABLED-1}"
  if [[ "$STRATEGY_ENV" == "practice" && "$KIS_ENV" == "practice" && "${KR_HISTORICAL_RECOVERY_ONCE_ENABLED-1}" != "1" ]]; then
    echo "[KR_AM][HISTORICAL_RECOVERY][WARNING] practice default overridden; per-tick legacy recovery may execute"
  fi
  KR_AM_SESSION_TIMEOUT_SEC="${KR_AM_SESSION_TIMEOUT_SEC:-3600}"
  echo "[KR_AM][EFFECTIVE_ENV] timeout_sec=${KR_AM_SESSION_TIMEOUT_SEC}"
  # Exit 137 can be SIGKILL/OOM/external termination as well as a timeout
  # kill escalation. Do not claim a timer expired without authoritative proof.
  start_epoch=$(date +%s)
  set +e
  timeout --kill-after=30s "${KR_AM_SESSION_TIMEOUT_SEC}" python -m trader.kr.runner.trade_session_runner --session am --env "$STRATEGY_ENV"
  rc=$?
  set -e
  child_end_epoch=$(date +%s)
  elapsed_sec=$(( child_end_epoch - start_epoch ))
  # The synchronous AM child has returned and this WSL wrapper still holds
  # the exclusive lock. Terminalize only this child's unfinished run-ledger
  # records, never broker orders/fills/positions. A SIGKILL of the wrapper
  # itself cannot be repaired here: the next start must use stale takeover.
  if [[ "${KIS_ENV:-}" == "practice" && "${STRATEGY_ENV:-}" == "practice" && "${LOCK_DELEGATED:-0}" != "1" ]]; then
    python -m scripts.kr_20261109_am_child_run_finalizer \
      --env practice --start-epoch "$start_epoch" --end-epoch "$child_end_epoch" \
      --child-rc "$rc" --exclusive-wrapper-lock --child-returned \
      || echo "[KR_AM][RUN_TERMINALIZE][FAIL] reason=DB_OR_SCOPE_UNAVAILABLE rc=$rc"
  else
    echo "[KR_AM][RUN_TERMINALIZE][SKIP] reason=UNPROVEN_EXCLUSIVE_LOCK_OR_NON_PRACTICE"
  fi
  if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]; then
    LAST_STAGE_FILE="runtime/state/kr/session_last_stage_am.json"
    LAST_STAGE=""
    if [[ -f "$LAST_STAGE_FILE" ]]; then LAST_STAGE=$(python -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8")).get("stage", ""))' "$LAST_STAGE_FILE" 2>/dev/null || true); fi
    if [[ "$rc" -eq 124 ]]; then
      export NULLIM_SESSION_FINAL_STATUS=TIMEOUT NULLIM_SESSION_FINAL_REASON=SESSION_TIMEOUT
      reason=SESSION_TIMEOUT
    else
      # A 137 exit alone cannot distinguish external SIGKILL, OOM, or
      # timeout --kill-after. Preserve the uncertainty for kernel/task logs.
      export NULLIM_SESSION_FINAL_STATUS=FAILED NULLIM_SESSION_FINAL_REASON=SESSION_KILLED_UNVERIFIED
      reason=SESSION_KILLED_UNVERIFIED
    fi
    echo "[KR_AM][ABNORMAL_EXIT] reason=${reason} rc=${rc} elapsed_sec=${elapsed_sec} timeout_sec=${KR_AM_SESSION_TIMEOUT_SEC} last_stage=${LAST_STAGE}"
    echo "[RUN_SUMMARY][RESULT] market=KR session=am status=FAIL reason=${reason} orders_intent=unknown orders_ack=unknown blocked=0"
  fi
  echo "[KR_AM][EXIT] ts=$(date -Is) exit_code=$rc elapsed_sec=${elapsed_sec}"
  exit $rc
} >> "$LOG_FILE" 2>&1
