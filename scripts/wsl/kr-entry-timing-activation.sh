#!/usr/bin/env bash
# KR PB1 practice only: activate the existing #291 completed-1m entry proof.
# No change to KIS real accounts, KR_INFINITE, US, SELL or CLOSE policies.
kr_pb1_entry_timing_defaults() {
  if [[ "${STRATEGY_ENV:-}" == "practice" && "${KIS_ENV:-}" == "practice" ]]; then
    # A legacy SHADOW from the old opt-in configuration is not the requested
    # practice cutover. Retain explicit OFF as the emergency rollback.
    case "${KR_PB1_ENTRY_TIMING_MODE:-ENFORCE}" in
      ENFORCE|enforce) export KR_PB1_ENTRY_TIMING_MODE=ENFORCE ;;
      SHADOW|shadow|Shadow)
        echo "[KR][ENTRY_TIMING][UPGRADE] legacy_SHADOW_to_ENFORCE=1"
        export KR_PB1_ENTRY_TIMING_MODE=ENFORCE
        ;;
      OFF|off) export KR_PB1_ENTRY_TIMING_MODE=OFF ;;
      *)
        echo "[KR][ENTRY_TIMING][CONFIG_ERROR] invalid_timing_mode=${KR_PB1_ENTRY_TIMING_MODE} action=ENFORCE_UNAPPROVED_BUY_FAIL_CLOSED" >&2
        export KR_PB1_ENTRY_TIMING_MODE=ENFORCE
        export KR_PB1_ENTRY_TIMING_APPROVED=0
        ;;
    esac
    export KR_PB1_ENTRY_TIMING_APPROVED="${KR_PB1_ENTRY_TIMING_APPROVED:-1}"
    export KR_PB1_ENTRY_TIMING_SHADOW_ENABLED="${KR_PB1_ENTRY_TIMING_SHADOW_ENABLED:-1}"
    export KR_PB1_OPENING_OBSERVER_ENABLED="${KR_PB1_OPENING_OBSERVER_ENABLED:-1}"
    if [[ "$KR_PB1_ENTRY_TIMING_MODE" == "OFF" ]]; then
      export KR_PB1_OPENING_OBSERVER_ENABLED=0
    fi
    export KR_PB1_OPENING_OBSERVER_INTERVAL_SEC="${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-120}"
  else
    export KR_PB1_ENTRY_TIMING_MODE="${KR_PB1_ENTRY_TIMING_MODE:-SHADOW}"
    export KR_PB1_ENTRY_TIMING_APPROVED="${KR_PB1_ENTRY_TIMING_APPROVED:-0}"
    export KR_PB1_ENTRY_TIMING_SHADOW_ENABLED="${KR_PB1_ENTRY_TIMING_SHADOW_ENABLED:-0}"
    export KR_PB1_OPENING_OBSERVER_ENABLED=0
  fi
  echo "[KR][ENTRY_TIMING][EFFECTIVE_CONFIG] strategy_env=${STRATEGY_ENV:-unknown} kis_env=${KIS_ENV:-unknown} mode=$KR_PB1_ENTRY_TIMING_MODE approved=$KR_PB1_ENTRY_TIMING_APPROVED legacy_quote_shadow=$KR_PB1_ENTRY_TIMING_SHADOW_ENABLED opening_observer=$KR_PB1_OPENING_OBSERVER_ENABLED interval_sec=${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-n/a}"
  if [[ "${STRATEGY_ENV:-}" == "practice" && "${KIS_ENV:-}" == "practice" ]] &&
     { [[ "$KR_PB1_ENTRY_TIMING_MODE" != "ENFORCE" ]] ||
       [[ "$KR_PB1_ENTRY_TIMING_APPROVED" != "1" ]] ||
       [[ "$KR_PB1_ENTRY_TIMING_SHADOW_ENABLED" != "1" ]] ||
       [[ "$KR_PB1_OPENING_OBSERVER_ENABLED" != "1" ]]; }; then
    echo "[KR][ENTRY_TIMING][CONFIG_OVERRIDE][WARN] default practice activation overridden; review .env before trading"
  fi
}

kr_pb1_opening_observer_start() {
  local log_dir="${1:?log directory required}" trade_date="${2:?trade date required}"
  [[ "${STRATEGY_ENV:-}" == "practice" && "${KIS_ENV:-}" == "practice" ]] || return 0
  [[ "${KR_PB1_OPENING_OBSERVER_ENABLED:-0}" == "1" ]] || {
    echo "[KR][ENTRY_TIMING][OBSERVER_DISABLED] practice entry timing proof may be UNKNOWN"
    return 0
  }
  if ! [[ "${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-}" =~ ^[0-9]+$ ]] ||
     (( KR_PB1_OPENING_OBSERVER_INTERVAL_SEC < 120 || KR_PB1_OPENING_OBSERVER_INTERVAL_SEC > 600 )); then
    echo "[KR][ENTRY_TIMING][OBSERVER_CONFIG_INVALID] interval_sec=${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-unset} action=NO_OBSERVER" >&2
    return 0
  fi
  local final30="runtime/kr/watchlist/${trade_date}/final30_scored.json"
  if [[ ! -s "$final30" ]]; then
    echo "[KR][ENTRY_TIMING][OBSERVER_INPUT_MISSING] file=$final30 action=NO_OBSERVER_BUY_FAIL_CLOSED"
    return 0
  fi
  mkdir -p "$log_dir" runtime/locks
  # Non-blocking, read-only sidecar. Do NOT let it inherit the AM/PM session
  # flock descriptor: an orphaned collector must never block protective SELL.
  # A separate same-day flock prevents overlapping AM/PM collectors. The
  # process ends at 15:05 or after a hard upper bound even if wrapper exits.
  (
    if [[ -n "${NULLIM_SESSION_LOCK_FD:-}" ]]; then
      eval "exec ${NULLIM_SESSION_LOCK_FD}>&-"
    fi
    unset NULLIM_SESSION_LOCK_FD NULLIM_SESSION_LOCK_FILE NULLIM_SESSION_LOCK_TOKEN
    exec flock -n "runtime/locks/kr-pb1-opening-observer-${trade_date}.lock" \
      timeout --kill-after=15s 24000 \
      env NO_TRADE=1 DISABLE_REAL_TRADING=1 DISABLE_LIVE_TRADING=1 \
          LIVE_TRADING_ENABLED=0 KR_LIVE_TRADING_ENABLED=0 KR_ORDER_ARMED=0 \
      python -m scripts.kr_opening_observer --loop --date "$trade_date" \
        --interval-sec "$KR_PB1_OPENING_OBSERVER_INTERVAL_SEC"
  ) >>"$log_dir/kr-opening-observer.log" 2>&1 </dev/null &
  local observer_pid=$!
  echo "[KR][ENTRY_TIMING][OBSERVER_LAUNCH] pid=$observer_pid mode=READ_ONLY independent_of_sell=1 date=$trade_date log=$log_dir/kr-opening-observer.log"
}
