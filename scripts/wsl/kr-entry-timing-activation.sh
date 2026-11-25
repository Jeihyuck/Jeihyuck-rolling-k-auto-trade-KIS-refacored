#!/usr/bin/env bash
# KR PB1 practice only: activate the existing #291 completed-1m entry proof.
# No change to KIS real accounts, KR_INFINITE, US, SELL or CLOSE policies.
kr_pb1_entry_timing_defaults() {
  if [[ "${STRATEGY_ENV:-}" == "practice" && "${KIS_ENV:-}" == "practice" ]]; then
    # Legacy SHADOW is never an order-execution mode in KR practice.
    # OFF is emergency BUY HALT (not fallback to setup-only orders).
    case "${KR_PB1_ENTRY_TIMING_MODE:-ENFORCE}" in
      ENFORCE|enforce) export KR_PB1_ENTRY_TIMING_MODE=ENFORCE ;;
      SHADOW|shadow|Shadow)
        echo "[KR][ENTRY_TIMING][UPGRADE] legacy_SHADOW_to_ENFORCE=1"
        export KR_PB1_ENTRY_TIMING_MODE=ENFORCE
        # A previous SHADOW profile commonly carried approval=0. The user
        # explicitly authorized ACTIVE practice cutover; upgrade that stale
        # pair. An independent explicit ENFORCE+approval=0 or OFF remains
        # an intentional fail-closed emergency BUY stop.
        if [[ "${KR_PB1_ENTRY_TIMING_APPROVED:-1}" != "1" ]]; then
          echo "[KR][ENTRY_TIMING][UPGRADE] legacy_shadow_approval_to_active=1"
          export KR_PB1_ENTRY_TIMING_APPROVED=1
        fi
        ;;
      OFF|off) export KR_PB1_ENTRY_TIMING_MODE=OFF ;;
      *)
        echo "[KR][ENTRY_TIMING][CONFIG_ERROR] invalid_timing_mode=${KR_PB1_ENTRY_TIMING_MODE} action=ENFORCE_UNAPPROVED_BUY_FAIL_CLOSED" >&2
        export KR_PB1_ENTRY_TIMING_MODE=ENFORCE
        export KR_PB1_ENTRY_TIMING_APPROVED=0
        ;;
    esac
    export KR_PB1_ENTRY_TIMING_APPROVED="${KR_PB1_ENTRY_TIMING_APPROVED:-1}"
    # V2 is active for the approved practice AM/PM profile, not shadow.
    export KR_PB1_ENTRY_OPPORTUNITY_POLICY="${KR_PB1_ENTRY_OPPORTUNITY_POLICY:-US_ALIGNED_V2}"
    case "$KR_PB1_ENTRY_OPPORTUNITY_POLICY" in
      US_ALIGNED_V2|LEGACY_V1) ;;
      *) export KR_PB1_ENTRY_TIMING_APPROVED=0 ;;
    esac
    # Active only inside the approved practice V2 enforcement profile.
    # Explicit 0 is the rollback to unmodified V2; LEGACY_V1 remains separate.
    export KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED="${KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED:-1}"
    case "$KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED" in
      0|1) ;;
      *) export KR_PB1_ENTRY_TIMING_APPROVED=0 ;;
    esac
    if [[ "$KR_PB1_ENTRY_OPPORTUNITY_POLICY" != "US_ALIGNED_V2" ]]; then
      export KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED=0
    fi
    # The old quote-only parallel SHADOW must not mask the mandatory
    # completed KIS 1m ENFORCE gate or be mistaken for live BUY authorization.
    export KR_PB1_ENTRY_TIMING_SHADOW_ENABLED=0
    export KR_PB1_OPENING_OBSERVER_ENABLED="${KR_PB1_OPENING_OBSERVER_ENABLED:-1}"
    if [[ "$KR_PB1_ENTRY_TIMING_MODE" == "OFF" ]]; then
      export KR_PB1_OPENING_OBSERVER_ENABLED=0
    fi
    export KR_PB1_OPENING_OBSERVER_INTERVAL_SEC="${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-45}"
    # Shared *observer-only* KIS quota and critical-traffic notice are KR
    # practice-scoped. Real order logic never waits on the read-only collector.
    export KR_PB1_KIS_OBSERVER_BUDGET_ENABLED="${KR_PB1_KIS_OBSERVER_BUDGET_ENABLED:-1}"
  else
    export KR_PB1_ENTRY_OPPORTUNITY_POLICY=LEGACY_V1
    export KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED=0
    export KR_PB1_ENTRY_TIMING_MODE="${KR_PB1_ENTRY_TIMING_MODE:-SHADOW}"
    export KR_PB1_ENTRY_TIMING_APPROVED="${KR_PB1_ENTRY_TIMING_APPROVED:-0}"
    export KR_PB1_ENTRY_TIMING_SHADOW_ENABLED="${KR_PB1_ENTRY_TIMING_SHADOW_ENABLED:-0}"
    export KR_PB1_OPENING_OBSERVER_ENABLED=0
    export KR_PB1_KIS_OBSERVER_BUDGET_ENABLED=0
  fi
  echo "[KR][ENTRY_TIMING][EFFECTIVE_CONFIG] strategy_env=${STRATEGY_ENV:-unknown} kis_env=${KIS_ENV:-unknown} mode=$KR_PB1_ENTRY_TIMING_MODE approved=$KR_PB1_ENTRY_TIMING_APPROVED policy=$KR_PB1_ENTRY_OPPORTUNITY_POLICY adaptive_v3=$KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED legacy_quote_shadow=DISABLED opening_observer=$KR_PB1_OPENING_OBSERVER_ENABLED interval_sec=${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-n/a}"
  if [[ "${STRATEGY_ENV:-}" == "practice" && "${KIS_ENV:-}" == "practice" ]] &&
     { [[ "$KR_PB1_ENTRY_TIMING_MODE" != "ENFORCE" ]] ||
       [[ "$KR_PB1_ENTRY_TIMING_APPROVED" != "1" ]] ||
       [[ "$KR_PB1_OPENING_OBSERVER_ENABLED" != "1" ]]; }; then
    echo "[KR][ENTRY_TIMING][CONFIG_OVERRIDE][WARN] default practice activation overridden; review .env before trading"
  fi
}

kr_pb1_opening_observer_start() {
  local log_dir="${1:?log directory required}" trade_date="${2:?trade date required}"
  [[ "${STRATEGY_ENV:-}" == "practice" && "${KIS_ENV:-}" == "practice" ]] || return 0
  # Read-only watchdog monitors this exact AM/PM run, including when the
  # observer is intentionally disabled. No inherited trading/mail locks.
  local watchdog_session="${NULLIM_SESSION_PURPOSE:-}"
  if [[ "${KR_PB1_SESSION_WATCHDOG_ENABLED:-1}" == "1" && -n "${NULLIM_RUN_ID:-}" ]] &&
     [[ "$watchdog_session" == "am" || "$watchdog_session" == "afternoon" ]]; then
    local watchdog_log="$log_dir/$watchdog_session/${NULLIM_RUN_ID}.watchdog.log"
    (
      for fd in "${NULLIM_SESSION_LOCK_FD:-}" "${NULLIM_SNAPSHOT_FD:-}"; do
        if [[ "$fd" =~ ^[0-9]+$ ]]; then eval "exec ${fd}>&-"; fi
      done
      unset NULLIM_SESSION_LOCK_FD NULLIM_SESSION_LOCK_FILE NULLIM_SESSION_LOCK_TOKEN NULLIM_SNAPSHOT_FD
      exec timeout --kill-after=5s 24000 env NO_TRADE=1 DISABLE_REAL_TRADING=1 \
        python -m scripts.kr_session_watchdog --date "$trade_date" \
          --session "$watchdog_session" --run-id "$NULLIM_RUN_ID" --parent-pid "$" \
          --observer-required "${KR_PB1_OPENING_OBSERVER_ENABLED:-0}"
    ) >>"$watchdog_log" 2>&1 </dev/null &
    echo "[KR][SESSION_WATCHDOG][LAUNCH] pid=$! log=$watchdog_log read_only=1"
  fi
  [[ "${KR_PB1_OPENING_OBSERVER_ENABLED:-0}" == "1" ]] || {
    echo "[KR][ENTRY_TIMING][OBSERVER_DISABLED] practice entry timing proof may be UNKNOWN"
    return 0
  }
  if ! [[ "${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-}" =~ ^[0-9]+$ ]] ||
     (( KR_PB1_OPENING_OBSERVER_INTERVAL_SEC < 15 || KR_PB1_OPENING_OBSERVER_INTERVAL_SEC > 600 )); then
    echo "[KR][ENTRY_TIMING][OBSERVER_CONFIG_INVALID] interval_sec=${KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-unset} action=NO_OBSERVER" >&2
    return 0
  fi
  local final30="runtime/kr/watchlist/${trade_date}/final30_scored.json"
  if [[ ! -s "$final30" ]]; then
    # PREP may still be completing when AM starts at 08:55.  The independent
    # collector must launch now and wait for a verified same-day contract,
    # rather than disappear for the entire trading session.
    echo "[KR][ENTRY_TIMING][OBSERVER_WAIT_PREP] file=$final30 action=LAUNCH_READ_ONLY_RETRY_UNTIL_VALID"
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
    # The observer also must not retain the mail packaging snapshot lock.
    if [[ "${NULLIM_SNAPSHOT_FD:-}" =~ ^[0-9]+$ ]]; then
      eval "exec ${NULLIM_SNAPSHOT_FD}>&-"
    fi
    unset NULLIM_SESSION_LOCK_FD NULLIM_SESSION_LOCK_FILE NULLIM_SESSION_LOCK_TOKEN NULLIM_SNAPSHOT_FD
    exec flock -n "runtime/locks/kr-pb1-opening-observer-${trade_date}.lock" \
      timeout --kill-after=15s 24000 \
      env NO_TRADE=1 DISABLE_REAL_TRADING=1 DISABLE_LIVE_TRADING=1 \
          KR_PB1_OBSERVER_ONLY=1 \
          LIVE_TRADING_ENABLED=0 KR_LIVE_TRADING_ENABLED=0 KR_ORDER_ARMED=0 \
      python -m scripts.kr_opening_observer --loop --date "$trade_date" \
        --interval-sec "$KR_PB1_OPENING_OBSERVER_INTERVAL_SEC"
  ) >>"$log_dir/kr-opening-observer.log" 2>&1 </dev/null &
  local observer_pid=$!
  echo "[KR][ENTRY_TIMING][OBSERVER_LAUNCH] pid=$observer_pid mode=READ_ONLY independent_of_sell=1 date=$trade_date log=$log_dir/kr-opening-observer.log"
}
