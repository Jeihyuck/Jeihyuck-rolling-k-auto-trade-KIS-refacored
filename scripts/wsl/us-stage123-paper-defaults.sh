#!/usr/bin/env bash
# Canonical Windows-scheduled US AM/Afternoon WSL profile.
# New Stage 1/2/3 features DEFAULT ON for authenticated KIS PRACTICE TRADE
# sessions. Never arm a real KIS account, offline, dry-run, or signal-only.
# Operator-set feature flags retain precedence (including explicit OFF).
# Unsupported authoritative WORKER is NEVER auto-enabled.
if [[ "${KIS_ENV:-}" == "practice" &&
      "${US_PAPER_TRADING_ENABLED:-0}" == "1" &&
      "${DRY_RUN:-1}" == "0" &&
      "${US_OFFLINE:-0}" != "1" &&
      "${US_SIGNAL_ONLY:-0}" != "1" &&
      "${US_RUN_MODE:-TRADE}" == "TRADE" ]]; then
  export US_STAGE123_PAPER_PROFILE="${US_STAGE123_PAPER_PROFILE:-ON}"
  case "${US_STAGE123_PAPER_PROFILE}" in
    ON)
      export US_ATOMIC_CLAIM_OUTBOX_ENABLED="${US_ATOMIC_CLAIM_OUTBOX_ENABLED:-1}"
      export US_ACCOUNT_RESERVATION_MODE="${US_ACCOUNT_RESERVATION_MODE:-ENFORCE}"
      export US_RECONCILE_WORKER_MODE="${US_RECONCILE_WORKER_MODE:-RESERVATION_ONLY}"
      export US_SHARED_KIS_LIMITER_ENABLED="${US_SHARED_KIS_LIMITER_ENABLED:-1}"
      export US_EXIT_QUOTE_PREFETCH_ENABLED="${US_EXIT_QUOTE_PREFETCH_ENABLED:-1}"
      export US_EARLY_HARD_STOP_PILOT_ENABLED="${US_EARLY_HARD_STOP_PILOT_ENABLED:-1}"
      export US_EARLY_MULTI_HARD_STOP_ENABLED="${US_EARLY_MULTI_HARD_STOP_ENABLED:-1}"
      export US_EARLY_SOFT_STOP_PILOT_ENABLED="${US_EARLY_SOFT_STOP_PILOT_ENABLED:-1}"
      ;;
    OFF)
      export US_ATOMIC_CLAIM_OUTBOX_ENABLED="${US_ATOMIC_CLAIM_OUTBOX_ENABLED:-0}"
      export US_ACCOUNT_RESERVATION_MODE="${US_ACCOUNT_RESERVATION_MODE:-OFF}"
      export US_RECONCILE_WORKER_MODE="${US_RECONCILE_WORKER_MODE:-LEGACY}"
      export US_SHARED_KIS_LIMITER_ENABLED="${US_SHARED_KIS_LIMITER_ENABLED:-0}"
      export US_EXIT_QUOTE_PREFETCH_ENABLED="${US_EXIT_QUOTE_PREFETCH_ENABLED:-0}"
      export US_EARLY_HARD_STOP_PILOT_ENABLED="${US_EARLY_HARD_STOP_PILOT_ENABLED:-0}"
      export US_EARLY_MULTI_HARD_STOP_ENABLED="${US_EARLY_MULTI_HARD_STOP_ENABLED:-0}"
      export US_EARLY_SOFT_STOP_PILOT_ENABLED="${US_EARLY_SOFT_STOP_PILOT_ENABLED:-0}"
      ;;
    *)
      echo "[US_STAGE123][CONFIG_ERROR] expected=ON_or_OFF" >&2
      return 43
      ;;
  esac
else
  # Explicit opt-in cannot bypass the underlying KIS production-account
  # safety gates, but refuse an inconsistent self-described ON profile.
  if [[ "${US_STAGE123_PAPER_PROFILE:-OFF}" == "ON" ]]; then
    echo "[US_STAGE123][CONFIG_ERROR] profile_ON_requires_KIS_practice_trade" >&2
    return 43
  fi
  export US_STAGE123_PAPER_PROFILE="OFF"
fi

# Configuration is one interdependent invariant, not eight independent switches.
# The new Stage4 account writer is opt-in only with explicit protective
# ledger enforcement. Never turn it on implicitly and never treat an
# unauthorized WORKER configuration as legacy RESERVATION_ONLY.
if [[ "${US_STAGE123_PAPER_PROFILE}" == "ON" ]]; then
  _us_reconcile_mode_authorized=0
  if [[ "${US_RECONCILE_WORKER_MODE}" == "RESERVATION_ONLY" ]]; then
    _us_reconcile_mode_authorized=1
  elif [[ "${US_RECONCILE_WORKER_MODE}" == "WORKER" &&
          "${US_STAGE4_WRITER_AUTHORIZED:-0}" == "1" &&
          "${US_PROTECTIVE_STOP_LEDGER_MODE:-SHADOW}" == "ENFORCE" ]]; then
    _us_reconcile_mode_authorized=1
  fi
  if [[ "${US_ATOMIC_CLAIM_OUTBOX_ENABLED}" != "1" ||
        "${US_ACCOUNT_RESERVATION_MODE}" != "ENFORCE" ||
        "${_us_reconcile_mode_authorized}" != "1" ||
        "${US_SHARED_KIS_LIMITER_ENABLED}" != "1" ||
        "${US_EXIT_QUOTE_PREFETCH_ENABLED}" != "1" ||
        "${US_EARLY_HARD_STOP_PILOT_ENABLED}" != "1" ||
        "${US_EARLY_MULTI_HARD_STOP_ENABLED}" != "1" ||
        "${US_EARLY_SOFT_STOP_PILOT_ENABLED}" != "1" ]]; then
    echo "[US_STAGE123][CONFIG_ERROR] profile_ON_requires_consistent_coordinator_and_sell_flags" >&2
    return 43
  fi
fi
echo "[US_STAGE123][PROFILE] mode=${US_STAGE123_PAPER_PROFILE} env=${KIS_ENV:-missing} worker=${US_RECONCILE_WORKER_MODE:-LEGACY} reservations=${US_ACCOUNT_RESERVATION_MODE:-OFF} native_http_permission=unchanged"
