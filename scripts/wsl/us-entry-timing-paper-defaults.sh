#!/usr/bin/env bash
# US PB1 opening-window timing: ON for canonical KIS practice trading sessions.
# Read-only observer and WS data gathering are not order permissions.
# Explicit OFF safety overrides remain; legacy SHADOW is upgraded to ENFORCE.
# Real accounts, offline, dry-run and signal-only sessions are not auto-armed.
if [[ "${KIS_ENV:-}" == "practice" &&
      "${US_PAPER_TRADING_ENABLED:-0}" == "1" &&
      "${DRY_RUN:-1}" == "0" &&
      "${US_OFFLINE:-0}" != "1" &&
      "${US_SIGNAL_ONLY:-0}" != "1" &&
      "${US_RUN_MODE:-TRADE}" == "TRADE" ]]; then
  # A legacy .env SHADOW is not an intentional ON cutover. The operator
  # explicitly requested the PRACTICE timing policy active, not observational.
  # Keep a deliberate OFF as the emergency rollback switch.
  case "${US_ENTRY_OPPORTUNITY_MODE:-ENFORCE}" in
    SHADOW|shadow|Shadow)
      echo "[US_ENTRY_TIMING][UPGRADE] legacy_shadow_to_practice_ENFORCE=1"
      export US_ENTRY_OPPORTUNITY_MODE="ENFORCE"
      ;;
    ENFORCE|OFF)
      export US_ENTRY_OPPORTUNITY_MODE="${US_ENTRY_OPPORTUNITY_MODE:-ENFORCE}"
      ;;
    *)
      echo "[US_ENTRY_TIMING][CONFIG_ERROR] unsupported_mode=${US_ENTRY_OPPORTUNITY_MODE} action=OFF protective_sell_unchanged=1" >&2
      export US_ENTRY_OPPORTUNITY_MODE="OFF"
      ;;
  esac
  export US_ENTRY_OPPORTUNITY_APPROVED="${US_ENTRY_OPPORTUNITY_APPROVED:-1}"
  export US_OPENING_OBSERVER_ENABLED="${US_OPENING_OBSERVER_ENABLED:-1}"
  export US_OPENING_OBSERVER_BAR_FETCH_ENABLED="${US_OPENING_OBSERVER_BAR_FETCH_ENABLED:-1}"
  export US_WS_COLLECTOR_ENABLED="${US_WS_COLLECTOR_ENABLED:-1}"
  export US_OPENING_OBSERVER_INTERVAL_SEC="${US_OPENING_OBSERVER_INTERVAL_SEC:-10}"
  # Do not make a deliberate operator override into a hidden Shadow test.
  # Even without READY evidence, protective SELL may continue independently.
  if [[ "${US_ENTRY_OPPORTUNITY_MODE}" != "ENFORCE" ||
        "${US_ENTRY_OPPORTUNITY_APPROVED}" != "1" ||
        "${US_OPENING_OBSERVER_ENABLED}" != "1" ||
        "${US_OPENING_OBSERVER_BAR_FETCH_ENABLED}" != "1" ||
        "${US_WS_COLLECTOR_ENABLED}" != "1" ]]; then
    echo "[US_ENTRY_TIMING][OVERRIDE] practice_mode=${US_ENTRY_OPPORTUNITY_MODE} approved=${US_ENTRY_OPPORTUNITY_APPROVED} observer=${US_OPENING_OBSERVER_ENABLED} bars=${US_OPENING_OBSERVER_BAR_FETCH_ENABLED} ws=${US_WS_COLLECTOR_ENABLED} entry_timing_on_not_guaranteed=1 sell_permission_unchanged=1" >&2
  fi
else
  # Safety: do not implicitly enable a new order gate outside approved practice.
  echo "[US_ENTRY_TIMING][PROFILE] auto_on=0 reason=not_order_capable_practice_trade sell_permission_unchanged=1"
fi
echo "[US_ENTRY_TIMING][PROFILE] env=${KIS_ENV:-missing} mode=${US_ENTRY_OPPORTUNITY_MODE:-UNSET} observer=${US_OPENING_OBSERVER_ENABLED:-UNSET} ws=${US_WS_COLLECTOR_ENABLED:-UNSET} native_order_permission=unchanged"
