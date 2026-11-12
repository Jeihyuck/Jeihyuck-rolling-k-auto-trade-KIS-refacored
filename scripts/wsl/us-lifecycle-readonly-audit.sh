#!/usr/bin/env bash
# Read-only, non-gating all-symbol lifecycle diagnostics; invoke ONLY at AM
# startup and CLOSE boundary. Never source this helper from a tick runner.
# No order, recovery mutation, claim release, or broker-side action is allowed.
nullim_us_lifecycle_readonly_audit() {
  local phase="${1:-unknown}"
  local as_of="${US_TRADE_DATE:-${NULLIM_TRADE_DATE:-}}"
  local py="${PYTHON_BIN:-python}"
  local dest="${LOG_FILE:-/dev/stderr}"
  if [[ "${US_LIFECYCLE_AUDIT_ENABLED:-1}" == "0" ]]; then
    echo "[US_LIFECYCLE][READONLY][SKIP] phase=${phase} reason=OPERATOR_DISABLED" >> "${dest}"
    return 0
  fi
  if [[ "${KIS_ENV:-}" != "practice" || "${WSL_RUN_MARKET:-}" != "US" ]]; then
    echo "[US_LIFECYCLE][READONLY][SKIP] phase=${phase} reason=NOT_US_PRACTICE" >> "${dest}"
    return 0
  fi
  if [[ ! "${as_of}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "[US_LIFECYCLE][READONLY][UNVERIFIED] phase=${phase} reason=TRADE_DATE_MISSING" >> "${dest}"
    return 0
  fi
  echo "[US_LIFECYCLE][READONLY][BEGIN] phase=${phase} as_of=${as_of} trade_authority=disabled_child_only" >> "${dest}"
  local audit_rc=0
  # Child-only scope, does not alter parent trade permission. The Python CLI
  # itself insists on PRACTICE+US+ORDER_ARMED=0+NO_TRADE=1+DRY_RUN=1,
  # then opens a PostgreSQL READ ONLY transaction.
  US_ORDER_ARMED=0 US_LIVE_TRADING_ENABLED=0 LIVE_TRADING_ENABLED=0 \
  NO_TRADE=1 DRY_RUN=1 "${py}" \
    scripts/us_all_symbols_lifecycle_readonly_audit_20261112.py \
    --as-of "${as_of}" --confirm-practice-readonly >> "${dest}" 2>&1 || audit_rc=$?
  if [[ "${audit_rc}" != "0" ]]; then
    echo "[US_LIFECYCLE][READONLY][UNVERIFIED] phase=${phase} rc=${audit_rc} action=MANUAL_REVIEW no_order_block=1" >> "${dest}"
  else
    echo "[US_LIFECYCLE][READONLY][DONE] phase=${phase} diagnostic_only=1 no_order_block=1" >> "${dest}"
  fi
  # An audit outage must not stop independent protective SELL or change trade
  # policy. Every failure is logged for AM/Close operator review instead.
  return 0
}
