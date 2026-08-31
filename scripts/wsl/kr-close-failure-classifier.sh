#!/usr/bin/env bash

classify_kr_close_failure() {
  local log_text="$1" original_rc="$2" reason="" retryable=0 final_rc="$2" status="FAIL"
  if grep -Eiq 'KisAuthError|AUTH_REFRESH|HTTP 401|HTTP 403' <<<"$log_text"; then reason="KIS_AUTH_ERROR"; retryable=1
  elif grep -Eiq 'KisTokenRateLimit|EGW00133|1분당 1회|tokenP' <<<"$log_text"; then reason="KIS_TOKEN_RATE_LIMIT"; retryable=1
  elif grep -Eiq 'BALANCE.*(timeout|timed out)|KisBalanceUnavailable' <<<"$log_text"; then reason="KIS_BALANCE_TIMEOUT"; retryable=1
  elif grep -Eiq 'circuit.*open|KIS_CIRCUIT_OPEN' <<<"$log_text"; then reason="KIS_CIRCUIT_OPEN"; retryable=1
  elif grep -Eiq 'Network.*timeout|ReadTimeout|ConnectTimeout' <<<"$log_text"; then reason="KIS_NETWORK_TIMEOUT"; retryable=1
  elif grep -Eiq 'KisTemporaryError|KIS_ORDER_ENDPOINT_ERROR|ORDER_ENDPOINT.*(temporary|timeout)' <<<"$log_text"; then reason="KIS_ORDER_ENDPOINT_ERROR"; retryable=1
  else reason="NON_KIS_RUNTIME_FAILURE"
  fi
  if [[ "$retryable" -eq 1 ]]; then status="RETRYABLE_DEGRADED"; final_rc=75; fi
  printf '%s\t%s\t%s\t%s\n' "$reason" "$retryable" "$final_rc" "$status"
}
