#!/usr/bin/env bash
set -euo pipefail
if ! command -v rg >/dev/null 2>&1; then
  echo "ripgrep is required" >&2
  exit 1
fi
patterns='REGIME_INDEX[[:space:]]*=[[:space:]]*"229200"|RS_BENCHMARK[[:space:]]*=[[:space:]]*"229200"|get_regime\(REGIME_INDEX\)|purpose=regime[[:space:]]+symbol=229200|symbol=229200[[:space:]]+purpose=regime-global|_fetch_daily\([[:space:]]*"(KOSPI|KOSDAQ|KOSPI200)"|ohlcv_provider\([[:space:]]*"(KOSPI|KOSDAQ|KOSPI200)"|evaluate_kr_market_state\(|_index_returns\(|apply_kr_market_state_to_budget\(|KR_INDEX_KOSPI_SYMBOL|KR_INDEX_KOSDAQ_SYMBOL|KR_INDEX_KOSPI200_PROXY'
set +e
matches=$(rg -n "$patterns" trader config 2>&1)
status=$?
set -e
if [ "$status" -eq 0 ]; then
  printf '%s\n' "$matches"
  echo "forbidden Korean legacy regime path found" >&2
  exit 1
elif [ "$status" -ne 1 ]; then
  printf '%s\n' "$matches" >&2
  echo "legacy regime scan failed" >&2
  exit "$status"
fi
