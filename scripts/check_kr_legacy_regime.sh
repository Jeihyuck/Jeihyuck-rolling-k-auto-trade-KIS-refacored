#!/usr/bin/env bash
set -euo pipefail
patterns='REGIME_INDEX[[:space:]]*=[[:space:]]*"229200"|RS_BENCHMARK[[:space:]]*=[[:space:]]*"229200"|get_regime\(REGIME_INDEX\)|purpose=regime[[:space:]]+symbol=229200'
if rg -n "$patterns" trader scripts config; then
  echo "forbidden Korean legacy regime path found" >&2
  exit 1
fi
