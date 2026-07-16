#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime/health
TRADE_DATE="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
CONTRACT="runtime/us/watchlist/${TRADE_DATE}/prep_contract.json"
MARKER="runtime/health/us-prep-missing-${TRADE_DATE}.json"
LOG_FILE="runtime/wsl-us-am-preflight.log"
locked_count=0
if [[ -f "runtime/us/watchlist/${TRADE_DATE}/locked_watchlist.json" ]]; then
  locked_count="$(python - <<PY
import json; p='runtime/us/watchlist/${TRADE_DATE}/locked_watchlist.json'
try:
 d=json.load(open(p)); print(len(d if isinstance(d,list) else d.get('rows',[])))
except Exception: print(0)
PY
)"
fi
if [[ -f "$CONTRACT" && "$locked_count" -ge 10 ]]; then
  echo "[$(date -Is)] [US_AM_PREFLIGHT][OK] trade_date=${TRADE_DATE} locked_watchlist_count=${locked_count}" >> "$LOG_FILE"
  exit 0
fi
echo "[$(date -Is)] [US_AM_PREFLIGHT][MISSING] trade_date=${TRADE_DATE} contract=$([[ -f "$CONTRACT" ]] && echo 1 || echo 0) locked_watchlist_count=${locked_count}" >> "$LOG_FILE"
if scripts/wsl/run-us-prep-recovery.sh >> "$LOG_FILE" 2>&1; then
  exit 0
fi
printf '{"trade_date":"%s","status":"EXIT_ONLY","reason":"prep_missing_after_preflight_recovery","entry_can_proceed":0,"exit_can_proceed":1,"close_can_proceed":1}\n' "$TRADE_DATE" > "$MARKER"
exit 0
