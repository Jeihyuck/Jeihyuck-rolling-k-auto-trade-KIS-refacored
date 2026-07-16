#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime/health runtime/locks
LOG_FILE="runtime/wsl-us-prep-recovery.log"
TRADE_DATE="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
MARKER="runtime/health/us-prep-missing-${TRADE_DATE}.json"
now_kst="$(TZ=Asia/Seoul date +%H%M)"
if [[ "$now_kst" > "2225" ]]; then
  printf '{"trade_date":"%s","status":"EXIT_ONLY","reason":"recovery_cutoff_passed","entry_can_proceed":0,"exit_can_proceed":1,"close_can_proceed":1}\n' "$TRADE_DATE" > "$MARKER"
  echo "[$(date -Is)] [US_PREP_RECOVERY][SKIP] trade_date=${TRADE_DATE} reason=cutoff_passed marker=${MARKER}" >> "$LOG_FILE"
  exit 2
fi
export US_PREP_RECOVERY_RUN=1
export US_PREP_RECOVERY_TRADE_DATE="$TRADE_DATE"
echo "[$(date -Is)] [US_PREP_RECOVERY][START] trade_date=${TRADE_DATE}" >> "$LOG_FILE"
if scripts/wsl/run-us-prep.sh >> "$LOG_FILE" 2>&1; then
  echo "[$(date -Is)] [US_PREP_RECOVERY][OK] trade_date=${TRADE_DATE}" >> "$LOG_FILE"
  exit 0
fi
printf '{"trade_date":"%s","status":"EXIT_ONLY","reason":"recovery_failed","entry_can_proceed":0,"exit_can_proceed":1,"close_can_proceed":1}\n' "$TRADE_DATE" > "$MARKER"
echo "[$(date -Is)] [US_PREP_RECOVERY][FAIL] trade_date=${TRADE_DATE} marker=${MARKER}" >> "$LOG_FILE"
exit 1
