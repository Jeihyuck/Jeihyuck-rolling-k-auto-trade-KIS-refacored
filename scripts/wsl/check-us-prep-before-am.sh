#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime/health
TRADE_DATE="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
CONTRACT="runtime/us/watchlist/${TRADE_DATE}/prep_contract.json"
FINAL30="runtime/us/watchlist/${TRADE_DATE}/final30_scored.json"
MARKER="runtime/health/us-prep-missing-${TRADE_DATE}.json"
LOG_FILE="runtime/wsl-us-am-preflight.log"
read -r contract_exists final30_count db_status db_locked_count ok_reason < <(python - <<PY
import json
from pathlib import Path
trade_date = "${TRADE_DATE}"
contract = Path("${CONTRACT}")
final30 = Path("${FINAL30}")
def count_rows(path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict):
            for key in ("rows", "items", "symbols", "final30_scored"):
                if isinstance(data.get(key), list):
                    return len(data[key])
        return 0
    except Exception:
        return 0
final30_count = count_rows(final30)
db_status = "UNKNOWN"
db_locked_count = 0
try:
    from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
    prep = load_latest_us_prep_status(trade_date, timeout_sec=20) or {}
    db_status = str(prep.get("status") or "UNKNOWN")
    db_locked_count = len(load_locked_us_watchlist(trade_date=trade_date, min_count=1, allow_degraded=True, timeout_sec=20) or [])
except Exception as exc:
    db_status = f"DB_ERROR:{type(exc).__name__}"
contract_ok = contract.exists()
ok_reason = "missing"
if contract_ok and final30_count >= 10:
    ok_reason = "contract_and_final30"
elif db_locked_count >= 10:
    ok_reason = "db_locked_watchlist"
print(int(contract_ok), final30_count, db_status, db_locked_count, ok_reason)
PY
)
if [[ "$ok_reason" != "missing" ]]; then
  echo "[$(date -Is)] [US_AM_PREFLIGHT][OK] trade_date=${TRADE_DATE} reason=${ok_reason} contract=${contract_exists} final30_scored_count=${final30_count} db_status=${db_status} db_locked_count=${db_locked_count}" >> "$LOG_FILE"
  exit 0
fi
echo "[$(date -Is)] [US_AM_PREFLIGHT][MISSING] trade_date=${TRADE_DATE} contract=${contract_exists} final30_scored_count=${final30_count} db_status=${db_status} db_locked_count=${db_locked_count}" >> "$LOG_FILE"
if bash scripts/wsl/run-us-prep-recovery.sh >> "$LOG_FILE" 2>&1; then
  exit 0
fi
printf '{"trade_date":"%s","status":"EXIT_ONLY","reason":"prep_missing_after_preflight_recovery","entry_can_proceed":0,"exit_can_proceed":1,"close_can_proceed":1,"final30_scored_count":%s,"db_locked_count":%s}\n' "$TRADE_DATE" "$final30_count" "$db_locked_count" > "$MARKER"
exit 0
