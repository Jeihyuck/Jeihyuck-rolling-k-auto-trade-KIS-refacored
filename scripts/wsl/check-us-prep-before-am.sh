#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p runtime/health
if [[ -n "${US_TRADE_DATE:-}" ]]; then
  trade_date="${US_TRADE_DATE}"
elif [[ -n "${US_FORCE_NOW:-}" ]]; then
  export FORCE_NOW_INPUT="${US_FORCE_NOW}"
  trade_date="$(python - <<'PYDATE'
from datetime import datetime
from zoneinfo import ZoneInfo
import os
print(datetime.fromisoformat(os.environ["US_FORCE_NOW"]).astimezone(ZoneInfo("America/New_York")).date().isoformat())
PYDATE
)"
else
  trade_date="$(TZ=America/New_York date +%F)"
fi
check() {
python - "$trade_date" <<'PY'
import json, sys
from pathlib import Path
trade_date=sys.argv[1]
base=Path('runtime/us/watchlist')/trade_date
contract=base/'prep_contract.json'
final30=base/'final30_scored.json'
count=0
if final30.exists():
    data=json.loads(final30.read_text() or '[]')
    count=len(data.get('rows', data) if isinstance(data, dict) else data)
if contract.exists() and count >= 10:
    print('OK artifact final30_scored_count=%d' % count); raise SystemExit(0)
try:
    from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
    prep=load_latest_us_prep_status(trade_date) or {}
    rows=load_locked_us_watchlist(trade_date=trade_date, min_count=10, allow_degraded=True) or []
    if (prep.get('status') in {'OK','OK_WITH_WARNINGS'} and len(rows) >= 10) or len(rows) >= 10:
        print('OK db locked_watchlist_count=%d' % len(rows)); raise SystemExit(0)
except Exception as exc:
    print('DB_CHECK_WARN %s' % exc)
raise SystemExit(1)
PY
}
if check; then exit 0; fi
bash scripts/wsl/run-us-prep-recovery.sh || true
if check; then exit 0; fi
cat > "runtime/health/us-prep-missing-${trade_date}.json" <<JSON
{
  "trade_date": "${trade_date}",
  "status": "EXIT_ONLY",
  "reason": "prep_missing_after_preflight_recovery",
  "entry_can_proceed": 0,
  "exit_can_proceed": 1,
  "close_can_proceed": 1
}
JSON
echo "[US_PREP_PREFLIGHT][EXIT_ONLY] trade_date=${trade_date}"
exit 0
