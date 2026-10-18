#!/usr/bin/env bash
# Verify real official KIS KOSPI + KOSDAQ master files. No broker, DB, or scheduler.
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$APP_DIR"
export KIS_ENV=practice STRATEGY_ENV=practice DRY_RUN=1
export KIS_ORDER_ALLOWED=0 PB1_KIS_ORDER_ALLOWED=0 US_KIS_ORDER_ALLOWED=0
export ALLOW_REAL_ORDER=0 KIS_HTTP_BLOCK=1
export PYTHONPATH="$APP_DIR${PYTHONPATH:+:$PYTHONPATH}"
PYTHON="$APP_DIR/.venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON=python3
timeout --signal=TERM --kill-after=10s "${KIS_MASTER_PROBE_TIMEOUT_SEC:-210}" "$PYTHON" - <<'PY'
import json
import re
from trader.time_utils import now_kst
from trader.universe.providers.kis_master_marketcap import KISMasterMarketcapProvider

provider = KISMasterMarketcapProvider()
today = now_kst().date().isoformat()
out = {}
all_codes = set()
for market in ("KOSPI", "KOSDAQ"):
    rows = provider.get_marketcap_top_with_meta(
        market, today, 150, readonly_probe=True)
    assert len(rows) == 150, (market, len(rows))
    assert all(row.get("actual_as_of") is None for row in rows)
    source_day = rows[0]["source_published_on"]
    assert source_day == today, (
        "KIS_MASTER_NOT_CURRENT", market, source_day, today)
    assert all(row["source_published_on"] == source_day for row in rows)
    for row in rows:
        assert re.fullmatch(r"[0-9]{6}", row["code"])
        assert row["market"] == market
        assert row["market_cap"] > 0
        assert row["code"] not in all_codes
        all_codes.add(row["code"])
    out[market] = {"count": len(rows),
                   "published_on": source_day,
                   "sha256": rows[0]["source_sha256"]}
assert len(all_codes) == 300
print("[KIS_MASTER_READONLY][PASS] " + json.dumps(
    {"observed_on": today, "counts": out, "unique": len(all_codes)},
    sort_keys=True))
PY
