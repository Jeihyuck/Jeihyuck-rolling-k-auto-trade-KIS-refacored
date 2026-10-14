#!/usr/bin/env bash
# Non-trading acceptance of dated KRX marketcap top150+top150.
# Never writes a Supabase universe snapshot, never runs PREP or places orders.
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$APP_DIR"

AS_OF="${1:-}"
if [[ ! "$AS_OF" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Usage: bash scripts/wsl/verify-krx-asof-readonly.sh YYYY-MM-DD" >&2
  exit 2
fi
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export KIS_ENV=practice STRATEGY_ENV=practice DRY_RUN=1
export KIS_ORDER_ALLOWED=0 PB1_KIS_ORDER_ALLOWED=0 US_KIS_ORDER_ALLOWED=0
export ALLOW_REAL_ORDER=0 KIS_HTTP_BLOCK=1
export PYTHONPATH="$APP_DIR${PYTHONPATH:+:$PYTHONPATH}"

# A fresh process is essential: no writable repo/DB fixture and no simulated
# marketcap success should be promoted as a real KRX observation.
PYTHON="$APP_DIR/.venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON=python3
AS_OF="$AS_OF" timeout --signal=TERM --kill-after=10s "${KRX_ASOF_PROBE_TIMEOUT_SEC:-210}" "$PYTHON" - <<'PY'
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date
from trader.universe.providers.krx_marketcap_asof import KRXAsOfMarketcapProvider

requested = date.fromisoformat(os.environ["AS_OF"]).isoformat()
provider = KRXAsOfMarketcapProvider()
seen: set[str] = set()
out: dict = {}
for market in ("KOSPI", "KOSDAQ"):
    try:
        rows = provider.get_marketcap_top_with_meta(market, requested, 150)
    except Exception as exc:
        print(f"[KRX_ASOF_READONLY][FAIL] market={market} as_of={requested} "
              f"reason={type(exc).__name__}:{exc}", file=sys.stderr)
        raise SystemExit(1)
    if len(rows) != 150:
        raise SystemExit(f"KRX_ASOF_COUNT_INVALID market={market} rows={len(rows)}")
    for row in rows:
        code = str(row.get("code") or "")
        if (not re.fullmatch(r"[0-9]{6}", code)
                or code in seen
                or row.get("market") != market
                or row.get("actual_as_of") != requested
                or float(row.get("market_cap") or 0) <= 0):
            raise SystemExit(f"KRX_ASOF_MEMBER_INVALID market={market} code={code}")
        seen.add(code)
    out[market] = {"count": len(rows), "sample": [row["code"] for row in rows[:3]]}
if len(seen) != 300:
    raise SystemExit(f"KRX_ASOF_TOTAL_INVALID count={len(seen)}")
print("[KRX_ASOF_READONLY][PASS] " + json.dumps({
    "as_of": requested, "total": len(seen), "by_market": out,
}, ensure_ascii=False, sort_keys=True))
PY
