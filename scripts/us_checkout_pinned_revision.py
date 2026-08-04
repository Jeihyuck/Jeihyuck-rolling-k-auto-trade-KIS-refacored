#!/usr/bin/env python3
"""Resolve the NY trade-day revision from durable prep state for workflow checkout."""
from __future__ import annotations
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from trader.us.db.repos import load_latest_us_prep_status


def main() -> int:
    trade_date = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    row = load_latest_us_prep_status(trade_date) or {}
    result = row.get("result") if isinstance(row.get("result"), dict) else row
    contract = result.get("contract") if isinstance(result.get("contract"), dict) else result
    revision = str(contract.get("run_revision") or contract.get("git_commit_sha") or "").strip()
    if len(revision) != 40 or any(c not in "0123456789abcdefABCDEF" for c in revision):
        raise SystemExit(f"RUN_REVISION_MISSING trade_date={trade_date}")
    print(json.dumps({"trade_date": trade_date, "run_revision": revision}))
    print(f"RUN_REVISION={revision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
