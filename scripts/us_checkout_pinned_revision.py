#!/usr/bin/env python3
"""Resolve the NY trade-day revision from durable prep state for workflow checkout."""
from __future__ import annotations
import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date")
    args = parser.parse_args()
    trade_date = args.trade_date or datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    datetime.strptime(trade_date, "%Y-%m-%d")
    db_url = (os.getenv("PBCORE_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        raise SystemExit("DB_URL_MISSING")
    import psycopg

    with psycopg.connect(db_url, connect_timeout=10) as conn:
        row = conn.execute(
            """SELECT result FROM us_agent_runs WHERE trade_date=%s
               AND agent_name IN ('us_prep','us_prep_dual_agent') AND mode='prep'
               ORDER BY COALESCE(finished_at,started_at) DESC,started_at DESC LIMIT 1""",
            (trade_date,),
        ).fetchone()
    if not row:
        raise SystemExit(f"PREP_CONTRACT_MISSING trade_date={trade_date}")
    result = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")
    contract = result.get("contract") if isinstance(result.get("contract"), dict) else result
    revision = str(contract.get("run_revision") or contract.get("git_commit_sha") or "").strip()
    if len(revision) != 40 or any(c not in "0123456789abcdefABCDEF" for c in revision):
        raise SystemExit(f"RUN_REVISION_MISSING trade_date={trade_date}")
    print(json.dumps({"trade_date": trade_date, "run_revision": revision}))
    print(f"RUN_REVISION={revision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
