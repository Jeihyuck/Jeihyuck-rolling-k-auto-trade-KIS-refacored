"""Read-only, account-safe-by-disclosure KR PB1 skip event report.

Usage: python -m scripts.kr_entry_funnel_audit --date YYYY-MM-DD [--out PATH]
Reads the existing credentialed Supabase/Postgres; no KIS calls or DB writes.
Events are scoped to practice pb1_pullback_close runs. The existing ledger
does NOT prove the account/epoch for every old event; report explicitly
states this rather than inventing a trading-epoch reconciliation.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.db.engine import get_engine
from trader.kr.entry_observability import summarize_buy_skips

KST = ZoneInfo("Asia/Seoul")


def report_for_date(*, day: date, engine, limit: int = 10000) -> dict:
    start_kst = datetime.combine(day, time.min, tzinfo=KST)
    stop_kst = datetime.combine(day.fromordinal(day.toordinal() + 1), time.min, tzinfo=KST)
    stmt = sa.text("""
        SELECT e.run_id, e.event_type, e.side, e.code,
               e.stage, e.reasons, e.ts
        FROM public.ledger_events e
        INNER JOIN public.runs r ON r.run_id = e.run_id
        WHERE r.env = 'practice'
          AND r.strategy = 'pb1_pullback_close'
          AND r.started_at >= :start_at
          AND r.started_at < :stop_at
          AND e.event_type = 'ORDER_SKIP'
          AND e.side = 'BUY'
        ORDER BY r.started_at, e.ts, e.code
        LIMIT :max_rows
    """)
    with engine.connect() as conn:
        records = [dict(row) for row in conn.execute(stmt, {
            "start_at": start_kst.astimezone(timezone.utc),
            "stop_at": stop_kst.astimezone(timezone.utc),
            "max_rows": limit,
        }).mappings()]
    report = summarize_buy_skips(records, trade_date=day.isoformat(), max_rows=limit)
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    report["query_scope"] = {
        "environment": "practice", "strategy_owner": "pb1_pullback_close",
        "from_utc": start_kst.astimezone(timezone.utc).isoformat(),
        "to_utc_exclusive": stop_kst.astimezone(timezone.utc).isoformat(),
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--out")
    parser.add_argument("--max-rows", type=int, default=10000)
    args = parser.parse_args()
    if args.max_rows < 1 or args.max_rows > 100000:
        parser.error("--max-rows must be 1..100000")
    day = date.fromisoformat(args.date)
    payload = report_for_date(day=day, engine=get_engine(), limit=args.max_rows)
    output = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.out:
        target = Path(args.out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
