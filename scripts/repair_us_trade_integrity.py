#!/usr/bin/env python3
"""Audit and conservatively quarantine corrupt US practice trade records.

No symbol is guessed.  ``--apply`` is practice-only and only quarantines rows
whose identity contradicts an actual KIS fill keyed by broker order number.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def _blank(value) -> bool:
    return value is None or str(value).strip().lower() in {"", "none", "null"}


def _meta(row: dict) -> dict:
    value = row.get("meta") or {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def audit(orders: list[dict], fills: list[dict]) -> dict:
    key_symbols, order_symbols = defaultdict(set), defaultdict(set)
    actual_by_order = {}
    for row in orders + fills:
        if not _blank(row.get("client_order_key")):
            key_symbols[str(row["client_order_key"])].add(str(row.get("symbol") or "").upper())
        if not _blank(row.get("order_no")):
            order_symbols[str(row["order_no"])].add(str(row.get("symbol") or "").upper())
    for fill in fills:
        if not bool(_meta(fill).get("synthetic")) and not _blank(fill.get("order_no")):
            actual_by_order[str(fill["order_no"])] = fill
    issues = []
    for table, rows in (("us_orders", orders), ("us_fills", fills)):
        for row in rows:
            reasons = []
            if _blank(row.get("client_order_key")): reasons.append("BLANK_CLIENT_ORDER_KEY")
            key = str(row.get("client_order_key") or "")
            ono = str(row.get("order_no") or "")
            if key and len(key_symbols[key]) > 1: reasons.append("CLIENT_KEY_MULTIPLE_SYMBOLS")
            if ono and len(order_symbols[ono]) > 1: reasons.append("ORDER_NO_MULTIPLE_SYMBOLS")
            actual = actual_by_order.get(ono)
            if actual and str(actual.get("symbol") or "").upper() != str(row.get("symbol") or "").upper(): reasons.append("KIS_SYMBOL_MISMATCH")
            if actual and str(actual.get("side") or "").upper() != str(row.get("side") or "").upper(): reasons.append("KIS_SIDE_MISMATCH")
            if table == "us_fills" and actual is not row and actual and bool(_meta(row).get("synthetic")):
                reasons.append("SYNTHETIC_DUPLICATES_KIS_ACTUAL")
            if reasons: issues.append({"table": table, "reasons": reasons, "row": row})
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "orders": len(orders), "fills": len(fills),
            "issue_count": len(issues), "issues": issues}


def apply_integrity_plan(engine, trade_date: str, result: dict) -> dict:
    """Quarantine and delete only identified rows in one transaction."""
    from sqlalchemy import text
    counts = {"quarantined_rows": 0, "corrected_rows": 0,
              "deleted_synthetic_duplicates": 0, "closed_stale_positions": 0,
              "row_ids": []}
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS us_trade_integrity_quarantine (
          id bigserial PRIMARY KEY, quarantined_at timestamptz NOT NULL DEFAULT now(),
          source_table text NOT NULL, reason text NOT NULL, original_row jsonb NOT NULL)"""))
        for issue in result.get("issues", []):
            table = issue.get("table")
            row = issue.get("row") or {}
            row_id = row.get("id")
            if table not in {"us_orders", "us_fills"} or row_id is None:
                raise RuntimeError(f"repair requires primary-key evidence: {table} id={row_id}")
            reason = ",".join(issue.get("reasons") or [])
            conn.execute(text("""INSERT INTO us_trade_integrity_quarantine(source_table,reason,original_row)
                VALUES (:table,:reason,CAST(:row AS jsonb))"""),
                {"table": table, "reason": reason, "row": json.dumps(row, default=str)})
            deleted = conn.execute(text(f"DELETE FROM {table} WHERE id=:id"), {"id": row_id})
            if int(deleted.rowcount or 0) != 1:
                raise RuntimeError(f"concurrent repair conflict: {table} id={row_id}")
            counts["quarantined_rows"] += 1
            counts["row_ids"].append({"table": table, "id": row_id})
            if "SYNTHETIC_DUPLICATES_KIS_ACTUAL" in issue.get("reasons", []):
                counts["deleted_synthetic_duplicates"] += 1
    counts["destructive_mutations"] = counts["quarantined_rows"] + counts["corrected_rows"] + counts["closed_stale_positions"]
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--env", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and args.env.lower() not in {"practice", "vps", "paper"}:
        parser.error("automatic repair is forbidden outside KIS practice")
    from trader.us.db.repos import load_today_fills, load_us_daily_orders_for_report, _get_engine_or_none
    engine = _get_engine_or_none()
    if args.apply and engine is None:
        parser.error("--apply requires a configured practice database")
    if engine is not None:
        from sqlalchemy import text
        with engine.begin() as conn:
            orders = [dict(r) for r in conn.execute(text("SELECT * FROM us_orders WHERE trade_date=:td"), {"td": args.trade_date}).mappings()]
            fills = [dict(r) for r in conn.execute(text("SELECT * FROM us_fills WHERE trade_date=:td"), {"td": args.trade_date}).mappings()]
    else:
        orders = load_us_daily_orders_for_report(args.trade_date) or []
        fills = load_today_fills(args.trade_date) or []
    result = audit(orders, fills)
    out = Path("reports/us_integrity")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{args.trade_date}-before.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    quarantine = {"trade_date": args.trade_date, "applied": bool(args.apply), "rows": result["issues"]}
    (out / f"{args.trade_date}-quarantine.json").write_text(json.dumps(quarantine, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    mutations = apply_integrity_plan(engine, args.trade_date, result) if args.apply else {
        "destructive_mutations": 0, "quarantined_rows": 0, "corrected_rows": 0,
        "deleted_synthetic_duplicates": 0, "closed_stale_positions": 0, "row_ids": []}
    after = {**result, "mode": "apply" if args.apply else "dry_run", **mutations}
    (out / f"{args.trade_date}-after.json").write_text(json.dumps(after, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps({"trade_date": args.trade_date, "issues": result["issue_count"], "applied": args.apply}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
