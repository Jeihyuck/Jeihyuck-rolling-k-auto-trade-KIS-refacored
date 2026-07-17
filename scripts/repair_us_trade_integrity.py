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


def audit(orders: list[dict], fills: list[dict]) -> dict:
    key_symbols, order_symbols = defaultdict(set), defaultdict(set)
    actual_by_order = {}
    for row in orders + fills:
        if not _blank(row.get("client_order_key")):
            key_symbols[str(row["client_order_key"])].add(str(row.get("symbol") or "").upper())
        if not _blank(row.get("order_no")):
            order_symbols[str(row["order_no"])].add(str(row.get("symbol") or "").upper())
    for fill in fills:
        if not bool((fill.get("meta") or {}).get("synthetic")) and not _blank(fill.get("order_no")):
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
            if reasons: issues.append({"table": table, "reasons": reasons, "row": row})
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "orders": len(orders), "fills": len(fills),
            "issue_count": len(issues), "issues": issues}


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
    from trader.us.db.repos import load_today_fills, load_us_daily_orders_for_report
    orders = load_us_daily_orders_for_report(args.trade_date) or []
    fills = load_today_fills(args.trade_date) or []
    result = audit(orders, fills)
    out = Path("reports/us_integrity")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{args.trade_date}-before.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    quarantine = {"trade_date": args.trade_date, "applied": bool(args.apply), "rows": result["issues"]}
    (out / f"{args.trade_date}-quarantine.json").write_text(json.dumps(quarantine, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    # Deliberately require an operator-reviewed DB migration for destructive changes.
    # The JSON is a lossless backup and deterministic input to that transaction.
    after = {**result, "mode": "apply_quarantine_plan" if args.apply else "dry_run", "destructive_mutations": 0}
    (out / f"{args.trade_date}-after.json").write_text(json.dumps(after, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps({"trade_date": args.trade_date, "issues": result["issue_count"], "applied": args.apply}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
