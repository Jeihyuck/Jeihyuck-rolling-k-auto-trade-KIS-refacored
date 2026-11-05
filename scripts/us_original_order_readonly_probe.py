#!/usr/bin/env python3
"""Read-only original-date KIS US order probe. Never submits/cancels/mutates DB.

Operator must explicitly run this in the *practice* WSL session. The query
includes the original US date through the existing KST two-day bridge and
KIS pagination. No account number, token, or raw broker payload is printed.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date


def classify_original_order(rows: list[dict], *, order_no: str, symbol: str,
                            expected_qty: int, trade_date: str) -> dict:
    from trader.us.data_provider import normalize_us_order_status_row
    from trader.us.utils.order_no import normalize_us_order_no

    result = {
        "classification": "UNKNOWN", "symbol": symbol,
        "trade_date": trade_date,
        "broker_order_no_canonical": normalize_us_order_no(order_no),
        "expected_qty": expected_qty,
        "matched_rows": 0, "source": "KIS_PRACTICE_ORIGINAL_DATE_READONLY",
        "side": "SELL", "orders_submitted": 0, "cancels_submitted": 0,
    }
    if not isinstance(rows, list) or expected_qty <= 0:
        return result
    expected_order = normalize_us_order_no(order_no)
    matched = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        n = normalize_us_order_status_row(raw)
        if (normalize_us_order_no(n.get("order_no")) == expected_order
                and n.get("symbol") == symbol and n.get("side") == "SELL"):
            matched.append(n)
    result["matched_rows"] = len(matched)
    if len(matched) != 1:
        return result
    n = matched[0]
    result["status_observed"] = n.get("status")
    result["cumulative_filled_qty"] = n.get("filled_qty")
    result["remaining_qty"] = n.get("remaining_qty")
    result["requested_qty"] = n.get("requested_qty")
    # A reused KIS order number from a different session, a cancel-request
    # row, or an inferred (rather than reported) remaining quantity must NOT
    # prove original order terminality. Keep UNKNOWN on any missing field.
    raw_order_day = str(n.get("trade_date") or "").replace("-", "")
    requested_day = str(trade_date).replace("-", "")
    if (n.get("normalization_result") != "normalized"
            or n.get("broker_operation") != "ORDER"
            or not raw_order_day or raw_order_day != requested_day
            or n.get("requested_qty_present") is not True
            or n.get("filled_qty_present") is not True
            or n.get("remaining_qty_present") is not True
            or n.get("requested_qty") != expected_qty
            or n.get("filled_qty") is None or n.get("remaining_qty") is None
            or n.get("filled_qty") < 0 or n.get("remaining_qty") < 0
            or n.get("filled_qty") + n.get("remaining_qty") > expected_qty):
        return result
    # Only the original broker snapshot can prove terminality.
    status = n.get("status")
    if status == "FILLED" and n["filled_qty"] == expected_qty and n["remaining_qty"] == 0:
        result["classification"] = "PROVEN_FILLED"
    elif status == "CANCELLED" and n["remaining_qty"] == 0:
        result["classification"] = "PROVEN_CANCELLED"
    elif status in {"OPEN", "PARTIALLY_FILLED"} and n["remaining_qty"] > 0:
        result["classification"] = "PROVEN_OPEN"
    return result


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--trade-date", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--order-no", required=True)
    p.add_argument("--expected-qty", required=True, type=int)
    p.add_argument("--confirm-readonly", action="store_true")
    args = p.parse_args()
    date.fromisoformat(args.trade_date)
    if (not args.confirm_readonly or os.getenv("KIS_ENV", "").lower() != "practice"):
        print(json.dumps({"classification": "BLOCKED", "reason": "PRACTICE_READONLY_CONFIRMATION_REQUIRED"}))
        return 42
    from trader.us.execution.kis_us_client import KisUSClient
    try:
        client = KisUSClient(env="practice")
        rows = client.get_us_today_orders(trade_date=args.trade_date)
        result = classify_original_order(
            rows, order_no=args.order_no, symbol=args.symbol.upper(),
            expected_qty=args.expected_qty, trade_date=args.trade_date,
        )
    except Exception as exc:
        result = {"classification": "UNKNOWN", "read_error_type": type(exc).__name__,
                  "orders_submitted": 0, "cancels_submitted": 0}
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("classification") in {
        "PROVEN_FILLED", "PROVEN_CANCELLED", "PROVEN_OPEN"
    } else 43


if __name__ == "__main__":
    raise SystemExit(main())
