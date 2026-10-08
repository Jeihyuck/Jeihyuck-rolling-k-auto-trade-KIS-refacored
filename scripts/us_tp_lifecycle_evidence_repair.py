"""Read-only by default; repair historical DONE/0 TP counters using KIS actual fills.

Usage: US_DB_URL=... PYTHONPATH=. python -m scripts.us_tp_lifecycle_evidence_repair --since 2026-09-22
Apply only after reviewing the exact output and setting
US_TP_LIFECYCLE_REPAIR_CONFIRM=YES with --apply.

This does not cancel/open/replace any broker order or release a claim.
"""
from __future__ import annotations

import argparse
import os

from sqlalchemy import create_engine, text

from trader.us.profit_capture_evidence import authoritative_tp_fill_for_backfill
from trader.us.db.repos import _active_us_epoch


CANDIDATES = text("""
SELECT l.trade_date, l.symbol, l.stage, l.stage_status, l.position_lifecycle_id,
       l.client_order_key, l.requested_qty, l.cumulative_filled_qty, l.trading_epoch_id,
       o.trade_date AS order_trade_date, o.symbol AS order_symbol,
       o.client_order_key AS order_key, o.qty_requested, o.qty_filled, o.status,
       o.side, o.meta AS order_meta, o.trading_epoch_id AS order_epoch
FROM us_profit_capture_lifecycle AS l
JOIN us_orders AS o
  ON o.trade_date = l.trade_date
 AND o.symbol = l.symbol
 AND o.client_order_key = l.client_order_key
 AND o.trading_epoch_id = l.trading_epoch_id
WHERE l.trade_date >= :since
  AND l.trading_epoch_id = :epoch
  AND l.stage_status = 'DONE'
  AND l.cumulative_filled_qty = 0
  AND l.trading_epoch_id IS NOT NULL
ORDER BY l.trade_date, l.symbol, l.stage
""")


UPDATE_STAGE = text("""
UPDATE us_profit_capture_lifecycle
SET cumulative_filled_qty = :filled,
    state = jsonb_set(
        COALESCE(state, '{}'::jsonb), '{meta}',
        COALESCE(state->'meta', '{}'::jsonb) ||
          jsonb_build_object(:filled_key, CAST(:filled AS integer),
             'tp_fill_counter_repair_source', 'MATCHED_KIS_ACTUAL_FILL'),
        true
    ),
    updated_at = NOW()
WHERE trade_date = :trade_date AND symbol = :symbol
  AND stage = :stage AND position_lifecycle_id = :lifecycle
  AND trading_epoch_id = :epoch AND client_order_key = :order_key
  AND stage_status = 'DONE' AND cumulative_filled_qty = 0
  AND EXISTS (
      SELECT 1 FROM us_orders o
      WHERE o.trade_date = :trade_date AND o.symbol = :symbol
        AND o.client_order_key = :order_key AND o.trading_epoch_id = :epoch
        AND o.status = 'FILLED' AND o.side = 'SELL'
        AND o.qty_requested = :filled AND o.qty_filled = :filled
        AND o.meta->>'profit_capture_stage' = :stage
        AND o.meta->>'fill_evidence_type' IN
            ('KIS_ORDER_CUMULATIVE_ACTUAL','KIS_EXECUTION_ACTUAL','KIS_TERMINAL_CANCEL')
        AND (o.meta->>'cumulative_filled_qty')::integer = :filled
  )
""")


def repair(engine, *, since: str, apply: bool = False) -> dict:
    repaired, provable, rejected = 0, [], 0
    with engine.begin() as conn:
        # Never replay historical practice/real generations when epochs reset.
        # This script is intentionally scoped to the currently active US epoch.
        epoch = _active_us_epoch(conn, required=True)
        if not epoch:
            raise RuntimeError("ACTIVE_US_TRADING_EPOCH_REQUIRED")
        rows = conn.execute(CANDIDATES, {"since": since, "epoch": epoch}).mappings().all()
        for row in rows:
            stage = {
                "trade_date": str(row["trade_date"]), "symbol": row["symbol"],
                "stage": row["stage"], "stage_status": row["stage_status"],
                "client_order_key": row["client_order_key"],
                "requested_qty": row["requested_qty"],
                "cumulative_filled_qty": row["cumulative_filled_qty"],
                "trading_epoch_id": str(row["trading_epoch_id"]),
            }
            order = {
                "trade_date": str(row["order_trade_date"]),
                "symbol": row["order_symbol"], "client_order_key": row["order_key"],
                "qty_requested": row["qty_requested"], "qty_filled": row["qty_filled"],
                "status": row["status"], "side": row["side"],
                "meta": row["order_meta"],
                "trading_epoch_id": str(row["order_epoch"]),
            }
            qty = authoritative_tp_fill_for_backfill(stage, order)
            if qty is None:
                rejected += 1
                continue
            provable.append((stage["trade_date"], stage["symbol"], stage["stage"], qty))
            if apply:
                result = conn.execute(UPDATE_STAGE, {
                    "filled": qty, "filled_key": f"{stage['stage']}_filled_qty",
                    "trade_date": stage["trade_date"], "symbol": stage["symbol"],
                    "stage": stage["stage"],
                    "lifecycle": row["position_lifecycle_id"],
                    "epoch": stage["trading_epoch_id"],
                    "order_key": stage["client_order_key"],
                })
                repaired += result.rowcount
        return {"candidates": len(rows), "provable": provable,
                "rejected_unproven": rejected, "applied": repaired}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="2026-09-22")
    parser.add_argument("--apply", action="store_true", help="Requires explicit env confirmation")
    args = parser.parse_args()
    if args.apply and os.getenv("US_TP_LIFECYCLE_REPAIR_CONFIRM") != "YES":
        parser.error("--apply requires US_TP_LIFECYCLE_REPAIR_CONFIRM=YES")
    url = os.getenv("US_DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        parser.error("US_DB_URL or DATABASE_URL is required")
    result = repair(create_engine(url), since=args.since, apply=args.apply)
    print(("APPLIED" if args.apply else "DRY_RUN"), result)


if __name__ == "__main__":
    main()
