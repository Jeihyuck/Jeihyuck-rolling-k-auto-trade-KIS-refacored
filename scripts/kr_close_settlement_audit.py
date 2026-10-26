"""Read-only KR close settlement audit.

ACK is never a fill. Broker holdings at zero must not silently imply native KIS
execution price, and a manually closed DB position remains economically pending
until every SELL order is matched to durable, priced execution evidence.
No writes to broker, orders, fills, or positions are performed here.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import sqlalchemy as sa


def _number(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0



def _broker_confirmed_nonexecution(order: dict, requested: int) -> int:
    """Count only KIS-proven rejected/cancelled shares, never infer from status.

    An order ACK, terminal DB state, or empty broker holdings is insufficient.
    Verify that one native daily-ccld row identifies this exact SELL order,
    accounts for every requested share, and has no live remaining quantity.
    """
    raw = order.get("response_json") or order.get("broker_confirmation") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return 0
    if not isinstance(raw, dict):
        return 0
    row = raw.get("kis_row") or (
        (raw.get("_reconcile_observation") or {}).get("kis_row")
        if isinstance(raw.get("_reconcile_observation"), dict) else None
    )
    if not isinstance(row, dict):
        # Root-level raw KIS daily-ccld rows are also acceptable.
        row = raw
    status = str(order.get("status") or "").upper()
    terminal_statuses = {
        "CANCELED", "CANCELLED", "REJECTED", "PARTIAL_CANCELED",
        "PARTIALLY_CANCELED", "PARTIAL_CANCELLED", "RECONCILED",
        "PARTIAL_FILLED",
    }
    if status not in terminal_statuses:
        return 0
    order_no = str(order.get("kis_odno") or "").strip()
    row_no = str(row.get("odno") or "").strip()
    if not order_no or not row_no or order_no.zfill(10) != row_no.zfill(10):
        return 0
    if str(row.get("sll_buy_dvsn_cd") or "") != "01":
        return 0
    if str(order.get("code") or "").zfill(6) != str(row.get("pdno") or "").zfill(6):
        return 0
    required = ("ord_qty", "tot_ccld_qty", "cncl_cfrm_qty", "rjct_qty", "rmn_qty")
    if any(row.get(key) in (None, "") for key in required):
        return 0
    values = {}
    for key in required:
        try:
            value = int(str(row[key]).strip())
        except (ValueError, TypeError):
            return 0
        if value < 0:
            return 0
        values[key] = value
    cancelled = values["cncl_cfrm_qty"]
    rejected = values["rjct_qty"]
    executed = values["tot_ccld_qty"]
    if values["ord_qty"] != requested or values["rmn_qty"] != 0:
        return 0
    if not cancelled and not rejected:
        return 0
    if executed + cancelled + rejected != requested:
        return 0
    return cancelled + rejected


def audit_sell_settlement(
    *, orders: list[dict], fills: list[dict], positions: list[dict],
) -> dict[str, Any]:
    """Return missing priced SELL fill quantities, never synthesize executions."""
    by_order: dict[str, list[dict]] = defaultdict(list)
    for fill in fills:
        if str(fill.get("side") or "").upper() == "SELL":
            by_order[str(fill.get("order_id") or "")].append(fill)
    pos_by_cycle = {
        str(row.get("position_cycle_id") or ""): row for row in positions
        if row.get("position_cycle_id")
    }
    gaps = []
    inspected = 0
    for order in orders:
        if str(order.get("side") or "").upper() != "SELL":
            continue
        inspected += 1
        order_id = str(order.get("order_id") or "")
        requested = _number(order.get("qty"))
        broker_nonexecution = _broker_confirmed_nonexecution(order, requested)
        # Only the KIS-confirmed unexecuted part is exempt from priced fills.
        # A cancelled/rejected DB status alone can never hide executed shares.
        required_priced_qty = requested - broker_nonexecution
        rows = by_order.get(order_id, [])
        # Per-order KIS cumulative snapshots must never be summed. Individual
        # trade executions can be summed only in their own evidence lane.
        cumulative = [
            _number(row.get("qty")) for row in rows
            if str(row.get("evidence_kind") or "").upper() == "KIS_ORDER_CUMULATIVE_EXECUTION"
            and float(row.get("price") or 0) > 0
        ]
        individual = [
            _number(row.get("qty")) for row in rows
            if str(row.get("evidence_kind") or "").upper() != "KIS_ORDER_CUMULATIVE_EXECUTION"
            and float(row.get("price") or 0) > 0
        ]
        durable_qty = max(max(cumulative, default=0), sum(individual))
        position = pos_by_cycle.get(str(order.get("position_cycle_id") or ""), {})
        closed = str(position.get("status") or "").upper() == "CLOSED"
        pending_reason = str(position.get("closed_reason") or "").upper() == "BROKER_ZERO_ECONOMIC_SETTLEMENT_PENDING"
        missing_qty = max(0, required_priced_qty - durable_qty)
        # An order status, even FILLED, cannot substitute for native priced fills.
        if missing_qty or pending_reason:
            gaps.append({
                "name": order.get("name") or order.get("code"),
                "code": order.get("code"),
                "order_id": order_id,
                "kis_odno": order.get("kis_odno"),
                "order_status": order.get("status"),
                "requested_sell_qty": requested,
                "broker_confirmed_nonexecution_qty": broker_nonexecution,
                "required_priced_sell_qty": required_priced_qty,
                "durable_priced_sell_qty": durable_qty,
                "missing_priced_sell_qty": missing_qty,
                "db_position_closed": closed,
                "db_economic_pending": pending_reason,
                "reason": "MISSING_NATIVE_KIS_PRICE_OR_QTY" if missing_qty else "MANUAL_SETTLEMENT_NOT_PROVEN",
            })
    return {
        "status": "ECONOMIC_SETTLEMENT_PENDING" if gaps else "PRICED_SELL_EVIDENCE_MATCHED",
        "sell_orders_checked": inspected,
        "gap_count": len(gaps),
        "gaps": gaps,
        "note": "Read-only DB evidence. Broker-native replay requires separate KIS read-only proof.",
    }


def load_snapshot(engine: sa.Engine, *, env: str, epoch: str, day: date) -> dict[str, list[dict]]:
    if not epoch or not day:
        raise ValueError("Trading epoch and local trade date are mandatory")
    from sqlalchemy import text
    # All SQL is SELECT-only. An epoch is required to prevent matching an old
    # practice account position with a new broker account's SELL.
    with engine.connect() as conn:
        orders = [dict(x) for x in conn.execute(text("""
            SELECT order_id, code, side, status, qty, kis_odno,
                   position_cycle_id, response_json
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
              AND created_at >= :start_utc AND created_at < :end_utc
        """), {
            "env": env, "epoch": epoch,
            "start_utc": __import__("datetime").datetime.combine(day, __import__("datetime").time.min, tzinfo=__import__("datetime").timezone(__import__("datetime").timedelta(hours=9))).astimezone(__import__("datetime").timezone.utc),
            "end_utc": __import__("datetime").datetime.combine(day + timedelta(days=1), __import__("datetime").time.min, tzinfo=__import__("datetime").timezone(__import__("datetime").timedelta(hours=9))).astimezone(__import__("datetime").timezone.utc),
        }).mappings()]
        fills = [dict(x) for x in conn.execute(text("""
            SELECT order_id, side, qty, price,
                   fill_meta_json ->> 'evidence_kind' AS evidence_kind
            FROM public.fills WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
        """), {"env": env, "epoch": epoch}).mappings()]
        positions = [dict(x) for x in conn.execute(text("""
            SELECT code, position_cycle_id, status, qty, closed_reason
            FROM public.positions WHERE env=:env AND trading_epoch_id=:epoch
        """), {"env": env, "epoch": epoch}).mappings()]
    # Use the broker's own product name when present, but never trust a JSON
    # label as strategy owner or settlement authority.
    for row in orders:
        try:
            raw = json.loads(row.get("response_json") or "{}")
            row["name"] = raw.get("prdt_name") or row.get("code")
        except (ValueError, TypeError):
            row["name"] = row.get("code")
    return {"orders": orders, "fills": fills, "positions": positions}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KR practice close SELL economic audit (read only)")
    parser.add_argument("--epoch", required=True, help="Exact trading_epoch_id of active KIS practice account")
    parser.add_argument("--trade-date", required=True, help="KR date YYYY-MM-DD")
    parser.add_argument("--env", default="practice", choices=["practice", "real"])
    args = parser.parse_args(argv)
    from trader.db.engine import get_engine
    engine = get_engine()
    report = audit_sell_settlement(**load_snapshot(
        engine, env=args.env, epoch=args.epoch, day=date.fromisoformat(args.trade_date),
    ))
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report["gap_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
