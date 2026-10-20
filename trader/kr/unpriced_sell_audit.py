"""KR SELL price/PnL recovery evidence audit (read-only).

October 8, 2026: LS ELECTRIC / SK hynix / LG Electronics quantity-confirmed
SELLs must not be reported as zero realized PnL while KIS execution price is
unresolved. This audit deliberately does not infer a fill from limit prices.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import json
import sqlalchemy as sa

from trader.db.schema import schema_for_engine

_KST = ZoneInfo("Asia/Seoul")


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw:
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _actual_broker_order_number(order: dict[str, Any]) -> bool:
    response = _json_dict(order.get("response_json"))
    output = _json_dict(response.get("output"))
    return any(
        len(str(value or "").strip()) == 10
        and str(value or "").strip().isdigit()
        for value in (output.get("ODNO"), order.get("broker_order_id"), order.get("kis_odno"))
    )


def audit_kr_unpriced_sell_orders(engine, *, env: str, trade_date: date) -> dict[str, Any]:
    """Read-only list of SELLs requiring **exact broker execution-price proof**.

    Caller must not treat qty-confirmed holdings deltas as price/PnL proof.
    No mutation, no synthetic KIS fills, no automatic order retry.
    """
    if env not in {"practice", "real"} or not isinstance(trade_date, date):
        raise ValueError("explicit supported env and KST trade_date required")
    # SQLite stores timezone-aware order timestamps as naive wall-clock
    # values in this repository; PostgreSQL retains real timestamptz UTC.
    # Use the same storage convention for *both* query boundaries.
    if engine.dialect.name == "sqlite":
        start = datetime.combine(trade_date, time.min)
        end = start + timedelta(days=1)
    else:
        start = datetime.combine(trade_date, time.min, _KST).astimezone(timezone.utc)
        end = (datetime.combine(trade_date, time.min, _KST) + timedelta(days=1)).astimezone(timezone.utc)
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        orders = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.orders).where(sa.and_(
                    schema.orders.c.env == env,
                    schema.orders.c.side == "SELL",
                    schema.orders.c.status.in_(("FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", "PARTIAL_FILLED")),
                    schema.orders.c.created_at >= start,
                    schema.orders.c.created_at < end,
                ))
            ).mappings().all()
        ]
    items = []
    for order in orders:
        response = _json_dict(order.get("response_json"))
        confirmed_qty = int(response.get("confirmed_fill_qty") or 0)
        actual_price = response.get("confirmed_fill_price")
        if confirmed_qty <= 0 or actual_price not in (None, "", 0):
            continue
        items.append({
            "code": str(order.get("code") or ""),
            "order_id": str(order.get("order_id") or ""),
            "confirmed_qty": confirmed_qty,
            "broker_order_identified": _actual_broker_order_number(order),
            "price_status": "BROKER_EXECUTION_PRICE_MISSING",
            "realized_pnl_status": "UNRESOLVED_NOT_ZERO",
        })
    return {
        "market": "KR",
        "env": env,
        "trade_date": trade_date.isoformat(),
        "unpriced_sell_count": len(items),
        "unproven_broker_order_count": sum(not it["broker_order_identified"] for it in items),
        "orders": sorted(items, key=lambda x: (x["code"], x["order_id"])),
    }


def audit_kr_pending_sell_orders(engine, *, env: str, trade_date: date) -> dict[str, Any]:
    """Read-only, broker-order-number queue for all unsettled KR SELLs.

    SUBMITTED is an ACK, *not* proof of execution. Holdings-confirmed qty
    without a broker execution price is also unresolved. Never synthesize a
    SELL fill or infer price from its limit/closing quotation.
    """
    if env not in {"practice", "real"} or not isinstance(trade_date, date):
        raise ValueError("explicit supported env and KST trade_date required")
    if engine.dialect.name == "sqlite":
        start = datetime.combine(trade_date, time.min)
        end = start + timedelta(days=1)
    else:
        start = datetime.combine(trade_date, time.min, _KST).astimezone(timezone.utc)
        end = (datetime.combine(trade_date, time.min, _KST) + timedelta(days=1)).astimezone(timezone.utc)
    unresolved_statuses = (
        "SUBMITTED", "ACKED", "ACCEPTED", "UNRESOLVED_ACK",
        "PARTIAL_FILLED", "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
    )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.orders).where(sa.and_(
            schema.orders.c.env == env,
            schema.orders.c.side == "SELL",
            schema.orders.c.status.in_(unresolved_statuses),
            schema.orders.c.created_at >= start,
            schema.orders.c.created_at < end,
        )).order_by(schema.orders.c.created_at)).mappings().all()
    queue = []
    for row in rows:
        order = dict(row)
        response = _json_dict(order.get("response_json"))
        output = _json_dict(response.get("output"))
        broker_order_no = str(
            order.get("kis_odno") or order.get("broker_order_id") or output.get("ODNO") or ""
        ).strip()
        qty = int(response.get("confirmed_fill_qty") or 0)
        price = response.get("confirmed_fill_price")
        queue.append({
            "order_id": str(order.get("order_id") or ""),
            "code": str(order.get("code") or ""),
            "side": "SELL",
            "status": str(order.get("status") or ""),
            "kis_odno": broker_order_no,
            "broker_order_identified": _actual_broker_order_number(order),
            "submitted_qty": int(order.get("qty") or 0),
            "confirmed_qty": qty,
            "broker_price_confirmed": price not in (None, "", 0),
            "required_action": "MATCH_KIS_ORDER_NUMBER_AND_EXECUTION_PRICE",
        })
    return {
        "market": "KR", "env": env, "trade_date": trade_date.isoformat(),
        "unsettled_sell_count": len(queue),
        "missing_order_number_count": sum(not x["broker_order_identified"] for x in queue),
        "missing_broker_price_count": sum(not x["broker_price_confirmed"] for x in queue),
        "orders": queue,
    }

def main(argv: list[str] | None = None) -> int:
    """Read-only operator audit: python -m trader.kr.unpriced_sell_audit."""
    import argparse

    parser = argparse.ArgumentParser(description="Audit KR quantity-only SELLs missing execution price")
    parser.add_argument("--env", required=True, choices=("practice", "real"))
    parser.add_argument("--trade-date", required=True, help="KST YYYY-MM-DD")
    parser.add_argument("--include-pending", action="store_true", help="Include ACKs with unresolved fill/price, read-only")
    args = parser.parse_args(argv)
    from trader.db.engine import get_engine

    trade_date = date.fromisoformat(args.trade_date)
    engine = get_engine()
    try:
        report = (
            audit_kr_pending_sell_orders(engine, env=args.env, trade_date=trade_date)
            if args.include_pending
            else audit_kr_unpriced_sell_orders(engine, env=args.env, trade_date=trade_date)
        )
    finally:
        engine.dispose()
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 2 if report.get("unpriced_sell_count", report.get("unsettled_sell_count", 0)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
