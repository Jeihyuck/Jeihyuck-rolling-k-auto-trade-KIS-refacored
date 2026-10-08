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
                    schema.orders.c.status == "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
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


def main(argv: list[str] | None = None) -> int:
    """Read-only operator audit: python -m trader.kr.unpriced_sell_audit."""
    import argparse

    parser = argparse.ArgumentParser(description="Audit KR quantity-only SELLs missing execution price")
    parser.add_argument("--env", required=True, choices=("practice", "real"))
    parser.add_argument("--trade-date", required=True, help="KST YYYY-MM-DD")
    args = parser.parse_args(argv)
    from trader.db.engine import get_engine

    trade_date = date.fromisoformat(args.trade_date)
    engine = get_engine()
    try:
        report = audit_kr_unpriced_sell_orders(engine, env=args.env, trade_date=trade_date)
    finally:
        engine.dispose()
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 2 if report["unpriced_sell_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
