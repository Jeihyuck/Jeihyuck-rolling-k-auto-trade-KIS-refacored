"""Original-day KIS proof for legacy PB1 orders missing the position cycle.

A single original PB1 BUY of N shares, paired with exactly two original PB1
SELLs totalling N, may be bound to one *unique* current-epoch lifecycle.
This only restores orders.position_cycle_id metadata, NEVER adjusts positions,
fills, execution claims, P&L, or issues broker orders. Default is dry-run.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import json

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id

_KST = timezone(timedelta(hours=9))


def _obj(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            y = json.loads(x)
            return y if isinstance(y, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _number(x):
    try:
        v = Decimal(str(x).replace(",", ""))
        return v if v.is_finite() and v >= 0 else None
    except (TypeError, ValueError, InvalidOperation):
        return None


def _day(x):
    if not isinstance(x, datetime):
        return None
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(_KST).strftime("%Y%m%d")


def certify_legacy_pb1_missing_cycle(
    *, engine, code: str, original_daily_pages: dict[str, dict],
    broker_balance: dict, balance_source: str, apply: bool = False,
) -> dict:
    def blocked(reason):
        return {"state": "BLOCKED", "reason": reason, "updated_orders": 0}
    if (
        len(code) != 6 or not code.isdigit()
        or balance_source != "api"
        or broker_balance.get("rt_cd") != "0"
        or not isinstance(broker_balance.get("output1"), list)
    ):
        return blocked("FRESH_BROKER_PROOF_REQUIRED")
    for row in broker_balance["output1"]:
        if not isinstance(row, dict):
            return blocked("INVALID_KIS_HOLDINGS_ROW")
        row_code = str(row.get("pdno") or row.get("code") or "").lstrip("A")
        if row_code == code and _number(row.get("hldg_qty")) != 0:
            return blocked("BROKER_HOLDING_NONZERO")
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        epoch = active_trading_epoch_id(
            conn, env="practice", account_id=get_account_key(env="practice"),
            required=True,
        )
        if not epoch:
            return blocked("ACTIVE_EPOCH_MISSING")
        q = sa.select(schema.orders).where(sa.and_(
            schema.orders.c.env == "practice",
            schema.orders.c.trading_epoch_id == str(epoch),
            schema.orders.c.code == code,
        ))
        if apply and conn.dialect.name == "postgresql":
            q = q.with_for_update()
        orders = [dict(row) for row in conn.execute(q).mappings().all()]
        pq = sa.select(schema.positions).where(sa.and_(
            schema.positions.c.env == "practice",
            schema.positions.c.trading_epoch_id == str(epoch),
            schema.positions.c.code == code,
            schema.positions.c.status == "OPEN",
        ))
        if apply and conn.dialect.name == "postgresql":
            pq = pq.with_for_update()
        positions = [dict(row) for row in conn.execute(pq).mappings().all()]
        if len(positions) != 1:
            return blocked("EXACT_OPEN_POSITION_NOT_UNIQUE")
        position = positions[0]
        if (
            position.get("strategy") != "pb1_pullback_close"
            or not position.get("position_cycle_id")
            or not position.get("portfolio_epoch_id")
            or position.get("sid") != 1 or position.get("mode") != 1
        ):
            return blocked("POSITION_OWNER_OR_CYCLE_UNPROVEN")
        if len(orders) != 3:
            return blocked("SAME_SYMBOL_ORDER_SET_AMBIGUOUS")
        buys = [o for o in orders if str(o.get("side")).upper() == "BUY"]
        sells = [o for o in orders if str(o.get("side")).upper() == "SELL"]
        if len(buys) != 1 or len(sells) != 2:
            return blocked("LEGACY_BUY_SELL_GROUP_NOT_UNIQUE")
        buy = buys[0]
        quantities = [_number(o.get("qty")) for o in orders]
        if any(x is None or x != x.to_integral_value() or x <= 0 for x in quantities):
            return blocked("ORDER_QUANTITY_INVALID")
        expected = int(quantities[0])  # overwritten from the sole BUY below
        expected = int(_number(buy["qty"]))
        if int(_number(position.get("qty")) or -1) != expected:
            return blocked("DB_PRE_SELL_POSITION_QUANTITY_MISMATCH")
        if sum(int(_number(o["qty"])) for o in sells) != expected:
            return blocked("SELL_ORDER_QUANTITY_NOT_EQUAL_TO_BUY")
        # The three original orders must identify one PB1 account/epoch/sleeve.
        portfolio = str(position["portfolio_epoch_id"])
        for order in orders:
            owner = str(_obj(order.get("request_json")).get("strategy_owner") or "").upper()
            if (
                order.get("strategy") != "pb1_pullback_close"
                or owner not in {"", "PB1", "KR_STANDARD"}
                or order.get("sid") != 1 or order.get("mode") != 1
                or str(order.get("portfolio_epoch_id")) != portfolio
                or str(order.get("trading_epoch_id")) != str(epoch)
                or order.get("position_cycle_id") is not None
                or str(order.get("status") or "").upper()
                   not in ({"RECONCILED", "FILLED"} if order is buy else {"FILLED"})
            ):
                return blocked("ORDER_OWNER_EPOCH_OR_CYCLE_CONFLICT")
        cumulative = {}
        for order in orders:
            day = _day(order.get("submitted_at") or order.get("created_at"))
            odno = str(order.get("kis_odno") or order.get("broker_order_id") or "").strip()
            if not day or len(odno) != 10 or not odno.isdigit():
                return blocked("ORIGINAL_ORDER_DATE_OR_ID_INVALID")
            payload = original_daily_pages.get(day)
            if (
                not isinstance(payload, dict)
                or payload.get("rt_cd") != "0"
                or payload.get("_sep29_complete") is not True
                or not isinstance(payload.get("output1"), list)
            ):
                return blocked("ORIGINAL_DAY_ALL_PAGES_UNVERIFIED")
            want_side = "02" if order is buy else "01"
            matched = [
                raw for raw in payload["output1"]
                if isinstance(raw, dict)
                and str(raw.get("ord_dt") or "") == day
                and str(raw.get("pdno") or "").lstrip("A") == code
                and str(raw.get("odno") or "").zfill(10) == odno
                and str(raw.get("sll_buy_dvsn_cd") or "") == want_side
            ]
            if len(matched) != 1:
                return blocked("ORIGINAL_ORDER_KIS_MATCH_NOT_UNIQUE")
            raw = matched[0]
            fill_qty = _number(raw.get("tot_ccld_qty"))
            fill_amount = _number(raw.get("tot_ccld_amt"))
            if (
                fill_qty != _number(order["qty"])
                or fill_amount is None or fill_amount <= 0
            ):
                return blocked("ORIGINAL_KIS_FULL_FILL_NOT_PROVEN")
            # No same-day exchange order-number reuse/cross-side confusion.
            key = (day, odno)
            if key in cumulative:
                return blocked("ORIGINAL_BROKER_ORDER_ID_COLLISION")
            cumulative[key] = {"qty": int(fill_qty), "notional": str(fill_amount)}
        buy_day = _day(buy.get("submitted_at") or buy.get("created_at"))
        buy_order_no = str(buy.get("kis_odno") or buy.get("broker_order_id") or "")
        broker_cost = _number(cumulative[(buy_day, buy_order_no)]["notional"])
        db_cost = _number(position.get("total_cost"))
        if db_cost is None or broker_cost is None or abs(db_cost-broker_cost) > Decimal("1.00"):
            return blocked("ORIGINAL_BUY_POSITION_COST_MISMATCH")
        sale_sum = sum(
            Decimal(cumulative[(_day(s.get("submitted_at") or s.get("created_at")),
                                str(s.get("kis_odno") or s.get("broker_order_id") or ""))]["notional"])
            for s in sells
        )
        plan = {
            "state": "READY_TO_BIND" if not apply else "BOUND",
            "code": code, "cycle": str(position["position_cycle_id"]),
            "orders": [str(o["order_id"]) for o in orders],
            "buy_qty": expected, "sell_qty": expected,
            "original_sell_notional": str(sale_sum),
            "updated_orders": 0,
        }
        if not apply:
            return plan
        # Exact CAS: no mutation if one identity became linked elsewhere.
        # This is order metadata ONLY: original KIS settlement still must be
        # independently performed by the existing Close all-pages reconciler.
        for order in orders:
            changed = conn.execute(sa.update(schema.orders).where(sa.and_(
                schema.orders.c.order_id == order["order_id"],
                schema.orders.c.env == "practice",
                schema.orders.c.trading_epoch_id == str(epoch),
                schema.orders.c.portfolio_epoch_id == position["portfolio_epoch_id"],
                schema.orders.c.position_cycle_id.is_(None),
                schema.orders.c.code == code,
                schema.orders.c.side == order["side"],
                schema.orders.c.qty == order["qty"],
                schema.orders.c.status == order["status"],
            )).values(position_cycle_id=position["position_cycle_id"]))
            if changed.rowcount != 1:
                raise RuntimeError("KR_LEGACY_CYCLE_REPAIR_CAS_FAILED")
            plan["updated_orders"] += 1
        return plan
