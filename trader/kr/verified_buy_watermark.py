"""Guarded certification of an already settled KR PB1 BUY application.

Only restores the per-order application watermark after exact KIS daily-ccld,
a fresh balance, a durable order and fill, and the active-cycle position agree.
NEVER changes quantity, cost basis, P&L, order status, or places a broker order.
"""
from __future__ import annotations

from datetime import timezone, timedelta
from decimal import Decimal, InvalidOperation
import json

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.schema import schema_for_engine, uuid_value_for_url
from trader.db.trading_epoch import active_trading_epoch_id

_KST = timezone(timedelta(hours=9))


def _obj(raw):
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _int(raw):
    try:
        number = Decimal(str(raw).replace(",", ""))
        if number != number.to_integral_value() or number < 0:
            return None
        return int(number)
    except (TypeError, ValueError, InvalidOperation):
        return None


def _money(raw):
    try:
        number = Decimal(str(raw).replace(",", ""))
        return number if number.is_finite() and number >= 0 else None
    except (TypeError, ValueError, InvalidOperation):
        return None


def certify_existing_pb1_buy_watermark(
    *, engine, order_id: str, kis_balance: dict, balance_source: str,
    original_day_ccld: dict, apply: bool = False,
) -> dict:
    """Strict practice-only single-cycle original-broker-proof certifier."""
    def block(reason):
        return {"state": "BLOCKED", "reason": reason, "applied": False}
    if balance_source != "api" or kis_balance.get("rt_cd") != "0":
        return block("FRESH_KIS_BALANCE_REQUIRED")
    if original_day_ccld.get("rt_cd") != "0":
        return block("ORIGINAL_DAY_KIS_CCLD_REQUIRED")
    if not isinstance(kis_balance.get("output1"), list) or not isinstance(original_day_ccld.get("output1"), list):
        return block("BROKER_ROWS_UNVERIFIED")

    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        epoch = active_trading_epoch_id(
            conn, env="practice", account_id=get_account_key(env="practice"),
            required=True,
        )
        key = uuid_value_for_url(str(engine.url), order_id)
        order_query = sa.select(schema.orders).where(
            schema.orders.c.order_id == key
        )
        if apply and conn.dialect.name == "postgresql":
            order_query = order_query.with_for_update()
        order = conn.execute(order_query).mappings().first()
        if not order:
            return block("DURABLE_ORDER_NOT_FOUND")
        order = dict(order)
        code = str(order.get("code") or "")
        cycle = str(order.get("position_cycle_id") or "")
        portfolio = str(order.get("portfolio_epoch_id") or "")
        if (
            str(order.get("env")) != "practice"
            or str(order.get("strategy")) != "pb1_pullback_close"
            or str(order.get("side")).upper() != "BUY"
            or str(order.get("status")).upper() != "FILLED"
            or str(order.get("trading_epoch_id")) != str(epoch)
            or len(code) != 6 or not code.isdigit()
            or not cycle or not portfolio
        ):
            return block("DURABLE_OWNER_EPOCH_OR_LIFECYCLE_CONFLICT")
        request = _obj(order.get("request_json"))
        if str(request.get("strategy_owner") or "").upper() not in {"PB1", "KR_STANDARD"}:
            return block("STRATEGY_OWNER_NOT_PROVEN")
        if _int(request.get("pre_order_holding_qty")) != 0:
            # No historical lot/cost reconstruction from a mixed position.
            return block("NONZERO_PREORDER_BASELINE_UNSUPPORTED")
        odno = str(order.get("kis_odno") or order.get("broker_order_id") or "")
        qty = _int(order.get("qty"))
        if len(odno) != 10 or not odno.isdigit() or not qty or qty < 1:
            return block("ORIGINAL_BROKER_ORDER_IDENTITY_INVALID")
        if not order.get("created_at"):
            return block("ORIGINAL_ORDER_DATE_MISSING")
        created_at = order["created_at"]
        if created_at.tzinfo is None:
            # SQLAlchemy's legacy SQLite fixture may drop tzinfo; persisted
            # database timestamps are UTC, never process-local time.
            created_at = created_at.replace(tzinfo=timezone.utc)
        order_date = created_at.astimezone(_KST).strftime("%Y%m%d")
        ccld_rows = [
            row for row in original_day_ccld["output1"]
            if isinstance(row, dict)
            and str(row.get("ord_dt") or "") == order_date
            and str(row.get("pdno") or "") == code
            and str(row.get("odno") or "").zfill(10) == odno
            and str(row.get("sll_buy_dvsn_cd") or "") == "02"
        ]
        if len(ccld_rows) != 1:
            return block("ORIGINAL_KIS_BUY_NOT_UNIQUE")
        ccld = ccld_rows[0]
        broker_qty = _int(ccld.get("tot_ccld_qty"))
        broker_amount = _money(ccld.get("tot_ccld_amt"))
        if (
            broker_qty != qty or broker_amount is None or broker_amount <= 0
            or _int(ccld.get("rmn_qty")) != 0
            or str(ccld.get("cncl_yn") or "N").upper() != "N"
        ):
            return block("ORIGINAL_KIS_CUMULATIVE_ECONOMICS_UNVERIFIED")
        ack = _obj(order.get("response_json"))
        if (
            _int(ack.get("confirmed_fill_qty")) != qty
            or _money(ack.get("confirmed_fill_notional")) != broker_amount
        ):
            return block("ORIGINAL_ORDER_CUMULATIVE_WATERMARK_CONFLICT")

        fills = [
            dict(row) for row in conn.execute(
                sa.select(schema.fills).where(sa.and_(
                    schema.fills.c.env == "practice",
                    schema.fills.c.order_id == key,
                    schema.fills.c.side == "BUY",
                    schema.fills.c.position_cycle_id == cycle,
                    schema.fills.c.portfolio_epoch_id == portfolio,
                ))
            ).mappings().all()
        ]
        if len(fills) != 1:
            return block("ORIGINAL_BUY_FILL_NOT_UNIQUE")
        fill = fills[0]
        fill_meta = _obj(fill.get("fill_meta_json"))
        original_fill = _obj(fill.get("raw_json"))
        original_kis = _obj(original_fill.get("kis_response"))
        if (
            str(fill_meta.get("evidence_kind")) != "KIS_ORDER_CUMULATIVE_EXECUTION"
            or str(fill.get("kis_odno") or "") != odno
            or _int(fill.get("qty")) != qty
            or _int(original_kis.get("tot_ccld_qty")) != qty
            or _money(original_kis.get("tot_ccld_amt")) != broker_amount
        ):
            return block("ORIGINAL_BUY_FILL_EVIDENCE_CONFLICT")
        other_order_count = conn.execute(
            sa.select(sa.func.count()).select_from(schema.orders).where(sa.and_(
                schema.orders.c.env == "practice",
                schema.orders.c.code == code,
                schema.orders.c.trading_epoch_id == str(epoch),
                schema.orders.c.position_cycle_id == cycle,
                schema.orders.c.order_id != key,
            ))
        ).scalar_one()
        if other_order_count:
            return block("OTHER_SAME_CYCLE_ORDER_PRESENT")
        positions = [
            dict(row) for row in conn.execute(
                sa.select(schema.positions).where(sa.and_(
                    schema.positions.c.env == "practice",
                    schema.positions.c.code == code,
                    schema.positions.c.trading_epoch_id == str(epoch),
                    schema.positions.c.status == "OPEN",
                ))
                .with_for_update() if apply and conn.dialect.name == "postgresql"
                else sa.select(schema.positions).where(sa.and_(
                    schema.positions.c.env == "practice",
                    schema.positions.c.code == code,
                    schema.positions.c.trading_epoch_id == str(epoch),
                    schema.positions.c.status == "OPEN",
                ))
            ).mappings().all()
        ]
        if len(positions) != 1:
            return block("OPEN_POSITION_OWNER_AMBIGUOUS")
        pos = positions[0]
        if (
            str(pos.get("strategy")) != "pb1_pullback_close"
            or str(pos.get("position_cycle_id")) != cycle
            or str(pos.get("portfolio_epoch_id")) != portfolio
            or _int(pos.get("qty")) != qty
            or _money(pos.get("total_cost")) is None
            or abs(_money(pos.get("total_cost")) - broker_amount) > Decimal("1.00")
        ):
            return block("OPEN_POSITION_CYCLE_OR_ECONOMICS_MISMATCH")
        balances = [
            row for row in kis_balance["output1"]
            if isinstance(row, dict) and str(row.get("pdno") or row.get("code") or "") == code
        ]
        if len(balances) != 1:
            return block("FRESH_BROKER_HOLDING_NOT_UNIQUE")
        balance = balances[0]
        if (
            _int(balance.get("hldg_qty")) != qty
            or _money(balance.get("pchs_amt")) is None
            or abs(_money(balance.get("pchs_amt")) - broker_amount) > Decimal("1.00")
        ):
            return block("FRESH_KIS_POSITION_ECONOMICS_MISMATCH")
        meta = _obj(pos.get("entry_meta_json"))
        applied = _obj(meta.get("broker_truth_buy_applied_orders"))
        previous = _obj(applied.get(order_id))
        if previous:
            if (
                _int(previous.get("qty")) != qty
                or _money(previous.get("notional")) != broker_amount
            ):
                return block("PREVIOUS_APPLICATION_WATERMARK_CONFLICT")
            return {"state": "ALREADY_CERTIFIED", "applied": False, "code": code, "qty": qty}
        if not apply:
            return {"state": "READY_TO_CERTIFY", "applied": False, "code": code,
                    "qty": qty, "broker_notional": str(broker_amount)}
        # CAS and row-level lock: metadata only. NEVER change qty, cost, PnL or owner.
        applied[order_id] = {"qty": qty, "notional": float(broker_amount),
                             "fee": 0.0, "tax": 0.0}
        updated = dict(meta)
        updated["broker_truth_buy_applied_orders"] = applied
        result = conn.execute(sa.update(schema.positions).where(sa.and_(
            schema.positions.c.position_id == pos["position_id"],
            schema.positions.c.env == "practice",
            schema.positions.c.strategy == "pb1_pullback_close",
            schema.positions.c.code == code,
            schema.positions.c.trading_epoch_id == str(epoch),
            schema.positions.c.position_cycle_id == cycle,
            schema.positions.c.portfolio_epoch_id == portfolio,
            schema.positions.c.status == "OPEN",
            schema.positions.c.qty == qty,
            schema.positions.c.total_cost == pos["total_cost"],
        )).values(entry_meta_json=updated))
        if result.rowcount != 1:
            raise RuntimeError("KR_CERTIFY_BUY_WATERMARK_CAS_FAILED")
        return {"state": "CERTIFIED", "applied": True, "code": code,
                "qty": qty, "broker_notional": str(broker_amount)}
