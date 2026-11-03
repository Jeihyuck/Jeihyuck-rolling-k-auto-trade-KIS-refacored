"""Practice-only proof and atomic repair of the 2026-10-27 Infinite TP2 freeze.

Never fabricates a broker fill, changes execution claims, or places orders.
Only a unique fully proved KR_INFINITE cycle can be marked COMPLETE.
A legacy PB1 *mirror* of the exact same broker orders is retired with zero
PB1 realized P&L, so this sleeve cannot be double-booked as PB1.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone, timedelta
from decimal import Decimal, InvalidOperation
import json
import hashlib

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id

KST = timezone(timedelta(hours=9))
SYMBOL = "122630"
OWNER = "KR_INFINITE"


def _obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            pass
    return {}


def _num(value):
    try:
        result = Decimal(str(value).replace(",", ""))
        return result if result.is_finite() and result >= 0 else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _integer(value):
    n = _num(value)
    return int(n) if n is not None and n == n.to_integral_value() else None


def _iso_day(value):
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value or "").replace("-", "")


def validate_frozen_tp2_proof(
    *, state, intents, claims, mirror_positions, mirror_orders,
    broker_balance, balance_source, original_pages, epoch_id,
):
    """Read-only evidence gate: no KIS queries or DB changes."""
    def block(reason):
        return {"state": "BLOCKED", "reason": reason, "updated_rows": 0}

    if (
        balance_source != "api" or not isinstance(broker_balance, dict)
        or broker_balance.get("rt_cd") != "0"
        or broker_balance.get("_kis_disabled") is True
        or broker_balance.get("_diag_stub") is True
        or not isinstance(broker_balance.get("output1"), list)
    ):
        return block("FRESH_KIS_BALANCE_REQUIRED")
    rows = broker_balance["output1"]
    if any(not isinstance(x, dict) for x in rows):
        return block("BROKER_HOLDINGS_ROWS_INVALID")
    matches = [x for x in rows if str(x.get("pdno") or x.get("code") or "").lstrip("A") == SYMBOL]
    if len(matches) > 1 or (matches and _integer(matches[0].get("hldg_qty")) != 0):
        return block("BROKER_POSITION_NOT_FLAT")

    meta = _obj(state.get("metadata"))
    if (
        state.get("strategy_id") != "KR_INFINITE_V1"
        or state.get("symbol") != SYMBOL
        or str(state.get("trading_epoch_id")) != str(epoch_id)
        or str(state.get("status")) != "FROZEN"
        or str(meta.get("profit_stage")) != "TP2_FILLED"
        or meta.get("pending_profit_stage") not in (None, "")
        or state.get("cycle_complete_date") is not None
        or state.get("last_exit_date") is not None
        or not state.get("cycle_id")
    ):
        return block("FROZEN_TP2_CYCLE_NOT_PROVEN")
    cycle = str(state["cycle_id"])
    if not (len(intents) == len(claims) == len(mirror_orders) == 3):
        return block("UNIQUE_THREE_ORDER_PROOF_REQUIRED")
    if not (len(mirror_positions) == 1):
        return block("LEGACY_MIRROR_POSITION_NOT_UNIQUE")
    buys = [x for x in intents if x.get("side") == "BUY"]
    sells = [x for x in intents if x.get("side") in ("SELL_PARTIAL", "SELL_ALL")]
    if len(buys) != 1 or len(sells) != 2:
        return block("INFINITE_TP1_TP2_SET_NOT_UNIQUE")
    buy, tp1s, tp2s = buys[0], [x for x in sells if x.get("side") == "SELL_PARTIAL"], [x for x in sells if x.get("side") == "SELL_ALL"]
    if len(tp1s) != 1 or len(tp2s) != 1:
        return block("INFINITE_STAGE_NOT_UNIQUE")
    tp1, tp2 = tp1s[0], tp2s[0]
    if (
        buy.get("reason") != "NEW_CYCLE_BUY"
        or tp1.get("reason") != "TAKE_PROFIT_TP1"
        or tp2.get("reason") != "TAKE_PROFIT_TP2"
        or not (_iso_day(buy.get("trade_date")) < _iso_day(tp1.get("trade_date")) == _iso_day(tp2.get("trade_date")))
        or _iso_day(state.get("cycle_start_date")) != _iso_day(buy.get("trade_date"))
        or _iso_day(state.get("last_buy_date")) != _iso_day(buy.get("trade_date"))
        or _iso_day(meta.get("last_profit_fill_date")) != _iso_day(tp2.get("trade_date"))
        or _integer(meta.get("last_profit_fill_qty")) != _integer(tp2.get("requested_qty"))
    ):
        return block("INFINITE_CYCLE_DATES_OR_FINAL_STAGE_INVALID")

    quantities = [_integer(i.get("requested_qty")) for i in (buy, tp1, tp2)]
    if any(x is None or x <= 0 for x in quantities) or quantities[0] != sum(quantities[1:]):
        return block("INFINITE_FULL_EXIT_QUANTITY_UNPROVEN")
    if _integer(state.get("units_used")) != 1 or _integer(state.get("core_units_used")) != 1:
        return block("INFINITE_UNIT_HISTORY_NOT_UNIQUE")

    claim_by_key = {str(c.get("action_key") or ""): c for c in claims}
    if len(claim_by_key) != 3 or "" in claim_by_key:
        return block("INFINITE_CLAIMS_NOT_UNIQUE")
    original_proofs = []
    for intent, side_code in ((buy, "02"), (tp1, "01"), (tp2, "01")):
        req_qty = _integer(intent.get("requested_qty"))
        filled_qty = _integer(intent.get("filled_qty"))
        money = _num(intent.get("filled_notional_krw"))
        day = _iso_day(intent.get("trade_date"))
        odno = str(intent.get("broker_order_id") or "")
        intent_meta = _obj(intent.get("metadata"))
        action_key = str(intent_meta.get("execution_claim_action_key") or "")
        claim = claim_by_key.get(action_key)
        if (
            str(intent.get("trading_epoch_id")) != str(epoch_id)
            or intent.get("symbol") != SYMBOL
            or intent.get("strategy_id") != "KR_INFINITE_V1"
            or str(intent.get("cycle_id")) != cycle
            or intent.get("status") != "FILLED"
            or intent_meta.get("strategy_owner") != OWNER
            or str(intent_meta.get("trading_epoch_id")) != str(epoch_id)
            or req_qty is None or req_qty <= 0 or filled_qty != req_qty
            or money is None or money <= 0
            or len(odno) != 10 or not odno.isdigit()
            or not claim or claim.get("action_state") != "SATISFIED"
            or str(claim.get("strategy_owner")) != OWNER
            or str(claim.get("trading_epoch_id")) != str(epoch_id)
            or str(claim.get("lifecycle_id")) != f"{SYMBOL}:{cycle}"
            or _integer(claim.get("target_qty")) != req_qty
            or _integer(claim.get("cumulative_filled_qty")) != req_qty
            or _integer(claim.get("remaining_target_qty")) != 0
        ):
            return block("INFINITE_ORDER_OR_CLAIM_NOT_FINAL")
        page = original_pages.get(day)
        if (
            not isinstance(page, dict) or page.get("rt_cd") != "0"
            or page.get("_sep29_complete") is not True
            or page.get("_diag_stub") is True
            or not isinstance(page.get("output1"), list)
        ):
            return block("ORIGINAL_KIS_FULL_HISTORY_REQUIRED")
        kis_matches = [
            raw for raw in page["output1"] if isinstance(raw, dict)
            and str(raw.get("odno") or "").zfill(10) == odno
            and str(raw.get("pdno") or "").lstrip("A") == SYMBOL
            and str(raw.get("ord_dt") or "") == day
            and str(raw.get("sll_buy_dvsn_cd") or "") == side_code
        ]
        if len(kis_matches) != 1:
            return block("ORIGINAL_KIS_IDENTITY_NOT_UNIQUE")
        kis_row = kis_matches[0]
        if (
            _integer(kis_row.get("tot_ccld_qty")) != req_qty
            or _num(kis_row.get("tot_ccld_amt")) != money
            or _integer(kis_row.get("rmn_qty")) != 0
            or str(kis_row.get("cncl_yn") or "N").upper() != "N"
        ):
            return block("ORIGINAL_KIS_ECONOMICS_CONFLICT")
        original_proofs.append((intent, kis_row))

    original_ids = {
        (_iso_day(i["trade_date"]), str(i["broker_order_id"]))
        for i in intents
    }
    for day, payload in original_pages.items():
        for raw in payload["output1"]:
            if (
                isinstance(raw, dict)
                and str(raw.get("pdno") or "").lstrip("A") == SYMBOL
                and str(raw.get("ord_dt") or "") == day
                and (day, str(raw.get("odno") or "").zfill(10)) not in original_ids
            ):
                return block("UNATTRIBUTED_ORIGINAL_KIS_SYMBOL_ORDER")
    # This PB1 position was a legacy *mirror* of KR_INFINITE executions, not
    # an independent PB1 trade. Never attach its orders to a new PB1 sell cycle.
    pos = mirror_positions[0]
    if (
        pos.get("code") != SYMBOL or pos.get("strategy") != "pb1_pullback_close"
        or pos.get("env") != "practice" or pos.get("status") != "OPEN"
        or str(pos.get("trading_epoch_id")) != str(epoch_id)
        or not pos.get("position_cycle_id")
        or _integer(pos.get("qty")) != quantities[0]
        or _num(pos.get("realized_pnl")) != Decimal(0)
    ):
        return block("LEGACY_POSITION_MIRROR_NOT_PROVEN")
    buy_cost = _num(buy.get("filled_notional_krw"))
    mirror_cost = _num(pos.get("total_cost"))
    if buy_cost is None or mirror_cost is None or abs(mirror_cost - buy_cost) > Decimal(quantities[0]):
        return block("LEGACY_MIRROR_COST_NOT_RECONCILABLE")
    if (\n        _num(pos.get("avg_buy_price")) is None\n        or abs(_num(pos.get("avg_buy_price")) - buy_cost / quantities[0]) > Decimal("1")\n    ):
        return block("LEGACY_MIRROR_AVG_NOT_PROVEN")
    if len({(str(x.get("broker_order_id")), _iso_day(x.get("trade_date"))) for x in intents}) != 3:
        return block("INFINITE_ORIGINAL_ORDER_IDS_COLLIDE")
    for order in mirror_orders:
        if (
            order.get("code") != SYMBOL
            or order.get("strategy") != "pb1_pullback_close"
            or order.get("env") != "practice"
            or str(order.get("trading_epoch_id")) != str(epoch_id)
            or str(order.get("portfolio_epoch_id")) != str(pos.get("portfolio_epoch_id"))
            or order.get("position_cycle_id") is not None
            or (order.get("side") == "BUY" and str(order.get("status")) not in {"RECONCILED", "FILLED"})
            or (order.get("side") == "SELL" and str(order.get("status")) != "FILLED")
            or str(_obj(order.get("request_json")).get("strategy_owner") or "") not in ("",)
        ):
            return block("LEGACY_ORDER_NOT_PASSIVE_MIRROR")
        original_day = order.get("submitted_at") or order.get("created_at")
        if not isinstance(original_day, datetime):
            return block("LEGACY_ORDER_DAY_MISSING")
        if original_day.tzinfo is None:
            original_day = original_day.replace(tzinfo=timezone.utc)
        day = original_day.astimezone(KST).strftime("%Y%m%d")
        odno = str(order.get("kis_odno") or order.get("broker_order_id") or "")
        matched = [
            i for i in (buy, tp1, tp2)
            if str(i["broker_order_id"]) == odno
            and _iso_day(i["trade_date"]) == day
            and ("BUY" if i is buy else "SELL") == order.get("side")
            and _integer(i.get("requested_qty")) == _integer(order.get("qty"))
        ]
        if len(matched) != 1:
            return block("LEGACY_ORDER_NOT_SAME_INFINITE_EXECUTION")

    final_row = original_proofs[-1][1]
    raw_time = str(final_row.get("ord_tmd") or "")
    if len(raw_time) != 6 or not raw_time.isdigit():
        return block("FINAL_KIS_ORDER_TIME_REQUIRED")
    try:
        final_dt = datetime.combine(
            datetime.strptime(_iso_day(tp2["trade_date"]), "%Y%m%d").date(),
            time.fromisoformat(f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:]}"),
            tzinfo=KST,
        )
    except ValueError:
        return block("FINAL_KIS_ORDER_TIME_INVALID")
    return {
        "state": "READY_TO_CERTIFY", "updated_rows": 0,
        "symbol": SYMBOL, "cycle_id": cycle,
        "exit_date": _iso_day(tp2["trade_date"]),
        "last_kis_order_time_utc": final_dt.astimezone(timezone.utc).replace(tzinfo=None),
        "mirror_position_id": pos["position_id"],
        "mirror_cost_gap_krw": str(mirror_cost - buy_cost),
        "original_buy_qty": quantities[0],
        "original_sell_qty": quantities[1] + quantities[2],
        "gross_realized_krw": str(
            sum(_num(x.get("filled_notional_krw")) for x in sells) - buy_cost
        ),
    }


def certify_frozen_infinite_cycle(*, engine, broker_balance, balance_source,
                                  original_pages, apply=False):
    """Atomic current-epoch metadata repair, deliberately no broker submission."""
    def blocked(reason):
        return {"state": "BLOCKED", "reason": reason, "updated_rows": 0}
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        epoch = active_trading_epoch_id(
            conn, env="practice", account_id=get_account_key(env="practice"),
            required=True,
        )
        if not epoch:
            return blocked("ACTIVE_PRACTICE_EPOCH_REQUIRED")
        epoch = str(epoch)
        st_query = sa.text("""SELECT * FROM kr_infinite_state WHERE
            trading_epoch_id=:epoch AND strategy_id='KR_INFINITE_V1'
            AND symbol='122630'""")
        if apply and conn.dialect.name == "postgresql":
            st_query = sa.text(str(st_query) + " FOR UPDATE")
        states = [dict(x) for x in conn.execute(st_query, {"epoch": epoch}).mappings().all()]
        if len(states) != 1:
            return blocked("UNIQUE_INFINITE_STATE_REQUIRED")
        state = states[0]
        iq = sa.text("""SELECT * FROM kr_infinite_order_intents
            WHERE trading_epoch_id=:epoch AND symbol='122630'""")
        intents = [dict(x) for x in conn.execute(iq, {"epoch": epoch}).mappings().all()]
        claim_rows = conn.execute(
            sa.select(schema.execution_claims).where(sa.and_(
                schema.execution_claims.c.env == "practice",
                schema.execution_claims.c.account_id == hashlib.sha256(\n                    get_account_key(env="practice").encode("utf-8")\n                ).hexdigest(),
                schema.execution_claims.c.trading_epoch_id == epoch,
                schema.execution_claims.c.market == "KR",
                schema.execution_claims.c.strategy_owner == OWNER,
                schema.execution_claims.c.lifecycle_id == f"{SYMBOL}:{state['cycle_id']}",
            ))
        ).mappings().all()
        positions_query = sa.select(schema.positions).where(sa.and_(
            schema.positions.c.env == "practice",
            schema.positions.c.trading_epoch_id == epoch,
            schema.positions.c.code == SYMBOL,
            schema.positions.c.status == "OPEN",
        ))
        if apply and conn.dialect.name == "postgresql":
            positions_query = positions_query.with_for_update()
        positions = [dict(x) for x in conn.execute(positions_query).mappings().all()]
        mirror_orders = [dict(x) for x in conn.execute(
            sa.select(schema.orders).where(sa.and_(
                schema.orders.c.env == "practice",
                schema.orders.c.trading_epoch_id == epoch,
                schema.orders.c.code == SYMBOL,
            ))
        ).mappings().all()]
        decision = validate_frozen_tp2_proof(
            state=state, intents=intents, claims=[dict(x) for x in claim_rows],
            mirror_positions=positions, mirror_orders=mirror_orders,
            broker_balance=broker_balance, balance_source=balance_source,
            original_pages=original_pages, epoch_id=epoch,
        )
        if decision["state"] != "READY_TO_CERTIFY" or not apply:
            return decision
        old_meta = _obj(state.get("metadata"))
        proof = {
            "source": "ORIGINAL_KIS_FULL_ORDER_CCLD_AND_EXECUTION_CLAIMS",
            "original_cycle": str(state["cycle_id"]),
            "last_exit_date": decision["exit_date"],
            "legacy_mirror_position_id": str(decision["mirror_position_id"]),
            "mirror_cost_gap_krw": decision["mirror_cost_gap_krw"],
            "result": "VERIFIED_FULL_TP2_EXIT",
            "closed_time_basis": "KIS_ORIGINAL_ORDER_TIME_PROXY_NOT_INDIVIDUAL_EXECUTION",
        }
        mirror = positions[0]
        next_position_meta = {
            **_obj(mirror.get("position_meta")),
            "kr_infinite_legacy_mirror_retired": proof,
        }
        changed_pos = conn.execute(
            sa.update(schema.positions).where(sa.and_(
                schema.positions.c.position_id == mirror["position_id"],
                schema.positions.c.trading_epoch_id == epoch,
                schema.positions.c.code == SYMBOL,
                schema.positions.c.status == "OPEN",
                schema.positions.c.qty == decision["original_buy_qty"],
                schema.positions.c.total_cost == mirror["total_cost"],
                schema.positions.c.realized_pnl == 0,
            )).values(
                qty=0, total_cost=0, status="CLOSED",
                closed_reason="KR_INFINITE_LEGACY_MIRROR_RETIRED",
                closed_ts=decision["last_kis_order_time_utc"],
                position_meta=next_position_meta,
                updated_at=sa.func.now(),
            )
        )
        if changed_pos.rowcount != 1:
            raise RuntimeError("KR_INF_MIRROR_RETIRE_CAS_FAILED")
        changed_inf = conn.execute(sa.text("""UPDATE kr_infinite_state
            SET status='COMPLETE',cycle_complete_date=:exit_date,
                last_exit_date=:exit_date,
                metadata=CAST(:new_meta AS jsonb),
                version=version+1,updated_at=NOW()
            WHERE strategy_id='KR_INFINITE_V1' AND symbol='122630'
              AND trading_epoch_id=:epoch AND cycle_id=:cycle
              AND status='FROZEN' AND version=:version
              AND cycle_complete_date IS NULL AND last_exit_date IS NULL
        """), {
            "exit_date": datetime.strptime(decision["exit_date"], "%Y%m%d").date(),
            "new_meta": json.dumps({
                **old_meta, "frozen_tp2_repair_proof": proof,
                "pending_profit_stage": None,
            }),
            "epoch": epoch, "cycle": state["cycle_id"],
            "version": state["version"],
        })
        if changed_inf.rowcount != 1:
            raise RuntimeError("KR_INF_FROZEN_STATE_REPAIR_CAS_FAILED")
        return {**decision, "state": "CERTIFIED",
                "updated_rows": 2}
