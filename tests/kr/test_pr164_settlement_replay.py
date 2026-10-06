"""KR 2026-10-06 regression: yesterday's holdings promotion and BUY watermark."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.holdings_promotion_repair import repair_promoted_buy_watermark
from trader.reconcile_kis import _promote_open_buy_orders_from_holdings
from tests.kr.test_kr_20261006_execution_convergence import _db, _open_position


def test_buy_watermark_is_atomic_with_position_quantity_and_duplicate_fails_closed():
    engine = _db()
    orders = OrdersRepo(engine)
    positions = PositionsRepo(engine)
    key = "pr164-buy-atomic-039030"
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="039030", market="J",
        side="BUY", ord_type="LIMIT", qty=2, limit_price=529000,
        stage="PB1-ENTRY", client_order_key=key,
        request_json={"pre_order_holding_qty": 0, "requested_qty": 2, "submitted_qty": 2},
        status="ACKED", account_id=get_account_key(env="practice"),
    )
    assert created
    order = orders.get_order_by_client_order_key("practice", key)
    cycle = str(order["position_cycle_id"])
    epoch = str(order["portfolio_epoch_id"])
    kwargs = dict(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="039030", market="J", side="BUY", qty=2, price=529000.0,
        fee=0.0, tax=0.0, filled_at=datetime.now(timezone.utc),
        position_cycle_id=cycle, portfolio_epoch_id=epoch,
        order_id=str(order_id), buy_application_cumulative_qty=2,
    )
    positions.apply_fill(**kwargs)
    position = positions.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="039030", position_cycle_id=cycle, portfolio_epoch_id=epoch,
    )
    assert position["qty"] == 2
    assert position["entry_meta_json"]["broker_truth_buy_applied_orders"][str(order_id)]["qty"] == 2

    import pytest
    with pytest.raises(RuntimeError, match="KR_BUY_APPLICATION_CUMULATIVE_CONFLICT"):
        positions.apply_fill(**kwargs)
    position_after = positions.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="039030", position_cycle_id=cycle, portfolio_epoch_id=epoch,
    )
    assert position_after["qty"] == 2


def test_legacy_holdings_buy_watermark_backfill_does_not_add_quantity_twice():
    engine = _db()
    orders, fills, positions = OrdersRepo(engine), FillsRepo(engine), PositionsRepo(engine)
    position = _open_position(engine, code="036930", qty=4, avg=258250.0)
    cycle = str(position["position_cycle_id"])
    epoch = str(position["portfolio_epoch_id"])
    key = "pr164-legacy-buy-036930"
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="036930", market="J", side="BUY", ord_type="LIMIT",
        qty=4, limit_price=258250.0, stage="PB1-ENTRY", client_order_key=key,
        request_json={"pre_order_holding_qty": 0, "requested_qty": 4, "submitted_qty": 4},
        status="ACKED", account_id=get_account_key(env="practice"),
        position_cycle_id=cycle, portfolio_epoch_id=epoch,
    )
    assert created
    now = datetime.now(timezone.utc)
    orders.upsert_reconciled_order(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="036930", market="J", side="BUY",
        ord_type="LIMIT", qty=4, limit_price=258250.0,
        stage="PB1-ENTRY", client_order_key=key, kis_odno="0000010307",
        status="FILLED", request_json={"pre_order_holding_qty": 0},
        response_json={
            "promotion_source": "kis_holdings",
            "pre_order_holding_qty": 0, "holding_qty": 4, "confirmed_fill_qty": 4,
        }, submitted_at=now, acked_at=now,
    )
    fills.upsert_fill(
        env="practice", run_id=None, order_id=str(order_id),
        kis_odno="0000010307", trade_id="PR164:036930:fill",
        code="036930", market="J", side="BUY",
        qty=4, price=258250.0, fee=0.0, tax=0.0,
        filled_at=now, raw_json={"promotion_source": "kis_holdings_fallback"},
    )
    order = orders.get_order_by_client_order_key("practice", key)
    assert repair_promoted_buy_watermark(engine=engine, env="practice", order=order)
    assert repair_promoted_buy_watermark(engine=engine, env="practice", order=order)
    restored = positions.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="036930", position_cycle_id=cycle, portfolio_epoch_id=epoch,
    )
    assert restored["qty"] == 4
    assert restored["entry_meta_json"]["broker_truth_buy_applied_orders"][str(order_id)]["qty"] == 4


def test_terminal_sell_from_previous_day_uses_saved_baseline_not_today_inference():
    engine = _db()
    orders, fills, positions = OrdersRepo(engine), FillsRepo(engine), PositionsRepo(engine)
    position = _open_position(engine, code="293490", qty=115, avg=9485.826)
    cycle, epoch = str(position["position_cycle_id"]), str(position["portfolio_epoch_id"])
    key = "pr164-previous-day-293490"
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="293490", market="J", side="SELL",
        ord_type="MARKET", qty=37, limit_price=10050.0,
        stage="PROFIT_PROTECT_PARTIAL_1", client_order_key=key,
        request_json={"pre_order_holding_qty": 115, "requested_qty": 37, "submitted_qty": 37},
        status="ACKED", account_id=get_account_key(env="practice"),
        position_cycle_id=cycle, portfolio_epoch_id=epoch,
    )
    assert created
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    orders.upsert_reconciled_order(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="293490", market="J", side="SELL",
        ord_type="MARKET", qty=37, limit_price=10050.0,
        stage="PROFIT_PROTECT_PARTIAL_1", client_order_key=key,
        kis_odno="0000012522", status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
        request_json={"pre_order_holding_qty": 115},
        response_json={"promotion_source": "kis_holdings",
                       "pre_order_holding_qty": 115, "holding_qty": 78,
                       "confirmed_fill_qty": 37},
        submitted_at=yesterday, acked_at=yesterday,
    )
    with engine.begin() as conn:
        conn.execute(sa.update(schema_for_engine(engine).orders).where(
            schema_for_engine(engine).orders.c.order_id == order_id
        ).values(created_at=yesterday))

    for _ in range(2):
        _promote_open_buy_orders_from_holdings(
            env="practice", strategy="pb1_pullback_close",
            ctx_run_id=None, tick_ts=datetime.now(timezone.utc),
            holdings_rows=[{"pdno": "293490", "hldg_qty": "78", "pchs_avg_pric": "9485.826"}],
            orders_repo=orders, fills_repo=fills, positions_repo=positions,
        )
    restored = positions.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="293490", position_cycle_id=cycle, portfolio_epoch_id=epoch,
    )
    assert restored["qty"] == 78
    assert restored["realized_pnl"] == 0.0
