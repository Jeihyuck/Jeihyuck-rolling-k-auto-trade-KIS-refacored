from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.reconcile_kis import _promote_open_buy_orders_from_holdings
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    return engine


def _open_position(engine, *, code: str, qty: int, avg: float):
    return PositionsRepo(engine).get_or_create_imported_cycle_for_kis_holding(
        env="practice",
        strategy="pb1_pullback_close",
        account_id=get_account_key(env="practice"),
        sid=1,
        mode=1,
        code=code,
        market="J",
        qty=qty,
        avg_price=avg,
    )[0]


def test_holdings_fallback_closes_sell_claim_and_converges_qty_without_price():
    engine = _db()
    orders = OrdersRepo(engine)
    fills = FillsRepo(engine)
    positions = PositionsRepo(engine)
    position = _open_position(engine, code="293490", qty=115, avg=9485.826)
    cycle = str(position["position_cycle_id"])
    epoch = str(position["portfolio_epoch_id"])
    client_key = "kr-20261006-293490-profit-protect"
    action = "SELL:PROFIT_CAPTURE:PROFIT_PROTECT_PARTIAL_1"

    order_id, created = orders.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        market="J",
        side="SELL",
        ord_type="MARKET",
        qty=37,
        limit_price=10050.0,
        stage="PROFIT_PROTECT_PARTIAL_1",
        client_order_key=client_key,
        request_json={
            "pre_order_holding_qty": 115,
            "pre_order_orderable_qty": 115,
            "requested_qty": 37,
            "submitted_qty": 37,
            "position_lifecycle_id": cycle,
            "semantic_action": action,
            "strategy_owner": "KR_STANDARD",
            "execution_meta_update": {"giveback_protect_done": True},
        },
        status="ACKED",
        account_id=get_account_key(env="practice"),
        portfolio_epoch_id=epoch,
        position_cycle_id=cycle,
    )
    assert created
    _, claim = orders.claim_execution_action(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=cycle,
        action=action,
        trade_date=date(2026, 10, 6),
        attempt_id="attempt-293490",
        requested_qty=37,
        client_order_key=client_key,
        fresh_validation=True,
    )
    assert claim.acquired
    orders.record_execution_claim_for_order(
        client_key,
        state="ACKED",
        cumulative_filled_qty=None,
        authoritative=False,
    )
    # Reproduce the live Oct-06 state: the old holdings fallback already
    # terminalized the order, but did not close the execution claim or reduce
    # the PB1 position quantity.
    terminal_ts = datetime(2026, 10, 6, 0, 48, tzinfo=timezone.utc)
    orders.upsert_reconciled_order(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        market="J",
        side="SELL",
        ord_type="MARKET",
        qty=37,
        limit_price=10050.0,
        stage="PROFIT_PROTECT_PARTIAL_1",
        client_order_key=client_key,
        kis_odno="0000012522",
        status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
        request_json={
            "pre_order_holding_qty": 115,
            "pre_order_orderable_qty": 115,
            "requested_qty": 37,
            "submitted_qty": 37,
            "position_lifecycle_id": cycle,
            "semantic_action": action,
            "strategy_owner": "KR_STANDARD",
            "execution_meta_update": {"giveback_protect_done": True},
        },
        response_json={
            "promotion_source": "kis_holdings",
            "pre_order_holding_qty": 115,
            "holding_qty": 78,
            "holding_delta": 37,
            "requested_qty": 37,
            "submitted_qty": 37,
            "confirmed_fill_qty": 37,
            "confirmed_fill_price": None,
            "fill_price_source": "UNRESOLVED",
            "realized_pnl_status": "REALIZED_PNL_UNRESOLVED",
        },
        submitted_at=terminal_ts,
        acked_at=terminal_ts,
    )

    result = _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 10, 6, 0, 48, tzinfo=timezone.utc),
        holdings_rows=[
            {"pdno": "293490", "hldg_qty": "78", "pchs_avg_pric": "9485.826"}
        ],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )

    stored = positions.get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="293490",
        position_cycle_id=cycle,
        portfolio_epoch_id=epoch,
    )
    snapshot = orders.get_execution_action_snapshot(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=cycle,
        action=action,
    )
    order = orders.get_order_by_client_order_key("practice", client_key)

    assert result["orders"] == 1
    assert result["fills"] == 0
    assert order["status"] == "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"
    assert stored["qty"] == 78
    assert stored["total_cost"] == pytest.approx(9485.826 * 78)
    assert stored["realized_pnl"] == pytest.approx(0.0)
    assert stored["position_meta"]["realized_pnl_status"] == "REALIZED_PNL_UNRESOLVED"
    assert stored["position_meta"]["giveback_protect_done"] is True
    assert snapshot.action_state == "SATISFIED"
    assert snapshot.active_attempt_id is None
    assert snapshot.cumulative_filled_qty == 37
    assert snapshot.remaining_target_qty == 0

    with engine.connect() as conn:
        sell_fills = conn.execute(
            sa.select(schema_for_engine(engine).fills).where(
                schema_for_engine(engine).fills.c.order_id == order_id
            )
        ).mappings().all()
    assert sell_fills == []


def test_sell_quantity_reconcile_books_later_price_once_without_double_qty_decrement():
    engine = _db()
    positions = PositionsRepo(engine)
    position = _open_position(engine, code="123450", qty=10, avg=100.0)
    cycle = str(position["position_cycle_id"])
    epoch = str(position["portfolio_epoch_id"])
    filled_at = datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc)

    first = positions.reconcile_sell_execution(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        market="J",
        confirmed_cumulative_qty=2,
        fill_price=None,
        filled_at=filled_at,
        position_cycle_id=cycle,
        portfolio_epoch_id=epoch,
        order_id="sell-order-1",
        pre_order_holding_qty=10,
        broker_holding_qty=8,
    )
    second = positions.reconcile_sell_execution(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        market="J",
        confirmed_cumulative_qty=2,
        fill_price=110.0,
        filled_at=filled_at,
        position_cycle_id=cycle,
        portfolio_epoch_id=epoch,
        order_id="sell-order-1",
        pre_order_holding_qty=10,
        broker_holding_qty=None,
    )
    third = positions.reconcile_sell_execution(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        market="J",
        confirmed_cumulative_qty=2,
        fill_price=110.0,
        filled_at=filled_at,
        position_cycle_id=cycle,
        portfolio_epoch_id=epoch,
        order_id="sell-order-1",
        pre_order_holding_qty=10,
        broker_holding_qty=None,
    )

    stored = positions.get_position(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="123450",
        position_cycle_id=cycle,
        portfolio_epoch_id=epoch,
    )
    assert first["qty_applied"] == 2
    assert first["pnl_qty_applied"] == 0
    assert second["qty_applied"] == 0
    assert second["pnl_qty_applied"] == 2
    assert third["qty_applied"] == 0
    assert third["pnl_qty_applied"] == 0
    assert stored["qty"] == 8
    assert stored["total_cost"] == pytest.approx(800.0)
    assert stored["realized_pnl"] == pytest.approx(20.0)
    assert stored["position_meta"]["realized_pnl_status"] == "CONFIRMED"


def test_holdings_fallback_satisfies_buy_execution_claim():
    engine = _db()
    orders = OrdersRepo(engine)
    fills = FillsRepo(engine)
    positions = PositionsRepo(engine)
    client_key = "kr-20261006-buy-claim"
    lifecycle = "PB1_ENTRY:pb1_pullback_close:J:1:039030"

    order_id, created = orders.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="039030",
        market="J",
        side="BUY",
        ord_type="LIMIT",
        qty=2,
        limit_price=531000.0,
        stage="PB1-AM",
        client_order_key=client_key,
        request_json={
            "pre_order_holding_qty": 0,
            "requested_qty": 2,
            "submitted_qty": 2,
        },
        status="ACKED",
        account_id=get_account_key(env="practice"),
    )
    assert created
    _, claim = orders.claim_execution_action(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle,
        action="BUY_ENTRY:PB1-AM",
        trade_date=date(2026, 10, 6),
        attempt_id="attempt-buy-039030",
        requested_qty=2,
        client_order_key=client_key,
        fresh_validation=True,
    )
    assert claim.acquired
    orders.record_execution_claim_for_order(
        client_key,
        state="ACKED",
        cumulative_filled_qty=None,
        authoritative=False,
    )

    _promote_open_buy_orders_from_holdings(
        env="practice",
        strategy="pb1_pullback_close",
        ctx_run_id=None,
        tick_ts=datetime(2026, 10, 6, 0, 33, tzinfo=timezone.utc),
        holdings_rows=[
            {"pdno": "039030", "hldg_qty": "2", "pchs_avg_pric": "529000"}
        ],
        orders_repo=orders,
        fills_repo=fills,
        positions_repo=positions,
    )

    snapshot = orders.get_execution_action_snapshot(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle,
        action="BUY_ENTRY:PB1-AM",
    )
    assert snapshot.action_state == "SATISFIED"
    assert snapshot.active_attempt_id is None
    assert snapshot.cumulative_filled_qty == 2
    assert snapshot.remaining_target_qty == 0
    assert orders.get_order_by_client_order_key("practice", client_key)["status"] == "FILLED"


def test_sell_reconcile_rejects_unattributed_db_quantity_drift():
    engine = _db()
    positions = PositionsRepo(engine)
    current = _open_position(engine, code="123451", qty=12, avg=100.0)
    with pytest.raises(RuntimeError, match="KR_SELL_RECONCILE_DB_BASELINE_DRIFT"):
        positions.reconcile_sell_execution(
            env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
            code="123451", market="J", confirmed_cumulative_qty=3,
            fill_price=None, filled_at=datetime(2026, 10, 6, 1, tzinfo=timezone.utc),
            position_cycle_id=str(current["position_cycle_id"]),
            portfolio_epoch_id=str(current["portfolio_epoch_id"]),
            order_id="sell-unrelated-drift",
            pre_order_holding_qty=10, broker_holding_qty=7,
        )
    stored = positions.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="123451", position_cycle_id=str(current["position_cycle_id"]),
        portfolio_epoch_id=str(current["portfolio_epoch_id"]),
    )
    assert stored["qty"] == 12


def test_closed_full_sell_accepts_late_price_without_reopening_or_double_booking():
    engine = _db()
    repo = PositionsRepo(engine)
    pos = _open_position(engine, code="123452", qty=5, avg=100.0)
    when = datetime(2026, 10, 6, 1, tzinfo=timezone.utc)
    kwargs = dict(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="123452", market="J", confirmed_cumulative_qty=5,
        filled_at=when, position_cycle_id=str(pos["position_cycle_id"]),
        portfolio_epoch_id=str(pos["portfolio_epoch_id"]),
        order_id="sell-full-late-price", pre_order_holding_qty=5,
    )
    first = repo.reconcile_sell_execution(**kwargs, fill_price=None, broker_holding_qty=0)
    assert first["qty_applied"] == 5
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        before = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_id == pos["position_id"]
        )).mappings().one()
    assert before["status"] == "CLOSED" and before["qty"] == 0
    assert before["realized_pnl"] == 0

    second = repo.reconcile_sell_execution(**kwargs, fill_price=120.0, broker_holding_qty=None)
    third = repo.reconcile_sell_execution(**kwargs, fill_price=120.0, broker_holding_qty=None)
    assert second["qty_applied"] == 0 and second["pnl_qty_applied"] == 5
    assert third["qty_applied"] == 0 and third["pnl_qty_applied"] == 0
    with engine.connect() as conn:
        after = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_id == pos["position_id"]
        )).mappings().one()
    assert after["status"] == "CLOSED" and after["qty"] == 0
    assert after["realized_pnl"] == pytest.approx(100.0)
    assert after["closed_ts"] == before["closed_ts"]

    with pytest.raises(RuntimeError, match="KR_SELL_RECONCILE_CONFIRMED_PRICE_CONFLICT"):
        repo.reconcile_sell_execution(**kwargs, fill_price=121.0, broker_holding_qty=None)


def test_partial_sell_later_cumulative_average_price_uses_incremental_proceeds():
    engine = _db()
    repo = PositionsRepo(engine)
    pos = _open_position(engine, code="123453", qty=10, avg=100.0)
    kwargs = dict(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="123453", market="J",
        filled_at=datetime(2026, 10, 6, 1, tzinfo=timezone.utc),
        position_cycle_id=str(pos["position_cycle_id"]),
        portfolio_epoch_id=str(pos["portfolio_epoch_id"]),
        order_id="sell-partial-cumulative", pre_order_holding_qty=10,
    )
    first = repo.reconcile_sell_execution(
        **kwargs, confirmed_cumulative_qty=2, fill_price=110.0,
        broker_holding_qty=8,
    )
    second = repo.reconcile_sell_execution(
        **kwargs, confirmed_cumulative_qty=5, fill_price=116.0,
        broker_holding_qty=5,
    )
    assert first["pnl_qty_applied"] == 2
    assert second["pnl_qty_applied"] == 3
    stored = repo.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="123453", position_cycle_id=str(pos["position_cycle_id"]),
        portfolio_epoch_id=str(pos["portfolio_epoch_id"]),
    )
    assert stored["qty"] == 5
    # 5 sold shares total at cumulative broker average 116; basis 100.
    assert stored["realized_pnl"] == pytest.approx(80.0)


def test_late_daily_ccld_partial_execution_prices_need_complete_order_proof():
    from trader.reconcile_kis import _verified_order_sell_average_from_ccld_fills

    engine = _db()
    orders, fills = OrdersRepo(engine), FillsRepo(engine)
    pos = _open_position(engine, code="123454", qty=10, avg=100.0)
    key = "sell-late-partial-ccld-proof"
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="123454", market="J", side="SELL",
        ord_type="LIMIT", qty=5, limit_price=110.0,
        stage="PROFIT_PROTECT_PARTIAL_1", client_order_key=key,
        request_json={"pre_order_holding_qty": 10},
        status="ACKED", account_id=get_account_key(env="practice"),
        position_cycle_id=str(pos["position_cycle_id"]),
        portfolio_epoch_id=str(pos["portfolio_epoch_id"]),
    )
    assert created
    order = orders.get_order_by_client_order_key("practice", key)
    common = dict(
        env="practice", run_id=None, order_id=str(order_id),
        kis_odno="0000099933", code="123454", market="J",
        side="SELL", fee=0, tax=0,
        filled_at=datetime(2026, 10, 6, 1, tzinfo=timezone.utc),
        raw_json={"source": "daily_ccld"},
        fill_meta_json={"fill_source": "daily_ccld"},
    )
    fills.upsert_fill(**common, trade_id="broker-exec-1", qty=2, price=110.0)
    assert _verified_order_sell_average_from_ccld_fills(
        fills_repo=fills, source_order=order, env="practice",
        confirmed_cumulative_qty=5,
    ) is None
    fills.upsert_fill(**common, trade_id="broker-exec-2", qty=3, price=120.0)
    assert _verified_order_sell_average_from_ccld_fills(
        fills_repo=fills, source_order=order, env="practice",
        confirmed_cumulative_qty=5,
    ) == pytest.approx(116.0)
    # A holdings-promotion synthetic record cannot change the broker price.
    fills.upsert_fill(
        **{**common, "fill_meta_json": {"fill_source": "kis_holdings_fallback"}},
        trade_id="synthetic-holdings", qty=5, price=1000.0,
    )
    assert _verified_order_sell_average_from_ccld_fills(
        fills_repo=fills, source_order=order, env="practice",
        confirmed_cumulative_qty=5,
    ) == pytest.approx(116.0)
