"""Nov-03 Mico: KIS 91 BUY must not turn a phantom DB 22 into 113.

All tests use in-memory SQLite and synthetic broker evidence: NO KIS or Supabase writes.
The existing NeoSem cross-day SELL is an ACK-only original-day inquiry case,
not permission to fabricate a SELL fill or retry an accepted order.
"""
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.cross_day_sell_auto_reconcile import classify_pending_historical_sells
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def _fixture():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    cycle = str(uuid4())
    order_id, created = OrdersRepo(engine).create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="059090", market="KOSDAQ",
        side="BUY", ord_type="LIMIT", qty=91, limit_price=21900,
        stage="PB1-AM", client_order_key="mico-cumulative-" + uuid4().hex,
        request_json={"pre_order_holding_qty": 0}, status="ACKED",
        account_id=get_account_key(env="practice"),
        position_cycle_id=cycle,
    )
    assert created
    with engine.connect() as conn:
        order = dict(conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id,
        )).mappings().one())
    return engine, schema, order, PositionsRepo(engine)


def _apply(repo, order, qty, cumulative=None, previous=None, notional=None, order_id=True):
    return repo.apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="059090", market="KOSDAQ", side="BUY", qty=qty,
        price=21784.6153846154 if qty else 0.0, fee=0.0, tax=0.0,
        filled_at=datetime(2026, 11, 3, 0, 32, 17, tzinfo=timezone.utc),
        portfolio_epoch_id=str(order["portfolio_epoch_id"]),
        position_cycle_id=str(order["position_cycle_id"]),
        order_id=str(order["order_id"]) if order_id else None,
        buy_application_cumulative_qty=cumulative,
        buy_application_previous_confirmed_qty=previous,
        buy_cost_delta_override=notional,
    )


def _position(engine, schema, order):
    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.positions).where(sa.and_(
            schema.positions.c.env == "practice",
            schema.positions.c.code == "059090",
            schema.positions.c.position_cycle_id == order["position_cycle_id"],
        ))).mappings().all()
    return [dict(row) for row in rows]


def test_mico_113_preexisting_22_is_blocked_and_immutable():
    engine, schema, order, repo = _fixture()
    # Reproduce the economic inconsistency preceding a valid 91-share broker BUY:
    # an unproven 22-share DB row has zero cost in the same cycle.
    _apply(repo, order, qty=22, order_id=False, notional=0.0)
    with pytest.raises(RuntimeError, match="KR_BUY_APPLICATION_BASELINE_DRIFT"):
        _apply(repo, order, qty=91, cumulative=91, previous=0, notional=1982400.0)
    rows = _position(engine, schema, order)
    assert len(rows) == 1
    assert rows[0]["qty"] == 22 and rows[0]["total_cost"] == 0
    engine.dispose()


def test_mico_exact_91_cost_and_idempotent_cumulative_replay():
    engine, schema, order, repo = _fixture()
    _apply(repo, order, qty=91, cumulative=91, previous=0, notional=1982400.0)
    _apply(repo, order, qty=0, cumulative=91, previous=91, notional=0.0)
    rows = _position(engine, schema, order)
    assert len(rows) == 1
    assert rows[0]["qty"] == 91
    assert abs(rows[0]["total_cost"] - 1982400.0) < 0.1
    assert rows[0]["entry_meta_json"]["broker_truth_buy_applied_orders"][str(order["order_id"])]["qty"] == 91
    engine.dispose()


def test_mico_valid_partial_20_then_91_preserves_economics():
    engine, schema, order, repo = _fixture()
    _apply(repo, order, qty=20, cumulative=20, previous=0, notional=432000.0)
    _apply(repo, order, qty=71, cumulative=91, previous=20, notional=1550400.0)
    rows = _position(engine, schema, order)
    assert len(rows) == 1
    assert rows[0]["qty"] == 91
    assert abs(rows[0]["total_cost"] - 1982400.0) < 0.1
    engine.dispose()


def test_neosem_110_original_ack_queues_no_fake_fill_or_retry():
    order = {
        "order_id": str(uuid4()), "code": "253590", "side": "SELL",
        "qty": 110, "status": "ACK_DB_FAILED_RECONCILE_REQUIRED",
        "position_cycle_id": str(uuid4()), "portfolio_epoch_id": str(uuid4()),
        "trading_epoch_id": "epoch-one",
        "created_at": datetime(2026, 10, 30, 0, 45, tzinfo=timezone.utc),
        "kis_odno": None, "broker_order_id": None,
        "strategy": "pb1_pullback_close",
        "request_json": {"strategy_owner": "KR_STANDARD"},
        "response_json": {"rt_cd": "0", "output": {"ODNO": "0000013020"}},
    }
    selected, blocked = classify_pending_historical_sells(
        [order], today=date(2026, 11, 3), epoch_id="epoch-one",
    )
    assert not blocked and len(selected) == 1
    assert selected[0]["_ack_only_odno"] == "0000013020"
    assert selected[0]["kis_odno"] is None
    assert order["status"] == "ACK_DB_FAILED_RECONCILE_REQUIRED"
    # Same ID from another epoch must never attach to this current epoch.
    cross_epoch, _ = classify_pending_historical_sells(
        [order], today=date(2026, 11, 3), epoch_id="epoch-two",
    )
    assert cross_epoch == []
