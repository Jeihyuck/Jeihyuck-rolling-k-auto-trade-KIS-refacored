"""Reproduce a crash between native SELL fill and position application."""
from datetime import date, datetime
import os
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

import trader.reconcile_kis as rk
from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.run_context import RunContext
from tests.kr.test_kr_20261006_execution_convergence import _db
from tests.kr.test_kr_pr249_kis_stub_end_to_end import KISDailyReplay, _accepted_sell
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


@pytest.fixture(params=["sqlite", "postgres"])
def isolated_engine(request):
    if request.param == "sqlite":
        engine = _db()
        try:
            yield engine
        finally:
            engine.dispose()
        return
    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("isolated PostgreSQL test database not configured")
    schema_name = "kr359_" + uuid4().hex
    admin = sa.create_engine(url)
    with admin.begin() as conn:
        conn.execute(sa.text(f'CREATE SCHEMA "{schema_name}"'))
    engine = sa.create_engine(url, connect_args={"options": f"-csearch_path={schema_name}"})
    create_schema_with_active_test_epoch(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(sa.text(f'DROP SCHEMA "{schema_name}" CASCADE'))
        admin.dispose()


@pytest.mark.parametrize("failure_point", ["position", "claim"])
def test_native_sell_failure_rolls_back_fill_then_replays_once(monkeypatch, failure_point, isolated_engine):
    engine = isolated_engine
    pos, order_id = _accepted_sell(
        engine, code="251970", ordered_qty=17, pre_qty=17,
        avg_price=68400.0, odno="0000040226", order_key="pr359-crash-repro",
    )
    kis = KISDailyReplay({
        "pdno": "251970", "sll_buy_dvsn_cd": "01", "ord_qty": "17",
        "tot_ccld_qty": "10", "tot_ccld_amt": "685000", "avg_prvs": "68500",
        "odno": "0000040226", "ord_dt": "20261022", "ord_tmd": "151810",
        "ord_stat_cd": "PARTIAL_FILLED", "market": "J",
    })
    ctx = RunContext(run_id=None, env="practice", strategy="pb1_pullback_close",
                     started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")), dry_run=True)
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)
    def crash(*args, **kwargs):
        raise RuntimeError("INJECTED_FAILURE")
    with monkeypatch.context() as failures:
        if failure_point == "position":
            failures.setattr(PositionsRepo, "reconcile_sell_execution", crash)
        else:
            failures.setattr(OrdersRepo, "record_execution_claim_for_order", crash)
        with pytest.raises(RuntimeError, match="INJECTED_FAILURE"):
            rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22))
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(schema.fills.c.order_id == order_id)).all()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id)).mappings().one()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"]))).mappings().one()
    assert position["qty"] == 17
    assert fills == [], "native fill committed although position update failed"
    assert order["status"] == "ACKED", "order state advanced despite failed settlement"
    for _ in range(2):
        rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22))
    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(schema.fills.c.order_id == order_id)).all()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"]))).mappings().one()
    assert len(fills) == 1
    assert position["qty"] == 7
    assert float(position["realized_pnl"]) == pytest.approx(1000)
