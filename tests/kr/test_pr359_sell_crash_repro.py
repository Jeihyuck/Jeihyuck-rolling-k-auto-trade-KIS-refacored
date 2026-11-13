"""Reproduce a crash between native SELL fill and position application."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
import os
from threading import Barrier, local
from types import SimpleNamespace
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


@pytest.mark.parametrize("failure_point", ["position", "claim", "order"])
def test_native_sell_failure_rolls_back_fill_then_replays_once(monkeypatch, failure_point, isolated_engine):
    engine = isolated_engine
    pos, order_id = _accepted_sell(
        engine, code="251970", ordered_qty=17, pre_qty=17,
        avg_price=68400.0, odno="0000040226", order_key="pr359-crash-repro",
    )
    row = {
        "pdno": "251970", "sll_buy_dvsn_cd": "01", "ord_qty": "17",
        "tot_ccld_qty": "10", "tot_ccld_amt": "685000", "avg_prvs": "68500",
        "odno": "0000040226", "ord_dt": "20261022", "ord_tmd": "151810",
        "ord_stat_cd": "PARTIAL_FILLED", "market": "J", "cncl_yn": "N",
    }
    kis = KISDailyReplay(row)
    schema = schema_for_engine(engine)
    ctx = RunContext(run_id=None, env="practice", strategy="pb1_pullback_close",
                     started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")), dry_run=True)
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)
    def crash(*args, **kwargs):
        raise RuntimeError("INJECTED_FAILURE")
    with monkeypatch.context() as failures:
        if failure_point == "position":
            failures.setattr(PositionsRepo, "reconcile_sell_execution", crash)
        elif failure_point == "claim":
            failures.setattr(OrdersRepo, "record_execution_claim_for_order", crash)
        elif failure_point == "order":
            failures.setattr(OrdersRepo, "upsert_reconciled_order", crash)
        with pytest.raises(RuntimeError, match="INJECTED_FAILURE"):
            rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22))
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



def test_concurrent_native_sell_reconciliation_rejects_stale_watermark_and_applies_once(
    monkeypatch, isolated_engine,
):
    engine = isolated_engine
    if engine.dialect.name != "postgresql":
        pytest.skip("row-level FOR UPDATE concurrency is PostgreSQL-only")
    pos, order_id = _accepted_sell(
        engine, code="251970", ordered_qty=17, pre_qty=17,
        avg_price=68400.0, odno="0000040226", order_key="pr359-concurrent-sell",
    )
    row = {
        "pdno": "251970", "sll_buy_dvsn_cd": "01", "ord_qty": "17",
        "tot_ccld_qty": "10", "tot_ccld_amt": "685000", "avg_prvs": "68500",
        "odno": "0000040226", "ord_dt": "20261022", "ord_tmd": "151810",
        "ord_stat_cd": "PARTIAL_FILLED", "market": "J",
    }
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)
    barrier = Barrier(2)
    thread_state = local()
    original_lookup = OrdersRepo.get_order_by_kis_odno_scoped

    def synchronized_lookup(self, *args, **kwargs):
        result = original_lookup(self, *args, **kwargs)
        if (
            result and str(result.get("order_id") or "") == order_id
            and not getattr(thread_state, "waited", False)
        ):
            thread_state.waited = True
            barrier.wait(timeout=15)
        return result

    monkeypatch.setattr(OrdersRepo, "get_order_by_kis_odno_scoped", synchronized_lookup)

    def reconcile_once():
        ctx = RunContext(
            run_id=None, env="practice", strategy="pb1_pullback_close",
            started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
            dry_run=True,
        )
        return rk.reconcile_today(
            engine=engine, kis=KISDailyReplay(row), ctx=ctx,
            trade_date=date(2026, 10, 22),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(reconcile_once) for _ in range(2)]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(("ok", future.result(timeout=30)))
            except Exception as exc:
                outcomes.append(("error", str(exc)))
    monkeypatch.setattr(OrdersRepo, "get_order_by_kis_odno_scoped", original_lookup)
    assert sum(kind == "ok" for kind, _ in outcomes) == 1, outcomes
    assert sum(
        kind == "error" and "KR_NATIVE_SELL_STALE_ORDER_RETRY" in str(value)
        for kind, value in outcomes
    ) == 1, outcomes

    ctx = RunContext(
        run_id=None, env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        dry_run=True,
    )
    rk.reconcile_today(
        engine=engine, kis=KISDailyReplay(row), ctx=ctx,
        trade_date=date(2026, 10, 22),
    )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).mappings().all()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        )).mappings().one()
    assert len(fills) == 1
    assert int(_json_dict_for_test(order["response_json"]).get("confirmed_fill_qty") or 0) == 10
    assert position["qty"] == 7
    assert float(position["realized_pnl"]) == pytest.approx(1000)


def _json_dict_for_test(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        import json
        return json.loads(value)
    return {}


def test_tp_stage_journal_failure_keeps_broker_economics_and_retries_stage(monkeypatch, isolated_engine):
    engine = isolated_engine
    pos, order_id = _accepted_sell(
        engine, code="251970", ordered_qty=17, pre_qty=17,
        avg_price=68400.0, odno="0000040226", order_key="pr359-tp-journal-retry",
    )
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        source = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
        request = dict(source["request_json"] or {})
        request.update({
            "kr_us_tp_contract": {
                "activation_mode": "ACTIVE_PRACTICE",
                "position_cycle_id": str(pos["position_cycle_id"]),
                "trading_epoch_id": str(source["trading_epoch_id"]),
                "sha256": "test-contract",
            },
            "kr_us_tp_stage": "TP1",
            "kr_us_tp_trade_date": "2026-10-22",
            "profit_capture_stage": "tp1",
        })
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == order_id
        ).values(request_json=request))
        conn.execute(sa.update(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        ).values(position_meta={"kr_tp1_pending": True}))

    row = {
        "pdno": "251970", "sll_buy_dvsn_cd": "01", "ord_qty": "17",
        "tot_ccld_qty": "10", "tot_ccld_amt": "685000", "avg_prvs": "68500",
        "odno": "0000040226", "ord_dt": "20261022", "ord_tmd": "151810",
        "ord_stat_cd": "PARTIAL_FILLED", "market": "J", "cncl_yn": "N",
    }
    kis = KISDailyReplay(row)
    ctx = RunContext(
        run_id=None, env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        dry_run=True,
    )
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)
    import trader.kr.us_profit_stage_journal as stage_journal
    import trader.kr.us_standard_profit_parity as parity
    calls = []

    def failed_stage(**kwargs):
        calls.append(kwargs.get("_conn"))
        return SimpleNamespace(
            accepted=False, status="BLOCK", reason="INJECTED_STAGE_JOURNAL_FAILURE"
        )

    with monkeypatch.context() as failure:
        failure.setattr(parity, "verified_contract", lambda *_a, **_k: True)
        failure.setattr(stage_journal, "observe_broker_stage", failed_stage)
        rk.reconcile_today(
            engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
        )

    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).mappings().all()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        )).mappings().one()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
    assert len(fills) == 1, "TP journal outage must not discard broker-proven fill"
    assert position["qty"] == 7
    assert position["position_meta"]["kr_tp1_pending"] is True
    assert _json_dict_for_test(order["response_json"])["confirmed_fill_qty"] == 10
    assert calls and all(conn is not None for conn in calls)

    def accepted_stage(**kwargs):
        calls.append(kwargs.get("_conn"))
        return SimpleNamespace(
            accepted=True, status="PARTIAL_FILLED", reason="REPLAY_ACCEPTED"
        )

    with monkeypatch.context() as replay:
        replay.setattr(parity, "verified_contract", lambda *_a, **_k: True)
        replay.setattr(stage_journal, "observe_broker_stage", accepted_stage)
        rk.reconcile_today(
            engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
        )

    with engine.connect() as conn:
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).mappings().all()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        )).mappings().one()
    assert len(fills) == 1
    assert position["qty"] == 7
    assert position["position_meta"]["kr_tp1_pending"] is False
    assert len(calls) == 2 and all(conn is not None for conn in calls)


def test_unpriced_native_sell_quantity_and_order_watermark_share_transaction(monkeypatch, isolated_engine):
    engine = isolated_engine
    pos, order_id = _accepted_sell(
        engine, code="251970", ordered_qty=17, pre_qty=17,
        avg_price=68400.0, odno="0000040226", order_key="pr359-unpriced-sell",
    )
    row = {
        "pdno": "251970", "sll_buy_dvsn_cd": "01", "ord_qty": "17",
        "tot_ccld_qty": "10",
        "odno": "0000040226", "ord_dt": "20261022", "ord_tmd": "151810",
        "ord_stat_cd": "PARTIAL_FILLED", "market": "J", "cncl_yn": "N",
    }
    kis = KISDailyReplay(row)
    ctx = RunContext(
        run_id=None, env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 22, 15, 18, tzinfo=ZoneInfo("Asia/Seoul")),
        dry_run=True,
    )
    schema = schema_for_engine(engine)
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)

    def crash(*args, **kwargs):
        raise RuntimeError("INJECTED_UNPRICED_SELL_POSITION_FAILURE")

    with monkeypatch.context() as failures:
        failures.setattr(PositionsRepo, "reconcile_sell_execution", crash)
        with pytest.raises(RuntimeError, match="INJECTED_UNPRICED_SELL_POSITION_FAILURE"):
            rk.reconcile_today(
                engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
            )

    with engine.connect() as conn:
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        )).mappings().one()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).all()
    assert position["qty"] == 17
    assert order["status"] == "ACKED"
    assert _json_dict_for_test(order["response_json"]).get("confirmed_fill_qty") is None
    assert fills == []

    rk.reconcile_today(
        engine=engine, kis=kis, ctx=ctx, trade_date=date(2026, 10, 22),
    )
    with engine.connect() as conn:
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"])
        )).mappings().one()
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id
        )).mappings().one()
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id
        )).all()
    assert position["qty"] == 7
    assert order["status"] == "PARTIAL_FILLED"
    assert int(_json_dict_for_test(order["response_json"])["confirmed_fill_qty"]) == 10
    assert fills == [], "quantity-only KIS proof must not fabricate a priced fill"


def test_tp_stage_journal_database_failure_isolated_by_nested_savepoint(monkeypatch, isolated_engine):
    engine = isolated_engine
    import trader.kr.us_profit_stage_journal as stage_journal

    table = schema_for_engine(engine).job_checkpoints
    monkeypatch.setattr(stage_journal, "_assert_scope", lambda **_kwargs: None)
    with engine.begin() as conn:
        outer_savepoint = conn.begin_nested()
        conn.execute(sa.schema.DropTable(table))
        result = stage_journal.observe_broker_stage(
            engine=engine,
            contract={"sha256": "test-contract"},
            account_id="practice:test",
            trading_epoch_id="test-epoch",
            position_cycle_id="test-cycle",
            code="251970",
            stage="TP1",
            client_order_key="pr359-tp-savepoint",
            broker_odno="0000040226",
            cumulative_filled_qty=1,
            broker_order_accepted=True,
            broker_fill_source="KIS_DAILY_CCLD_VERIFIED",
            source_trade_date=date(2026, 10, 22),
            _conn=conn,
        )
        assert result.accepted is False
        # The failed journal SELECT must roll back only its savepoint. The
        # enclosing broker-settlement transaction remains usable.
        assert conn.execute(sa.text("SELECT 1")).scalar_one() == 1
        outer_savepoint.rollback()
