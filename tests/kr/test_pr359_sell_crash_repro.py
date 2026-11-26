"""Reproduce a crash between native SELL fill and position application."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
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
    assert len(calls) == 2 and all(conn is not None for conn in calls)


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


def test_kr401_native_partial_cancel_preserves_position_and_releases_stage(monkeypatch, isolated_engine):
    """Real PG repos/reconcile_today with a synthetic native KIS GET response."""
    engine = isolated_engine
    if engine.dialect.name != "postgresql":
        pytest.skip("TP durable journal requires PostgreSQL")
    from trader.account_state import get_account_key
    from trader.kr.us_standard_profit_parity import build_new_swing_parity_contract
    from trader.kr.us_profit_stage_journal import reserve_tp_stage, load_frozen_stage_states
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    monkeypatch.setenv("NULLIM_KR_SETTLEMENT_ACTIVATE", "0")
    code, odno, key = "251970", "0000040226", "kr401-native-cancel"
    pos, order_id = _accepted_sell(
        engine, code=code, ordered_qty=17, pre_qty=68,
        avg_price=68400.0, odno=odno, order_key=key,
    )
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id)).mappings().one()
        args = dict(engine=engine, account_id=get_account_key(env="practice"),
                    trading_epoch_id=str(order["trading_epoch_id"]),
                    position_cycle_id=str(order["position_cycle_id"]), code=code)
        contract = build_new_swing_parity_contract(
            strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
            trade_horizon="SWING", position_action="NEW_POSITION_BUY",
            runner_version="US_V3",
            **{k:v for k,v in args.items() if k!="engine"},
        )
        request = dict(order["request_json"])
        request.update(kr_us_tp_contract=contract, kr_us_tp_stage="TP1",
                       kr_us_tp_trade_date="2026-10-22")
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == order_id).values(request_json=request))
    args["contract"] = contract
    day = date(2026, 10, 22)
    assert reserve_tp_stage(**args, stage="TP1", target_qty=17,
                            client_order_key=key, trade_date=day).accepted
    row = {"pdno":code, "sll_buy_dvsn_cd":"01", "ord_qty":"17",
           "tot_ccld_qty":"10", "tot_ccld_amt":"685000", "avg_prvs":"68500",
           "odno":odno, "ord_dt":"20261022", "ord_tmd":"151810",
           "ord_stat_cd":"PARTIAL_FILLED", "market":"J", "cncl_yn":"N",
           "cncl_cfrm_qty":"0", "rjct_qty":"0", "rmn_qty":"7"}
    kis = KISDailyReplay(row)
    ctx = RunContext(run_id=None, env="practice", strategy="pb1_pullback_close",
                     started_at=datetime(2026,10,22,15,18,tzinfo=ZoneInfo("Asia/Seoul")),
                     dry_run=True)
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *a, **k: None)
    rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
    assert load_frozen_stage_states(**args)["TP1"]["status"] == "PARTIAL_FILLED"
    kis.row.update(ord_stat_cd="CANCELLED", cncl_cfrm_qty="7", rmn_qty="0")
    for _ in range(2):
        rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
    state = load_frozen_stage_states(**args)["TP1"]
    assert state["status"] == "RETRY_READY"
    assert state["target_qty"] == 17 and state["cumulative_filled_qty"] == 10
    assert state["cancelled_attempts"][0]["cancelled_qty"] == 7
    assert len(state["cancelled_attempts"]) == 1
    with engine.connect() as conn:
        stored = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id)).mappings().one()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"]))).mappings().one()
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id)).mappings().all()
    assert stored["status"] == "CANCELLED"
    assert position["qty"] == 58
    assert float(position["realized_pnl"]) == pytest.approx(1000)
    assert len(fills) == 1


@pytest.mark.parametrize("filled,simultaneous", [(0, False), (10, False), (10, True)])
def test_kr401_terminal_cancel_releases_real_claim_without_new_fill(monkeypatch, isolated_engine, filled, simultaneous):
    """Native zero-fill or already stored individual-fill terminal proof."""
    engine = isolated_engine
    if engine.dialect.name != "postgresql":
        pytest.skip("TP durable journal requires PostgreSQL")
    from trader.account_state import get_account_key
    from trader.kr.us_standard_profit_parity import build_new_swing_parity_contract
    from trader.kr.us_profit_stage_journal import reserve_tp_stage, load_frozen_stage_states
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("TRADING_EPOCH_ENFORCE", "1")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    monkeypatch.setenv("NULLIM_KR_SETTLEMENT_ACTIVATE", "0")
    create_schema_with_active_test_epoch(engine)
    # The scoped reconcile path checks the other sleeve's durable owner
    # journal even for PB1 rows. Use the real KR migration and its epoch
    # upgrade in this disposable schema; never mock/bypass owner proof.
    migrations = Path(__file__).resolve().parents[2] / "migrations"
    infinite_ddl = (migrations / "0049_add_kr_infinite_state.sql").read_text()
    epoch_ddl = (migrations / "0052_unified_trading_epoch.sql").read_text()
    kr_epoch_start = epoch_ddl.index("ALTER TABLE kr_infinite_state ADD COLUMN")
    kr_epoch_end = epoch_ddl.index("CREATE INDEX IF NOT EXISTS ix_us_tqqq_inf_trading_epoch", kr_epoch_start)
    with engine.begin() as conn:
        conn.exec_driver_sql(infinite_ddl)
        conn.exec_driver_sql(epoch_ddl[kr_epoch_start:kr_epoch_end])
    code, odno, key = "251970", "0000040226", "kr401-terminal-cancel"
    pos, order_id = _accepted_sell(engine, code=code, ordered_qty=17, pre_qty=68,
                                    avg_price=68400.0, odno=odno, order_key=key)
    schema = schema_for_engine(engine)
    day = date(2026, 10, 22)
    action = "SELL:TAKE_PROFIT_TP1:TP1"
    with engine.begin() as conn:
        order = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id)).mappings().one()
        args = dict(engine=engine, account_id=get_account_key(env="practice"),
                    trading_epoch_id=str(order["trading_epoch_id"]),
                    position_cycle_id=str(order["position_cycle_id"]), code=code)
        contract = build_new_swing_parity_contract(
            strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
            trade_horizon="SWING", position_action="NEW_POSITION_BUY",
            runner_version="US_V3", **{k:v for k,v in args.items() if k!="engine"},
        )
        request = dict(order["request_json"])
        request.update(kr_us_tp_contract=contract, kr_us_tp_stage="TP1",
                       kr_us_tp_trade_date=day.isoformat(), semantic_action=action)
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == order_id).values(request_json=request))
    args["contract"] = contract
    repo = OrdersRepo(engine)
    claim_args = dict(env="practice", market="KR", strategy_owner="PB1",
                      lifecycle_id=str(pos["position_cycle_id"]), action=action,
                      trade_date=day, fresh_validation=True, allow_partial_retry=True)
    _, claim = repo.claim_execution_action(**claim_args, attempt_id="original",
                                            requested_qty=17, client_order_key=key)
    assert claim.acquired
    repo.record_execution_claim_for_order(key, state="ACKED",
                                         cumulative_filled_qty=None, authoritative=False)
    assert reserve_tp_stage(**args, stage="TP1", target_qty=17,
                            client_order_key=key, trade_date=day).accepted
    row = {"pdno":code, "sll_buy_dvsn_cd":"01", "ord_qty":"17",
           "tot_ccld_qty":str(filled), "tot_ccld_amt":str(filled*68500),
           "avg_prvs":"68500" if filled else "0", "odno":odno,
           "ord_dt":"20261022", "ord_tmd":"151810", "ord_stat_cd":"PARTIAL_FILLED",
           "market":"J", "cncl_yn":"N", "cncl_cfrm_qty":"0", "rjct_qty":"0",
           "rmn_qty":str(17-filled)}
    if filled:
        row.update(ccld_no="kr401-native-execution-1", ccld_qty=str(filled), ccld_prc="68500")
    kis = KISDailyReplay(row)
    ctx = RunContext(run_id=None, env="practice", strategy="pb1_pullback_close",
                     started_at=datetime(2026,10,22,15,18,tzinfo=ZoneInfo("Asia/Seoul")),
                     dry_run=True)
    if filled and not simultaneous:
        rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
        assert load_frozen_stage_states(**args)["TP1"]["status"] == "PARTIAL_FILLED"
    # An explicit CANCELLED label alone is insufficient.
    kis.row.update(ord_stat_cd="CANCELLED", cncl_cfrm_qty=str(17-filled), rmn_qty="1")
    rejected = rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
    assert not rejected["ok"]
    assert load_frozen_stage_states(**args)["TP1"]["status"] != "RETRY_READY"
    kis.row.update(rmn_qty="0")
    if simultaneous:
        rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
        assert load_frozen_stage_states(**args)["TP1"]["status"] == "PARTIAL_FILLED"
    # A failure between stage journal and semantic claim must roll back
    # the terminal state and leave the same cancellation replayable.
    before_failure = load_frozen_stage_states(**args)["TP1"]
    def fail_claim(*a, **k):
        raise RuntimeError("INJECTED_TERMINAL_CLAIM_FAILURE")
    with monkeypatch.context() as failure:
        failure.setattr(OrdersRepo, "record_execution_claim_for_order", fail_claim)
        result = rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
        assert not result["ok"]
    assert load_frozen_stage_states(**args)["TP1"] == before_failure
    with engine.connect() as conn:
        assert conn.execute(sa.select(schema.orders.c.status).where(
            schema.orders.c.order_id == order_id)).scalar_one() != "CANCELLED"
    for _ in range(2):
        rk.reconcile_today(engine=engine, kis=kis, ctx=ctx, trade_date=day)
    state = load_frozen_stage_states(**args)["TP1"]
    assert state["status"] == "RETRY_READY"
    assert state["target_qty"] == 17 and state["cumulative_filled_qty"] == filled
    assert len(state["cancelled_attempts"]) == 1
    assert state["cancelled_attempts"][0]["cancelled_qty"] == 17-filled
    with engine.connect() as conn:
        stored = conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == order_id)).mappings().one()
        position = conn.execute(sa.select(schema.positions).where(
            schema.positions.c.position_cycle_id == str(pos["position_cycle_id"]))).mappings().one()
        fills = conn.execute(sa.select(schema.fills).where(
            schema.fills.c.order_id == order_id)).mappings().all()
    assert stored["status"] == "CANCELLED"
    assert position["qty"] == 68-filled
    assert float(position["realized_pnl"] or 0) == pytest.approx(filled*100)
    assert len(fills) == int(bool(filled))
    assert not reserve_tp_stage(**args, stage="TP1", target_qty=16-filled,
                                client_order_key="wrong", trade_date=day).accepted
    assert reserve_tp_stage(**args, stage="TP1", target_qty=17-filled,
                            client_order_key="residual", trade_date=day).accepted
    _, retry = repo.claim_execution_action(**claim_args, attempt_id="retry",
                                            requested_qty=17-filled, client_order_key="residual")
    assert retry.acquired
