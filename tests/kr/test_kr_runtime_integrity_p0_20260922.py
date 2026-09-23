from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import sqlalchemy as sa

from trader.db.engine import _is_runner_tick_timeout, safe_read_mappings
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kr import broker_truth_hardening as broker_truth
from trader.kr.infinite.runner import prewarm_kr_infinite_price
from trader.kis_wrapper import KisAPI
from trader.pb1_engine import PB1Engine
from trader.pb1_runner import TickTimeoutError, _exception_chain_has_tick_timeout
from trader.reconcile_kis import _resolve_reconcile_run_id, reconcile_today


class _FakeDialect:
    name = "postgresql"


class _TimeoutDuringSetConnection:
    dialect = _FakeDialect()

    def __init__(self) -> None:
        self.query_called = False

    def execution_options(self, **_kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def in_transaction(self):
        return False

    def exec_driver_sql(self, sql: str):
        if str(sql).startswith("SET statement_timeout"):
            raise TickTimeoutError(
                "tick_hard_timeout timeout_sec=90 "
                "last_stage=entry.buyable_gate.finalize.047050"
            )
        return None

    def execute(self, _stmt):
        self.query_called = True
        raise AssertionError("poisoned connection must never execute the read query")


class _FakeEngine:
    dialect = _FakeDialect()
    url = "postgresql+psycopg://example.invalid/db"

    def __init__(self) -> None:
        self.connection = _TimeoutDuringSetConnection()
        self.disposed = False

    def connect(self):
        return self.connection

    def dispose(self):
        self.disposed = True


def test_wrapped_tick_timeout_identity_survives_exception_chain():
    root = TickTimeoutError(
        "tick_hard_timeout timeout_sec=90 last_stage=entry.buyable_gate.finalize.047050"
    )
    wrapper = RuntimeError("SQLAlchemy wrapper")
    wrapper.__cause__ = root

    assert _is_runner_tick_timeout(wrapper) is True
    assert _exception_chain_has_tick_timeout(wrapper) is True


def test_timeout_during_db_timeout_setup_disposes_and_never_reuses_connection(monkeypatch):
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KRX")
    monkeypatch.setenv("KRX_DB_READ_FAIL_OPEN", "1")
    engine = _FakeEngine()

    rows, degraded = safe_read_mappings(
        engine,
        sa.text("select 1"),
        op_name="incident.20260922.timeout_set",
        fail_open=False,
    )

    assert rows == []
    assert degraded is True
    assert engine.disposed is True
    assert engine.connection.query_called is False


def test_reconciled_order_drops_orphan_run_id_instead_of_violating_fk(monkeypatch):
    monkeypatch.delenv("WSL_RUN_MARKET", raising=False)
    monkeypatch.delenv("TRADING_EPOCH_ENFORCE", raising=False)

    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = OrdersRepo(engine)

    missing_run_id = str(uuid4())
    key = f"practice:reconcile:orphan:{uuid4()}"
    order_id = repo.upsert_reconciled_order(
        env="practice",
        run_id=missing_run_id,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="RECONCILED",
        qty=1,
        limit_price=70000.0,
        stage="RECONCILE",
        client_order_key=key,
        kis_odno=f"KIS-{uuid4()}",
        status="ACKED",
        request_json={},
        response_json={"source": "incident-regression"},
        submitted_at=None,
        acked_at=None,
    )

    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.orders).where(schema.orders.c.order_id == order_id)
        ).mappings().one()

    assert row["run_id"] is None
    assert row["response_json"]["reconcile_run_id_orphan"] == missing_run_id
    assert row["response_json"]["reconcile_run_id_action"] == "NULL_RUN_ID"



def test_reconcile_run_identity_uses_only_existing_runs(monkeypatch):
    monkeypatch.delenv("WSL_RUN_MARKET", raising=False)
    monkeypatch.delenv("TRADING_EPOCH_ENFORCE", raising=False)

    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    durable_run_id = str(uuid4())
    missing_run_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.runs).values(
                run_id=durable_run_id,
                env="practice",
                strategy="pb1_pullback_close",
                status="STARTED",
                config_json={},
            )
        )

    assert _resolve_reconcile_run_id(engine, missing_run_id, durable_run_id) == durable_run_id
    assert _resolve_reconcile_run_id(engine, missing_run_id) is None



def test_daily_ccld_output2_summary_never_creates_synthetic_order(monkeypatch):
    monkeypatch.delenv("WSL_RUN_MARKET", raising=False)
    monkeypatch.delenv("TRADING_EPOCH_ENFORCE", raising=False)

    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    class FakeKis:
        def inquire_daily_ccld(self, **_kwargs):
            return {
                "rt_cd": "0",
                "output1": [],
                "output2": [{
                    "tot_ord_qty": "0",
                    "tot_ccld_qty": "0",
                    "tot_ccld_amt": "0",
                }],
            }

    ctx = SimpleNamespace(
        env="practice",
        strategy="pb1_pullback_close",
        run_id=None,
    )
    result = reconcile_today(engine=engine, kis=FakeKis(), ctx=ctx)

    assert result["orders"] == 0
    assert result["fills"] == 0
    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.orders)).mappings().all()
    assert rows == []


def test_daily_ccld_malformed_output1_row_is_skipped_before_db_write(monkeypatch):
    monkeypatch.delenv("WSL_RUN_MARKET", raising=False)
    monkeypatch.delenv("TRADING_EPOCH_ENFORCE", raising=False)

    engine = sa.create_engine("sqlite:///:memory:", future=True)
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    class FakeKis:
        def inquire_daily_ccld(self, **_kwargs):
            return {
                "rt_cd": "0",
                "output1": [{
                    "ord_qty": "0",
                    "tot_ccld_qty": "0",
                    "ord_stat_cd": "RECONCILED",
                }],
                "output2": [],
            }

    ctx = SimpleNamespace(
        env="practice",
        strategy="pb1_pullback_close",
        run_id=None,
    )
    result = reconcile_today(engine=engine, kis=FakeKis(), ctx=ctx)

    assert result["orders"] == 0
    assert result["fills"] == 0
    with engine.connect() as conn:
        rows = conn.execute(sa.select(schema.orders)).mappings().all()
    assert rows == []


def test_kr_infinite_122630_prewarm_and_fresh_ws_quote_avoid_rest(monkeypatch):
    class FakeWsService:
        def __init__(self):
            self.subscriptions = []
            self.quote_reads = []

        def subscribe_kr(self, symbol):
            self.subscriptions.append(str(symbol))

        def get_fresh_quote(self, market, symbol, *, max_age_sec):
            self.quote_reads.append((market, str(symbol), float(max_age_sec)))
            return {
                "market": "KR",
                "symbol": str(symbol),
                "last": 50125.0,
                "prpr": 50125.0,
                "bid": 50100.0,
                "ask": 50150.0,
                "age_sec": 0.01,
                "source": "KIS_WEBSOCKET",
            }

        def wait_for_fresh_quote(self, *args, **kwargs):
            raise AssertionError("fresh prewarmed quote should not require waiting")

    service = FakeWsService()
    monkeypatch.setenv("KR_INFINITE_ENABLED", "1")
    monkeypatch.setenv("KIS_HTTP_ENABLED", "1")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setattr(
        "trader.marketdata.kis_ws_price.get_kis_ws_price_service",
        lambda: service,
    )
    monkeypatch.setattr(
        "trader.kis_wrapper.get_kis_ws_price_service",
        lambda: service,
    )

    prewarm_kr_infinite_price("122630")
    assert service.subscriptions == ["122630"]

    kis = object.__new__(KisAPI)
    kis._safe_request = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("REST quote must not run when a fresh WS quote exists")
    )
    price = kis.get_current_price("122630")

    assert price == 50125.0
    assert service.quote_reads
    assert all(item[0] == "KR" and item[1] == "122630" for item in service.quote_reads)


def test_kr_infinite_prewarm_is_before_pb1_heavy_engine_run():
    source = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    prewarm = 'prewarm_kr_infinite_price("122630")'
    run_call = "result = engine_runner.run()"
    assert prewarm in source
    assert run_call in source
    assert source.index(prewarm) < source.index(run_call)


def test_reconcile_kis_source_defines_run_id_before_holding_promotion():
    source = Path("trader/reconcile_kis.py").read_text(encoding="utf-8")
    fn = source[source.index("def reconcile_kis("):]
    assert "reconcile_run_id = _resolve_reconcile_run_id(" in fn
    assert fn.index("reconcile_run_id = _resolve_reconcile_run_id(") < fn.index(
        "ctx_run_id=reconcile_run_id"
    )


def test_broker_truth_defers_without_network_when_tick_budget_is_exhausted(monkeypatch):
    class FakeKis:
        def __init__(self) -> None:
            self.balance_called = False

        def invalidate_balance_cache(self, **_kwargs):
            return None

        def get_balance_cached(self, **_kwargs):
            self.balance_called = True
            raise AssertionError("broker truth must defer before network fetch")

    kis = FakeKis()

    class EngineObj:
        dry_run = False
        intended_live = True
        env = "practice"
        engine = object()
        STRATEGY_NAME = "pb1_pullback_close"
        run_id = "trace-only-run-id"
        _run_summary_payload = {"api_submitted": 0}

        def __init__(self):
            self.kis = kis

    monkeypatch.setattr(broker_truth, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 1.0)
    monkeypatch.setenv("KR_BROKER_TRUTH_MIN_REMAINING_SEC", "12")

    obj = EngineObj()
    broker_truth._post_pb1_tick_reconcile(obj)

    assert kis.balance_called is False
    assert obj._run_summary_payload["broker_truth_reconcile_deferred"] == 1
    assert obj._run_summary_payload["broker_truth_reconcile_defer_reason"] == "INSUFFICIENT_TICK_BUDGET"
    assert obj._run_summary_payload["broker_truth_health_status"] == "DEFERRED"


def test_broker_truth_snapshot_reuse_still_defers_when_tick_budget_is_exhausted(monkeypatch):
    class FakeKis:
        def get_balance_cached(self, **_kwargs):
            raise AssertionError("snapshot-reuse path must not fetch balance when budget is exhausted")

    reconcile_called = {"value": False}

    def fail_if_reconcile_runs(**_kwargs):
        reconcile_called["value"] = True
        raise AssertionError("reconcile_kis must not run when post-tick budget is exhausted")

    class EngineObj:
        dry_run = False
        intended_live = True
        env = "practice"
        engine = object()
        kis = FakeKis()
        STRATEGY_NAME = "pb1_pullback_close"
        run_id = "trace-only-run-id"
        _run_summary_payload = {"api_submitted": 0}
        _balance_snapshot = {
            "rt_cd": "0",
            "output1": [],
            "output2": [{"tot_evlu_amt": "1000000"}],
        }

    monkeypatch.setattr(broker_truth, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 0.0)
    monkeypatch.setenv("KR_BROKER_TRUTH_MIN_REMAINING_SEC", "12")
    monkeypatch.setattr("trader.reconcile_kis.reconcile_kis", fail_if_reconcile_runs)

    obj = EngineObj()
    broker_truth._post_pb1_tick_reconcile(obj)

    assert reconcile_called["value"] is False
    assert obj._run_summary_payload["broker_truth_reconcile_ran"] == 0
    assert obj._run_summary_payload["broker_truth_reconcile_deferred"] == 1
    assert obj._run_summary_payload["broker_truth_reconcile_defer_reason"] == "INSUFFICIENT_TICK_BUDGET"
    assert obj._run_summary_payload["broker_truth_remaining_sec"] == 0.0
    assert obj._run_summary_payload["broker_truth_health_status"] == "DEFERRED"


def test_broker_truth_uses_reconcile_final_holdings_after_async_fill(monkeypatch):
    original_snapshot = {
        "rt_cd": "0",
        "output1": [{"pdno": "005930", "hldg_qty": "10", "ord_psbl_qty": "10"}],
        "output2": [{"tot_evlu_amt": "1000000"}],
    }
    refreshed_holdings = [
        {"pdno": "005930", "hldg_qty": "11", "ord_psbl_qty": "11"}
    ]
    captured = {}

    class FakeKis:
        def get_balance_cached(self, **_kwargs):
            raise AssertionError("reconcile_final holdings should avoid a duplicate balance fetch")

    def fake_reconcile_kis(**_kwargs):
        return {
            "ok": True,
            "orders": 1,
            "fills": 1,
            "promoted_fills": 0,
            "linked_fills": 0,
            "_final_holdings_rows": refreshed_holdings,
        }

    def fake_policy(**kwargs):
        captured["policy_holdings"] = list(kwargs["holdings_rows"])
        return {"recovered": [], "review_required": []}

    def fake_health(**kwargs):
        captured["health_holdings"] = list(kwargs["holdings_rows"])
        return {"qty_mismatch_count": 0, "stale_open_order_count": 0}

    class EngineObj:
        dry_run = False
        intended_live = True
        env = "practice"
        engine = object()
        kis = FakeKis()
        STRATEGY_NAME = "pb1_pullback_close"
        run_id = "durable-run-id"
        _run_summary_payload = {"api_submitted": 0}
        _balance_snapshot = original_snapshot

    monkeypatch.setattr(broker_truth, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 30.0)
    monkeypatch.setenv("KR_BROKER_TRUTH_MIN_REMAINING_SEC", "12")
    monkeypatch.setattr("trader.reconcile_kis.reconcile_kis", fake_reconcile_kis)
    monkeypatch.setattr(broker_truth, "_recover_proven_policy_positions", fake_policy)
    monkeypatch.setattr(broker_truth, "_health_after_reconcile", fake_health)

    obj = EngineObj()
    broker_truth._post_pb1_tick_reconcile(obj)

    assert captured["policy_holdings"] == refreshed_holdings
    assert captured["health_holdings"] == refreshed_holdings
    assert captured["policy_holdings"] != original_snapshot["output1"]
    assert obj._run_summary_payload["broker_truth_balance_source"] == "engine_tick_snapshot"
    assert obj._run_summary_payload["broker_truth_holdings_source"] == "reconcile_final"
    assert obj._run_summary_payload["broker_truth_health_status"] == "OK"


def test_reconcile_kis_publishes_final_holdings_for_post_tick_consumers():
    source = Path("trader/reconcile_kis.py").read_text(encoding="utf-8")
    assert 'reconcile_result["_final_holdings_rows"] = list(holdings_rows)' in source


def test_broker_truth_budget_defer_is_red_when_broker_activity_exists(monkeypatch):
    class FakeKis:
        def get_balance_cached(self, **_kwargs):
            raise AssertionError("must not start a doomed fetch")

    class EngineObj:
        dry_run = False
        intended_live = True
        env = "practice"
        engine = object()
        kis = FakeKis()
        STRATEGY_NAME = "pb1_pullback_close"
        run_id = "trace-only-run-id"
        _run_summary_payload = {"api_submitted": 1, "accepted": 1}

    monkeypatch.setattr(broker_truth, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 2.0)
    monkeypatch.setenv("KR_BROKER_TRUTH_MIN_REMAINING_SEC", "12")

    obj = EngineObj()
    broker_truth._post_pb1_tick_reconcile(obj)

    assert obj._run_summary_payload["broker_truth_reconcile_deferred"] == 1
    assert obj._run_summary_payload["broker_truth_health_status"] == "RED"


def test_entry_stage_name_tracks_actual_session():
    engine = object.__new__(PB1Engine)

    engine.window_internal = "morning"
    engine.window_label = "morning"
    engine.phase = "entry"
    assert engine._entry_stage_name() == "PB1-AM"

    engine.window_internal = "afternoon"
    engine.window_label = "afternoon"
    engine.phase = "pm_entry"
    assert engine._entry_stage_name() == "PB1-PM"

    engine.window_internal = "close"
    engine.window_label = "close"
    engine.phase = "close"
    assert engine._entry_stage_name() == "PB1-CLOSE"


def test_entry_skip_paths_do_not_hardcode_close_stage():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    offending = [
        line.strip()
        for line in source.splitlines()
        if "_log_order_skip" in line and '"PB1-CLOSE"' in line
    ]
    assert offending == []


def test_entry_disabled_order_candidates_initialize_submit_result_before_policy_summary():
    """Regression for 2026-09-23 AM UnboundLocalError on submit_result."""
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    anchor = source.index('with self._stage_timer("entry.order_submit")')
    section = source[anchor:source.index('if entry_allowed and self.phase in {"entry", "pm_entry"}', anchor)]
    init = 'submit_result = {'
    blocked = "if not entry_allowed:"
    policy_read = 'for result in (submit_result.get("results") or [])'
    assert init in section
    assert blocked in section
    assert policy_read in section
    assert section.index(init) < section.index(blocked) < section.index(policy_read)


def test_broker_truth_health_is_scoped_to_active_trading_epoch():
    source = Path("trader/kr/broker_truth_hardening.py").read_text(encoding="utf-8")
    start = source.index("def _health_after_reconcile(")
    end = source.index("def _install_reconcile_guards(", start)
    section = source[start:end]
    assert "active_trading_epoch_id(" in section
    assert "schema.positions.c.trading_epoch_id == epoch_id" in section
    assert 'order.get("trading_epoch_id")' in section
