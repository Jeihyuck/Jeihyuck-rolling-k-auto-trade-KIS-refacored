from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa

from trader.db.engine import _is_runner_tick_timeout, safe_read_mappings
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kr import broker_truth_hardening as broker_truth
from trader.pb1_engine import PB1Engine
from trader.pb1_runner import TickTimeoutError, _exception_chain_has_tick_timeout
from trader.reconcile_kis import _resolve_reconcile_run_id


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
