from __future__ import annotations

import inspect
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError


def _tick_timeout():
    from trader.pb1_runner import TickTimeoutError
    return TickTimeoutError(
        "tick_hard_timeout timeout_sec=90 last_stage=entry.buyable_gate.finalize.047050"
    )


class _TimeoutDuringSetConn:
    dialect = SimpleNamespace(name="postgresql")

    def __init__(self, exc):
        self.exc = exc
        self.execute_calls = 0

    def execution_options(self, **_kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def exec_driver_sql(self, _sql):
        raise self.exc

    def execute(self, _stmt):
        self.execute_calls += 1
        raise AssertionError("poisoned connection must not execute the read query")


class _TimeoutDuringSetEngine:
    dialect = SimpleNamespace(name="postgresql")
    url = "postgresql://test/test"

    def __init__(self, exc):
        self.conn = _TimeoutDuringSetConn(exc)
        self.dispose_calls = 0

    def connect(self):
        return self.conn

    def dispose(self):
        self.dispose_calls += 1


def test_sep22_timeout_during_set_aborts_before_query():
    from trader.db.engine import safe_read_mappings

    engine = _TimeoutDuringSetEngine(_tick_timeout())
    env = {
        "PB1_MARKET_SCOPE": "KRX",
        "KRX_DB_READ_FAIL_OPEN": "1",
        "GITHUB_WORKFLOW": "",
    }
    with patch.dict(os.environ, env, clear=False):
        rows, degraded = safe_read_mappings(
            engine,
            sa.text("select 1"),
            op_name="orders.get_order_by_client_order_key",
            fail_open=False,
        )

    assert rows == []
    assert degraded is True
    assert engine.conn.execute_calls == 0
    assert engine.dispose_calls >= 1


class _ConnectBoomEngine:
    dialect = SimpleNamespace(name="postgresql")
    url = "postgresql://test/test"

    def __init__(self, exc):
        self.exc = exc
        self.dispose_calls = 0

    def connect(self):
        raise self.exc

    def dispose(self):
        self.dispose_calls += 1


def test_sep22_wrapped_tick_timeout_is_recognized_before_dbapi_fatal():
    from trader.db.engine import safe_read_mappings

    inner = _tick_timeout()
    wrapped = OperationalError("select 1", {}, inner)
    engine = _ConnectBoomEngine(wrapped)

    with patch.dict(
        os.environ,
        {"PB1_MARKET_SCOPE": "KRX", "KRX_DB_READ_FAIL_OPEN": "1"},
        clear=False,
    ):
        rows, degraded = safe_read_mappings(
            engine,
            sa.text("select 1"),
            op_name="orders.get_order_by_client_order_key",
            fail_open=False,
        )

    assert rows == []
    assert degraded is True
    assert engine.dispose_calls >= 1


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _RunLookupConn:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def execute(self, _stmt):
        self.calls += 1
        return _ScalarResult(self.value)


def test_sep22_missing_reconcile_run_id_resolves_to_null_fk():
    from trader.db.repos import _resolve_internal_run_id_or_none

    table = sa.table("runs", sa.column("run_id"))
    schema = SimpleNamespace(runs=table)
    missing = "acad79c8-8318-4009-9e84-8e13d95ea483"

    conn = _RunLookupConn(None)
    resolved = _resolve_internal_run_id_or_none(
        conn, schema, missing, context="reconcile_today"
    )
    assert resolved is None
    assert conn.calls == 1

    conn2 = _RunLookupConn(missing)
    resolved2 = _resolve_internal_run_id_or_none(
        conn2, schema, missing, context="reconcile_today"
    )
    assert resolved2 == missing


def test_sep22_reconcile_uses_resolved_run_id_for_orders_fills_and_ledger():
    import trader.reconcile_kis as rk

    source = inspect.getsource(rk.reconcile_today)
    assert "resolved_run_id = runs_repo.resolve_existing_run_id_or_none" in source
    assert "run_id=resolved_run_id" in source
    assert "run_id=ctx.run_id" not in source


class _FakeKis:
    def __init__(self):
        self.force_args = []

    def invalidate_balance_cache(self, **_kwargs):
        return None

    def get_balance_cached(self, *, force=False):
        self.force_args.append(bool(force))
        return {"output1": [], "output2": []}


class _FakeEngineObj:
    dry_run = False
    intended_live = True
    env = "practice"
    engine = object()
    STRATEGY_NAME = "pb1_pullback_close"
    run_id = "run"

    def __init__(self, submitted: int):
        self.kis = _FakeKis()
        self._run_summary_payload = {"api_submitted": submitted}
        self._balance_snapshot = {"output1": [], "output2": []} if submitted == 0 else None


@pytest.mark.parametrize("submitted, expected_force", [(0, False), (1, True)])
def test_sep22_post_tick_balance_refresh_only_after_broker_submit(
    monkeypatch, submitted, expected_force
):
    import trader.kr.broker_truth_hardening as hardening
    import trader.reconcile_kis as rk

    monkeypatch.setattr(
        rk,
        "reconcile_kis",
        lambda **_kwargs: {
            "orders": 0,
            "fills": 0,
            "promoted_fills": 0,
            "linked_fills": 0,
        },
    )
    monkeypatch.setattr(
        hardening,
        "_recover_proven_policy_positions",
        lambda **_kwargs: {"recovered": [], "review_required": []},
    )
    monkeypatch.setattr(
        hardening,
        "_health_after_reconcile",
        lambda **_kwargs: {"qty_mismatch_count": 0, "stale_open_order_count": 0},
    )

    obj = _FakeEngineObj(submitted)
    hardening._post_pb1_tick_reconcile(obj)
    if submitted == 0:
        assert obj.kis.force_args == []
    else:
        assert obj.kis.force_args == [expected_force]


def test_sep22_entry_skip_stage_is_session_aware():
    from trader.pb1_engine import PB1Engine

    source = inspect.getsource(PB1Engine.run)
    assert 'self._log_order_skip(cf, reasons, "PB1-CLOSE")' not in source
    assert "self._log_order_skip(cf, reasons, self._entry_stage_name())" in source

    engine = object.__new__(PB1Engine)
    engine.window_internal = "morning"
    engine.window_label = "morning"
    engine.phase = "entry"
    assert engine._entry_stage_name() == "PB1-AM"

    engine.window_internal = "afternoon"
    engine.window_label = "afternoon"
    assert engine._entry_stage_name() == "PB1-PM"
