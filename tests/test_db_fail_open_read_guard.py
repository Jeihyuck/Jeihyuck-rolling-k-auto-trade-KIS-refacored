from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trader.db.engine import safe_read_mappings
from trader.pb1_engine import PB1Engine


class _DummyMappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _DummyExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _DummyMappingsResult(self._rows)


class _DummyConnection:
    def __init__(self, rows):
        self.rows = rows
        self.entered = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def in_transaction(self):
        return True

    def execute(self, _stmt):
        return _DummyExecuteResult(self.rows)


class _DummyEngine:
    def __init__(self, conn):
        self.conn = conn
        self.begin_called = False

    def connect(self):
        return self.conn

    def begin(self):
        self.begin_called = True
        raise AssertionError("safe_read_mappings must not use engine.begin()")


def test_safe_read_mappings_uses_connect_only() -> None:
    engine = _DummyEngine(_DummyConnection([{"code": "005930"}]))
    rows, fail_open = safe_read_mappings(engine, sa.text("select 1"), op_name="orders.list_today_orders", fail_open=False)
    assert rows == [{"code": "005930"}]
    assert fail_open is False
    assert engine.begin_called is False


def test_safe_read_mappings_fail_open_returns_empty() -> None:
    class FailingEngine:
        def connect(self):
            raise sa.exc.DBAPIError("stmt", {}, Exception("boom"))

    rows, fail_open = safe_read_mappings(FailingEngine(), sa.text("select 1"), op_name="orders.list_today_orders", fail_open=True)
    assert rows == []
    assert fail_open is True


def test_safe_read_mappings_fills_repo_fail_open_returns_empty() -> None:
    """FillsRepo가 사용하는 safe_read_mappings 경로에서 fail-open이 정상 동작하는지 확인"""
    class FailingConn:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, _stmt):
            raise sa.exc.OperationalError("stmt", {}, Exception("timeout"))

    class FailingEngine:
        def connect(self): return FailingConn()

    rows, fail_open = safe_read_mappings(
        FailingEngine(), sa.text("select 1"),
        op_name="fills.list_today_fills", fail_open=True
    )
    assert rows == []
    assert fail_open is True


def test_safe_read_mappings_positions_repo_fail_open_returns_empty() -> None:
    """PositionsRepo가 사용하는 safe_read_mappings 경로에서 fail-open이 정상 동작하는지 확인"""
    class FailingConn:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, _stmt):
            raise sa.exc.ProgrammingError("stmt", {}, Exception("active tx"))

    class FailingEngine:
        disposed = False
        def connect(self): return FailingConn()
        def dispose(self): self.disposed = True

    eng = FailingEngine()
    rows, fail_open = safe_read_mappings(
        eng, sa.text("select 1"),
        op_name="positions.list_positions", fail_open=True
    )
    assert rows == []
    assert fail_open is True


def test_safe_read_mappings_order_repo_fail_open_returns_none_like() -> None:
    """OrdersRepo.get_order_by_client_order_key fail-open — empty list (caller handles None)"""
    class FailingConn:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, _stmt):
            raise sa.exc.OperationalError("stmt", {}, Exception("pgbouncer timeout"))

    class FailingEngine:
        def connect(self): return FailingConn()

    rows, fail_open = safe_read_mappings(
        FailingEngine(), sa.text("select 1"),
        op_name="orders.get_order_by_client_order_key", fail_open=True
    )
    assert rows == []
    assert fail_open is True


def test_tick_scope_cache_prevents_repeated_today_fill_queries() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine._tick_db_cache = {}
    engine._tick_warning_counts = {}
    engine._warning_counts = {}
    engine._consume_fill_lookup_fail_open = lambda _op: False
    engine._bump_warning = lambda *_args, **_kwargs: None
    calls = {"count": 0}

    class DummyFillsRepo:
        def list_today_fills(self, _env, code=None, side=None):
            calls["count"] += 1
            return [{"code": code, "side": side}]

    engine.fills_repo = DummyFillsRepo()

    first = PB1Engine._safe_list_today_fills(engine, code="005930", side="BUY")
    second = PB1Engine._safe_list_today_fills(engine, code="005930", side="BUY")
    assert first == second
    assert calls["count"] == 1