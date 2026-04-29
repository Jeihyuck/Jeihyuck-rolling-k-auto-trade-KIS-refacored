"""
tests/test_db_engine_dispose_on_active_tx.py

목적:
- safe_read_mappings에서 ProgrammingError("can't change autocommit now: connection in transaction status ACTIVE")가 발생 시
  engine.dispose()가 호출되는가 검증
- fail_open=True면 [] 반환되는가 검증
"""
from __future__ import annotations

import sys
from pathlib import Path

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trader.db.engine import safe_read_mappings


class _DummyEngine:
    def __init__(self, exc_to_raise: Exception):
        self.disposed = False
        self._exc = exc_to_raise

    def connect(self):
        raise self._exc

    def dispose(self):
        self.disposed = True


def test_safe_read_mappings_disposes_on_active_transaction_programming_error():
    """ProgrammingError + active tx poison → dispose 호출 + fail_open=True면 [] 반환"""
    engine = _DummyEngine(
        sa.exc.ProgrammingError(
            "stmt",
            {},
            Exception("can't change 'autocommit' now: connection in transaction status ACTIVE"),
        )
    )
    rows, fail_open_triggered = safe_read_mappings(
        engine,
        sa.text("select 1"),
        op_name="orders.get_order_by_client_order_key",
        fail_open=True,
    )

    assert rows == []
    assert fail_open_triggered is True
    assert engine.disposed is True, "engine.dispose() must be called on poison connection error"


def test_safe_read_mappings_disposes_on_statement_timeout():
    """OperationalError + statement timeout → dispose 호출"""
    engine = _DummyEngine(
        sa.exc.OperationalError(
            "stmt",
            {},
            Exception("ERROR:  canceling statement due to statement timeout"),
        )
    )
    rows, fail_open_triggered = safe_read_mappings(
        engine,
        sa.text("select 1"),
        op_name="fills.list_fills_in_window",
        fail_open=True,
    )

    assert rows == []
    assert fail_open_triggered is True
    assert engine.disposed is True


def test_safe_read_mappings_no_dispose_on_generic_error():
    """일반 DB 에러는 dispose를 호출하지 않는다 (fail_open이면 [] 반환)"""
    engine = _DummyEngine(
        sa.exc.OperationalError(
            "stmt",
            {},
            Exception("deadlock detected"),  # poison 아님
        )
    )
    rows, fail_open_triggered = safe_read_mappings(
        engine,
        sa.text("select 1"),
        op_name="orders.list_today_orders",
        fail_open=True,
    )

    assert rows == []
    assert fail_open_triggered is True
    # dispose는 poison 에러일 때만 호출 (deadlock은 poison 아님)
    assert engine.disposed is False


def test_safe_read_mappings_raises_on_poison_when_fail_open_false():
    """fail_open=False이면 poison 에러도 raise"""
    engine = _DummyEngine(
        sa.exc.ProgrammingError(
            "stmt",
            {},
            Exception("can't change 'autocommit' now: connection in transaction status ACTIVE"),
        )
    )
    import pytest
    with pytest.raises(sa.exc.ProgrammingError):
        safe_read_mappings(
            engine,
            sa.text("select 1"),
            op_name="orders.test_op",
            fail_open=False,
        )
    # dispose는 raise 전에도 호출되어야 함
    assert engine.disposed is True
