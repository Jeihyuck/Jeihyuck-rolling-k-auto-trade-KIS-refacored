from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError, OperationalError

from trader.db.repos import OrdersRepo


class _FakeMappingsResult:
    def all(self):
        return []


class _FakeConn:
    def __init__(self, exc: Exception | None = None):
        self._exc = exc

    def execution_options(self, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, stmt):
        if self._exc is not None:
            raise self._exc
        return self

    def mappings(self):
        return _FakeMappingsResult()


class _FakeEngine:
    def __init__(self, exc: Exception | None = None):
        self._exc = exc

    def connect(self):
        return _FakeConn(self._exc)


@pytest.mark.parametrize(
    "exc",
    [
        OperationalError("SELECT 1", {}, Exception("lock timeout")),
        DBAPIError("SELECT 1", {}, Exception("dbapi timeout")),
    ],
)
def test_read_mappings_with_guard_fail_open_returns_empty(exc) -> None:
    repo = OrdersRepo.__new__(OrdersRepo)
    repo.engine = _FakeEngine(exc)
    repo._last_read_fail_open_op = None

    rows = repo._read_mappings_with_guard(object(), op_name="orders.get_open_orders", fail_open=True)

    assert rows == []
    assert repo.consume_fail_open_marker("orders.get_open_orders") is True


@pytest.mark.parametrize(
    "exc",
    [
        OperationalError("SELECT 1", {}, Exception("lock timeout")),
        DBAPIError("SELECT 1", {}, Exception("dbapi timeout")),
    ],
)
def test_read_mappings_with_guard_fail_closed_raises(exc) -> None:
    repo = OrdersRepo.__new__(OrdersRepo)
    repo.engine = _FakeEngine(exc)
    repo._last_read_fail_open_op = None

    with pytest.raises(type(exc)):
        repo._read_mappings_with_guard(object(), op_name="orders.get_open_orders", fail_open=False)
