from __future__ import annotations

import sqlalchemy as sa
import pytest

from trader.db.engine import _apply_postgres_timeout_options, _connect_args_for_db_url, safe_read_mappings


def test_postgres_connect_args_include_timeout_options(monkeypatch):
    monkeypatch.setenv("DB_CONNECT_TIMEOUT_SEC", "5")
    args = _connect_args_for_db_url("postgresql+psycopg://u:p@host/db")
    assert args["connect_timeout"] == 5
    assert "-c lock_timeout=5000" in args["options"]
    assert "-c statement_timeout=15000" in args["options"]
    assert "-c idle_in_transaction_session_timeout=15000" in args["options"]


def test_postgres_timeout_options_append_existing(monkeypatch):
    monkeypatch.setenv("DB_LOCK_TIMEOUT_MS", "5000")
    args = _apply_postgres_timeout_options({"options": "-c search_path=public", "connect_timeout": 5})
    assert args["options"].startswith("-c search_path=public")
    assert "-c lock_timeout=5000" in args["options"]


class _Rows:
    def __init__(self, rows):
        self._rows = rows
    def mappings(self):
        return self
    def all(self):
        return self._rows


class _Conn:
    def __init__(self, *, fail=None):
        self.fail = fail
        self.autocommit_called = False
        self.dialect = type("D", (), {"name": "postgresql"})()
    def execution_options(self, **kwargs):
        if kwargs.get("isolation_level") == "AUTOCOMMIT":
            self.autocommit_called = True
        return self
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def exec_driver_sql(self, _sql):
        return None
    def execute(self, _stmt):
        if self.fail:
            raise self.fail
        return _Rows([{"ok": 1}])


class _Engine:
    def __init__(self, conn):
        self.conn = conn
        self.dialect = type("D", (), {"name": "postgresql"})()
        self.url = "postgresql+psycopg://u:p@host/db"
    def connect(self):
        return self.conn


def test_safe_read_mappings_calls_autocommit():
    conn = _Conn()
    rows, fail_open = safe_read_mappings(_Engine(conn), sa.text("select 1"), op_name="orders.get_open_orders")
    assert conn.autocommit_called is True
    assert rows == [{"ok": 1}]
    assert fail_open is False


def test_safe_read_mappings_timeout_fail_open_returns_empty(monkeypatch):
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    exc = sa.exc.OperationalError("stmt", {}, Exception("statement timeout"))
    rows, fail_open = safe_read_mappings(_Engine(_Conn(fail=exc)), sa.text("select 1"), op_name="orders.get_open_orders", fail_open=True)
    assert rows == []
    assert fail_open is True


def test_safe_read_mappings_real_fail_closed_reraises(monkeypatch):
    monkeypatch.setenv("STRATEGY_ENV", "real")
    exc = sa.exc.OperationalError("stmt", {}, Exception("statement timeout"))
    with pytest.raises(sa.exc.OperationalError):
        safe_read_mappings(_Engine(_Conn(fail=exc)), sa.text("select 1"), op_name="orders.get_open_orders", fail_open=False)
