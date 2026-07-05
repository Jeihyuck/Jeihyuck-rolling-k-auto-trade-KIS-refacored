import logging
import pytest

import trader.db.repos as repos
from trader.db.repos import OrdersRepo, FillsRepo


class DummyEngine:
    pass


def _boom(*args, **kwargs):
    raise TimeoutError("db timeout")


def test_practice_order_lookup_fail_open_returns_empty_list(monkeypatch, caplog):
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT", "1")
    monkeypatch.setattr(repos, "safe_read_mappings", _boom)
    repo = OrdersRepo.__new__(OrdersRepo)
    repo.engine = DummyEngine()
    repo._last_read_fail_open_op = None
    with caplog.at_level(logging.WARNING):
        rows = repo._read_mappings_with_guard(object(), op_name="orders.get_open_orders", fail_open=repos._resolve_lookup_fail_open("practice"))
    assert rows == []
    assert repo.consume_fail_open_marker("orders.get_open_orders") is True
    assert "[DB][READ][FAIL_OPEN] op=orders.get_open_orders -> returning fallback=list" in caplog.text


def test_real_order_lookup_fail_open_default_disabled_raises(monkeypatch, caplog):
    monkeypatch.setenv("STRATEGY_ENV", "real")
    monkeypatch.setenv("PB1_REAL_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT", "0")
    monkeypatch.setattr(repos, "safe_read_mappings", _boom)
    repo = OrdersRepo.__new__(OrdersRepo)
    repo.engine = DummyEngine()
    repo._last_read_fail_open_op = None
    with caplog.at_level(logging.WARNING), pytest.raises(TimeoutError):
        repo._read_mappings_with_guard(object(), op_name="orders.list_today_orders", fail_open=repos._resolve_lookup_fail_open("real"))
    assert "op=orders.list_today_orders fail_open=0" in caplog.text


def test_practice_order_lookup_fail_open_disabled_raises(monkeypatch, caplog):
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT", "0")
    monkeypatch.setattr(repos, "safe_read_mappings", _boom)
    repo = OrdersRepo.__new__(OrdersRepo)
    repo.engine = DummyEngine()
    repo._last_read_fail_open_op = None
    with caplog.at_level(logging.WARNING), pytest.raises(TimeoutError):
        repo._read_mappings_with_guard(object(), op_name="orders.has_blocking_order_today", fail_open=repos._resolve_lookup_fail_open("practice"))
    assert "op=orders.has_blocking_order_today fail_open=0" in caplog.text


def test_practice_fills_lookup_fail_open_returns_empty_list(monkeypatch, caplog):
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT", "1")
    monkeypatch.setattr(repos, "safe_read_mappings", _boom)
    repo = FillsRepo.__new__(FillsRepo)
    repo.engine = DummyEngine()
    repo._last_read_fail_open_op = None
    with caplog.at_level(logging.WARNING):
        rows = repo._read_mappings_with_guard(object(), op_name="fills.list_today_fills", fail_open=repos._resolve_lookup_fail_open("practice"))
    assert rows == []
    assert repo.consume_fail_open_marker("fills.list_today_fills") is True
    assert "[DB][READ][FAIL_OPEN] op=fills.list_today_fills -> returning fallback=list" in caplog.text
