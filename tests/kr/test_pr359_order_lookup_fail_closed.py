"""Order-state reads used by trading must never turn DB errors into empty sets."""
from datetime import date, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

import trader.db.repos as repos_module
from trader.db.repos import OrdersRepo
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def test_practice_order_lookups_fail_closed_even_if_global_fail_open_is_enabled(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    repo = OrdersRepo(engine)

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setattr(repos_module, "_resolve_lookup_fail_open", lambda *_a, **_k: True)
    monkeypatch.setattr(repos_module, "_order_lookup_fail_open_default", lambda: True)
    monkeypatch.setattr(repos_module, "active_trading_epoch_id", lambda *_a, **_k: None)

    def database_timeout(*_args, **_kwargs):
        raise RuntimeError("INJECTED_ORDER_LOOKUP_DB_TIMEOUT")

    monkeypatch.setattr(repos_module, "safe_read_mappings", database_timeout)
    start = datetime(2026, 11, 13, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    end = start + timedelta(hours=1)
    lookups = {
        "get_open_orders": lambda: repo.get_open_orders("practice"),
        "has_open_order_for_code": lambda: repo.has_open_order_for_code(
            "practice", "251970", "BUY", date(2026, 11, 13),
        ),
        "has_client_order_key": lambda: repo.has_client_order_key("practice", "client-key"),
        "get_order_by_client_order_key": lambda: repo.get_order_by_client_order_key(
            "practice", "client-key",
        ),
        "has_blocking_order_today": lambda: repo.has_blocking_order_today(
            "practice", "251970", "BUY", trade_date="2026-11-13",
        ),
        "list_today_orders": lambda: repo.list_today_orders("practice"),
        "list_today_buy_orders": lambda: repo.list_today_buy_orders("practice"),
        "list_today_sell_orders": lambda: repo.list_today_sell_orders("practice"),
        "list_orders_in_window": lambda: repo.list_orders_in_window(
            "practice", start_at=start, end_at=end, side="BUY", code="251970",
        ),
        "get_today_session_marker_payload": lambda: repo.get_today_session_marker_payload(
            "practice", "am", date(2026, 11, 13),
        ),
        "get_order_by_kis_odno": lambda: repo.get_order_by_kis_odno(
            "practice", "0000040226",
        ),
        "get_order_by_kis_odno_scoped": lambda: repo.get_order_by_kis_odno_scoped(
            "practice", "0000040226", trade_date=date(2026, 11, 13),
            code="251970", side="BUY",
        ),
        "list_orders_by_client_order_keys": lambda: repo.list_orders_by_client_order_keys(
            "practice", ["client-key"],
        ),
        "list_existing_client_order_keys_for_retry": lambda: repo.list_existing_client_order_keys_for_retry(
            "practice", "client-key",
        ),
    }
    for lookup in lookups.values():
        with pytest.raises(RuntimeError, match="INJECTED_ORDER_LOOKUP_DB_TIMEOUT"):
            lookup()


def test_session_marker_lookup_does_not_fail_open_to_false(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    create_schema_with_active_test_epoch(engine)
    repo = OrdersRepo(engine)
    monkeypatch.setattr(repos_module, "_order_lookup_fail_open_default", lambda: True)

    def database_timeout(*_args, **_kwargs):
        raise OperationalError("connect", {}, RuntimeError("INJECTED_SESSION_MARKER_TIMEOUT"))

    monkeypatch.setattr(engine, "connect", database_timeout)
    with pytest.raises(OperationalError, match="INJECTED_SESSION_MARKER_TIMEOUT"):
        repo.has_today_session_marker(
            "practice", "am", "completed", date(2026, 11, 13),
        )
