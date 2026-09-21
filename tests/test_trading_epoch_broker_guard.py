from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from trader.db import trading_epoch_broker_guard as guard


def _complete_us_balance():
    return {
        "positions": [],
        "balance_parse_status": "OK",
        "balance_complete": True,
        "balance_authoritative": True,
        "failed_exchanges": {},
        "queried_exchanges": ["NASD", "NYSE", "AMEX"],
        "exchange_result_counts": {"NASD": 0, "NYSE": 0, "AMEX": 0},
    }


def _kr_page(rows=None, *, cursor="", status="D", **overrides):
    return {
        "rt_cd": "0",
        "output1": [] if rows is None else rows,
        "ctx_area_fk100": cursor,
        "ctx_area_nk100": cursor,
        "_response_meta": {"tr_cont": status},
        **overrides,
    }


@pytest.fixture
def broker(monkeypatch):
    kr = SimpleNamespace(
        get_balance_cached=Mock(return_value={"output1": []}),
        inquire_daily_ccld=Mock(return_value=_kr_page()),
    )
    us = SimpleNamespace(
        get_balance=Mock(return_value=_complete_us_balance()),
        get_today_orders=Mock(return_value=[]),
    )
    seen = {"kr_env": None, "us_env": None}

    def make_kr(**kwargs):
        seen["kr_env"] = kwargs.get("kis_env")
        return kr

    def make_us(**kwargs):
        seen["us_env"] = kwargs.get("env")
        assert kwargs.get("offline") is False
        return us

    monkeypatch.setattr(guard, "KisAPI", make_kr)
    monkeypatch.setattr(guard, "USDataProvider", make_us)
    return SimpleNamespace(kr=kr, us=us, seen=seen)


def test_broker_flat_guard_accepts_only_complete_flat_evidence(broker):
    result = guard.verify_broker_flat(env="practice")
    assert result["status"] == "BROKER_FLAT_VERIFIED"
    assert result["kr_holdings"] == 0
    assert result["us_holdings"] == 0
    assert broker.seen == {"kr_env": "practice", "us_env": "practice"}
    broker.kr.get_balance_cached.assert_called_once_with(force=True)
    broker.us.get_balance.assert_called_once_with(force_refresh=True)


def test_broker_flat_guard_is_real_env_aware(broker):
    result = guard.verify_broker_flat(env="real")
    assert result["env"] == "real"
    assert broker.seen == {"kr_env": "real", "us_env": "real"}


@pytest.mark.parametrize("balance,error", [
    ({"output1": [{"pdno": "005930", "hldg_qty": "1"}]}, "KR_KIS_ACCOUNT_NOT_FLAT"),
    ({"output1": None}, "KR_KIS_BALANCE_NOT_AUTHORITATIVE"),
    ({"output1": [{"pdno": "005930", "hldg_qty": "bad"}]}, "BROKER_QUANTITY_INVALID"),
    ({"output1": [], "_diag_stub": True}, "KR_KIS_BALANCE_NOT_AUTHORITATIVE"),
])
def test_broker_flat_guard_rejects_bad_kr_balance(broker, balance, error):
    broker.kr.get_balance_cached.return_value = balance
    with pytest.raises(guard.TradingEpochBrokerGuardError, match=error):
        guard.verify_broker_flat(env="practice")
    broker.us.get_balance.assert_not_called()


@pytest.mark.parametrize("pages,error", [
    ([_kr_page([{"ord_qty": 10, "tot_ccld_qty": 3, "rmn_qty": 7}])], "KR_KIS_PENDING_ORDERS_EXIST"),
    ([_kr_page(cursor="p2", status="M"), _kr_page(rt_cd="1")], "KR_KIS_ORDER_QUERY_NOT_AUTHORITATIVE"),
    ([_kr_page(cursor="p2", status="M"), _kr_page(cursor="p2", status="M")], "KR_KIS_ORDER_PAGINATION_STALLED"),
    ([_kr_page(status="M")], "KR_KIS_ORDER_PAGINATION_CURSOR_MISSING"),
    ([_kr_page(status="?")], "KR_KIS_ORDER_PAGINATION_UNKNOWN_STATUS"),
    ([_kr_page([{"ord_qty": 10, "tot_ccld_qty": 0, "rmn_qty": "bad"}])], "BROKER_QUANTITY_INVALID"),
])
def test_broker_flat_guard_rejects_kr_order_gaps(broker, pages, error):
    broker.kr.inquire_daily_ccld.side_effect = deepcopy(pages)
    with pytest.raises(guard.TradingEpochBrokerGuardError, match=error):
        guard.verify_broker_flat(env="practice")
    broker.us.get_balance.assert_not_called()


def test_kr_order_pagination_follows_cursor(broker):
    broker.kr.inquire_daily_ccld.side_effect = [
        _kr_page(cursor="p2", status="M"),
        _kr_page(cursor="p2", status="D"),
    ]
    guard.verify_broker_flat(env="practice")
    assert broker.kr.inquire_daily_ccld.call_count == 2
    assert broker.kr.inquire_daily_ccld.call_args.kwargs["ctx_area_fk100"] == "p2"


@pytest.mark.parametrize("field,value,error", [
    ("balance_complete", False, "US_KIS_BALANCE_INCOMPLETE"),
    ("balance_authoritative", False, "US_KIS_BALANCE_NOT_AUTHORITATIVE"),
    ("balance_parse_status", "ERROR", "US_KIS_BALANCE_PARSE_NOT_OK"),
    ("failed_exchanges", {"NYSE": "timeout"}, "US_KIS_BALANCE_EXCHANGE_FAILURE"),
    ("queried_exchanges", ["NASD", "NYSE"], "US_KIS_BALANCE_EXCHANGE_COVERAGE_INCOMPLETE"),
    ("exchange_result_counts", {"NASD": 0, "NYSE": 0}, "US_KIS_BALANCE_EXCHANGE_COUNTS_INCOMPLETE"),
    ("exchange_result_counts", None, "US_KIS_BALANCE_EXCHANGE_COUNTS_MISSING"),
    ("positions", [{"symbol": "AAPL", "qty": 1}], "US_KIS_ACCOUNT_NOT_FLAT"),
    ("positions", [{"symbol": "AAPL", "qty": "bad"}], "BROKER_QUANTITY_INVALID"),
])
def test_broker_flat_guard_rejects_bad_us_balance(broker, field, value, error):
    broker.us.get_balance.return_value[field] = value
    with pytest.raises(guard.TradingEpochBrokerGuardError, match=error):
        guard.verify_broker_flat(env="practice")
    broker.us.get_today_orders.assert_not_called()


@pytest.mark.parametrize("orders,error", [
    ([{"remaining_qty": 1, "normalization_result": "normalized"}], "US_KIS_PENDING_ORDERS_EXIST"),
    ([{"remaining_qty": 0, "normalization_result": "quarantined"}], "US_KIS_ORDER_QUERY_QUARANTINED"),
    ([{"remaining_qty": "bad", "normalization_result": "normalized"}], "BROKER_QUANTITY_INVALID"),
    (None, "US_KIS_ORDER_QUERY_NOT_AUTHORITATIVE"),
])
def test_broker_flat_guard_rejects_us_order_gaps(broker, orders, error):
    broker.us.get_today_orders.return_value = orders
    with pytest.raises(guard.TradingEpochBrokerGuardError, match=error):
        guard.verify_broker_flat(env="practice")


def test_broker_flat_guard_rejects_unsupported_env(broker):
    with pytest.raises(guard.TradingEpochBrokerGuardError, match="TRADING_EPOCH_ENV_UNSUPPORTED"):
        guard.verify_broker_flat(env="paper")
    broker.kr.get_balance_cached.assert_not_called()
    broker.us.get_balance.assert_not_called()


def test_start_epoch_checks_broker_before_db_mutation(monkeypatch):
    from scripts import start_new_trading_epoch as start

    events = []
    monkeypatch.setenv("TRADING_EPOCH_CONFIRM", "YES")
    monkeypatch.setenv("TRADING_EPOCH_BROKER_FLAT_CONFIRMED", "YES")
    monkeypatch.setenv("TRADING_EPOCH_REASON", "TEST_RESET")
    monkeypatch.setattr(start, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(start, "get_account_key", lambda **kwargs: "practice:test")
    monkeypatch.setattr(start, "get_masked_account_key", lambda **kwargs: "masked")
    monkeypatch.setattr(start, "verify_broker_flat", lambda **kwargs: events.append("broker") or {"status": "BROKER_FLAT_VERIFIED"})
    engine = object()
    monkeypatch.setattr(start, "make_engine", lambda: events.append("engine") or engine)
    monkeypatch.setattr(start, "run_migrations", lambda value: events.append("migration"))
    monkeypatch.setattr(start, "start_new_trading_epoch", lambda *args, **kwargs: events.append("epoch") or "epoch-1")

    assert start.main() == 0
    assert events == ["broker", "engine", "migration", "epoch"]


def test_start_epoch_broker_failure_mutates_no_db(monkeypatch):
    from scripts import start_new_trading_epoch as start

    monkeypatch.setenv("TRADING_EPOCH_CONFIRM", "YES")
    monkeypatch.setenv("TRADING_EPOCH_BROKER_FLAT_CONFIRMED", "YES")
    monkeypatch.setenv("TRADING_EPOCH_REASON", "TEST_RESET")
    monkeypatch.setattr(start, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(start, "get_account_key", lambda **kwargs: "practice:test")
    monkeypatch.setattr(
        start,
        "verify_broker_flat",
        lambda **kwargs: (_ for _ in ()).throw(
            guard.TradingEpochBrokerGuardError("US_KIS_BALANCE_INCOMPLETE")
        ),
    )
    make_engine = Mock()
    migrate = Mock()
    epoch = Mock()
    monkeypatch.setattr(start, "make_engine", make_engine)
    monkeypatch.setattr(start, "run_migrations", migrate)
    monkeypatch.setattr(start, "start_new_trading_epoch", epoch)

    with pytest.raises(guard.TradingEpochBrokerGuardError, match="US_KIS_BALANCE_INCOMPLETE"):
        start.main()
    make_engine.assert_not_called()
    migrate.assert_not_called()
    epoch.assert_not_called()


def test_kr_wrapper_carries_pagination_cursor_and_response_status(monkeypatch):
    from trader import kis_wrapper

    monkeypatch.setattr(kis_wrapper, "kis_http_enabled", lambda: True)
    monkeypatch.setattr(kis_wrapper, "_pick_tr", lambda *args: ["TEST_TR"])
    response = SimpleNamespace(
        status_code=200,
        headers={"tr_cont": "D"},
        json=lambda: _kr_page(),
    )
    kis = object.__new__(kis_wrapper.KisAPI)
    kis.env = "practice"
    kis.CANO = "test"
    kis.ACNT_PRDT_CD = "01"
    kis._headers = lambda tr_id: {"tr_id": tr_id}
    kis.session = SimpleNamespace(request=Mock(return_value=response))
    result = kis.inquire_daily_ccld(
        start_date="20260921",
        end_date="20260921",
        ctx_area_fk100="FK",
        ctx_area_nk100="NK",
    )
    kwargs = kis.session.request.call_args.kwargs
    assert kwargs["headers"]["tr_cont"] == "N"
    assert kwargs["params"]["CTX_AREA_FK100"] == "FK"
    assert kwargs["params"]["CTX_AREA_NK100"] == "NK"
    assert result["_response_meta"]["tr_cont"] == "D"
