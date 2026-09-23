from __future__ import annotations

from datetime import date

import pytest
import requests

from trader.kis_wrapper import (
    KisAPI,
    KisAuthError,
    KisOrderOutcomeUnknown,
    KisTemporaryError,
    is_kr_order_submit_outcome_ambiguous,
)
from trader.reconcile_kis import _has_unresolved_broker_activity


def test_kr_order_submit_ambiguity_classification():
    assert is_kr_order_submit_outcome_ambiguous(
        KisTemporaryError("KR_TICK_DEADLINE_EXHAUSTED_BEFORE_KIS_RETRY")
    )
    assert not is_kr_order_submit_outcome_ambiguous(
        KisTemporaryError("KR_TICK_DEADLINE_EXHAUSTED_BEFORE_KIS_REQUEST")
    )
    assert not is_kr_order_submit_outcome_ambiguous(
        KisTemporaryError("request failed after retries: /uapi/hashkey")
    )


def test_domestic_order_post_is_never_blind_retried(monkeypatch):
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("KIS_HTTP_ENABLED", "1")
    monkeypatch.setenv("PB1_KIS_RATE_LIMIT_SAFE", "0")
    monkeypatch.setenv("KIS_ORDER_RETRY_MAX", "5")
    monkeypatch.setattr(KisAPI, "get_valid_token", lambda self: "TEST")
    monkeypatch.setattr(KisAPI, "_load_safe_mode_state", lambda self: None)

    class Gate:
        def acquire(self, **_kwargs):
            return 0.0

        def set_global_cooldown(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr("trader.kis_wrapper.get_kis_gate", lambda: Gate())
    monkeypatch.setattr("trader.kis_wrapper._breaker_check", lambda *_a, **_k: (False, None))
    monkeypatch.setattr("trader.kis_wrapper._breaker_record_temp_failure", lambda *_a, **_k: None)

    api = KisAPI(kis_env="practice")
    calls = {"n": 0}

    def timeout(*_args, **_kwargs):
        calls["n"] += 1
        raise requests.Timeout("accepted but ACK lost")

    monkeypatch.setattr(api.session, "request", timeout)

    with pytest.raises(KisTemporaryError):
        api._safe_request(
            "POST",
            "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash",
            headers={},
            data=b"{}",
            timeout=(0.05, 0.05),
        )
    assert calls["n"] == 1


def test_unresolved_ack_blocks_empty_holdings_purge_without_db_lookup():
    class Orders:
        def get_open_orders(self, *_args, **_kwargs):
            return [{"status": "UNRESOLVED_ACK"}]

    assert _has_unresolved_broker_activity(
        engine=object(),
        orders_repo=Orders(),
        env="practice",
        trade_date=date(2026, 9, 23),
    )


def test_accepted_limit_buy_never_falls_back_to_second_market_order(monkeypatch):
    from trader.execution import place_buy_with_fallback

    monkeypatch.setattr("trader.execution.time.sleep", lambda *_args, **_kwargs: None)

    class FakeKIS:
        def __init__(self):
            self.limit_calls = 0
            self.market_calls = 0

        def buy_stock_limit_guarded(self, code, qty, price):
            self.limit_calls += 1
            return {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": "O1"}}

        def check_filled(self, _result):
            return False

        def buy_stock_market_guarded(self, code, qty):
            self.market_calls += 1
            return {"rt_cd": "0", "output": {"ODNO": "O2"}}

        def get_current_price(self, code):
            return 10000

    kis = FakeKIS()
    result = place_buy_with_fallback(kis, "000660", 1, 10000)

    assert result["output"]["ODNO"] == "O1"
    assert kis.limit_calls == 1
    assert kis.market_calls == 0


def test_explicit_limit_reject_never_falls_back_to_market(monkeypatch):
    from trader.execution import place_buy_with_fallback

    monkeypatch.setattr("trader.execution.time.sleep", lambda *_args, **_kwargs: None)

    class FakeKIS:
        def __init__(self):
            self.market_calls = 0

        def buy_stock_limit_guarded(self, code, qty, price):
            return {"rt_cd": "1", "msg_cd": "BROKER_REJECT", "msg1": "rejected"}

        def check_filled(self, _result):
            return False

        def buy_stock_market_guarded(self, code, qty):
            self.market_calls += 1
            return {"rt_cd": "0", "output": {"ODNO": "O2"}}

        def get_current_price(self, code):
            return 10000

    kis = FakeKIS()
    result = place_buy_with_fallback(kis, "000660", 1, 10000)

    assert result["rt_cd"] == "1"
    assert kis.market_calls == 0


def test_sell_timeout_is_not_blindly_retried():
    from trader.execution import _sell_once

    class FakeKIS:
        def __init__(self):
            self.sell_calls = 0

        def get_current_price(self, code):
            return 10000

        def sell_stock_market(self, code, qty):
            self.sell_calls += 1
            raise KisTemporaryError("ACK lost after POST")

    kis = FakeKIS()
    with pytest.raises(KisTemporaryError):
        _sell_once(kis, "000660", 1, prefer_market=True)

    assert kis.sell_calls == 1



def _configure_direct_order_http_test(monkeypatch):
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("KIS_HTTP_ENABLED", "1")
    monkeypatch.setenv("PB1_KIS_RATE_LIMIT_SAFE", "0")
    monkeypatch.setenv("KIS_ORDER_RETRY_MAX", "5")
    monkeypatch.setattr(KisAPI, "get_valid_token", lambda self: "TEST")
    monkeypatch.setattr(KisAPI, "_load_safe_mode_state", lambda self: None)

    class Gate:
        def acquire(self, **_kwargs):
            return 0.0

        def set_global_cooldown(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr("trader.kis_wrapper.get_kis_gate", lambda: Gate())
    monkeypatch.setattr("trader.kis_wrapper._breaker_check", lambda *_a, **_k: (False, None))
    monkeypatch.setattr("trader.kis_wrapper._breaker_record_temp_failure", lambda *_a, **_k: None)


def test_order_http_200_invalid_json_becomes_unresolved_ack(monkeypatch):
    _configure_direct_order_http_test(monkeypatch)
    api = KisAPI(kis_env="practice")
    calls = {"n": 0}

    class BadJsonResponse:
        status_code = 200
        text = "<html>gateway returned non-json</html>"

        def json(self):
            raise ValueError("invalid json")

    def respond(*_args, **_kwargs):
        calls["n"] += 1
        return BadJsonResponse()

    monkeypatch.setattr(api.session, "request", respond)

    with pytest.raises(KisOrderOutcomeUnknown) as exc_info:
        api._safe_request(
            "POST",
            "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash",
            headers={},
            data=b"{}",
            timeout=(0.05, 0.05),
        )

    assert calls["n"] == 1
    assert is_kr_order_submit_outcome_ambiguous(exc_info.value) is True


def test_order_401_is_explicit_auth_reject_not_unresolved(monkeypatch):
    _configure_direct_order_http_test(monkeypatch)
    api = KisAPI(kis_env="practice")
    calls = {"n": 0}
    refreshes = {"n": 0}

    class AuthRejectResponse:
        status_code = 401
        text = "unauthorized"

        def json(self):
            return {"rt_cd": "1", "msg_cd": "AUTH", "msg1": "unauthorized"}

    def respond(*_args, **_kwargs):
        calls["n"] += 1
        return AuthRejectResponse()

    monkeypatch.setattr(api.session, "request", respond)
    monkeypatch.setattr(api, "refresh_token", lambda: refreshes.__setitem__("n", refreshes["n"] + 1))

    with pytest.raises(KisAuthError) as exc_info:
        api._safe_request(
            "POST",
            "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash",
            headers={},
            data=b"{}",
            timeout=(0.05, 0.05),
        )

    assert calls["n"] == 1
    assert refreshes["n"] == 1
    assert is_kr_order_submit_outcome_ambiguous(exc_info.value) is False



def _bare_order_cash_api(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("NO_TRADE", "0")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("FORCE_RUN", "1")
    monkeypatch.setenv("INTENDED_LIVE", "1")
    monkeypatch.setattr(
        "trader.config.get_live_gate_status_fresh",
        lambda **_kwargs: SimpleNamespace(
            allow_live_gate=True,
            force_block_live=False,
            reason="ok",
            window="day",
            now_kst=__import__("datetime").datetime.now(),
        ),
    )
    monkeypatch.setattr("trader.kis_wrapper._assert_orders_allowed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trader.kis_wrapper._pick_tr", lambda *_args, **_kwargs: ["VTTC0802U"])
    monkeypatch.setattr("trader.kis_wrapper.emit_event", lambda **_kwargs: None)

    api = KisAPI.__new__(KisAPI)
    api.env = "practice"
    api._create_hashkey = lambda _body: "HASH"
    api._headers = lambda _tr, _hk=None: {}
    api._wait_before_order_submit = lambda: None
    return api


def _market_order_body():
    return {
        "CANO": "00000000",
        "ACNT_PRDT_CD": "01",
        "PDNO": "000660",
        "ORD_QTY": "1",
        "ORD_DVSN": "01",
        "ORD_UNPR": "0",
    }


def test_order_cash_outer_loop_does_not_retry_ambiguous_post(monkeypatch):
    api = _bare_order_cash_api(monkeypatch)
    calls = {"n": 0}

    def ambiguous(*_args, **_kwargs):
        calls["n"] += 1
        raise KisOrderOutcomeUnknown("ACK lost after POST")

    api._safe_request = ambiguous

    with pytest.raises(KisOrderOutcomeUnknown):
        api._order_cash(_market_order_body(), is_sell=False)

    assert calls["n"] == 1


def test_order_cash_outer_loop_does_not_retry_auth_reject(monkeypatch):
    api = _bare_order_cash_api(monkeypatch)
    calls = {"n": 0}

    def rejected(*_args, **_kwargs):
        calls["n"] += 1
        raise KisAuthError("HTTP 401 order auth rejection")

    api._safe_request = rejected

    with pytest.raises(KisAuthError):
        api._order_cash(_market_order_body(), is_sell=True)

    assert calls["n"] == 1


def test_order_cash_explicit_gateway_reject_does_not_change_order_mode(monkeypatch):
    api = _bare_order_cash_api(monkeypatch)
    calls = {"n": 0}

    class GatewayReject:
        status_code = 200

        def json(self):
            return {"rt_cd": "1", "msg_cd": "IGW00008", "msg1": "gateway reject"}

    def rejected(*_args, **_kwargs):
        calls["n"] += 1
        return GatewayReject()

    api._safe_request = rejected
    result = api._order_cash(_market_order_body(), is_sell=False)

    assert result["rt_cd"] == "1"
    assert result["msg_cd"] == "IGW00008"
    assert calls["n"] == 1
