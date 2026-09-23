from __future__ import annotations

from datetime import date

import pytest
import requests

from trader.kis_wrapper import (
    KisAPI,
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
