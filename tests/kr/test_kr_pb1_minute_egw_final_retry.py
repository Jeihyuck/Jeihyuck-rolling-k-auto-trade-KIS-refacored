"""Observer EGW002 last-attempt path must not waste the SELL/entry deadline.

Fully offline, no KIS credentials or HTTP. Other callers' retry policy stays
unchanged; this only asserts that a configured final failure does not sleep.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trader import kis_wrapper as kw
from trader.kr import kis_observer_budget as budget


def test_single_request_observer_egw002_fails_closed_without_final_backoff(monkeypatch):
    monkeypatch.setenv("KR_PB1_OBSERVER_ONLY", "1")
    monkeypatch.setenv("KR_PB1_KIS_OBSERVER_BUDGET_ENABLED", "1")
    monkeypatch.setenv("KIS_DATA_RETRY_MAX", "3")
    monkeypatch.setenv("PB1_KIS_RATE_LIMIT_SAFE", "0")
    monkeypatch.setenv("STRATEGY_MODE", "")
    monkeypatch.setenv("DIAG_KIS_CALLS_ENABLED", "1")
    monkeypatch.setenv("FORCE_HTTP", "1")

    body = {"rt_cd": "1", "msg_cd": "EGW00201",
            "msg1": "초당 거래건수 초과"}
    response = SimpleNamespace(status_code=200, text="", json=lambda: body)
    api = object.__new__(kw.KisAPI)
    api.env = "practice"
    api.CANO = "123"
    api.ACNT_PRDT_CD = "01"
    api._safe_attempts = 3
    api.session = SimpleNamespace(request=MagicMock(return_value=response))
    api._rate_limit_safe_enabled = lambda: False
    api._wait_for_http_gap = lambda *a, **kw: None
    api._wait_before_data_request = lambda *a, **kw: None

    gate = SimpleNamespace(acquire=lambda **kw: 0.0,
                           set_global_cooldown=lambda *a, **kw: None)
    monkeypatch.setattr(kw, "get_kis_gate", lambda: gate)
    monkeypatch.setattr(kw, "_breaker_check", lambda *a, **kw: (False, None))
    monkeypatch.setattr(kw, "_breaker_record_temp_failure", lambda *a, **kw: None)
    monkeypatch.setattr(kw, "_price_cache",
                        SimpleNamespace(open_circuit=lambda **kw: None))
    monkeypatch.setattr(kw, "_egw002_backoff_sleep",
                        lambda **kw: pytest.fail("final EGW002 must not sleep"))
    monkeypatch.setattr(budget, "acquire_observer_slot_bounded",
                        lambda **kw: (True, "GRANTED", 0.0))
    monkeypatch.setattr(kw, "kis_http_allowed", lambda *a, **kw: True)

    with pytest.raises(kw.KisTemporaryError, match="EGW002"):
        api._safe_request(
            "GET",
            "https://example.invalid/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            endpoint="inquire-time-itemchartprice",
        )
    # KIS returned an explicit rate-limit denial; never classify it as BUY
    # consent or generate extra HTTP from the low-priority observer.
    assert api.session.request.call_count == 1
