"""Read-only KR KIS request accounting: correlate real HTTP attempts and errors.

All HTTP is mocked; no credentials, orders, exchange calls, or DB activity.
"""
from __future__ import annotations

import logging
import re
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trader import kis_wrapper as kw


def _api(monkeypatch, *, body):
    monkeypatch.setenv("KR_KIS_NATIVE_HTTP_AUDIT", "1")
    monkeypatch.setenv("STRATEGY_MODE", "")
    monkeypatch.setenv("DIAG_KIS_CALLS_ENABLED", "1")
    monkeypatch.setenv("FORCE_HTTP", "1")
    monkeypatch.setenv("PB1_KIS_RATE_LIMIT_SAFE", "0")
    monkeypatch.setenv("KR_PB1_KIS_OBSERVER_BUDGET_ENABLED", "0")
    response = SimpleNamespace(
        status_code=200, text="", json=lambda: dict(body),
    )
    api = object.__new__(kw.KisAPI)
    api.env = "practice"
    api.CANO = "987654321"
    api.ACNT_PRDT_CD = "01"
    api._safe_attempts = 1
    api._safe_max_seconds = 10.0
    api._safe_backoff_base = 1.0
    api._safe_backoff_cap = 4.0
    api.session = SimpleNamespace(request=MagicMock(return_value=response))
    api._rate_limit_safe_enabled = lambda: False
    api._wait_for_http_gap = lambda *args, **kwargs: None
    api._wait_before_data_request = lambda *args, **kwargs: None
    monkeypatch.setattr(kw, "kis_http_allowed", lambda *args, **kwargs: True)
    monkeypatch.setattr(kw, "_breaker_check", lambda *args, **kwargs: (False, None))
    monkeypatch.setattr(kw, "_breaker_record_success", lambda *args, **kwargs: None)
    monkeypatch.setattr(kw, "_breaker_record_temp_failure", lambda *args, **kwargs: None)
    monkeypatch.setattr(kw, "get_kis_gate", lambda: SimpleNamespace(
        acquire=lambda **kwargs: 0.0,
        set_global_cooldown=lambda *args, **kwargs: None,
    ))
    return api


def test_real_http_boundary_logs_same_attempt_id_across_response_and_result(monkeypatch, caplog):
    api = _api(monkeypatch, body={"rt_cd": "0", "msg_cd": "0", "output2": []})
    url = "https://example.invalid/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    with caplog.at_level(logging.INFO):
        response = api._safe_request("GET", url)
    assert api.session.request.call_count == 1
    rid = response._nullim_http_request_id
    assert re.fullmatch(r"[a-f0-9]{32}", rid)
    for stage in ("ATTEMPT", "RESPONSE", "BROKER_RESULT"):
        lines = [line for line in caplog.messages
                 if f"[KR_KIS_HTTP][{stage}]" in line]
        assert len(lines) == 1, (stage, lines)
        assert f"rid={rid}" in lines[0]
    assert "987654321" not in caplog.text
    assert "appsecret" not in caplog.text.lower()


def test_egw002_is_recorded_once_per_http_attempt_even_when_original_logs_repeat(
    monkeypatch, caplog
):
    body = {"rt_cd": "1", "msg_cd": "EGW00201",
            "msg1": "초당 거래건수 초과"}
    api = _api(monkeypatch, body=body)
    url = "https://example.invalid/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    monkeypatch.setattr(kw, "_egw002_backoff_sleep",
                        lambda **kwargs: pytest.fail("final attempt must not sleep"))
    with caplog.at_level(logging.INFO), pytest.raises(kw.KisTemporaryError):
        api._safe_request("GET", url)
    assert api.session.request.call_count == 1
    assert caplog.text.count("[KR_KIS_HTTP][ATTEMPT]") == 1
    assert caplog.text.count("[KR_KIS_HTTP][RESPONSE]") == 1
    assert caplog.text.count("[KR_KIS_HTTP][BROKER_RESULT]") == 1
    assert "msg_cd=EGW00201 egw002=1" in caplog.text
