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


def test_orderable_cash_provenance_rejects_diag_stub_even_when_cash_positive():
    """Simulated data looks like KIS cash but cannot authorize Infinite ADD."""
    api = object.__new__(kw.KisAPI)
    api._last_cash = None
    api._orderable_cash_cache = None
    api._orderable_cash_cache_at = None
    api._write_orderable_cash_status = lambda **kwargs: None
    api._inquire_psbl_order = lambda *args: {
        "rt_cd": "0", "_diag_stub": True,
        "output": {"ord_psbl_cash": "4000000"},
    }
    amount, metadata = api.get_orderable_cash("122630", 63000)
    assert amount == 4000000
    assert metadata["native_source_verified"] is False
    from trader.kr.infinite.executor import KISExecutor
    with pytest.raises(RuntimeError, match="CASH_UNVERIFIED"):
        KISExecutor(api, "practice").verified_add_orderable_cash("122630", 63000)


def test_orderable_cash_provenance_accepts_only_native_kis_psbl_field():
    api = object.__new__(kw.KisAPI)
    api._last_cash = None
    api._orderable_cash_cache = None
    api._orderable_cash_cache_at = None
    api._write_orderable_cash_status = lambda **kwargs: None
    api._inquire_psbl_order = lambda *args: {
        "rt_cd": "0",
        "output": {"ord_psbl_cash": "741311", "dnca_tot_amt": "15000000"},
    }
    from trader.kr.infinite.executor import KISExecutor
    amount, metadata = api.get_orderable_cash("122630", 63000)
    assert amount == 741311
    assert metadata["selected_key"] == "ord_psbl_cash"
    assert metadata["native_source_verified"] is True
    assert KISExecutor(api, "practice").verified_add_orderable_cash(
        "122630", 63000
    ) == 741311.0


def test_500_egw002_publishes_shared_cooldown_and_retry_requires_new_admission(monkeypatch):
    from trader.kr import kis_shared_http_budget as shared
    api = _api(monkeypatch, body={"rt_cd": "0"})
    api._safe_attempts = 2
    monkeypatch.setenv("KR_SHARED_HTTP_BUDGET_ENABLED", "1")
    monkeypatch.setenv("KIS_DATA_RETRY_MAX", "2")
    admissions, penalties = [], []
    monkeypatch.setattr(shared, "acquire", lambda **kw_: (
        admissions.append(kw_) or (True, "GRANTED", 0.0)))
    monkeypatch.setattr(shared, "penalize", lambda **kw_: penalties.append(kw_) or True)
    monkeypatch.setattr(kw, "_kr_sleep_with_budget", lambda *a, **kw_: True)
    denied = SimpleNamespace(status_code=500, text="EGW00201", json=lambda: {
        "rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."})
    success = SimpleNamespace(status_code=200, text="", json=lambda: {"rt_cd": "0", "output": {}})
    api.session.request.side_effect = [denied, success]
    url = "https://example.invalid/uapi/domestic-stock/v1/quotations/inquire-price"
    assert api._safe_request("GET", url) is success
    assert api.session.request.call_count == len(admissions) == 2
    assert len(penalties) == 1


def test_shared_budget_denial_never_crosses_order_http_boundary(monkeypatch):
    from trader.kr import kis_shared_http_budget as shared
    api = _api(monkeypatch, body={"rt_cd": "0"})
    monkeypatch.setenv("KR_SHARED_HTTP_BUDGET_ENABLED", "1")
    monkeypatch.setattr(shared, "acquire", lambda **kw_: (False, "DEADLINE", 0.0))
    url = "https://example.invalid/uapi/domestic-stock/v1/trading/order-cash"
    with pytest.raises(kw.KisOrderPreSubmitBlocked) as caught:
        api._safe_request("POST", url, json={"PDNO": "003490"})
    assert not kw.is_kr_order_submit_outcome_ambiguous(caught.value)
    assert api.session.request.call_count == 0
