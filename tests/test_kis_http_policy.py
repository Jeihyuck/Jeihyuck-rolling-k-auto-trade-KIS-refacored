from __future__ import annotations

import logging

from trader import kis_wrapper


def test_diag_token_real_http_allowed(monkeypatch, caplog):
    monkeypatch.setenv("STRATEGY_MODE", "DIAG")
    monkeypatch.setenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "1")
    monkeypatch.delenv("DIAG_KIS_CALLS_ENABLED", raising=False)
    monkeypatch.delenv("KIS_EXPLICIT_OFFLINE", raising=False)

    class _Resp:
        @staticmethod
        def json() -> dict:
            return {"access_token": "REAL_TOKEN", "expires_in": 3600}

    def _fake_safe_request(self, method: str, url: str, **kwargs):
        return _Resp()

    monkeypatch.setattr(kis_wrapper.KisAPI, "_safe_request", _fake_safe_request)
    caplog.set_level(logging.INFO)

    api = kis_wrapper.KisAPI(kis_env="practice")
    token, _expires_in = api._issue_token_and_expire()

    assert token == "REAL_TOKEN"
    assert "source=real_http" in caplog.text
    assert "dummy_token" not in caplog.text


def test_diag_blocks_order_but_allows_data(monkeypatch):
    monkeypatch.setenv("STRATEGY_MODE", "DIAG")
    monkeypatch.setenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "1")

    assert kis_wrapper.kis_http_allowed("/uapi/domestic-stock/v1/trading/order-cash", "DIAG", True) is False
    assert kis_wrapper.kis_http_allowed("/oauth2/tokenP", "DIAG", True) is True
    assert kis_wrapper.kis_http_allowed("/uapi/domestic-stock/v1/quotations/inquire-investor", "DIAG", True) is True


def test_data_endpoint_failure_does_not_open_global_breaker(monkeypatch):
    monkeypatch.setattr(kis_wrapper, "_KIS_BREAKER_STATE", None)
    investor_url = "https://example.test/uapi/domestic-stock/v1/quotations/inquire-investor"

    for _ in range(3):
        kis_wrapper._breaker_record_temp_failure("GET", investor_url)

    breaker_open, _until = kis_wrapper._breaker_check("GET", investor_url)

    assert breaker_open is False