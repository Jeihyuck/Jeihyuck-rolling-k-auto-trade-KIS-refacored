"""2026-10-13 KIS EGW00201 HTTP-500 and retry quota regressions."""
from __future__ import annotations

import json

from trader.kis_wrapper import KisAPI


class _Response:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.text = json.dumps(body, ensure_ascii=False)
        self.headers = {}
    def json(self):
        return dict(self._body)


class _Gate:
    def __init__(self):
        self.acquire_calls = []
        self.cooldowns = []
    def acquire(self, **kw):
        self.acquire_calls.append(kw)
        return 0.0
    def set_global_cooldown(self, *args, **kwargs):
        self.cooldowns.append((args, kwargs))


def _prepare(monkeypatch):
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("KIS_HTTP_ENABLED", "1")
    monkeypatch.setenv("PB1_KIS_RATE_LIMIT_SAFE", "0")
    monkeypatch.setenv("KIS_DATA_RETRY_MAX", "2")
    monkeypatch.setenv("KIS_ORDER_RETRY_MAX", "9")
    monkeypatch.setattr(KisAPI, "get_valid_token", lambda self: "TEST")
    monkeypatch.setattr(KisAPI, "_load_safe_mode_state", lambda self: None)
    monkeypatch.setattr("trader.kis_wrapper._breaker_check", lambda *_a, **_kw: (False, None))
    monkeypatch.setattr("trader.kis_wrapper._breaker_record_temp_failure", lambda *_a, **_kw: None)
    monkeypatch.setattr("trader.kis_wrapper._breaker_record_success", lambda *_a, **_kw: None)
    monkeypatch.setattr("trader.kis_wrapper._kr_sleep_with_budget", lambda *_a, **_kw: True)
    monkeypatch.setattr("trader.kis_wrapper.time.sleep", lambda *_a, **_kw: None)
    monkeypatch.setattr("trader.kis_wrapper.random.uniform", lambda lo, hi: lo)
    gate = _Gate()
    monkeypatch.setattr("trader.kis_wrapper.get_kis_gate", lambda: gate)
    api = KisAPI(kis_env="practice")
    api._safe_attempts = 2
    api._safe_max_seconds = 40
    api._safe_backoff_base = 0.01
    api._safe_backoff_cap = 0.02
    api._wait_for_http_gap = lambda *_args: None
    return api, gate


def test_http500_egw00201_cools_down_and_retry_consumes_quota(monkeypatch):
    api, gate = _prepare(monkeypatch)
    responses = [
        _Response(500, {"rt_cd": "1", "msg_cd": "EGW00201",
                        "msg1": "초당 거래건수를 초과하였습니다."}),
        _Response(200, {"rt_cd": "0", "output": [{"mksc_shrn_iscd": "005930"}]}),
    ]
    submitted = []
    def fake_request(method, url, **_kw):
        submitted.append((method, url))
        return responses.pop(0)
    monkeypatch.setattr(api.session, "request", fake_request)
    answer = api._safe_request(
        "GET",
        "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/ranking/market-cap",
        headers={}, params={"FID_INPUT_ISCD": "0001"}, timeout=(0.05, 0.05),
    )
    assert answer.status_code == 200
    assert len(submitted) == 2
    assert len(gate.acquire_calls) == 2
    assert len(gate.cooldowns) == 1
    assert all(call["endpoint_category"] == "data" for call in gate.acquire_calls)


def test_order_http500_explicit_egw_reject_never_blind_retry(monkeypatch):
    api, gate = _prepare(monkeypatch)
    submitted = []
    def fake_request(method, url, **_kw):
        submitted.append((method, url))
        return _Response(500, {"rt_cd": "1", "msg_cd": "EGW00201",
                               "msg1": "초당 거래건수를 초과하였습니다."})
    monkeypatch.setattr(api.session, "request", fake_request)
    answer = api._safe_request(
        "POST",
        "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash",
        headers={}, data=b"{}", timeout=(0.05, 0.05),
    )
    assert answer.status_code == 500
    assert len(submitted) == 1
    assert len(gate.acquire_calls) == 1
    assert len(gate.cooldowns) == 1
