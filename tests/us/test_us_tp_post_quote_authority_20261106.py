"""TP native POST boundary revalidation after a delayed quote GET.

All tests are side-effect-free. The would-be HTTP POST is a local list append.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from trader.us.execution.kis_us_client import KisUSPreSubmitError
from trader.us.execution.order_router import _assert_tp_post_quote_submit_authority


def _tp():
    return {
        "symbol": "HCA", "side": "SELL", "limit_price": 103,
        "client_order_key": "US_PC_HCA_TAKE_PROFIT_TP1",
        "meta": {
            "profit_capture_stage": "tp1",
            "last_profit_capture_at": datetime.now(timezone.utc).isoformat(),
            "broker_avg_price": 100,
            "tp_threshold_fraction": "0.03",
        },
    }


def _quote(age=0.0):
    return {
        "_quote_quality": "FRESH", "_quote_source": "KIS_LIVE",
        "_quote_asof_epoch": time.time() - age,
        "output": {"last": "104"},
    }


def _call(*, quote=None, health=None, authority=None, context=None):
    return _assert_tp_post_quote_submit_authority(
        _tp(), _quote() if quote is None else quote,
        account_env="practice", context=context,
        account_authority=authority, attempt_id="hca-exact-attempt",
        client_order_key="US_PC_HCA_TAKE_PROFIT_TP1",
        strategy_owner="US_STANDARD", symbol="HCA", side="SELL", qty=1,
        max_decision_age_sec=30.0, worker_health_reader=health,
    )


def _ready(**kwargs):
    return {"reconcile_status": "OK", "replay_status": "UNRESOLVED"}


def test_slow_31_second_quote_get_expired_stage4_cannot_reach_post(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    would_post = []
    def expired_stage4(*, env):
        assert env == "practice"
        raise RuntimeError("US_STAGE4_WRITER_STALE")
    with pytest.raises(KisUSPreSubmitError, match="US_STAGE4_WRITER_FENCED_BEFORE_HTTP"):
        _call(health=expired_stage4)
        would_post.append("HTTP_POST")
    assert would_post == []


def test_quote_older_than_five_seconds_cannot_reach_post(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    would_post = []
    with pytest.raises(KisUSPreSubmitError, match="US_TP_LIVE_QUOTE_EXPIRED"):
        _call(quote=_quote(age=31.0), health=_ready)
        would_post.append("HTTP_POST")
    assert would_post == []


def test_reservation_db_delay_cannot_keep_previous_stage4_health(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE")
    calls = []
    class DelayedReservation:
        def assert_reserved_submit_authority(self, **kwargs):
            calls.append("DB_RESERVATION")
            assert kwargs["attempt_id"] == "hca-exact-attempt"
    def stage4(*, env):
        calls.append("STAGE4")
        if calls.count("STAGE4") > 1:
            raise RuntimeError("US_STAGE4_WRITER_STALE")
        return _ready()
    with pytest.raises(KisUSPreSubmitError, match="US_STAGE4_WRITER_FENCED_BEFORE_HTTP"):
        _call(health=stage4, authority=DelayedReservation())
    assert calls == ["STAGE4", "DB_RESERVATION", "STAGE4"]


def test_expired_tick_authority_after_quote_is_pre_http_fence(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    class CancelledTick:
        def broker_submit_allowed(self):
            return False
    with pytest.raises(KisUSPreSubmitError, match="US_TICK_AUTHORITY_EXPIRED_BEFORE_HTTP"):
        _call(context=CancelledTick(), health=_ready)


def test_fresh_authority_preserves_normal_tp_path(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    class ValidTick:
        def broker_submit_allowed(self):
            return True
    assert _call(context=ValidTick(), health=_ready) is None


def test_unknown_replay_still_blocks_post(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    with pytest.raises(KisUSPreSubmitError, match="US_STAGE4_WRITER_FENCED_BEFORE_HTTP"):
        _call(health=lambda **kw: {"reconcile_status": "FAILED", "replay_status": "ERROR"})


def _stub_native_transport():
    from trader.us.execution.kis_us_client import KisUSClient
    client = KisUSClient.__new__(KisUSClient)
    client._base_url = "https://broker.invalid"
    client._assert_not_offline = lambda _name: None
    client._apply_rate_limit = lambda _path: None
    client._request_budget = lambda: 10.0
    client._check_rt_cd = lambda _response: None
    client.stats = {}
    return client


def test_native_kis_post_never_runs_after_tp_quote_authority_expires(monkeypatch):
    """Exercise the real KisUSClient._post callback, not just helper logic."""
    import requests
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    would_post = []
    monkeypatch.setattr(
        requests, "post",
        lambda *args, **kwargs: would_post.append((args, kwargs)),
    )
    client = _stub_native_transport()
    # A quote was GET-fresh when received but aged past 5s while the native
    # submitter waited for its rate limiter. It MUST not become HTTP POST.
    client.set_before_order_http(lambda: _call(quote=_quote(age=6), health=_ready))
    with pytest.raises(KisUSPreSubmitError, match="US_TP_LIVE_QUOTE_EXPIRED"):
        client._post("/uapi/overseas-stock/v1/trading/order",
                     headers={}, body={"symbol": "HCA"})
    assert would_post == []
    assert client._before_order_http is None


def test_native_kis_post_uses_valid_final_tp_authority_once(monkeypatch):
    """Valid authority reaches exactly one synthetic requests.post call."""
    import requests
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    calls = []
    class StubResponse:
        def raise_for_status(self):
            return None
        def json(self):
            return {"rt_cd": "0", "msg1": "SIMULATED"}
    def simulated_post(*args, **kwargs):
        calls.append((args, kwargs))
        return StubResponse()
    monkeypatch.setattr(requests, "post", simulated_post)
    from trader.us.execution import kis_us_client as client_module
    monkeypatch.setattr(client_module, "record_kis_http_call",
                        lambda *_args, **_kw: None)
    client = _stub_native_transport()
    client.set_before_order_http(lambda: _call(health=_ready))
    result = client._post("/uapi/overseas-stock/v1/trading/order",
                          headers={}, body={"symbol": "HCA"})
    assert result["rt_cd"] == "0"
    assert len(calls) == 1
    assert client._before_order_http is None


def test_native_post_rechecks_tick_budget_after_callback(monkeypatch):
    """No HTTP when authoritative DB checks consume the remaining deadline."""
    import requests
    from trader.us.execution.kis_us_client import KisUSTemporaryError
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    would_post = []
    monkeypatch.setattr(
        requests, "post",
        lambda *args, **kwargs: would_post.append((args, kwargs)),
    )
    client = _stub_native_transport()
    budgets = []
    def budget():
        budgets.append("CHECK")
        if len(budgets) == 2:
            raise KisUSTemporaryError("tick deadline exhausted after callback")
        return 10.0
    client._request_budget = budget
    client.set_before_order_http(lambda: _call(health=_ready))
    with pytest.raises(KisUSPreSubmitError, match="failed before broker HTTP"):
        client._post("/uapi/overseas-stock/v1/trading/order",
                     headers={}, body={"symbol": "HCA"})
    assert budgets == ["CHECK", "CHECK"]
    assert would_post == []
