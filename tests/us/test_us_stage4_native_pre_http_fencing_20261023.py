"""A KIS POST never escapes a late tick fence and cannot fabricate HTTP evidence.

These are offline tests: requests.post is stubbed and no KIS credential is used.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from trader.us.execution.kis_us_client import KisUSClient, KisUSPreSubmitError
from trader.us.execution.tick_context import TickExecutionContext


def test_pre_http_rejection_is_not_counted_as_a_kis_http_attempt(monkeypatch):
    from trader.us.execution import kis_us_client as kis
    client = KisUSClient(offline=False)
    client._apply_rate_limit = lambda _path: None
    client._request_budget = lambda: 10.0
    order = []
    monkeypatch.setattr(kis, "record_kis_http_call",
                        lambda *_a, **_kw: order.append("HTTP_AUDIT"))
    monkeypatch.setattr(kis.requests, "post",
                        lambda *_a, **_kw: order.append("POST") or
                        pytest.fail("real HTTP must never be invoked"))
    def reject():
        order.append("FENCE")
        raise KisUSPreSubmitError("US_TICK_AUTHORITY_EXPIRED_BEFORE_HTTP")
    client.set_before_order_http(reject)
    with pytest.raises(KisUSPreSubmitError):
        client._post("/uapi/overseas-stock/v1/trading/order", {}, {})
    assert order == ["FENCE"]
    assert client.stats["post_retry_count"] == 0


def test_successful_pre_http_callback_is_logged_before_exactly_one_post(monkeypatch):
    from trader.us.execution import kis_us_client as kis
    client = KisUSClient(offline=False)
    client._apply_rate_limit = lambda _path: None
    client._request_budget = lambda: 10.0
    sequence = []
    monkeypatch.setattr(kis, "record_kis_http_call",
                        lambda *_a, **_kw: sequence.append("HTTP_AUDIT"))
    class Response:
        def raise_for_status(self): pass
        def json(self): return {"rt_cd": "0", "output": {"ODNO": "TEST-ONLY"}}
    monkeypatch.setattr(kis.requests, "post",
                        lambda *_a, **_kw: sequence.append("POST") or Response())
    client.set_before_order_http(lambda: sequence.append("FENCE_PASS"))
    response = client._post("/uapi/overseas-stock/v1/trading/order", {}, {})
    assert response["rt_cd"] == "0"
    assert sequence == ["FENCE_PASS", "HTTP_AUDIT", "POST"]


def test_tick_deadline_expiring_during_client_throttle_aborts_before_http(monkeypatch):
    from trader.us.execution import order_router, order_journal
    from trader.us.db import repos

    events = []
    observed = []
    class DelayedClient:
        def __init__(self):
            self.before_http = None
            self.http_calls = 0
        def bind_tick_context(self, _context): return self
        def set_before_order_http(self, callback): self.before_http = callback
        def clear_before_order_http(self): self.before_http = None
        def place_us_buy_order(self, *_args):
            # This is after the router's earlier check, just before HTTP.
            context.deadline = time.monotonic() - 1
            self.before_http()
            self.http_calls += 1
            return {"output": {"ODNO": "NEVER"}}

    client = DelayedClient()
    context = TickExecutionContext(
        "2026-09-01", "am", "run", 1, "tick",
        deadline=time.monotonic() + 30,
    )
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "LEGACY")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "0")
    monkeypatch.setattr(order_router, "canonical_order_risk_check",
                        lambda *_a, **_kw: None, raising=False)
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order",
                        lambda: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda **_kw: set())
    monkeypatch.setattr(repos, "save_order_intent", lambda *_a, **_kw: True)
    monkeypatch.setattr(repos, "claim_execution_action",
                        lambda *_a, **_kw: SimpleNamespace(
                            acquired=True, action_key="offline-claim", reason=""))
    monkeypatch.setattr(repos, "record_execution_action_observation",
                        lambda *_a, **kw: observed.append(kw))
    monkeypatch.setattr(repos, "release_execution_action_before_submit",
                        lambda *_a, **_kw: True)
    monkeypatch.setattr(order_journal, "append_order_event",
                        lambda event_type, *_a, **_kw: events.append(event_type))
    result = order_router.route_order({
        "symbol": "MSFT", "exchange": "NASDAQ", "side": "BUY",
        "qty": 1, "limit_price": 100.0, "notional_usd": 100.0,
        "client_order_key": "stage4-post-callback-throttle",
        "trade_date": "2026-09-01",
    }, kis_client=client, context=context)
    assert result["status"] == "BROKER_SUBMIT_PRE_IO_FAILED"
    assert "US_TICK_AUTHORITY_EXPIRED_BEFORE_HTTP" in result["execution_integrity_error"]
    assert result["broker_submit"] is False
    assert result["requires_reconcile"] is False
    assert client.http_calls == 0
    assert events == ["BROKER_SUBMIT_STARTED", "BROKER_SUBMIT_ABORTED_PRE_IO"]
    assert not any(x.get("state") == "SUBMITTED" for x in observed)
