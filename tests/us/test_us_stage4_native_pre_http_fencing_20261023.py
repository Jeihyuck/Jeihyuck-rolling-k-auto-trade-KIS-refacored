"""A KIS POST never escapes a late tick fence and cannot fabricate HTTP evidence.

These are offline tests: requests.post is stubbed and no KIS credential is used.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
import requests

from trader.us.execution.kis_us_client import KisUSClient, KisUSPreSubmitError
from trader.us.execution.tick_context import TickExecutionContext


def test_pre_http_rejection_is_not_counted_as_a_kis_http_attempt(monkeypatch):
    from trader.us.execution import kis_us_client as kis
    monkeypatch.setenv("KIS_HTTP_BLOCK", "0")
    monkeypatch.setenv("OFFLINE_MODE", "0")
    monkeypatch.setenv("US_OFFLINE_MODE", "0")
    client = KisUSClient(offline=False)
    client._apply_rate_limit = lambda _path: None
    client._request_budget = lambda: 10.0
    order = []
    monkeypatch.setattr(kis, "record_kis_http_call",
                        lambda *_a, **_kw: order.append("HTTP_AUDIT"))
    monkeypatch.setattr(requests, "post",
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
    monkeypatch.setenv("KIS_HTTP_BLOCK", "0")
    monkeypatch.setenv("OFFLINE_MODE", "0")
    monkeypatch.setenv("US_OFFLINE_MODE", "0")
    client = KisUSClient(offline=False)
    client._apply_rate_limit = lambda _path: None
    client._request_budget = lambda: 10.0
    sequence = []
    monkeypatch.setattr(kis, "record_kis_http_call",
                        lambda *_a, **_kw: sequence.append("HTTP_AUDIT"))
    class Response:
        def raise_for_status(self): pass
        def json(self): return {"rt_cd": "0", "output": {"ODNO": "TEST-ONLY"}}
    monkeypatch.setattr(requests, "post",
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
    # A real router demands an immutable active epoch and canonical account
    # identity before claim construction; supply fixed test-only values.
    monkeypatch.setattr("trader.account_state.get_account_key",
                        lambda **_kw: "offline-practice-account")
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
        "trade_date": "2026-09-01", "trading_epoch_id": "offline-epoch",
        "strategy_owner": "US_STANDARD", "position_action": "NEW_POSITION_BUY",
    }, kis_client=client, context=context)
    assert result["status"] == "BROKER_SUBMIT_PRE_IO_FAILED"
    assert "US_TICK_AUTHORITY_EXPIRED_BEFORE_HTTP" in result["execution_integrity_error"]
    assert result["broker_submit"] is False
    assert result["requires_reconcile"] is False
    assert client.http_calls == 0
    assert events == ["BROKER_SUBMIT_STARTED", "BROKER_SUBMIT_ABORTED_PRE_IO"]
    assert not any(x.get("state") == "SUBMITTED" for x in observed)


def test_stage4_health_accepts_uuid_hex_and_hyphenated_database_value(monkeypatch):
    """Live writer historically generated .hex while PostgreSQL renders UUID."""
    import uuid
    from datetime import datetime, timezone
    from trader.us.db import repos
    from trader.us.execution import authoritative_reconcile_worker as worker
    writer_id = uuid.uuid4()
    class Result:
        def mappings(self): return self
        def first(self):
            return {
                "evidence_at": datetime.now(timezone.utc),
                "worker_state": "READY", "reconcile_status": "OK",
                "replay_status": "OK", "pending_count": 0,
                "unresolved_count": 0, "filled_rows": 0,
                "writer_generation": writer_id,
            }
    class Connection:
        def execute(self, *_args, **_kwargs): return Result()
        def __enter__(self): return self
        def __exit__(self, *_args): pass
    class Engine:
        def connect(self): return Connection()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": "a"*64,
        "trading_epoch_id": "offline-epoch",
    })
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_STAGE4_WRITER_AUTHORIZED", "1")
    monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE", "ENFORCE")
    monkeypatch.setenv("US_STAGE4_WORKER_ALIVE", "1")
    monkeypatch.setenv("US_STAGE4_EXPECTED_GENERATION", writer_id.hex)
    assert worker.read_worker_health()["worker_state"] == "READY"
    monkeypatch.setenv("US_STAGE4_EXPECTED_GENERATION", str(uuid.uuid4()))
    with pytest.raises(RuntimeError, match="GENERATION_MISMATCH"):
        worker.read_worker_health()
    monkeypatch.setenv("US_STAGE4_EXPECTED_GENERATION", "not-a-uuid")
    with pytest.raises(RuntimeError, match="GENERATION_MISMATCH"):
        worker.read_worker_health()


def test_session_runner_uses_canonical_uuid_for_stage4_generation():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[2]
           / "trader/us/runner/trade_session_runner.py").read_text()
    assert 'os.environ["US_STAGE4_EXPECTED_GENERATION"] = str(_stage4_uuid.uuid4())' in src
