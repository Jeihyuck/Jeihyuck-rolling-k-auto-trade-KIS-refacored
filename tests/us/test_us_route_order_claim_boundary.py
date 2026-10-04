from __future__ import annotations

from datetime import date

import sqlalchemy as sa
import pytest

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.us.db.execution_claim_schema import metadata as claim_metadata, us_execution_claims
from trader.us.execution.order_router import route_order
from trader.us.execution.kis_us_client import KisUSPreSubmitError


class _Broker:
    def __init__(self, *, fail_submit: bool = False, fail_before_submit: bool = False):
        self.calls = 0
        self.fail_submit = fail_submit
        self.fail_before_submit = fail_before_submit

    def place_us_buy_order(self, symbol, exchange, qty, price):
        self.calls += 1
        if self.fail_before_submit:
            raise KisUSPreSubmitError("order setup failed before HTTP")
        if self.fail_submit:
            raise TimeoutError("response lost after request")
        return {"ok": True}


def _identity(trade_date: str) -> SemanticActionIdentity:
    return SemanticActionIdentity(
        env="practice",
        account_id="route-test-account",
        market="US",
        trading_epoch_id="route-test-epoch",
        strategy_owner="US_STANDARD",
        lifecycle_id="route-test-lifecycle",
        action="ENTRY",
        trade_date=date.fromisoformat(trade_date),
    )


def _route_fixture(monkeypatch, *, fail_submit: bool = False):
    from trader.us.execution import order_router

    engine = sa.create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(engine, us_execution_claims)
    broker = _Broker(fail_submit=fail_submit)

    monkeypatch.setattr(order_router, "same_day_semantic_sell_exists", lambda _intent: False)
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    monkeypatch.setattr(
        order_router, "_semantic_action_identity",
        lambda intent, *, account_env: _identity(str(intent["trade_date"])),
    )
    monkeypatch.setattr(order_router, "canonical_order_risk_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order", lambda: False)
    monkeypatch.setattr(order_router, "assert_order_allowed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("trader.us.db.repos.claim_execution_action", claim_repo.acquire)
    monkeypatch.setattr(
        "trader.us.db.repos.record_execution_action_observation",
        claim_repo.record_observation,
    )
    monkeypatch.setattr(
        "trader.us.db.repos.release_execution_action_before_submit",
        claim_repo.release_before_submit,
    )
    monkeypatch.setattr(
        "trader.us.db.repos._active_us_epoch",
        lambda *_args, **_kwargs: "route-test-epoch",
    )
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda **_kwargs: set())
    monkeypatch.setattr(
        "trader.us.db.repos.load_us_positions_by_symbols",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr("trader.us.db.repos.save_order_intent", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("trader.us.db.repos.save_order_ack", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        "trader.us.db.repos.mark_order_intent_sent",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "trader.us.execution.order_journal.append_order_event",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "trader.us.execution.kis_us_response_parser.extract_order_no",
        lambda _response: "ROUTE-ORDER-1",
    )
    return engine, claim_repo, broker


def _intent(trade_date: str) -> dict:
    return {
        "trade_date": trade_date,
        "client_order_key": "route-boundary-test",
        "symbol": "TEST_SYMBOL",
        "exchange": "NASDAQ",
        "side": "BUY",
        "qty": 1,
        "limit_price": 10.0,
        "notional_usd": 10.0,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "route-test-lifecycle",
        "semantic_action": "ENTRY",
        "meta": {},
    }


def _native_kis_client(monkeypatch, http_events):
    from trader.us.execution import kis_us_client
    import requests

    client = kis_us_client.KisUSClient(env="practice", offline=True)
    monkeypatch.setattr(client, "_assert_not_offline", lambda _operation: None)
    monkeypatch.setattr(client, "_build_headers", lambda _tr_id: {})
    monkeypatch.setattr(client, "_apply_rate_limit", lambda _path: None)
    monkeypatch.setattr(client, "_request_budget", lambda: 1.0)
    monkeypatch.setattr(client, "_is_temporary_error", lambda *_args: False)
    monkeypatch.setattr(
        client, "get_us_orderable_cash",
        lambda *_args, **_kwargs: {"orderable_cash": "1000"},
    )
    monkeypatch.setattr(kis_us_client.us_cfg, "assert_us_paper_order_allowed", lambda: None)
    monkeypatch.setattr(
        kis_us_client, "get_tr_info",
        lambda _operation: {
            "tr_id": "paper-buy",
            "path": "/uapi/overseas-stock/v1/trading/order",
        },
    )
    monkeypatch.setattr(kis_us_client, "record_kis_http_call", lambda *_args: None)

    def post(*_args, **_kwargs):
        http_events.append("HTTP")
        raise TimeoutError("simulated response loss")

    monkeypatch.setattr(requests, "post", post)
    return client


def test_route_submit_claim_observation_failure_before_io_releases_claim(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    from trader.us.db import repos

    record_observation = claim_repo.record_observation
    failed_once = False

    def fail_before_submit(identity, **kwargs):
        nonlocal failed_once
        if kwargs["state"] == "SUBMITTED" and not failed_once:
            failed_once = True
            raise OSError("claim store unavailable before broker I/O")
        return record_observation(identity, **kwargs)

    monkeypatch.setattr(repos, "record_execution_action_observation", fail_before_submit)
    result = route_order(_intent("2026-10-02"), kis_client=broker)

    assert broker.calls == 0
    assert result["broker_submit"] is False
    assert result["requires_reconcile"] is False
    assert result["execution_integrity_error"]
    assert claim_repo.health()["unresolved_execution_actions"] == 0

    retry = route_order(_intent("2026-10-02"), kis_client=broker)
    assert retry["status"] == "ACK"
    assert broker.calls == 1


def test_route_known_pre_http_failure_releases_claim_for_retry(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    broker.fail_before_submit = True

    result = route_order(_intent("2026-10-02"), kis_client=broker)

    assert result["status"] == "BROKER_SUBMIT_PRE_IO_FAILED"
    assert result["broker_submit"] is False
    assert result["retry_order"] is True
    assert result["requires_reconcile"] is False
    assert claim_repo.health()["unresolved_execution_actions"] == 0

    broker.fail_before_submit = False
    retry = route_order(_intent("2026-10-02"), kis_client=broker)
    assert retry["status"] == "ACK"
    assert broker.calls == 2


def test_kis_order_post_throttle_failure_is_identified_as_pre_http(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient

    client = KisUSClient(env="practice")
    monkeypatch.setattr(
        client, "_apply_rate_limit",
        lambda _path: (_ for _ in ()).throw(RuntimeError("rate gate failed")),
    )

    with pytest.raises(KisUSPreSubmitError, match="before broker HTTP"):
        client._post(
            "/uapi/overseas-stock/v1/trading/order",
            headers={},
            body={},
        )


def test_route_records_submitted_at_native_kis_http_boundary(monkeypatch):
    _engine, claim_repo, _broker = _route_fixture(monkeypatch)
    events = []
    client = _native_kis_client(monkeypatch, events)

    def record_observation(identity, **kwargs):
        if kwargs["state"] == "SUBMITTED":
            events.append("SUBMITTED")
        return claim_repo.record_observation(identity, **kwargs)

    monkeypatch.setattr(
        "trader.us.db.repos.record_execution_action_observation",
        record_observation,
    )

    result = route_order(_intent("2026-10-02"), kis_client=client)

    assert events == ["SUBMITTED", "HTTP"]
    assert result["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert result["requires_reconcile"] is True
    assert client._before_order_http is None
    assert claim_repo.health()["unresolved_execution_actions"] == 1


def test_native_kis_http_is_blocked_when_submitted_observation_fails(monkeypatch):
    _engine, claim_repo, _broker = _route_fixture(monkeypatch)
    events = []
    client = _native_kis_client(monkeypatch, events)
    record_observation = claim_repo.record_observation

    def fail_submitted(identity, **kwargs):
        if kwargs["state"] == "SUBMITTED":
            raise OSError("claim store unavailable before broker HTTP")
        return record_observation(identity, **kwargs)

    monkeypatch.setattr(
        "trader.us.db.repos.record_execution_action_observation",
        fail_submitted,
    )
    result = route_order(_intent("2026-10-02"), kis_client=client)

    assert events == []
    assert result["status"] == "ORDER_DISABLED_CLAIM_OBSERVATION_FAILED"
    assert result["broker_submit"] is False
    assert result["retry_order"] is True
    assert result["requires_reconcile"] is False
    assert claim_repo.health()["unresolved_execution_actions"] == 0
    assert client._before_order_http is None


def test_route_response_loss_keeps_durable_fence_across_restart_and_date(monkeypatch):
    engine, claim_repo, broker = _route_fixture(monkeypatch, fail_submit=True)
    first = route_order(_intent("2026-10-02"), kis_client=broker)

    assert first["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert first["requires_reconcile"] is True
    assert broker.calls == 1

    restarted_repo = DurableExecutionClaimRepo(engine, us_execution_claims)
    monkeypatch.setattr("trader.us.db.repos.claim_execution_action", restarted_repo.acquire)
    monkeypatch.setattr(
        "trader.us.db.repos.record_execution_action_observation",
        restarted_repo.record_observation,
    )
    next_day = route_order(_intent("2026-10-05"), kis_client=broker)

    assert next_day["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert next_day["requires_reconcile"] is True
    assert broker.calls == 1
    assert restarted_repo.health()["unresolved_execution_actions"] == 1


def test_route_post_io_observation_failure_surfaces_reconcile_integrity(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(monkeypatch, fail_submit=True)
    record_observation = claim_repo.record_observation

    def fail_unresolved(identity, **kwargs):
        if kwargs["state"] == "UNRESOLVED":
            raise OSError("claim store unavailable after broker I/O")
        return record_observation(identity, **kwargs)

    monkeypatch.setattr("trader.us.db.repos.record_execution_action_observation", fail_unresolved)
    result = route_order(_intent("2026-10-02"), kis_client=broker)

    assert result["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert result["requires_reconcile"] is True
    assert result["execution_integrity_error"]
    assert broker.calls == 1
    assert claim_repo.health()["unresolved_execution_actions"] == 1
