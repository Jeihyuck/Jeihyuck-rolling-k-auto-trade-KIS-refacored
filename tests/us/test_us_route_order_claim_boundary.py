from __future__ import annotations

from datetime import date
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import sqlalchemy as sa
import pytest

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.us.db.execution_claim_schema import metadata as claim_metadata, us_execution_claims
from trader.us.execution.order_router import route_order
from trader.us.execution.kis_us_client import KisUSPreSubmitError


class _Broker:
    def __init__(
        self,
        *,
        fail_submit: bool = False,
        fail_before_submit: bool = False,
        reject_once: bool = False,
    ):
        self.calls = 0
        self.fail_submit = fail_submit
        self.fail_before_submit = fail_before_submit
        self.reject_once = reject_once

    def place_us_buy_order(self, symbol, exchange, qty, price):
        self.calls += 1
        if self.fail_before_submit:
            raise KisUSPreSubmitError("order setup failed before HTTP")
        if self.reject_once:
            self.reject_once = False
            raise RuntimeError("invalid quantity")
        if self.fail_submit:
            raise TimeoutError("response lost after request")
        return {"ok": True, "order_no": f"ROUTE-ORDER-{self.calls}"}

    def place_us_sell_order(self, symbol, exchange, qty, price):
        return self.place_us_buy_order(symbol, exchange, qty, price)


def _identity(trade_date: str, action: str = "ENTRY") -> SemanticActionIdentity:
    return SemanticActionIdentity(
        env="practice",
        account_id="route-test-account",
        market="US",
        trading_epoch_id="route-test-epoch",
        strategy_owner="US_STANDARD",
        lifecycle_id="route-test-lifecycle",
        action=action,
        trade_date=date.fromisoformat(trade_date),
    )


def _route_fixture(
    monkeypatch,
    *,
    fail_submit: bool = False,
    reject_once: bool = False,
    engine=None,
):
    from trader.us.execution import order_router

    engine = engine or sa.create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False},
    )
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(engine, us_execution_claims)
    broker = _Broker(fail_submit=fail_submit, reject_once=reject_once)

    monkeypatch.setattr(order_router, "same_day_semantic_sell_exists", lambda _intent: False)
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    monkeypatch.setattr(
        order_router, "_semantic_action_identity",
        lambda intent, *, account_env: _identity(
            str(intent["trade_date"]), str(intent.get("semantic_action") or "ENTRY"),
        ),
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
        lambda response: response.get("order_no", "ROUTE-ORDER-1"),
    )
    return engine, claim_repo, broker


def _intent(
    trade_date: str,
    *,
    qty: int = 1,
    action: str = "ENTRY",
    client_order_key: str = "route-boundary-test",
    side: str = "BUY",
) -> dict:
    intent = {
        "trade_date": trade_date,
        "client_order_key": client_order_key,
        "symbol": "TEST_SYMBOL",
        "exchange": "NASDAQ",
        "side": side,
        "qty": qty,
        "limit_price": 10.0,
        "notional_usd": qty * 10.0,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "route-test-lifecycle",
        "semantic_action": action,
        "meta": {},
    }
    if side == "SELL":
        intent.update({
            "holding_qty": qty,
            "orderable_qty": qty,
            "sellable_qty": qty,
            "available_qty": qty,
        })
    return intent


def _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    monkeypatch.setattr(repos, "_MEM_ORDERS", [])
    monkeypatch.setattr(repos, "_MEM_FILLS", [])
    monkeypatch.setattr(repos, "save_order_ack", save_ack)
    return repos


def _native_kis_client(monkeypatch, http_events):
    from trader.us.execution import kis_us_client
    import requests

    client = kis_us_client.KisUSClient(env="practice", offline=True)
    client._offline = False
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
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


def test_two_concurrent_route_workers_only_one_submit_same_semantic_action(
    monkeypatch, tmp_path,
):
    from trader.us.db import repos

    engine = sa.create_engine(f"sqlite:///{tmp_path / 'route-claims.sqlite'}")
    _engine, claim_repo, broker = _route_fixture(monkeypatch, engine=engine)
    barrier = Barrier(2)

    def synchronized_claim(identity, **kwargs):
        barrier.wait(timeout=5)
        return claim_repo.acquire(identity, **kwargs)

    monkeypatch.setattr(repos, "claim_execution_action", synchronized_claim)
    intents = [
        _intent("2026-10-02", client_order_key=f"concurrent-worker-{worker}")
        for worker in range(2)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda intent: route_order(intent, kis_client=broker), intents))

    assert sorted(result["status"] for result in results) == [
        "ACK", "ORDER_FENCED_UNRESOLVED_ACTION",
    ]
    assert broker.calls == 1
    assert claim_repo.health()["unresolved_execution_actions"] == 1
    engine.dispose()


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


def test_route_explicit_reject_retries_after_fresh_claim_validation(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(monkeypatch, reject_once=True)

    rejected = route_order(_intent("2026-10-02"), kis_client=broker)

    assert rejected["status"] == "REJECT"
    assert broker.calls == 1
    assert rejected["requires_reconcile"] is False
    assert claim_repo.get(_identity("2026-10-02")).action_state == "RETRYABLE"

    retried = route_order(_intent("2026-10-02"), kis_client=broker)

    assert retried["status"] == "ACK"
    assert broker.calls == 2
    assert claim_repo.get(_identity("2026-10-02")).action_state == "IN_FLIGHT"


def test_reconcile_cancel_requires_authoritative_zero_fill_before_retry(monkeypatch):
    from trader.us.db import repos

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)
    intent = _intent("2026-10-02", qty=5)
    assert route_order(intent, kis_client=broker)["status"] == "ACK"

    observation = {
        "trade_date": "2026-10-02",
        "client_order_key": intent["client_order_key"],
        "raw_order_no": "ROUTE-ORDER-1",
        "canonical_order_no": "ROUTE-ORDER-1",
        "symbol": intent["symbol"],
        "side": "BUY",
        "requested_qty": 5,
        "filled_qty": 0,
        "remaining_qty": 5,
        "broker_status": "CANCELLED",
        "evidence_type": "KIS_ORDER_STATUS",
    }
    unknown_fill = repos.apply_broker_order_observation(
        **observation, raw_row={"requested_qty": 5, "remaining_qty": 5},
    )
    identity = _identity("2026-10-02")
    assert unknown_fill["order_status"] == "CANCELLED"
    assert claim_repo.get(identity).action_state == "UNCERTAIN"
    assert route_order(
        _intent("2026-10-02", qty=5, client_order_key="cancel-retry"),
        kis_client=broker,
    )["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 1

    authoritative_zero = repos.apply_broker_order_observation(
        **observation,
        raw_row={"requested_qty": 5, "filled_qty": 0, "remaining_qty": 5},
    )
    assert authoritative_zero["order_status"] == "CANCELLED"
    assert claim_repo.get(identity).action_state == "RETRYABLE"

    retried = route_order(
        _intent("2026-10-02", qty=5, client_order_key="cancel-retry"),
        kis_client=broker,
    )
    assert retried["status"] == "ACK"
    assert broker.calls == 2


def test_reconcile_partial_cancel_preserves_fill_and_retries_only_remainder(monkeypatch):
    from trader.us.db import repos

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)
    intent = _intent("2026-10-02", qty=5)
    assert route_order(intent, kis_client=broker)["status"] == "ACK"

    observation = {
        "trade_date": "2026-10-02",
        "client_order_key": intent["client_order_key"],
        "raw_order_no": "ROUTE-ORDER-1",
        "canonical_order_no": "ROUTE-ORDER-1",
        "symbol": intent["symbol"],
        "side": "BUY",
        "requested_qty": 5,
        "filled_qty": 2,
        "remaining_qty": 3,
        "broker_status": "CANCELLED",
        "evidence_type": "KIS_ORDER_STATUS",
    }
    raw_row = {
        "requested_qty": 5,
        "filled_qty": 2,
        "remaining_qty": 3,
        "avg_price": 10.0,
    }
    first = repos.apply_broker_order_observation(**observation, raw_row=raw_row)
    second = repos.apply_broker_order_observation(**observation, raw_row=raw_row)
    snapshot = claim_repo.get(_identity("2026-10-02"))

    assert first["order_status"] == second["order_status"] == "CANCELLED"
    assert snapshot.action_state == "PARTIALLY_SATISFIED"
    assert snapshot.cumulative_filled_qty == 2
    assert snapshot.remaining_target_qty == 3
    assert sum(fill.get("qty", 0) for fill in repos._MEM_FILLS) == 2

    retried = route_order(
        _intent("2026-10-02", qty=3, client_order_key="partial-cancel-retry"),
        kis_client=broker,
    )
    assert retried["status"] == "ACK"
    assert broker.calls == 2
    assert retried["intent"]["qty"] == 3


def test_route_filled_tp1_leaves_tp2_and_tp3_eligible(monkeypatch):
    from trader.us.db import repos

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)

    for stage in ("TP1", "TP2", "TP3"):
        intent = _intent(
            "2026-10-02",
            action=stage,
            client_order_key=f"route-{stage.lower()}",
            side="SELL",
        )
        result = route_order(intent, kis_client=broker)
        assert result["status"] == "ACK"
        reconciled = repos.apply_broker_order_observation(
            trade_date="2026-10-02",
            client_order_key=intent["client_order_key"],
            raw_order_no=f"ROUTE-ORDER-{broker.calls}",
            canonical_order_no=f"ROUTE-ORDER-{broker.calls}",
            symbol=intent["symbol"],
            side="SELL",
            requested_qty=1,
            filled_qty=1,
            remaining_qty=0,
            broker_status="FILLED",
            evidence_type="KIS_ORDER_STATUS",
            raw_row={"requested_qty": 1, "filled_qty": 1, "avg_price": 10.0},
        )
        assert reconciled.get("order_status") == "FILLED", reconciled
        assert claim_repo.get(_identity("2026-10-02", stage)).action_state == "SATISFIED"

    assert broker.calls == 3


def test_unresolved_sell_blocks_emergency_until_reconciled_remaining_qty(monkeypatch):
    from trader.us.db import repos

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(monkeypatch, fail_submit=True)
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)
    unresolved = _intent(
        "2026-10-02", qty=3, action="TP1", client_order_key="route-tp1-unresolved",
        side="SELL",
    )
    result = route_order(unresolved, kis_client=broker)
    assert result["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"

    emergency = _intent(
        "2026-10-02", qty=3, action="HARD_STOP",
        client_order_key="route-emergency", side="SELL",
    )
    blocked = route_order(emergency, kis_client=broker)
    assert blocked["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 1

    assert repos._MEM_ORDERS == []
    repos._MEM_ORDERS.append({
        "trade_date": "2026-10-02",
        "client_order_key": unresolved["client_order_key"],
        "symbol": unresolved["symbol"],
        "side": "SELL",
        "qty_requested": 3,
        "qty_filled": 0,
        "avg_price_usd": 10.0,
        "order_no": "BROKER-TP1",
        "status": "ACK",
        "meta": result["intent"]["meta"],
    })
    reconciliation = repos.apply_broker_order_observation(
        trade_date="2026-10-02",
        client_order_key=unresolved["client_order_key"],
        raw_order_no="BROKER-TP1",
        canonical_order_no="BROKER-TP1",
        symbol=unresolved["symbol"],
        side="SELL",
        requested_qty=3,
        filled_qty=1,
        remaining_qty=2,
        broker_status="CANCELLED",
        evidence_type="KIS_ORDER_STATUS",
        raw_row={
            "requested_qty": 3,
            "filled_qty": 1,
            "remaining_qty": 2,
            "avg_price": 10.0,
        },
    )
    assert reconciliation.get("order_status") == "CANCELLED", reconciliation
    assert claim_repo.get(_identity("2026-10-02", "TP1")).remaining_target_qty == 2, (
        reconciliation, result["intent"]["meta"],
    )

    broker.fail_submit = False
    emergency["holding_qty"] = emergency["orderable_qty"] = emergency["sellable_qty"] = 2
    emergency["available_qty"] = emergency["qty"] = 2
    emergency["notional_usd"] = 20.0
    allowed = route_order(emergency, kis_client=broker)
    assert allowed["status"] == "ACK"
    assert allowed["intent"]["qty"] == 2
    assert broker.calls == 2


def test_kis_order_post_throttle_failure_is_identified_as_pre_http(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient

    client = KisUSClient(env="practice")
    client._offline = False
    monkeypatch.setattr(client, "_assert_not_offline", lambda _operation: None)
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
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
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
    assert repos.load_execution_claim_health()["unresolved_execution_actions"] == 1
