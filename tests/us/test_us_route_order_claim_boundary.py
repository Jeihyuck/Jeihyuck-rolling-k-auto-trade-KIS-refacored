from __future__ import annotations

from datetime import date
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import time

import sqlalchemy as sa
import pytest

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.us.db.execution_claim_schema import metadata as claim_metadata, us_execution_claims
from trader.us.execution.tick_context import TickExecutionContext
from trader.us.execution.order_router import _semantic_action_identity, route_order
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
    production_identity: bool = False,
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
        "trader.us.db.repos.has_same_day_exit",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    if production_identity:
        monkeypatch.setattr(
            "trader.account_state.get_account_key",
            lambda **_kwargs: "route-test-account",
        )
        monkeypatch.setattr(
            "trader.us.db.repos._active_us_epoch",
            lambda *_args, **_kwargs: "route-test-epoch",
        )
    else:
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
    monkeypatch.setattr(order_router, "_get_broker_orderable_cash", lambda *_args, **_kwargs: 1_000_000.0)
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


def _defense_intent(trade_date: str, reason: str, *, order_key: str) -> dict:
    from trader.us.market_state_overlay import build_defense_trim_intents

    market_state = (
        "DEFENSE_CRASH" if reason == "DEFENSE_CRASH_TRIM" else "DEFENSE_RISK_OFF"
    )
    intent = build_defense_trim_intents(
        [{
            "symbol": "TEST_SYMBOL",
            "qty": 10,
            "orderable_qty": 10,
            "current_price": 10.0,
            "avg_price": 10.0,
            "sector": "AI_SEMI",
            "exchange": "NASDAQ",
            "position_lifecycle_id": "route-test-lifecycle",
        }],
        {"market_state": market_state},
        trade_date=trade_date,
    )[0]
    intent.update({
        "client_order_key": order_key,
        "strategy_owner": "US_STANDARD",
        "holding_qty": 10,
        "available_qty": 10,
        "sellable_qty": 10,
    })
    return intent


def _reconcile_route_fill(repos, intent: dict, broker, *, trade_date: str) -> None:
    order_no = f"ROUTE-ORDER-{broker.calls}"
    repos.apply_broker_order_observation(
        trade_date=trade_date,
        client_order_key=intent["client_order_key"],
        raw_order_no=order_no,
        canonical_order_no=order_no,
        symbol=intent["symbol"],
        side="SELL",
        requested_qty=intent["qty"],
        filled_qty=intent["qty"],
        remaining_qty=0,
        broker_status="FILLED",
        evidence_type="KIS_ORDER_STATUS",
        raw_row={
            "requested_qty": intent["qty"],
            "filled_qty": intent["qty"],
            "avg_price": 10.0,
        },
    )


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


def test_tick_expiry_after_submit_journal_releases_claim_for_fresh_retry(monkeypatch):
    engine, claim_repo, broker = _route_fixture(monkeypatch)
    from trader.us.execution import order_journal

    first_context = TickExecutionContext(
        "2026-10-02", "am", "run-1", 1, "tick-1", deadline=time.monotonic() + 10,
    )
    journal_events = []

    def expire_after_submit_start(event_type, *_args, **_kwargs):
        journal_events.append(event_type)
        if event_type == "BROKER_SUBMIT_STARTED":
            first_context.deadline = time.monotonic() - 1
        return {}

    monkeypatch.setattr(order_journal, "append_order_event", expire_after_submit_start)
    first_intent = _intent("2026-10-02", client_order_key="expired-before-boundary")
    expired = route_order(first_intent, kis_client=broker, context=first_context)

    assert expired["status"] == "ORDER_FENCED_BEFORE_BROKER_SUBMIT"
    assert expired["broker_submit"] is False
    assert expired["retry_order"] is True
    assert expired["requires_reconcile"] is False
    assert journal_events == [
        "BROKER_SUBMIT_STARTED",
        "BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY",
    ]
    assert broker.calls == 0
    released = claim_repo.get(_identity("2026-10-02"))
    assert released.action_state == "OPEN"
    assert released.active_attempt_id is None
    first_attempt_id = expired["intent"]["submit_attempt_id"]
    with engine.connect() as conn:
        attempt_state = conn.execute(
            sa.select(claim_repo.attempts.c.attempt_state).where(
                claim_repo.attempts.c.attempt_id == first_attempt_id
            )
        ).scalar_one()
    assert attempt_state == "ABANDONED_PRE_SUBMIT"

    fresh_context = TickExecutionContext(
        "2026-10-02", "am", "run-1", 1, "tick-2", deadline=time.monotonic() + 10,
    )
    retry = route_order(
        _intent("2026-10-02", client_order_key="fresh-retry"),
        kis_client=broker,
        context=fresh_context,
    )
    assert retry["status"] == "ACK"
    assert retry["intent"]["submit_attempt_id"] != first_attempt_id
    assert broker.calls == 1
    assert claim_repo.get(_identity("2026-10-02")).action_state == "IN_FLIGHT"


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
        raw_row={"requested_qty": 5, "filled_qty": 0, "remaining_qty": 0},
    )
    assert authoritative_zero["order_status"] == "CANCELLED"
    assert authoritative_zero["authoritative"] is True
    assert authoritative_zero["semantic_action_remaining_qty"] == 5
    assert authoritative_zero["broker_open_qty"] == 0
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
        "remaining_qty": 0,
        "avg_price": 10.0,
    }
    first = repos.apply_broker_order_observation(**observation, raw_row=raw_row)
    second = repos.apply_broker_order_observation(**observation, raw_row=raw_row)
    snapshot = claim_repo.get(_identity("2026-10-02"))

    assert first["order_status"] == second["order_status"] == "CANCELLED"
    assert first["authoritative"] is True
    assert first["semantic_action_remaining_qty"] == 3
    assert first["broker_open_qty"] == 0
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


@pytest.mark.parametrize(
    (
        "filled_qty", "open_qty", "include_requested", "include_filled",
        "include_open", "expected_cancelled", "expected_claim",
    ),
    [
        (0, 0, True, True, True, 1, "RETRYABLE"),
        (2, 0, True, True, True, 1, "PARTIALLY_SATISFIED"),
        (None, 0, True, False, True, 0, "UNCERTAIN"),
        (0, None, True, True, False, 0, "UNCERTAIN"),
        (0, 0, False, True, True, 0, "UNCERTAIN"),
    ],
    ids=(
        "zero-fill-terminal", "partial-fill-terminal", "missing-fill",
        "missing-open-qty", "missing-requested-qty",
    ),
)
def test_reconcile_terminal_cancel_requires_complete_endpoint_evidence(
    monkeypatch, filled_qty, open_qty, include_requested, include_filled,
    include_open, expected_cancelled, expected_claim,
):
    from trader.us.db import repos
    from trader.us.execution.reconcile import reconcile_ack_orders_with_balance

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)
    intent = _intent("2026-10-02", qty=5, action="CANCEL_CASE")
    assert route_order(intent, kis_client=broker)["status"] == "ACK"

    class Provider:
        def get_balance(self, **_kwargs):
            return {"positions": []}

        def get_fills_by_order_no(self, **_kwargs):
            row = {
                "status": "CANCELLED",
                "avg_price": 10.0,
            }
            if include_requested:
                row.update({"requested_qty": 5, "requested_qty_present": True})
            else:
                row["requested_qty_present"] = False
            if include_filled:
                row.update({"filled_qty": filled_qty, "filled_qty_present": True})
            else:
                row["filled_qty_present"] = False
            if include_open:
                row.update({"remaining_qty": open_qty, "remaining_qty_present": True})
            else:
                row["remaining_qty_present"] = False
            return row

    result = reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-10-02",
    )
    identity = _identity("2026-10-02", "CANCEL_CASE")

    assert result["cancelled_count"] == expected_cancelled
    assert result["unresolved_count"] == (0 if expected_cancelled else 1)
    assert claim_repo.get(identity).action_state == expected_claim
    assert claim_repo.health()["unresolved_execution_actions"] == int(
        expected_claim == "UNCERTAIN"
    )
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"
    if filled_qty:
        assert repos._MEM_ORDERS[0]["qty_filled"] == filled_qty
        assert claim_repo.get(identity).cumulative_filled_qty == filled_qty


def test_reconcile_repeated_terminal_cancel_is_idempotent(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution.reconcile import reconcile_ack_orders_with_balance

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(monkeypatch)
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)
    intent = _intent("2026-10-02", qty=5, action="CANCEL_REPEAT")
    assert route_order(intent, kis_client=broker)["status"] == "ACK"

    class Provider:
        def get_balance(self, **_kwargs):
            return {"positions": []}

        def get_fills_by_order_no(self, **_kwargs):
            return {
                "status": "CANCELLED",
                "requested_qty": 5,
                "requested_qty_present": True,
                "filled_qty": 0,
                "filled_qty_present": True,
                "remaining_qty": 0,
                "remaining_qty_present": True,
            }

    first = reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-10-02")
    state_after_first = claim_repo.get(_identity("2026-10-02", "CANCEL_REPEAT")).action_state
    second = reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-10-02")

    assert first["cancelled_count"] == 1
    assert second["cancelled_count"] == 0
    assert second["unresolved_count"] == 0
    assert state_after_first == "RETRYABLE"
    assert claim_repo.get(_identity("2026-10-02", "CANCEL_REPEAT")).action_state == "RETRYABLE"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 0


def test_reconcile_does_not_count_non_authoritative_ok_cancel_as_clean(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution.reconcile import reconcile_ack_orders_with_balance

    save_ack = repos.save_order_ack
    _engine, _claim_repo, broker = _route_fixture(monkeypatch)
    repos = _enable_in_memory_reconciliation(monkeypatch, _claim_repo, save_ack)
    intent = _intent("2026-10-02", qty=5, action="CANCEL_NONAUTH")
    assert route_order(intent, kis_client=broker)["status"] == "ACK"
    monkeypatch.setattr(
        repos,
        "apply_broker_order_observation",
        lambda **_kwargs: {
            "status": "OK", "order_status": "CANCELLED",
            "authoritative": False, "requires_reconcile": True,
        },
    )

    class Provider:
        def get_balance(self, **_kwargs):
            return {"positions": []}

        def get_fills_by_order_no(self, **_kwargs):
            return {
                "status": "CANCELLED", "requested_qty": 5,
                "filled_qty": 0, "remaining_qty": 0,
            }

    result = reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-10-02",
    )

    assert result["cancelled_count"] == 0
    assert result["unresolved_count"] == 1


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


@pytest.mark.parametrize(
    "reason",
    ("DEFENSE_RISK_OFF_TRIM", "DEFENSE_CRASH_TRIM"),
)
def test_completed_defense_trim_is_same_day_deduped_but_repeats_next_day(
    monkeypatch, reason,
):
    from trader.us.db import repos
    from trader.us.execution.order_router import _semantic_action_identity

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, production_identity=True,
    )
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)
    suffix = reason.lower()
    first = _defense_intent(
        "2026-10-02", reason, order_key=f"defense-day-one-{suffix}",
    )

    assert route_order(first, kis_client=broker)["status"] == "ACK"
    action_identity = _semantic_action_identity(first, account_env="practice")
    assert action_identity.action_instance == "2026-10-02"
    assert claim_repo.get(action_identity).action_instance == "2026-10-02"
    _reconcile_route_fill(repos, first, broker, trade_date="2026-10-02")

    duplicate = _defense_intent(
        "2026-10-02", reason, order_key=f"defense-same-day-duplicate-{suffix}",
    )
    assert route_order(duplicate, kis_client=broker)["status"] == (
        "ORDER_FENCED_UNRESOLVED_ACTION"
    )
    assert broker.calls == 1

    next_day = _defense_intent(
        "2026-10-05", reason, order_key=f"defense-next-day-{suffix}",
    )
    assert route_order(next_day, kis_client=broker)["status"] == "ACK"
    next_identity = _semantic_action_identity(next_day, account_env="practice")
    assert next_identity.action_key != action_identity.action_key
    assert claim_repo.get(next_identity).action_instance == "2026-10-05"
    assert broker.calls == 2


@pytest.mark.parametrize(
    "reason",
    ("DEFENSE_RISK_OFF_TRIM", "DEFENSE_CRASH_TRIM"),
)
def test_unresolved_defense_instance_fences_same_and_later_dates_and_emergency(
    monkeypatch, reason,
):
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, fail_submit=True, production_identity=True,
    )
    first = _defense_intent("2026-10-02", reason, order_key="defense-uncertain")

    assert route_order(first, kis_client=broker)["status"] == (
        "BROKER_SUBMIT_RESULT_UNKNOWN"
    )
    same_day = _defense_intent(
        "2026-10-02", reason, order_key="defense-duplicate",
    )
    assert route_order(same_day, kis_client=broker)["status"] == (
        "ORDER_FENCED_UNRESOLVED_ACTION"
    )
    next_day = _defense_intent(
        "2026-10-05", reason, order_key="defense-next-day-uncertain",
    )
    assert route_order(next_day, kis_client=broker)["status"] == (
        "ORDER_FENCED_UNRESOLVED_ACTION"
    )

    emergency = _intent(
        "2026-10-05", qty=2, action="HARD_STOP",
        client_order_key="emergency-while-defense-uncertain", side="SELL",
    )
    assert route_order(emergency, kis_client=broker)["status"] == (
        "ORDER_FENCED_UNRESOLVED_ACTION"
    )
    assert broker.calls == 1
    assert claim_repo.health()["unresolved_execution_actions"] == 1


def test_satisfied_tp1_stays_one_shot_across_dates_while_tp2_tp3_remain_distinct(
    monkeypatch,
):
    from trader.us.db import repos
    from trader.us.execution.order_router import _semantic_action_identity

    save_ack = repos.save_order_ack
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, production_identity=True,
    )
    repos = _enable_in_memory_reconciliation(monkeypatch, claim_repo, save_ack)

    tp1 = _intent(
        "2026-10-02", action="TP1", client_order_key="tp1-first", side="SELL",
    )
    assert route_order(tp1, kis_client=broker)["status"] == "ACK"
    assert _semantic_action_identity(tp1, account_env="practice").action_instance is None
    _reconcile_route_fill(repos, tp1, broker, trade_date="2026-10-02")

    repeated_tp1 = _intent(
        "2026-10-05", action="TP1", client_order_key="tp1-next-date", side="SELL",
    )
    assert route_order(repeated_tp1, kis_client=broker)["status"] == (
        "ORDER_FENCED_UNRESOLVED_ACTION"
    )

    for stage in ("TP2", "TP3"):
        intent = _intent(
            "2026-10-05", action=stage, client_order_key=f"{stage.lower()}-later",
            side="SELL",
        )
        assert route_order(intent, kis_client=broker)["status"] == "ACK"
        _reconcile_route_fill(repos, intent, broker, trade_date="2026-10-05")
    assert broker.calls == 3


def test_tqqq_run_sleeve_uses_cycle_lifecycle_and_one_daily_buy_action(monkeypatch):
    from datetime import date, datetime
    from zoneinfo import ZoneInfo
    from trader.us.infinite.integration import run_sleeve
    from trader.us.infinite.models import Action, Decision, InfiniteState, Status
    from trader.us.execution.order_router import _semantic_action_identity

    monkeypatch.setattr(
        "trader.us.market_calendar.now_ny",
        lambda: datetime(2026, 10, 2, 12, 0, tzinfo=ZoneInfo("America/New_York")),
    )
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("DISABLE_REAL_TRADING", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setenv("US_BLOCK_NEW_ENTRY_AFTER_ET", "23:59")
    monkeypatch.setattr(
        "trader.us.market_calendar.now_ny",
        lambda: datetime(2026, 10, 2, 10, 0, tzinfo=ZoneInfo("America/New_York")),
    )
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, fail_submit=True, production_identity=True,
    )
    state = InfiniteState(
        cycle_id="tqqq-cycle-route-test",
        cycle_start_date=date(2026, 10, 1),
        core_filled_notional=100.0,
        status=Status.ACTIVE,
        metadata={"position_lifecycle_id": "stale-position-lifecycle"},
    )

    class _InfiniteRepo:
        def __init__(self):
            self.state = state

        def ensure_schema(self):
            return None

        def load_state(self, **_kwargs):
            return self.state

        def save_state(self, value):
            self.state = value

        def pending_sides(self, *_args):
            return False, False

        def fill_accounting(self, *_args):
            return 0.0, 0.0, 0.0, None, None

        def reconcile_metadata(self, current, **_kwargs):
            return current

    reason = {"value": "FAST_DIP_ADD_BUY"}
    monkeypatch.setattr(
        "trader.us.infinite.integration.evaluate",
        lambda **_kwargs: Decision(
            Action.BUY, reason["value"], qty=1, notional=50.0,
            next_status=Status.ACTIVE,
        ),
    )

    def run(trading_date):
        return run_sleeve(
            positions=[], price=50.0, trading_date=date.fromisoformat(trading_date),
            overlay={"market_state": "NORMAL"}, repository=_InfiniteRepo(),
            route=lambda intent: route_order(intent, kis_client=broker),
        )

    day_one = run("2026-10-02")
    first_intent = day_one["orders"][0]["intent"]
    identity = _semantic_action_identity(first_intent, account_env="practice")
    assert identity.lifecycle_id == "tqqq-cycle-route-test"
    assert identity.action == "TQQQ_INFINITE_BUY"
    assert identity.action_instance == "2026-10-02"
    assert day_one["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"

    reason["value"] = "ROUTINE_ADD_BUY"
    same_day = run("2026-10-02")
    assert same_day["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    next_day_unresolved = run("2026-10-05")
    assert next_day_unresolved["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 1

    claim = claim_repo.get(identity)
    claim_repo.record_observation(
        identity, attempt_id=claim.active_attempt_id, state="FILLED",
        cumulative_filled_qty=1, authoritative=True,
    )
    broker.fail_submit = False
    next_day_terminal = run("2026-10-05")
    assert next_day_terminal["status"] == "ACK"
    next_identity = _semantic_action_identity(
        next_day_terminal["orders"][0]["intent"], account_env="practice",
    )
    assert next_identity.lifecycle_id == identity.lifecycle_id
    assert next_identity.action == identity.action
    assert next_identity.action_instance == "2026-10-05"
    assert next_identity.action_key != identity.action_key
    assert broker.calls == 2

    # A different TQQQ BUY reason does not create a second same-day authority.
    reason["value"] = "FAST_DIP_ADD_BUY"
    same_day_again = run("2026-10-05")
    assert same_day_again["status"] == "BLOCKED"
    assert broker.calls == 2
    sell_identity = _semantic_action_identity(
        {
            "side": "SELL", "strategy_owner": "TQQQ_INFINITE",
            "position_lifecycle_id": "tqqq-cycle-route-test",
            "trade_date": "2026-10-05", "reason": "TAKE_PROFIT_TQQQ_INFINITE",
            "profit_capture_stage": "TP1",
        },
        account_env="practice",
    )
    assert sell_identity.action_instance is None


def test_sell_without_source_lifecycle_fails_closed_without_synthesizing(monkeypatch):
    _engine, _claim_repo, broker = _route_fixture(
        monkeypatch, production_identity=True,
    )
    intent = _intent(
        "2026-10-02", qty=1, action="HARD_STOP",
        client_order_key="missing-source-lifecycle", side="SELL",
    )
    intent.pop("position_lifecycle_id")
    result = route_order(intent, kis_client=broker)
    assert result["status"] == "POLICY_LIFECYCLE_INTEGRITY_MISSING"
    assert result["reason"] == "position_lifecycle_identity_missing"
    assert result["broker_submit"] is False
    assert broker.calls == 0


def _generated_pb1_buy(monkeypatch, *, trading_day: str, symbol: str = "ENTRY_CASE_A",
                       held: bool = False):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from trader.us.db import repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **_kwargs: False)
    monkeypatch.setattr(repos, "has_position", lambda _symbol: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda **_kwargs: set())
    monkeypatch.setattr(
        repos, "load_us_positions_by_symbols",
        lambda _symbols, **_kwargs: {},
    )
    monkeypatch.setattr(
        "trader.us.pb1.us_entry_engine._load_held_position_snapshot",
        lambda *_args, **_kwargs: {
            "qty": 2, "avg_price": 90.0, "current_price": 100.0,
            "market_value_usd": 200.0, "current_weight": 0.01,
            "position_lifecycle_id": "held-source-lifecycle",
        },
    )

    class _Provider:
        def get_current_price(self, _symbol, _exchange):
            return {"last": 100.0}

    intents = generate_entry_intents(
        tickers=None,
        provider=_Provider(),
        sold_today=set(),
        available_cash_usd=10_000.0,
        position_count=int(held),
        capital_usd_cap=20_000.0,
        now=datetime.fromisoformat(f"{trading_day}T12:00:00-04:00").astimezone(
            ZoneInfo("America/New_York")
        ),
        max_new_entries=1,
        watchlist_entries=[{
            "symbol": symbol,
            "exchange": "NASDAQ",
            "score": 0.8,
            "score_final": 0.8,
            "entry_style_selected": "ENTRY_PULLBACK",
            "current_price": 100.0,
        }],
        current_position_symbols={symbol} if held else set(),
    )
    assert len(intents) == 1
    return intents[0]


def test_generated_us_standard_entry_keeps_unresolved_identity_across_dates(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, fail_submit=True, production_identity=True,
    )
    first = _generated_pb1_buy(monkeypatch, trading_day="2026-10-02")
    first_result = route_order(first, kis_client=broker)
    routed_first = first_result.get("intent") or first
    routed_first.setdefault("strategy_owner", "US_STANDARD")
    first_identity = _semantic_action_identity(routed_first, account_env="practice")

    next_day = _generated_pb1_buy(monkeypatch, trading_day="2026-10-05")
    next_result = route_order(next_day, kis_client=broker)
    routed_next = next_result.get("intent") or next_day
    routed_next.setdefault("strategy_owner", "US_STANDARD")
    next_identity = _semantic_action_identity(routed_next, account_env="practice")

    assert first_result["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert first_identity.lifecycle_id == "ENTRY:US_STANDARD:ENTRY_CASE_A"
    assert next_identity.lifecycle_id == first_identity.lifecycle_id
    assert first_identity.action_instance == "2026-10-02"
    assert next_identity.action_instance == "2026-10-05"
    assert next_result["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert claim_repo.health()["unresolved_execution_actions"] == 1
    assert broker.calls == 1


def test_generated_us_standard_entry_allows_later_instance_after_terminal_fill(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, production_identity=True,
    )
    first = _generated_pb1_buy(monkeypatch, trading_day="2026-10-02")
    first_result = route_order(first, kis_client=broker)
    assert first_result["status"] == "ACK"
    routed_first = first_result.get("intent") or first
    routed_first.setdefault("strategy_owner", "US_STANDARD")
    first_identity = _semantic_action_identity(routed_first, account_env="practice")
    first_claim = claim_repo.get(first_identity)
    claim_repo.record_observation(
        first_identity,
        attempt_id=first_claim.active_attempt_id,
        state="FILLED",
        cumulative_filled_qty=int(first["qty"]),
        authoritative=True,
    )

    next_day = _generated_pb1_buy(monkeypatch, trading_day="2026-10-05")
    next_result = route_order(next_day, kis_client=broker)
    routed_next = next_result.get("intent") or next_day
    routed_next.setdefault("strategy_owner", "US_STANDARD")
    next_identity = _semantic_action_identity(routed_next, account_env="practice")
    assert next_identity.lifecycle_id == first_identity.lifecycle_id
    assert next_identity.action_instance != first_identity.action_instance
    assert next_result["status"] == "ACK"
    assert broker.calls == 2


def test_generated_us_standard_entry_explicit_reject_retries_same_daily_instance(monkeypatch):
    _engine, claim_repo, broker = _route_fixture(
        monkeypatch, reject_once=True, production_identity=True,
    )
    rejected_intent = _generated_pb1_buy(monkeypatch, trading_day="2026-10-02")
    rejected = route_order(rejected_intent, kis_client=broker)
    retry_intent = _generated_pb1_buy(monkeypatch, trading_day="2026-10-02")
    retried = route_order(retry_intent, kis_client=broker)

    assert rejected["status"] == "REJECT"
    assert retried["status"] == "ACK"
    assert rejected_intent["action_instance"] == retry_intent["action_instance"]
    assert broker.calls == 2
    assert claim_repo.health()["unresolved_execution_actions"] == 1


def test_generated_add_buy_inherits_held_position_lifecycle(monkeypatch):
    _route_fixture(monkeypatch, production_identity=True)
    intent = _generated_pb1_buy(
        monkeypatch, trading_day="2026-10-02", held=True,
    )

    assert intent["position_action"] == "ADD_TO_EXISTING_BUY"
    assert intent["position_lifecycle_id"] == "held-source-lifecycle"
    assert intent["meta"]["position_lifecycle_id"] == "held-source-lifecycle"


def test_generated_us_standard_entries_for_distinct_symbols_have_distinct_scopes(monkeypatch):
    _engine, _claim_repo, broker = _route_fixture(
        monkeypatch, fail_submit=True, production_identity=True,
    )
    first = _generated_pb1_buy(monkeypatch, trading_day="2026-10-02")
    second = _generated_pb1_buy(
        monkeypatch, trading_day="2026-10-02", symbol="ENTRY_CASE_B",
    )

    first_result = route_order(first, kis_client=broker)
    second_result = route_order(second, kis_client=broker)
    routed_first = first_result.get("intent") or first
    routed_second = second_result.get("intent") or second
    routed_first.setdefault("strategy_owner", "US_STANDARD")
    routed_second.setdefault("strategy_owner", "US_STANDARD")
    first_identity = _semantic_action_identity(routed_first, account_env="practice")
    second_identity = _semantic_action_identity(routed_second, account_env="practice")
    assert first_identity.lifecycle_id == "ENTRY:US_STANDARD:ENTRY_CASE_A"
    assert second_identity.lifecycle_id == "ENTRY:US_STANDARD:ENTRY_CASE_B"
    assert first_identity.action_key != second_identity.action_key
    assert first_result["status"] == second_result["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert broker.calls == 2


@pytest.mark.parametrize(
    ("exit_kind", "current_price"),
    (("hard_stop", 91.0), ("trend_exit", 100.0), ("day_exit", 103.0)),
)
def test_pb1_generated_sell_keeps_source_lifecycle_through_durable_route(
    monkeypatch, exit_kind, current_price,
):
    from datetime import datetime, timezone
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    from trader.us.execution.order_router import _semantic_action_identity
    from trader.us.pb1.us_exit_engine import generate_exit_intents
    from trader.us.runner.trade_tick_runner import route_exit_orders_immediately

    _engine, claim_repo, broker = _route_fixture(monkeypatch, production_identity=True)
    lifecycle_id = f"pb1-route-lifecycle-{exit_kind}"
    book = "DAY_BOOK" if exit_kind == "day_exit" else "SWING_BOOK"
    horizon = "DAY_TRADE" if exit_kind == "day_exit" else "SWING_CARRY"
    contract = build_us_entry_exit_contract({
        "symbol": "SPY", "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "book": book, "horizon": horizon,
        "exit_policy": "DAY_BOOK" if book == "DAY_BOOK" else "US_SWING_DEFAULT",
        "entry_strategy": "us_pb1", "entry_signal_type": "pullback",
        "entry_style_selected": "ENTRY_PULLBACK", "reasons": ["ENTRY_PULLBACK"],
        "score_breakdown": {"pullback": 0.8}, "filters_passed": ["score", "risk"],
    })
    position = {
        "symbol": "SPY", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 100.0, "position_lifecycle_id": lifecycle_id,
        "entry_time": "2026-10-01T10:00:00-04:00",
        "book": book, "horizon": horizon,
        "meta": {
            "position_lifecycle_id": lifecycle_id,
            "entry_exit_contract": contract,
        },
    }
    if exit_kind == "trend_exit":
        monkeypatch.setattr(
            "trader.us.position_trend_state.choose_trend_time_exit",
            lambda *_args, **_kwargs: ("trend_deterioration_exit", 5, "TREND_EXIT"),
        )
    if exit_kind == "day_exit":
        monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.025")
    class _Provider:
        def get_current_price(self, _symbol, _exchange):
            return current_price

    generated = generate_exit_intents(
        [position], _Provider(), now=datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc),
    )
    assert len(generated) == 1
    intent = generated[0]
    assert intent.get("position_lifecycle_id") == lifecycle_id
    assert intent.get("meta", {}).get("position_lifecycle_id") == lifecycle_id

    routed = route_exit_orders_immediately(
        generated, buy_daily_notional=0.0, position_count=1, effective_budget=5000.0,
        signal_only=False, kis_order_allowed=True, current_position_symbols={"SPY"},
        kis_client=broker,
    )
    assert routed["orders"][0]["status"] == "ACK"
    routed_intent = routed["orders"][0].get("intent") or intent
    identity = _semantic_action_identity(routed_intent, account_env="practice")
    assert identity.lifecycle_id == lifecycle_id
    claim = claim_repo.get(identity)
    assert claim.active_attempt_id is not None
    assert broker.calls == 1


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
            "remaining_qty": 0,
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
    assert first["retry_order"] is False
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


def test_possible_post_boundary_dns_failure_remains_fenced(monkeypatch):
    import requests

    engine, claim_repo, broker = _route_fixture(monkeypatch)

    def fail_during_transport(*_args, **_kwargs):
        broker.calls += 1
        raise requests.exceptions.ConnectionError("temporary failure in name resolution")

    broker.place_us_buy_order = fail_during_transport
    first = route_order(_intent("2026-10-02"), kis_client=broker)

    assert first["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert first["requires_reconcile"] is True
    assert first["retry_order"] is False
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
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice",
        "account_id": "route-test-account",
        "market": "US",
        "trading_epoch_id": "route-test-epoch",
    })
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
