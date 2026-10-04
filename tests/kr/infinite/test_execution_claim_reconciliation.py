from datetime import date

import sqlalchemy as sa
from sqlalchemy import text

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.kr.infinite.models import (
    BrokerOrderState,
    OrderIntent,
    State,
    Status,
)
from trader.kr.infinite.repository import InfiniteRepository
from trader.db.schema import schema_for_engine


DAY = date(2026, 10, 2)


def _repository(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    sa.event.listen(
        engine,
        "connect",
        lambda connection, _record: connection.create_function(
            "NOW", 0, lambda: "2026-10-02 12:00:00",
        ),
    )
    schema = schema_for_engine(engine)
    schema.metadata.create_all(
        engine, tables=[schema.execution_claims, schema.execution_attempts],
    )
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE kr_infinite_order_intents (
                id INTEGER PRIMARY KEY,
                status TEXT,
                filled_qty INTEGER,
                filled_notional_krw FLOAT,
                filled_avg_price FLOAT,
                updated_at TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO kr_infinite_order_intents
                (id, status, filled_qty, filled_notional_krw, filled_avg_price)
            VALUES (1, 'SUBMITTED', NULL, NULL, NULL)
        """))
    repo = InfiniteRepository(engine)
    monkeypatch.setattr(repo, "_epoch_id", lambda _bind, required=None: "test-epoch")
    monkeypatch.setattr(repo, "_save_state", lambda *_args, **_kwargs: None)
    identity = SemanticActionIdentity(
        env="practice",
        account_id="test-account",
        market="KR",
        trading_epoch_id="test-epoch",
        strategy_owner="KR_INFINITE",
        lifecycle_id="test-symbol:test-cycle",
        action="SELL:TP1",
        trade_date=DAY,
    )
    claims = DurableExecutionClaimRepo(
        engine, schema.execution_claims, schema.execution_attempts,
    )
    claim = claims.acquire(
        identity, attempt_id="attempt-1", requested_qty=5,
        client_order_key="order-1",
    )
    assert claim.acquired
    intent = OrderIntent(
        id=1,
        cycle_id="test-cycle",
        trade_date=DAY,
        side="SELL_PARTIAL",
        idempotency_key="order-1",
        requested_qty=5,
        status="SUBMITTED",
        metadata={"execution_claim_action_key": claim.action_key},
    )
    state = State(cycle_id="test-cycle", status=Status.EXIT_PENDING)
    return engine, repo, claims, identity, intent, state


def _persist(repo, intent, state, status, filled_qty):
    repo.persist_reconciliation(
        state,
        [(intent, BrokerOrderState(status=status, filled_qty=filled_qty))],
    )


def test_cancel_without_fill_quantity_stays_uncertain_and_blocks_retry(monkeypatch):
    _engine, repo, claims, identity, intent, state = _repository(monkeypatch)

    _persist(repo, intent, state, "CANCELLED", None)

    snapshot = claims.get(identity)
    assert snapshot.action_state == "UNCERTAIN"
    assert snapshot.cumulative_filled_qty is None
    assert snapshot.remaining_target_qty is None
    retry = claims.acquire(
        identity, attempt_id="attempt-2", requested_qty=5,
        fresh_validation=True, client_order_key="order-2",
    )
    assert not retry.acquired


def test_authoritative_zero_fill_cancel_allows_fresh_validated_retry(monkeypatch):
    _engine, repo, claims, identity, intent, state = _repository(monkeypatch)

    _persist(repo, intent, state, "CANCELLED", 0)

    assert claims.get(identity).action_state == "RETRYABLE"
    retry = claims.acquire(
        identity, attempt_id="attempt-2", requested_qty=5,
        fresh_validation=True, client_order_key="order-2",
    )
    assert retry.acquired


def test_partial_cancel_observation_is_monotonic_idempotent_and_retries_remainder(
    monkeypatch,
):
    _engine, repo, claims, identity, intent, state = _repository(monkeypatch)
    observation = BrokerOrderState(
        status="CANCELLED", filled_qty=2, filled_notional_krw=200.0,
        filled_avg_price=100.0,
    )
    repo.persist_reconciliation(state, [(intent, observation)])
    repo.persist_reconciliation(state, [(intent, observation)])

    snapshot = claims.get(identity)
    assert snapshot.action_state == "PARTIALLY_SATISFIED"
    assert snapshot.cumulative_filled_qty == 2
    assert snapshot.remaining_target_qty == 3
    retry = claims.acquire(
        identity, attempt_id="attempt-2", requested_qty=3,
        fresh_validation=True, client_order_key="order-2",
    )
    assert retry.acquired


def test_explicit_cumulative_zero_is_preserved_for_cancel(monkeypatch):
    _engine, repo, claims, identity, intent, state = _repository(monkeypatch)

    _persist(repo, intent, state, "EXPIRED", 0)

    assert claims.get(identity).action_state == "RETRYABLE"
