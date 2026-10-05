from dataclasses import replace
from datetime import date

import sqlalchemy as sa
from sqlalchemy import text

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.db.repos import OrdersRepo
from trader.kr.infinite.models import (
    Action,
    BrokerOrderState,
    Decision,
    OrderIntent,
    State,
    Status,
)
from trader.kr.infinite.repository import InfiniteRepository
from trader.kr.infinite.executor import KISExecutor
from trader.db.schema import schema_for_engine
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


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


def test_broker_row_without_fill_quantity_stays_fenced_through_executor(monkeypatch):
    _engine, repo, claims, identity, intent, state = _repository(monkeypatch)

    class FakeKis:
        def inquire_daily_ccld(self, **_kwargs):
            return {
                "rt_cd": "0",
                "output1": [{
                    "odno": "broker-order-1",
                    "ord_stat_cd": "CANCELLED",
                }],
            }

    intent = replace(intent, broker_order_id="broker-order-1")
    observed = KISExecutor(FakeKis(), "practice").order_state(intent, DAY)

    assert observed.status == "RECONCILE_PENDING"
    assert observed.filled_qty is None
    repo.persist_reconciliation(state, [(intent, observed)])
    with _engine.connect() as conn:
        persisted_qty = conn.execute(
            text("SELECT filled_qty FROM kr_infinite_order_intents WHERE id=1")
        ).scalar_one()
    assert persisted_qty is None
    assert claims.get(identity).action_state == "UNCERTAIN"
    assert not claims.acquire(
        identity, attempt_id="attempt-2", requested_qty=5,
        fresh_validation=True, client_order_key="order-2",
    ).acquired


def test_infinite_submit_claim_isolated_from_pb1_actions_in_both_directions():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    schema.metadata.create_all(
        engine, tables=[schema.execution_claims, schema.execution_attempts],
    )
    symbol = "122630"
    cycle_id = "shared-test-cycle"
    lifecycle_id = f"{symbol}:{cycle_id}"
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE kr_infinite_order_intents (
                trading_epoch_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                metadata TEXT,
                updated_at TEXT,
                PRIMARY KEY (trading_epoch_id, idempotency_key)
            )
        """))
        for key in ("infinite-tp1", "infinite-tp2"):
            conn.execute(text("""
                INSERT INTO kr_infinite_order_intents
                    (trading_epoch_id, idempotency_key, metadata)
                SELECT trading_epoch_id, :key, '{}'
                FROM trading_epochs
                WHERE env='practice' AND status='ACTIVE'
            """), {"key": key})

    infinite_repo = InfiniteRepository(engine)
    pb1_orders = OrdersRepo(engine)
    pb1_identity, pb1_claim = pb1_orders.claim_execution_action(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle_id,
        action="SELL:TP1",
        trade_date=DAY,
        attempt_id="pb1-tp1",
        requested_qty=5,
        client_order_key="pb1-tp1",
        fresh_validation=True,
    )
    state = State(cycle_id=cycle_id, status=Status.ACTIVE)
    tp1_decision = Decision(
        Action.SELL_PARTIAL,
        "TAKE_PROFIT",
        qty=5,
        idempotency_key="infinite-tp1",
        metadata={"desired_profit_stage": "TP1"},
    )
    infinite_identity, infinite_claim = infinite_repo.claim_submit(
        state=state,
        decision=tp1_decision,
        trade_date=DAY,
        symbol=symbol,
        env="practice",
    )

    assert pb1_claim.acquired and infinite_claim.acquired
    assert pb1_identity.strategy_owner == "PB1"
    assert infinite_identity.strategy_owner == "KR_INFINITE"
    assert pb1_identity.lifecycle_id == infinite_identity.lifecycle_id == lifecycle_id
    assert pb1_identity.action == infinite_identity.action == "SELL:TP1"
    assert pb1_identity.action_key != infinite_identity.action_key

    claims = DurableExecutionClaimRepo(
        engine, schema.execution_claims, schema.execution_attempts,
    )
    claims.record_observation(
        pb1_identity,
        attempt_id="pb1-tp1",
        state="UNRESOLVED",
        cumulative_filled_qty=None,
        authoritative=False,
    )
    assert claims.get(infinite_identity).action_state == "IN_FLIGHT"

    infinite_repo.record_execution_claim_observation(
        infinite_identity,
        attempt_id=str(infinite_claim.attempt_id),
        state="FILLED",
        cumulative_filled_qty=5,
        authoritative=True,
    )
    tp2_decision = Decision(
        Action.SELL_PARTIAL,
        "TAKE_PROFIT",
        qty=3,
        idempotency_key="infinite-tp2",
        metadata={"desired_profit_stage": "TP2"},
    )
    infinite_tp2_identity, infinite_tp2_claim = infinite_repo.claim_submit(
        state=state,
        decision=tp2_decision,
        trade_date=DAY,
        symbol=symbol,
        env="practice",
    )
    assert infinite_tp2_claim.acquired
    assert claims.get(pb1_identity).action_state == "UNCERTAIN"

    infinite_repo.record_execution_claim_observation(
        infinite_tp2_identity,
        attempt_id=str(infinite_tp2_claim.attempt_id),
        state="UNRESOLVED",
        cumulative_filled_qty=None,
        authoritative=False,
    )
    claims.record_observation(
        pb1_identity,
        attempt_id="pb1-tp1",
        state="REJECTED_EXPLICIT",
        cumulative_filled_qty=0,
        authoritative=True,
    )
    pb1_hard_stop_identity, pb1_hard_stop_claim = pb1_orders.claim_execution_action(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle_id,
        action="SELL:HARD_STOP:FULL_EXIT",
        trade_date=DAY,
        attempt_id="pb1-hard-stop",
        requested_qty=18,
        client_order_key="pb1-hard-stop",
        fresh_validation=True,
    )

    assert pb1_hard_stop_claim.acquired
    assert pb1_hard_stop_identity.strategy_owner == "PB1"
    assert claims.get(infinite_tp2_identity).action_state == "UNCERTAIN"
    with engine.connect() as conn:
        owner_rows = conn.execute(
            sa.select(schema.execution_claims.c.strategy_owner)
            .where(schema.execution_claims.c.action_key.in_([
                pb1_identity.action_key,
                infinite_identity.action_key,
                infinite_tp2_identity.action_key,
                pb1_hard_stop_identity.action_key,
            ]))
        ).scalars().all()
    assert owner_rows.count("PB1") == 2
    assert owner_rows.count("KR_INFINITE") == 2
