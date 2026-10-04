from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import os
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.us.db.execution_claim_schema import metadata as us_claim_metadata, us_execution_claims


def _identity(*, stage: str = "TP2", lifecycle: str | None = None) -> SemanticActionIdentity:
    return SemanticActionIdentity(
        env="practice",
        account_id="test-account",
        market="US",
        trading_epoch_id="epoch-test",
        strategy_owner="US_STANDARD",
        lifecycle_id=lifecycle or str(uuid4()),
        action=stage,
        trade_date=date(2026, 10, 2),
    )


def _repo():
    engine = sa.create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    us_claim_metadata.create_all(engine)
    return engine, DurableExecutionClaimRepo(engine, us_execution_claims)


def test_semantic_identity_is_date_independent_but_owner_lifecycle_and_stage_specific():
    action = _identity(lifecycle="cycle-1")
    next_date = SemanticActionIdentity(
        env=action.env,
        account_id=action.account_id,
        market=action.market,
        trading_epoch_id=action.trading_epoch_id,
        strategy_owner=action.strategy_owner,
        lifecycle_id=action.lifecycle_id,
        action=action.action,
        trade_date=date(2026, 10, 5),
    )
    assert action.action_key == next_date.action_key
    assert action.action_key != _identity(stage="TP3", lifecycle="cycle-1").action_key
    assert action.action_key != _identity(stage="TP2", lifecycle="cycle-2").action_key
    assert action.action_key != _identity(stage="TP2", lifecycle="cycle-1").__class__(
        env="practice", account_id="test-account", market="US", trading_epoch_id="epoch-test",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="cycle-1", action="TP2",
    ).action_key


def test_unresolved_claim_persists_across_trade_date_and_process_restart():
    engine, repo = _repo()
    identity = _identity(lifecycle="cycle-1")
    first = repo.acquire(identity, attempt_id="attempt-1", requested_qty=10)
    assert first.acquired
    repo.record_observation(
        identity, attempt_id="attempt-1", state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )

    restarted_repo = DurableExecutionClaimRepo(engine, us_execution_claims)
    next_day = SemanticActionIdentity(
        env=identity.env, account_id=identity.account_id, market=identity.market,
        trading_epoch_id=identity.trading_epoch_id, strategy_owner=identity.strategy_owner,
        lifecycle_id=identity.lifecycle_id, action=identity.action, trade_date=date(2026, 10, 5),
    )
    assert not restarted_repo.acquire(next_day, attempt_id="attempt-2", requested_qty=10).acquired


def test_unknown_fill_stays_unknown_and_is_visible_in_claim_health():
    _, repo = _repo()
    identity = _identity(lifecycle="cycle-unknown")
    assert repo.acquire(identity, attempt_id="attempt-unknown", requested_qty=5).acquired
    repo.record_observation(
        identity, attempt_id="attempt-unknown", state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    snapshot = repo.get(identity)
    assert snapshot.cumulative_filled_qty is None
    assert snapshot.remaining_target_qty is None
    assert repo.health()["unresolved_execution_actions"] == 1
    assert not repo.acquire(identity, attempt_id="attempt-duplicate", requested_qty=5).acquired
    assert repo.health()["execution_claim_conflicts"] == 1


def test_explicit_reject_and_authoritative_zero_fill_cancel_are_retryable_only_after_validation():
    engine, repo = _repo()
    rejected_identity = _identity(lifecycle="cycle-reject")
    assert repo.acquire(rejected_identity, attempt_id="reject-1", requested_qty=4).acquired
    repo.record_observation(
        rejected_identity, attempt_id="reject-1", state="REJECTED_EXPLICIT",
        cumulative_filled_qty=0, authoritative=True,
    )
    assert not repo.acquire(rejected_identity, attempt_id="reject-2", requested_qty=4).acquired
    assert repo.acquire(
        rejected_identity, attempt_id="reject-2", requested_qty=4, fresh_validation=True,
    ).acquired

    cancelled_identity = _identity(lifecycle="cycle-cancel")
    assert repo.acquire(cancelled_identity, attempt_id="cancel-1", requested_qty=4).acquired
    repo.record_observation(
        cancelled_identity, attempt_id="cancel-1", state="CANCELLED",
        cumulative_filled_qty=0, authoritative=False,
    )
    assert not repo.acquire(cancelled_identity, attempt_id="cancel-2", requested_qty=4, fresh_validation=True).acquired
    repo.record_observation(
        cancelled_identity, attempt_id="cancel-1", state="CANCELLED",
        cumulative_filled_qty=0, authoritative=True,
    )
    assert repo.acquire(
        cancelled_identity, attempt_id="cancel-2", requested_qty=4, fresh_validation=True,
    ).acquired


def test_partial_cancel_preserves_cumulative_fill_and_only_remaining_target():
    engine, repo = _repo()
    identity = _identity(lifecycle="cycle-partial")
    assert repo.acquire(identity, attempt_id="partial-1", requested_qty=10).acquired
    repo.record_observation(
        identity, attempt_id="partial-1", state="PARTIALLY_FILLED",
        cumulative_filled_qty=3, authoritative=True,
    )
    repo.record_observation(
        identity, attempt_id="partial-1", state="CANCELLED_PARTIAL_FILL",
        cumulative_filled_qty=3, authoritative=True,
    )
    row = repo.get(identity)
    assert row.cumulative_filled_qty == 3
    assert row.remaining_target_qty == 7
    assert repo.acquire(identity, attempt_id="partial-2", requested_qty=7, fresh_validation=True).acquired
    repo.record_observation(
        identity, attempt_id="partial-2", state="PARTIALLY_FILLED",
        cumulative_filled_qty=1, authoritative=True,
    )
    assert repo.get(identity).cumulative_filled_qty == 4


def test_unresolved_sell_stage_blocks_overlapping_emergency_until_broker_truth():
    _, repo = _repo()
    lifecycle = "cycle-sell-overlap"
    tp1 = _identity(stage="TP1", lifecycle=lifecycle)
    emergency = _identity(stage="EMERGENCY_EXIT", lifecycle=lifecycle)

    assert repo.acquire(tp1, attempt_id="tp1-attempt", requested_qty=5).acquired
    repo.record_observation(
        tp1,
        attempt_id="tp1-attempt",
        state="UNRESOLVED",
        cumulative_filled_qty=None,
        authoritative=False,
    )
    blocked = repo.acquire(
        emergency, attempt_id="emergency-blocked", requested_qty=5,
        fresh_validation=True,
    )
    assert not blocked.acquired
    assert blocked.reason == "unresolved_lifecycle_action"

    repo.record_observation(
        tp1,
        attempt_id="tp1-attempt",
        state="FILLED",
        cumulative_filled_qty=2,
        authoritative=True,
    )
    assert repo.acquire(
        emergency, attempt_id="emergency-after-truth", requested_qty=3,
        fresh_validation=True,
    ).acquired


def test_two_workers_cannot_claim_the_same_action_in_postgresql():
    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("PBCORE_TEST_POSTGRES_URL not configured")
    engine = sa.create_engine(url, future=True)
    us_claim_metadata.create_all(engine)
    identity = _identity(lifecycle=f"concurrent-{uuid4()}")

    def acquire(worker: int):
        return DurableExecutionClaimRepo(engine, us_execution_claims).acquire(
            identity, attempt_id=f"worker-{worker}", requested_qty=1,
        ).acquired

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(acquire, range(2)))
    assert sorted(results) == [False, True]
    engine.dispose()
