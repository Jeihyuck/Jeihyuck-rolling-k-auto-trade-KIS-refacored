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


def test_action_instances_are_opt_in_and_keep_one_shot_actions_date_independent():
    first = _identity(stage="DEFENSE_RISK_OFF_TRIM", lifecycle="cycle-defense")
    repeated = SemanticActionIdentity(
        env=first.env,
        account_id=first.account_id,
        market=first.market,
        trading_epoch_id=first.trading_epoch_id,
        strategy_owner=first.strategy_owner,
        lifecycle_id=first.lifecycle_id,
        action=first.action,
        trade_date=date(2026, 10, 5),
        action_instance="2026-10-05",
    )
    assert first.action_key != repeated.action_key
    assert repeated.action_instance == "2026-10-05"
    assert _identity(stage="TP1", lifecycle="cycle-defense").action_key == SemanticActionIdentity(
        env=first.env,
        account_id=first.account_id,
        market=first.market,
        trading_epoch_id=first.trading_epoch_id,
        strategy_owner=first.strategy_owner,
        lifecycle_id=first.lifecycle_id,
        action="TP1",
        trade_date=date(2026, 10, 5),
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


def test_entry_retry_reuses_partial_action_across_dates_and_only_remaining_qty():
    _, repo = _repo()
    lifecycle = "PB1_ENTRY:strategy:KR:1:sample-symbol"
    first = _identity(stage="BUY_ENTRY:PB1:attempt-1", lifecycle=lifecycle)
    assert repo.acquire(first, attempt_id="entry-1", requested_qty=10).acquired
    repo.record_observation(
        first,
        attempt_id="entry-1",
        state="CANCELLED_PARTIAL_FILL",
        cumulative_filled_qty=3,
        authoritative=True,
    )

    next_day = SemanticActionIdentity(
        env=first.env,
        account_id=first.account_id,
        market=first.market,
        trading_epoch_id=first.trading_epoch_id,
        strategy_owner=first.strategy_owner,
        lifecycle_id=first.lifecycle_id,
        action="BUY_ENTRY:PB1:attempt-2",
        trade_date=date(2026, 10, 5),
    )
    prior_action = repo.find_retryable_action(
        next_day,
        action_prefix="BUY_ENTRY:PB1",
    )

    assert prior_action == first.action.upper()
    stale_worker = repo.acquire(
        next_day,
        attempt_id="stale-entry-worker",
        requested_qty=10,
        fresh_validation=True,
        retry_action_prefix="BUY_ENTRY:PB1",
    )
    assert not stale_worker.acquired
    assert stale_worker.reason == "retryable_lifecycle_action_exists"
    retry = SemanticActionIdentity(
        env=next_day.env,
        account_id=next_day.account_id,
        market=next_day.market,
        trading_epoch_id=next_day.trading_epoch_id,
        strategy_owner=next_day.strategy_owner,
        lifecycle_id=next_day.lifecycle_id,
        action=prior_action,
        trade_date=next_day.trade_date,
    )
    assert repo.acquire(
        retry,
        attempt_id="entry-2",
        requested_qty=7,
        fresh_validation=True,
    ).acquired


def test_new_entry_generation_is_blocked_while_previous_submit_is_unresolved():
    _, repo = _repo()
    lifecycle = "PB1_ENTRY:strategy:KR:1:sample-symbol"
    first = _identity(stage="BUY_ENTRY:PB1:attempt-1", lifecycle=lifecycle)
    assert repo.acquire(first, attempt_id="entry-1", requested_qty=10).acquired
    repo.record_observation(
        first,
        attempt_id="entry-1",
        state="UNRESOLVED",
        cumulative_filled_qty=None,
        authoritative=False,
    )

    next_generation = SemanticActionIdentity(
        env=first.env,
        account_id=first.account_id,
        market=first.market,
        trading_epoch_id=first.trading_epoch_id,
        strategy_owner=first.strategy_owner,
        lifecycle_id=lifecycle,
        action="BUY_ENTRY:PB1:attempt-2",
        trade_date=date(2026, 10, 5),
    )
    assert repo.find_retryable_action(
        next_generation,
        action_prefix="BUY_ENTRY:PB1",
    ) is None
    blocked = repo.acquire(
        next_generation,
        attempt_id="entry-2",
        requested_qty=10,
        fresh_validation=True,
    )
    assert not blocked.acquired
    assert blocked.reason == "unresolved_lifecycle_action"


def test_orders_repo_reuses_retryable_entry_action_across_trade_dates():
    from trader.account_state import get_account_key
    from trader.db.repos import OrdersRepo
    from trader.db.schema import schema_for_engine

    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    env = "practice"
    account_id = get_account_key(env=env)
    epoch_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            schema.trading_epochs.insert().values(
                trading_epoch_id=epoch_id,
                env=env,
                account_id=account_id,
                status="ACTIVE",
                reason="unit test",
            )
        )

    orders_repo = OrdersRepo(engine)
    lifecycle = "PB1_ENTRY:strategy:KR:1:sample-symbol"
    first_identity, first_claim = orders_repo.claim_execution_action(
        env=env,
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle,
        action="BUY_ENTRY:PB1",
        trade_date=date(2026, 10, 2),
        attempt_id="pb1-entry-1",
        requested_qty=10,
        client_order_key="entry-2026-10-02",
        fresh_validation=True,
        retry_action_prefix="BUY_ENTRY:PB1",
    )
    assert first_claim.acquired
    orders_repo.record_execution_claim_for_order(
        "entry-2026-10-02",
        state="CANCELLED_PARTIAL_FILL",
        cumulative_filled_qty=3,
        authoritative=True,
    )

    next_identity, retry_claim = orders_repo.claim_execution_action(
        env=env,
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle,
        action="BUY_ENTRY:PB1",
        trade_date=date(2026, 10, 5),
        attempt_id="pb1-entry-2",
        requested_qty=7,
        client_order_key="entry-2026-10-05",
        fresh_validation=True,
        retry_action_prefix="BUY_ENTRY:PB1",
    )

    assert retry_claim.acquired
    assert next_identity.action == first_identity.action
    assert next_identity.action_key == first_identity.action_key


def _kr_orders_repo_for_entry_generation():
    from trader.account_state import get_account_key
    from trader.db.repos import OrdersRepo
    from trader.db.schema import schema_for_engine

    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    env = "practice"
    account_id = get_account_key(env=env)
    with engine.begin() as conn:
        conn.execute(
            schema.trading_epochs.insert().values(
                trading_epoch_id=str(uuid4()),
                env=env,
                account_id=account_id,
                status="ACTIVE",
                reason="PB1 entry generation regression",
            )
        )
    return engine, OrdersRepo(engine)


def _claim_kr_pb1_entry_generation(
    orders_repo, *, lifecycle: str, trade_day: date, order_key: str, qty: int = 5,
):
    return orders_repo.claim_execution_action(
        env="practice",
        market="KR",
        strategy_owner="PB1",
        lifecycle_id=lifecycle,
        action="BUY_ENTRY:PB1:INITIAL",
        trade_date=trade_day,
        attempt_id=f"attempt-{order_key}",
        requested_qty=qty,
        client_order_key=order_key,
        fresh_validation=True,
        retry_action_prefix="BUY_ENTRY:PB1:INITIAL",
        entry_generation=True,
    )


def test_kr_pb1_initial_entry_unresolved_generation_fences_next_date():
    engine, orders_repo = _kr_orders_repo_for_entry_generation()
    lifecycle = "PB1_ENTRY:strategy:KR:mode:SAMPLE_CODE"
    first_identity, first_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle=lifecycle,
        trade_day=date(2026, 10, 2),
        order_key="pb1-entry-day-one",
    )
    assert first_claim.acquired
    assert first_identity.action_instance
    orders_repo.record_execution_claim_for_order(
        "pb1-entry-day-one",
        state="UNRESOLVED",
        cumulative_filled_qty=None,
        authoritative=False,
    )

    next_identity, next_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle=lifecycle,
        trade_day=date(2026, 10, 5),
        order_key="pb1-entry-day-two",
    )
    assert next_identity.lifecycle_id == first_identity.lifecycle_id
    assert not next_claim.acquired
    assert next_claim.reason == "unresolved_lifecycle_action"
    engine.dispose()


@pytest.mark.parametrize(
    ("terminal_state", "filled_qty"),
    (("REJECTED_EXPLICIT", 0), ("CANCELLED", 0)),
)
def test_kr_pb1_reject_or_zero_cancel_retry_reuses_entry_generation(
    terminal_state, filled_qty,
):
    engine, orders_repo = _kr_orders_repo_for_entry_generation()
    lifecycle = "PB1_ENTRY:strategy:KR:mode:SAMPLE_CODE"
    first_identity, first_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle=lifecycle,
        trade_day=date(2026, 10, 2),
        order_key="pb1-entry-retry-first",
    )
    assert first_claim.acquired
    orders_repo.record_execution_claim_for_order(
        "pb1-entry-retry-first",
        state=terminal_state,
        cumulative_filled_qty=filled_qty,
        authoritative=True,
    )

    retry_identity, retry_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle=lifecycle,
        trade_day=date(2026, 10, 5),
        order_key="pb1-entry-retry-second",
    )
    assert retry_claim.acquired
    assert retry_identity.action_instance == first_identity.action_instance
    assert retry_identity.action_key == first_identity.action_key
    engine.dispose()


def test_kr_pb1_terminal_entry_advances_generation_and_code_scopes_are_independent():
    engine, orders_repo = _kr_orders_repo_for_entry_generation()
    first_lifecycle = "PB1_ENTRY:strategy:KR:mode:SAMPLE_CODE"
    first_identity, first_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle=first_lifecycle,
        trade_day=date(2026, 10, 2),
        order_key="pb1-entry-terminal-first",
    )
    assert first_claim.acquired
    orders_repo.record_execution_claim_for_order(
        "pb1-entry-terminal-first",
        state="FILLED",
        cumulative_filled_qty=5,
        authoritative=True,
    )

    next_identity, next_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle=first_lifecycle,
        trade_day=date(2026, 10, 5),
        order_key="pb1-entry-terminal-next-generation",
    )
    assert next_claim.acquired
    assert next_identity.lifecycle_id == first_identity.lifecycle_id
    assert next_identity.action_instance != first_identity.action_instance

    other_identity, other_claim = _claim_kr_pb1_entry_generation(
        orders_repo,
        lifecycle="PB1_ENTRY:strategy:KR:mode:OTHER_SAMPLE_CODE",
        trade_day=date(2026, 10, 2),
        order_key="pb1-entry-other-code",
    )
    assert other_claim.acquired
    assert other_identity.lifecycle_id != first_identity.lifecycle_id
    engine.dispose()


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
