"""Late broker fills across superseded submit attempts.

Test deterministic action semantics without live KIS, WSL, or a production DB.
"""
from datetime import date
import sqlalchemy as sa
import pytest

from trader.execution_claims import (
    DurableExecutionClaimRepo, build_execution_claim_tables,
)
from trader.execution_state import SemanticActionIdentity


@pytest.fixture
def ledger():
    metadata = sa.MetaData()
    action, attempt = build_execution_claim_tables(
        metadata, "test_us_actions", "test_us_attempts"
    )
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return DurableExecutionClaimRepo(engine, action, attempt)


@pytest.fixture
def action():
    return SemanticActionIdentity(
        env="practice", account_id="late-fill-account",
        market="US", trading_epoch_id="late-fill-epoch",
        strategy_owner="TQQQ_INFINITE", lifecycle_id="cycle-late",
        action="BUY", trade_date=date(2026, 10, 20),
    )


def _acquire(ledger, action, attempt_id, qty):
    acquired = ledger.acquire(
        action, attempt_id=attempt_id, requested_qty=qty,
        fresh_validation=True, client_order_key=f"client-{attempt_id}",
    )
    assert acquired.acquired, acquired.reason


def _record(ledger, action, attempt_id, state, qty, *, authoritative=True):
    return ledger.record_observation(
        action, attempt_id=attempt_id, state=state,
        cumulative_filled_qty=qty, authoritative=authoritative,
    )


def _two_attempts(ledger, action):
    _acquire(ledger, action, "old-38776", 5)
    assert _record(ledger, action, "old-38776", "CANCELLED", 0).action_state == "RETRYABLE"
    _acquire(ledger, action, "new-38777", 5)


def test_late_old_partial_fill_keeps_new_live_claim_fenced(ledger, action):
    _two_attempts(ledger, action)
    snap = _record(ledger, action, "old-38776", "PARTIALLY_FILLED", 2)
    assert snap.cumulative_filled_qty == 2
    assert snap.action_state == "UNCERTAIN"
    assert snap.active_attempt_id == "new-38777"
    assert snap.remaining_target_qty == 3
    again = _record(ledger, action, "old-38776", "PARTIALLY_FILLED", 2)
    assert again == snap
    with ledger.engine.connect() as conn:
        old = conn.execute(sa.select(ledger.attempts).where(
            ledger.attempts.c.attempt_id == "old-38776"
        )).mappings().one()
        assert old["cumulative_filled_qty"] == 2
        assert old["attempt_state"] == "CANCELLED_PARTIAL_FILL"
    blocked = ledger.acquire(
        action, attempt_id="third", requested_qty=3,
        client_order_key="third", fresh_validation=True,
    )
    assert not blocked.acquired
    assert blocked.reason == "unresolved_attempt_active"


def test_new_attempt_finish_reaggregates_old_fill_exactly_once(ledger, action):
    _two_attempts(ledger, action)
    _record(ledger, action, "old-38776", "PARTIALLY_FILLED", 2)
    done = _record(ledger, action, "new-38777", "FILLED", 3)
    assert done.cumulative_filled_qty == 5
    assert done.remaining_target_qty == 0
    assert done.action_state == "SATISFIED"
    assert done.active_attempt_id is None


def test_old_open_ack_cannot_roll_back_terminal_or_latest(ledger, action):
    _two_attempts(ledger, action)
    before = ledger.get(action)
    for state in ("ACKED", "SUBMITTED", "UNRESOLVED"):
        after = _record(ledger, action, "old-38776", state, None, authoritative=False)
        assert after == before
    with ledger.engine.connect() as conn:
        old_state = conn.execute(
            sa.select(ledger.attempts.c.attempt_state).where(
                ledger.attempts.c.attempt_id == "old-38776"
            )
        ).scalar_one()
    assert old_state == "CANCELLED_ZERO_FILL"


def test_broker_overfill_is_visible_and_fences_new_submission(ledger, action):
    _two_attempts(ledger, action)
    already = _record(ledger, action, "new-38777", "FILLED", 5)
    assert already.action_state == "SATISFIED"
    overflow = _record(ledger, action, "old-38776", "PARTIALLY_FILLED", 2)
    assert overflow.cumulative_filled_qty == 7
    assert overflow.action_state == "UNCERTAIN"
    assert overflow.remaining_target_qty is None
    assert overflow.active_attempt_id is None
    blocked = ledger.acquire(action, attempt_id="third", requested_qty=1,
                             client_order_key="third", fresh_validation=True)
    assert not blocked.acquired


def test_non_authoritative_superseded_fill_does_not_change_economics(ledger, action):
    _two_attempts(ledger, action)
    before = ledger.get(action)
    after = _record(ledger, action, "old-38776",
                    "PARTIALLY_FILLED", 3, authoritative=False)
    assert after == before


def test_first_attempt_normal_cancel_keeps_original_retry_semantics(ledger, action):
    _acquire(ledger, action, "first", 5)
    result = _record(ledger, action, "first", "CANCELLED", 0)
    assert result.action_state == "RETRYABLE"
    assert result.cumulative_filled_qty == 0
    assert result.remaining_target_qty == 5
