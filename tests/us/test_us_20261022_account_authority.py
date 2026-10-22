"""PostgreSQL contract for US account-wide fenced reservations.

Runs only against disposable PBCORE_TEST_POSTGRES_URL in CI, never Supabase.
No real broker credentials or order calls.
"""
import hashlib
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from trader.us.execution.account_authority import (
    AccountExecutionAuthority, AuthorityUnavailable, ReservationConflict,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL integration URL not configured",
)


@pytest.fixture
def authority():
    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS us_execution_account_reservations"))
        conn.execute(text("DROP TABLE IF EXISTS us_execution_authority_leases"))
        conn.exec_driver_sql(
            Path("migrations/0056_us_execution_authority_reservations.sql").read_text()
        )
    repo = AccountExecutionAuthority(
        engine, env="practice",
        account_fingerprint=hashlib.sha256(b"account-alpha").hexdigest(),
        trading_epoch_id="test-epoch",
    )
    yield repo
    engine.dispose()


def test_account_cash_reservation_serializes_strategy_owners(authority):
    lease = authority.acquire_lease(holder_id="session-A")
    one = authority.reserve(
        lease=lease, action_key="pb1-buy-A", strategy_owner="US_STANDARD",
        symbol="AMD", side="BUY", qty=3, price_usd="100",
        broker_cash_usd="450",
    )
    assert one["idempotent"] is False
    again = authority.reserve(
        lease=lease, action_key="pb1-buy-A", strategy_owner="US_STANDARD",
        symbol="AMD", side="BUY", qty=3, price_usd="100",
        broker_cash_usd="450",
    )
    assert again["idempotent"] is True
    with pytest.raises(ReservationConflict, match="account_capacity_reserved"):
        authority.reserve(
            lease=lease, action_key="infinite-buy-B",
            strategy_owner="TQQQ_INFINITE", symbol="TQQQ",
            side="BUY", qty=2, price_usd="100", broker_cash_usd="450",
        )
    with pytest.raises(ReservationConflict, match="idempotency_collision"):
        authority.reserve(
            lease=lease, action_key="pb1-buy-A", strategy_owner="US_STANDARD",
            symbol="AMD", side="BUY", qty=1, price_usd="100",
            broker_cash_usd="450",
        )
    # Even an otherwise identical action key belongs to exactly one
    # account/epoch; colliding keys must never impersonate an existing hold.
    other_scope = AccountExecutionAuthority(
        authority.engine, env="practice",
        account_fingerprint=hashlib.sha256(b"account-beta").hexdigest(),
        trading_epoch_id="test-epoch",
    )
    other_lease = other_scope.acquire_lease(holder_id="session-B")
    with pytest.raises(ReservationConflict, match="idempotency_collision"):
        other_scope.reserve(
            lease=other_lease, action_key="pb1-buy-A",
            strategy_owner="US_STANDARD", symbol="AMD",
            side="BUY", qty=3, price_usd="100", broker_cash_usd="450",
        )
    with pytest.raises(ReservationConflict, match="authoritative_terminal_evidence_required"):
        authority.terminalize(
            lease=lease, action_key="pb1-buy-A", terminal_evidence="ACK"
        )
    assert authority.terminalize(
        lease=lease, action_key="pb1-buy-A",
        terminal_evidence="KIS_TERMINAL:cancelled_zero_open", filled=False,
    )
    second = authority.reserve(
        lease=lease, action_key="infinite-buy-B",
        strategy_owner="TQQQ_INFINITE", symbol="TQQQ",
        side="BUY", qty=2, price_usd="100", broker_cash_usd="450",
    )
    assert second["state"] == "RESERVED"


def test_account_sell_holds_and_stale_worker_fencing(authority):
    first = authority.acquire_lease(holder_id="session-old", ttl_seconds=30)
    authority.reserve(
        lease=first, action_key="sell-1", strategy_owner="US_STANDARD",
        symbol="AMD", side="SELL", qty=4, price_usd="100",
        broker_available_shares=5,
    )
    with pytest.raises(ReservationConflict, match="account_capacity_reserved"):
        authority.reserve(
            lease=first, action_key="sell-2", strategy_owner="TQQQ_INFINITE",
            symbol="AMD", side="SELL", qty=2, price_usd="100",
            broker_available_shares=5,
        )
    with pytest.raises(AuthorityUnavailable, match="LEASE_HELD"):
        authority.acquire_lease(holder_id="session-other")
    with authority.engine.begin() as conn:
        conn.execute(text("""
            UPDATE us_execution_authority_leases
            SET expires_at=clock_timestamp()-INTERVAL '2 seconds'
            WHERE env='practice'
        """))
    new = authority.acquire_lease(holder_id="session-new", ttl_seconds=30)
    assert new.fencing_token > first.fencing_token
    with pytest.raises(AuthorityUnavailable, match="FENCED"):
        authority.reserve(
            lease=first, action_key="stale", strategy_owner="US_STANDARD",
            symbol="AMD", side="SELL", qty=1, price_usd="100",
            broker_available_shares=5,
        )
    with pytest.raises(AuthorityUnavailable, match="FENCED"):
        authority.terminalize(
            lease=first, action_key="sell-1",
            terminal_evidence="KIS_TERMINAL:stale_cancel",
        )
    assert authority.terminalize(
        lease=new, action_key="sell-1",
        terminal_evidence="KIS_TERMINAL:verified_cancel",
    )


def test_non_practice_and_unverified_scope_rejected(authority):
    with pytest.raises(AuthorityUnavailable, match="PRACTICE_ONLY"):
        AccountExecutionAuthority(
            authority.engine, env="live",
            account_fingerprint="a" * 64, trading_epoch_id="epoch",
        )
    with pytest.raises(AuthorityUnavailable, match="SCOPE_UNVERIFIED"):
        AccountExecutionAuthority(
            authority.engine, env="practice",
            account_fingerprint="plain-account-number",
            trading_epoch_id="epoch",
        )
