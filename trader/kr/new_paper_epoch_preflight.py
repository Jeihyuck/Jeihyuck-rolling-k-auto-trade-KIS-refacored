"""Read-only KR first-day practice account, capital and epoch readiness.

The first trading day after operator creates a new practice account is different
from an ordinary trading day: an old account can still have a valid ACTIVE
generation. A generic 'some active epoch exists' check is NOT sufficient.
No schema change, order, reset, destructive write, or KIS HTTP is performed.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import text

KR_GO_LIVE_DATE = date(2026, 10, 26)
KR_NEW_ACCOUNT_EPOCH_REASON = "NEW_KIS_PRACTICE_ACCOUNT_20261025"
KR_EXPECTED_PRACTICE_CAPITAL_KRW = 100_000_000


class KrPracticeReadinessError(RuntimeError):
    pass


def verify_kr_practice_first_day(
    engine: Any,
    *,
    env: str,
    account_id: str,
    trade_date: date,
    expected_capital_krw: int,
    max_capital_krw: int,
) -> dict[str, str | int]:
    """Verify immutable account-specific new epoch without exposing account ID.

    Only the 2026-10-26 KR practice rollout is subject to the new-account
    requirement. Outside the rollout date, legacy epoch verification continues
    to apply; no future scheduled execution is silently pinned to an old UUID.
    """
    if trade_date != KR_GO_LIVE_DATE:
        return {"status": "OUTSIDE_GO_LIVE_DATE"}
    if env != "practice" or not account_id.startswith("practice:"):
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_PRACTICE_SCOPE_REQUIRED")
    if (
        expected_capital_krw != KR_EXPECTED_PRACTICE_CAPITAL_KRW
        or max_capital_krw != KR_EXPECTED_PRACTICE_CAPITAL_KRW
    ):
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_100M_CAPITAL_MISMATCH")

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trading_epoch_id, started_at, reason
            FROM trading_epochs
            WHERE env=:env AND account_id=:account_id AND status='ACTIVE'
        """), {"env": env, "account_id": account_id}).mappings().all()
        if len(rows) != 1:
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_ACTIVE_EPOCH_AMBIGUOUS_OR_MISSING")
        row = rows[0]
        epoch = str(row["trading_epoch_id"])
        started = row["started_at"]
        if (
            str(row["reason"] or "") != KR_NEW_ACCOUNT_EPOCH_REASON
            or not isinstance(started, datetime)
            or started.date() < date(2026, 10, 25)
        ):
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_EPOCH_PROVENANCE_MISMATCH")

        foreign_children = conn.execute(text("""
            SELECT count(*) FROM portfolio_epochs
            WHERE env=:env AND account_id=:account_id
              AND status='ACTIVE'
              AND (trading_epoch_id IS NULL OR trading_epoch_id <> :epoch)
        """), {"env": env, "account_id": account_id, "epoch": epoch}).scalar_one()
        if foreign_children:
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_STALE_PORTFOLIO_EPOCH")

        current_children = conn.execute(text("""
            SELECT count(*) FROM portfolio_epochs
            WHERE env=:env AND account_id=:account_id
              AND strategy='pb1_pullback_close'
              AND status='ACTIVE' AND trading_epoch_id=:epoch
        """), {"env": env, "account_id": account_id, "epoch": epoch}).scalar_one()
        if current_children > 1:
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_AMBIGUOUS_PB1_PORTFOLIO")

    # Do not print account IDs or make a new portfolio child here. Existing
    # _ensure_active_epoch atomically creates the child's first economic write.
    return {
        "status": "KR_NEW_ACCOUNT_EPOCH_AND_100M_OK",
        "trading_epoch_id": epoch,
        "pb1_portfolio_epoch_count": int(current_children),
        "capital_krw": KR_EXPECTED_PRACTICE_CAPITAL_KRW,
    }
