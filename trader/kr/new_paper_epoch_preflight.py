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
        if isinstance(started, str):
            try:
                started = datetime.fromisoformat(started.replace("Z", "+00:00"))
            except ValueError:
                started = None
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


def verify_fresh_kr_practice_balance(
    snapshot: Any,
    *,
    source: str,
    expected_capital_krw: int = KR_EXPECTED_PRACTICE_CAPITAL_KRW,
) -> dict[str, str | int]:
    """Check a *forced fresh* KIS response, not an orderable-cash estimate.

    `KisAPI.inquire_balance_all()` aggregates KIS paginated replies into
    output1/output2 and does not return top-level rt_cd. Requiring that
    field incorrectly rejected a successful 2026-10-26 broker response.
    The caller MUST use get_balance_cached(force=True, return_source=True)
    and require source='api'. Any explicit nonzero rt_cd is still rejected.
    """
    from decimal import Decimal, InvalidOperation

    if source != "api":
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_SOURCE_NOT_FRESH")
    if not isinstance(snapshot, dict):
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_BALANCE_NOT_PROVEN")
    if (
        snapshot.get("_stub") or snapshot.get("_diag_stub")
        or snapshot.get("_kis_disabled")
        or str(snapshot.get("_source") or "").lower() == "http_disabled_stub"
    ):
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_BALANCE_STUB")
    if snapshot.get("rt_cd") is not None and str(snapshot["rt_cd"]) != "0":
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_BROKER_ERROR")

    # An empty portfolio is output1=[], not a failed/missing output1.
    rows = snapshot.get("output1")
    if not isinstance(rows, list):
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_HOLDINGS_NOT_PROVEN")
    output2 = snapshot.get("output2")
    if isinstance(output2, list):
        output2 = output2[0] if len(output2) == 1 else None
    if not isinstance(output2, dict) or not output2:
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_CASH_NOT_PROVEN")

    # On a fresh, unused simulation account ord_psbl_cash may report 0
    # outside the trading window, while dnca_tot_amt is 100,000,000 KRW.
    # The funded balance is determined from deposit, not orderable cash.
    cash = None
    for key in ("dnca_tot_amt", "nxdy_excc_amt", "prvs_rcdl_excc_amt"):
        val = output2.get(key)
        if val is None or str(val).strip() == "":
            continue
        try:
            amount = Decimal(str(val).replace(",", "").strip())
        except (InvalidOperation, ValueError):
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_CASH_INVALID")
        if not amount.is_finite() or amount < 0 or amount != amount.to_integral_value():
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_CASH_INVALID")
        cash = int(amount)
        break
    if cash is None:
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_CASH_NOT_PROVEN")

    holdings_count = 0
    for row in rows:
        if not isinstance(row, dict):
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_HOLDINGS_NOT_PROVEN")
        try:
            quantity = Decimal(str(row.get("hldg_qty") or "0").replace(",", "").strip())
        except (InvalidOperation, ValueError):
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_HOLDINGS_NOT_PROVEN")
        if (
            not quantity.is_finite() or quantity < 0
            or quantity != quantity.to_integral_value()
        ):
            raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_HOLDINGS_NOT_PROVEN")
        if quantity > 0:
            holdings_count += 1

    expected = int(expected_capital_krw)
    if abs(cash - expected) > max(1_000_000, int(expected * 0.01)):
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_CASH_NOT_100M")
    if holdings_count != 0:
        raise KrPracticeReadinessError("KR_NEW_ACCOUNT_KIS_HOLDINGS_NOT_EMPTY")
    return {
        "kis_balance_status": "AUTHENTIC_PRACTICE_100M_CASH_NO_HOLDINGS",
        "kis_cash_krw": cash,
        "kis_holdings_count": holdings_count,
    }
