"""KR close economic state: ACK is not native-priced SELL settlement.

This module is read-only.  Native KIS order/price proof is mandatory before
classifying the close as economically settled; no retry or broker write here.
"""
from __future__ import annotations

from datetime import date
from typing import Any


def assess_close_economic_settlement(
    *, accepted_policy_sells: int,
    postsubmit_reconcile_ok: bool,
    audit_report: dict[str, Any] | None,
    audit_attempted: bool = False,
) -> dict[str, Any]:
    """Classify a close run using durable priced SELL fills, never ACK status."""
    accepted = max(0, int(accepted_policy_sells or 0))
    report = audit_report if isinstance(audit_report, dict) else {}
    checked = max(0, int(report.get("sell_orders_checked") or 0))
    gaps = report.get("gap_count")
    gap_count = int(gaps) if isinstance(gaps, int) and gaps >= 0 else -1
    audit_proven = (
        report.get("status") == "PRICED_SELL_EVIDENCE_MATCHED"
        and gap_count == 0
        and checked >= accepted
    )
    if accepted == 0 and audit_proven:
        state = "NO_NEW_SELL_PRICED_EVIDENCE_MATCHED"
    elif accepted == 0 and not report:
        # A live audit that failed to return evidence is *not* an intentional
        # dry-run skip. Do not silently report OK after the broker lookup fails.
        state = (
            "ECONOMIC_SETTLEMENT_PENDING" if audit_attempted
            else "NO_NEW_SELL_SETTLEMENT_UNCHECKED"
        )
    elif postsubmit_reconcile_ok and audit_proven:
        state = "ECONOMIC_SETTLED"
    else:
        state = "ECONOMIC_SETTLEMENT_PENDING"
    # For zero newly accepted SELLs, an existing missing-price gap remains
    # pending even when there was no post-submit pass in this run.
    if accepted == 0 and report and not audit_proven:
        state = "ECONOMIC_SETTLEMENT_PENDING"
    # Preserve strict all-epoch audit status while reporting today's truth
    # separately. A pending Oct27 KR_INFINITE mirror must never be silently
    # mislabelled as a failure to settle a new Nov09 SELL.
    today_count = report.get("today_sell_orders_checked")
    today_gaps = report.get("today_gap_count")
    dated_proof = (
        isinstance(today_count, int) and today_count >= 0
        and isinstance(today_gaps, int) and today_gaps >= 0
        and report.get("undated_sell_orders_checked") == 0
    )
    if not dated_proof:
        today_state = "TODAY_SETTLEMENT_UNVERIFIED"
    elif accepted == 0:
        today_state = (
            "NO_NEW_SELL_PRICED_EVIDENCE_MATCHED"
            if today_gaps == 0 else "TODAY_ECONOMIC_SETTLEMENT_PENDING"
        )
    elif postsubmit_reconcile_ok and today_gaps == 0 and today_count >= accepted:
        today_state = "TODAY_ECONOMIC_SETTLED"
    else:
        today_state = "TODAY_ECONOMIC_SETTLEMENT_PENDING"
    return {
        "economic_settlement_state": state,
        "today_economic_settlement_state": today_state,
        "historical_economic_settlement_state": report.get(
            "historical_settlement_status", "HISTORICAL_UNVERIFIED"
        ),
        "historical_priced_sell_gap_count": report.get("historical_gap_count"),
        "today_priced_sell_gap_count": report.get("today_gap_count"),
        "economic_settlement_pending": int(state == "ECONOMIC_SETTLEMENT_PENDING"),
        "economic_audit_attempted": int(bool(audit_attempted)),
        "postsubmit_reconcile_ok": int(bool(postsubmit_reconcile_ok)),
        "native_priced_sell_gap_count": gap_count,
        "native_sell_orders_checked": checked,
    }


def audit_close_priced_sell_evidence(*, engine, env: str, day: date) -> dict[str, Any]:
    """Strictly read the current account epoch; never infer from a broker ACK."""
    from trader.account_state import get_account_key
    from trader.db.trading_epoch import active_trading_epoch_id, trading_epoch_enforced
    from scripts.kr_close_settlement_audit import load_snapshot, audit_sell_settlement

    with engine.connect() as conn:
        epoch = active_trading_epoch_id(
            conn, env=env, account_id=get_account_key(env=env),
            required=trading_epoch_enforced(),
        )
    if not epoch:
        raise RuntimeError("KR_CLOSE_SETTLEMENT_ACTIVE_EPOCH_REQUIRED")
    snapshot = load_snapshot(engine, env=env, epoch=str(epoch), day=day)
    return audit_sell_settlement(**snapshot, trade_date=day)
