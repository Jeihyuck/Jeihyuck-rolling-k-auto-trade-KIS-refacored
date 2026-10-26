"""PR257 post-exit settlement policy: strict, read-only fastpath decision.

No order is authorized by this gate. It only decides whether the expensive
historical reconcile MAY be skipped when TWO independent exact KIS/ledger
proofs bracket a tick that did not submit or attempt a PB1 protective SELL.
"""
from __future__ import annotations
from typing import Any


def can_skip_post_exit_full_reconcile(
    *, pre_exit_exact: bool, post_exit_exact: bool,
    exit_pass_ok: bool, exit_summary: dict[str, Any] | None,
    account_reset_mode: bool = False,
) -> tuple[bool, str]:
    if account_reset_mode:
        return False, "ACCOUNT_RESET_REQUIRES_FULL_RECONCILE"
    if not pre_exit_exact or not post_exit_exact:
        return False, "TWO_SIDED_EXACT_BROKER_PROOF_REQUIRED"
    if not exit_pass_ok or not isinstance(exit_summary, dict):
        return False, "EXIT_PASS_NOT_PROVEN"
    try:
        holdings = int(exit_summary.get("holdings"))
        checked = int(exit_summary.get("checked"))
        attempts = int(exit_summary.get("submit_attempt_count"))
        accepted = int(exit_summary.get("accepted_sell_count"))
        fills = int(exit_summary.get("fill_confirmed_sell_count"))
        submitted = int(exit_summary.get("submitted"))
        blocked = int(exit_summary.get("blocked_count"))
    except (TypeError, ValueError, OverflowError):
        return False, "EXIT_SUMMARY_NOT_AUTHORITATIVE"
    if min(holdings, checked, attempts, accepted, fills, submitted, blocked) < 0:
        return False, "EXIT_SUMMARY_NEGATIVE"
    if checked < holdings or blocked or any((attempts, accepted, fills, submitted)):
        return False, "EXIT_ACTIVITY_REQUIRES_POST_RECONCILE"
    return True, "TWO_SIDED_EXACT_PROOF_NO_SELL_SKIP_HISTORICAL_RECOVERY"
