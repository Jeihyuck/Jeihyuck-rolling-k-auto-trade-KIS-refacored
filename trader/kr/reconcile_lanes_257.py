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



def prove_no_kr_infinite_unresolved(
    *, engine: Any, env: str, account_id: str,
) -> tuple[bool, str]:
    """Fail closed on *any* open dedicated KR_INFINITE intent in active epoch.

    PB1 open orders and claim health alone do not cover KR_INFINITE's
    separate durable intent table. This is a read-only account+epoch proof,
    NOT a new trading policy for the dedicated KR_INFINITE owner.
    """
    if env != "practice" or not str(account_id or "").strip():
        return False, "CROSS_OWNER_ACCOUNT_SCOPE_INVALID"
    try:
        import sqlalchemy as sa
        from trader.db.trading_epoch import active_trading_epoch_id

        with engine.connect() as conn:
            epoch = active_trading_epoch_id(
                conn, env=env, account_id=account_id, required=True,
            )
            if not epoch:
                return False, "KR_INFINITE_TRADING_EPOCH_UNKNOWN"
            if not sa.inspect(conn).has_table("kr_infinite_order_intents"):
                return False, "KR_INFINITE_INTENT_TABLE_MISSING"
            counts = conn.execute(sa.text("""
                SELECT COUNT(*) FROM kr_infinite_order_intents
                WHERE trading_epoch_id=:epoch
                  AND status IN (
                    'INTENT_CREATED','SUBMITTED','ACK','ACKED','PENDING',
                    'PARTIALLY_FILLED','PARTIAL_FILLED','RECONCILE_PENDING',
                    'UNRESOLVED_ACK','RECONCILE_ERROR','BROKER_RESULT_UNKNOWN'
                  )
            """), {"epoch": str(epoch)}).scalar()
            if counts is None or int(counts) != 0:
                return False, "KR_INFINITE_UNRESOLVED_ORDERS"
            return True, "KR_INFINITE_NO_OPEN_ACTIVE_EPOCH_INTENTS"
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            "[KR_RECONCILE_LANES][CROSS_OWNER_PROOF_FAIL] type=%s "
            "action=REQUIRE_FULL_RECONCILE_AND_FENCE_NEW_BUY",
            type(exc).__name__, exc_info=True,
        )
        return False, f"KR_INFINITE_INTENT_READ_FAILED_{type(exc).__name__}"
