"""Optional fenced US reservation cleanup worker (no KIS order or HTTP).

This is NOT the final authoritative broker-reconciliation writer. It only
releases account-wide holds after the existing KIS reconciliation path has
atomically persisted exact original-order and Claim terminal evidence. It
cannot infer terminality from age, ACK, cancel request, or balance alone.
Default operation remains LEGACY and disabled. Practice account only.
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from sqlalchemy import text

from trader.us.execution.account_authority import (
    AccountExecutionAuthority, AuthorityUnavailable, ReservationConflict,
)

logger = logging.getLogger(__name__)


def reconcile_proven_reservations_once(
    *, env: str, trade_date: str, max_candidates: int = 50,
) -> dict[str, Any]:
    """One bounded, account-scoped DB-only sweep.

    A separate Worker lease serializes this sweep with optional native Router
    transactional reservation acquisition. Every release re-checks broker
    order, Attempt and Claim truth under a fresh row-locked DB transaction.
    """
    if str(env or "").strip().lower() != "practice":
        raise RuntimeError("US_RESERVATION_RECONCILER_PRACTICE_ONLY")
    if os.getenv("US_ACCOUNT_RESERVATION_MODE", "OFF") != "ENFORCE":
        raise RuntimeError("US_RESERVATION_RECONCILER_NOT_ARMED")
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    scope = _us_execution_claim_scope()
    if scope["env"] != "practice" or not scope["trading_epoch_id"]:
        raise RuntimeError("US_RESERVATION_RECONCILER_SCOPE_INVALID")
    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("US_RESERVATION_RECONCILER_DB_UNAVAILABLE")
    authority = AccountExecutionAuthority(
        engine, env=scope["env"], account_fingerprint=scope["account_id"],
        trading_epoch_id=scope["trading_epoch_id"],
    )
    lease = authority.acquire_lease(
        holder_id="reservation-worker:" + uuid.uuid4().hex, ttl_seconds=30,
    )
    result = {"mode": "RESERVATION_ONLY", "attempted": 0,
              "released": 0, "consumed": 0, "unresolved": 0,
              "write_authority": "FENCED_ACCOUNT_RESERVATIONS_ONLY"}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT r.action_key AS attempt_id, a.client_order_key,
                       o.trade_date AS source_trade_date
                FROM us_execution_account_reservations AS r
                JOIN us_execution_attempts AS a
                  ON a.attempt_id=r.action_key
                JOIN us_execution_claims AS c
                  ON c.action_key=a.action_key
                JOIN us_orders AS o ON o.client_order_key=a.client_order_key
                  AND o.env=r.env AND o.trading_epoch_id=r.trading_epoch_id
                  AND o.meta->>'submit_attempt_id'=a.attempt_id
                WHERE r.env=:env AND r.account_fingerprint=:account
                  AND r.trading_epoch_id=:epoch
                  AND c.env=:env AND c.account_id=:account
                  AND c.trading_epoch_id=:epoch
                  AND r.state IN ('RESERVED','UNCERTAIN')
                  AND a.authoritative IS TRUE
                  AND a.attempt_state IN (
                    'FILLED','CANCELLED_ZERO_FILL','CANCELLED_PARTIAL_FILL'
                  )
                  AND a.client_order_key IS NOT NULL
                  AND o.trade_date<=CAST(:trade_date AS DATE)
                ORDER BY r.created_at
                LIMIT :limit
            """), {
                "env": "practice", "account": scope["account_id"],
                "epoch": scope["trading_epoch_id"],
                "trade_date": trade_date, "limit": min(100, max(1, int(max_candidates))),
            }).mappings().all()
        for row in rows:
            result["attempted"] += 1
            try:
                receipt = authority.terminalize_verified_attempt(
                    lease=lease, attempt_id=str(row["attempt_id"]),
                    client_order_key=str(row["client_order_key"]),
                    trade_date=str(row["source_trade_date"]),
                )
                if receipt["state"] == "RELEASED":
                    result["released"] += 1
                elif receipt["state"] == "CONSUMED":
                    result["consumed"] += 1
            except (ReservationConflict, AuthorityUnavailable) as exc:
                # Fail closed and leave the hold in place. Don't print
                # account credentials or broker evidence contents.
                result["unresolved"] += 1
                logger.warning(
                    "[US_RESERVATION_WORKER][NOT_PROVEN] reason=%s",
                    type(exc).__name__,
                )
    finally:
        authority.release_lease(lease=lease)
    return result
