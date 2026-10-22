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
    """Reconcile exact terminal evidence for ALL epochs of one broker account.

    Reconciliation follows original-order source trade date and per-epoch
    fencing tokens, but capacity holds are account-global. Never strand an
    earlier epoch's unresolved reservation when AM turns into Afternoon.
    This worker cannot place/cancel any KIS order or fabricate terminality.
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
    import hashlib
    digest = hashlib.sha256((
        scope["env"] + "|" + scope["account_id"] + "|" + scope["trading_epoch_id"]
    ).encode("utf-8")).hexdigest()
    result = {
        "mode": "RESERVATION_ONLY", "attempted": 0, "released": 0,
        "consumed": 0, "unresolved": 0,
        "account_scope_fingerprint": digest,
        "write_authority": "FENCED_ACCOUNT_RESERVATIONS_ONLY",
    }
    limit = min(100, max(1, int(max_candidates)))
    with engine.connect() as conn:
        all_epochs = conn.execute(text("""
            SELECT DISTINCT trading_epoch_id
            FROM us_execution_account_reservations
            WHERE env=:env AND account_fingerprint=:account
              AND state IN ('RESERVED','UNCERTAIN')
            ORDER BY trading_epoch_id
            LIMIT 9
        """), {"env": "practice", "account": scope["account_id"]}).scalars().all()
    # Unbounded stale epochs must not silently become an empty/clean read.
    if len(all_epochs) > 8:
        result["unresolved"] += 1
        result["incomplete_epoch_scan"] = True
        return result
    epochs = list(dict.fromkeys(
        [str(epoch) for epoch in all_epochs] + [str(scope["trading_epoch_id"])]
    ))
    result["epoch_count"] = len(epochs)
    for epoch in epochs:
        authority = AccountExecutionAuthority(
            engine, env="practice", account_fingerprint=scope["account_id"],
            trading_epoch_id=epoch,
        )
        lease = authority.acquire_lease(
            holder_id="reservation-worker:" + uuid.uuid4().hex, ttl_seconds=30,
        )
        params = {
            "env": "practice", "account": scope["account_id"],
            "epoch": epoch, "trade_date": trade_date, "limit": limit,
        }
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT r.action_key AS attempt_id, a.client_order_key,
                           o.trade_date AS source_trade_date,
                           a.attempt_state
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
                    ORDER BY r.created_at LIMIT :limit
                """), params).mappings().all()
                abandoned = conn.execute(text("""
                    SELECT r.action_key AS attempt_id, a.client_order_key,
                           NULL::DATE AS source_trade_date,
                           a.attempt_state
                    FROM us_execution_account_reservations AS r
                    JOIN us_execution_attempts AS a
                      ON a.attempt_id=r.action_key
                    JOIN us_execution_claims AS c
                      ON c.action_key=a.action_key
                    WHERE r.env=:env AND r.account_fingerprint=:account
                      AND r.trading_epoch_id=:epoch
                      AND c.env=:env AND c.account_id=:account
                      AND c.trading_epoch_id=:epoch
                      AND r.state='RESERVED'
                      AND a.authoritative IS TRUE
                      AND a.attempt_state='ABANDONED_PRE_SUBMIT'
                      AND c.active_attempt_id IS DISTINCT FROM a.attempt_id
                      AND a.client_order_key IS NOT NULL
                    ORDER BY r.created_at LIMIT :limit
                """), params).mappings().all()
            for row in list(rows) + list(abandoned):
                result["attempted"] += 1
                try:
                    if row["attempt_state"] == "ABANDONED_PRE_SUBMIT":
                        released = authority.release_proven_pre_submit_abort(
                            lease=lease, attempt_id=str(row["attempt_id"]),
                            client_order_key=str(row["client_order_key"]),
                        )
                        result["released"] += int(released)
                    else:
                        receipt = authority.terminalize_verified_attempt(
                            lease=lease, attempt_id=str(row["attempt_id"]),
                            client_order_key=str(row["client_order_key"]),
                            trade_date=str(row["source_trade_date"]),
                        )
                        if not receipt.get("idempotent"):
                            if receipt["state"] == "RELEASED":
                                result["released"] += 1
                            elif receipt["state"] == "CONSUMED":
                                result["consumed"] += 1
                except (ReservationConflict, AuthorityUnavailable) as exc:
                    # A failed exact proof leaves broker capacity reserved.
                    result["unresolved"] += 1
                    logger.warning(
                        "[US_RESERVATION_WORKER][NOT_PROVEN] reason=%s",
                        type(exc).__name__,
                    )
        finally:
            authority.release_lease(lease=lease)
    return result


def reservation_report_path(trade_date: str, session_run_id: str):
    """A separate diagnostic file, never the immutable trading journal."""
    from pathlib import Path
    import re
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(trade_date)):
        raise ValueError("reservation worker requires ISO US trade date")
    from trader.us.execution.reconcile_shadow_worker import safe_scope_token
    root = Path(os.getenv("US_RESERVATION_RECONCILE_DIR", "runtime/us/reconcile_reservations"))
    return root / str(trade_date) / (safe_scope_token(session_run_id) + ".json")


def assert_fresh_reservation_worker(
    *, trade_date: str, session_run_id: str,
    env: str, account_fingerprint: str, trading_epoch_id: str,
    max_age_sec: float = 45.0,
) -> None:
    """Fail closed if the exactly scoped Session reservation worker is absent.

    A recent file is liveness evidence only, NOT broker terminal proof or
    permission to bypass strategy/Risk/Claim/HTTP checks.
    """
    import hashlib
    import json
    from datetime import datetime, timezone
    if env != "practice" or not session_run_id or not trading_epoch_id:
        raise AuthorityUnavailable("US_RESERVATION_WORKER_SCOPE_MISSING")
    path = reservation_report_path(trade_date, session_run_id)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        sampled_at = datetime.fromisoformat(
            str(payload["sampled_at_utc"]).replace("Z", "+00:00")
        )
        if sampled_at.tzinfo is None:
            raise ValueError("worker timestamp lacks timezone")
        lag = (datetime.now(timezone.utc) - sampled_at.astimezone(timezone.utc)).total_seconds()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise AuthorityUnavailable("US_RESERVATION_WORKER_HEALTH_UNAVAILABLE") from exc
    expected = hashlib.sha256((
        env + "|" + account_fingerprint + "|" + trading_epoch_id
    ).encode("utf-8")).hexdigest()
    if (
        payload.get("mode") != "RESERVATION_ONLY"
        or payload.get("write_authority") != "FENCED_ACCOUNT_RESERVATIONS_ONLY"
        or payload.get("account_scope_fingerprint") != expected
        or payload.get("trade_date") != trade_date
        or payload.get("session_run_id") != session_run_id
        or payload.get("read_status") != "OK"
        or lag < 0 or lag > max_age_sec
    ):
        raise AuthorityUnavailable("US_RESERVATION_WORKER_HEALTH_STALE_OR_SCOPE_MISMATCH")


def run_reservation_loop(
    *, trade_date: str, session_run_id: str, env: str,
    interval_sec: float = 15.0, max_cycles: int = 0,
) -> int:
    """Session-owned reservation-only writer, no broker API credentials."""
    import json
    import signal
    import time
    from datetime import datetime, timezone
    from trader.us.execution.reconcile_shadow_worker import save_diagnostic_atomic
    path = reservation_report_path(trade_date, session_run_id)
    state = {"stop": False}
    signal.signal(signal.SIGTERM, lambda *_: state.__setitem__("stop", True))
    signal.signal(signal.SIGINT, lambda *_: state.__setitem__("stop", True))
    cycles = 0
    while not state["stop"]:
        try:
            payload = reconcile_proven_reservations_once(
                env=env, trade_date=trade_date,
            )
            payload["read_status"] = "OK" if payload["unresolved"] == 0 else "DEGRADED"
        except Exception as exc:
            payload = {
                "mode": "RESERVATION_ONLY",
                "write_authority": "FENCED_ACCOUNT_RESERVATIONS_ONLY",
                "read_status": "ERROR",
                "reason": type(exc).__name__,
            }
            logger.exception("[US_RESERVATION_WORKER][ERROR]")
        payload.update({
            "trade_date": trade_date,
            "session_run_id": session_run_id,
            "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        save_diagnostic_atomic(path, payload)
        cycles += 1
        if max_cycles and cycles >= max_cycles:
            break
        until = time.monotonic() + max(5.0, float(interval_sec))
        while not state["stop"] and time.monotonic() < until:
            time.sleep(min(0.5, until - time.monotonic()))
    return 0


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="US practice-only fenced reservation cleanup")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--session-run-id", required=True)
    parser.add_argument("--env", default="practice")
    parser.add_argument("--interval-sec", type=float, default=15.0)
    parser.add_argument("--max-cycles", type=int, default=0)
    a = parser.parse_args()
    if a.env != "practice":
        parser.error("US reservation cleanup requires practice")
    if os.getenv("US_ACCOUNT_RESERVATION_MODE", "OFF") != "ENFORCE":
        parser.error("US reservation cleanup is not armed")
    return run_reservation_loop(
        trade_date=a.trade_date, session_run_id=a.session_run_id,
        env=a.env, interval_sec=a.interval_sec, max_cycles=a.max_cycles,
    )


if __name__ == "__main__":
    raise SystemExit(main())
