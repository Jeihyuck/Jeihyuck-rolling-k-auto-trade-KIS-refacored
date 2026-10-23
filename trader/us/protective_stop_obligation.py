"""Durable US_STANDARD stop obligation: record *before* pending-order suppression.

This ledger neither submits orders nor releases an unresolved historical claim.
The protected router and KIS broker proof remain authoritative.  Only explicit
ENFORCE mode requires persistence before an order is eligible. SHADOW records
when possible without altering existing order semantics.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
_STOP_SEVERITY = {
    "soft_stop_loss": 1, "soft_stop": 1,
    "persistent_soft_stop_full_exit": 2,
    "hard_stop": 3, "hard_stop_loss": 3, "hard_stop_full_exit": 3,
}


class StopObligationUnavailable(RuntimeError):
    pass


def record_confirmed_stop(position: dict, *, exit_type: str, target_qty: int,
                          pnl_pct: float, signal_price: float, policy: dict,
                          now: datetime | None = None) -> bool:
    """Idempotent lifecycle-scoped obligation, never broker execution authority."""
    severity = _STOP_SEVERITY.get(str(exit_type).lower())
    if severity is None:
        return False
    mode = str(os.getenv("US_PROTECTIVE_STOP_LEDGER_MODE", "SHADOW")).upper()
    if mode not in {"SHADOW", "ENFORCE", "OFF"}:
        raise ValueError("unsupported US_PROTECTIVE_STOP_LEDGER_MODE")
    if mode == "OFF":
        return False
    # Avoid external persistence for explicit offline test harness sessions.
    if os.getenv("KIS_HTTP_BLOCK", "0") == "1" or os.getenv("OFFLINE_MODE", "0") == "1":
        return False
    from trader.us.stop_policy_revision import scope_for_position
    symbol, lifecycle, account = scope_for_position(position)
    if not symbol or target_qty <= 0 or signal_price <= 0:
        if mode == "ENFORCE":
            raise StopObligationUnavailable("STOP_OBLIGATION_SCOPE_UNVERIFIED")
        logger.warning("[US_PROTECTIVE_STOP][UNTRACKED] symbol=%s reason=scope_missing",position.get("symbol"))
        return False
    if os.getenv("KIS_ENV", "practice").lower() != "practice":
        if mode == "ENFORCE":
            raise StopObligationUnavailable("STOP_OBLIGATION_REAL_ENV_FORBIDDEN")
        return False
    try:
        from sqlalchemy import text
        from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
        engine = _get_engine_or_none()
        if engine is None:
            raise StopObligationUnavailable("STOP_OBLIGATION_DATABASE_UNAVAILABLE")
        signal_time = now or datetime.now(timezone.utc)
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)
        with engine.begin() as conn:
            epoch = _active_us_epoch(conn)
            if not epoch:
                raise StopObligationUnavailable("STOP_OBLIGATION_EPOCH_MISSING")
            row = conn.execute(text("""
                INSERT INTO us_protective_stop_obligations
                (trading_epoch_id, env, account_id, strategy_owner, symbol, lifecycle_id,
                 stop_family, severity, target_qty, signal_price, signal_pnl_pct,
                 hard_stop_pct, soft_stop_pct, first_signal_at, last_signal_at,
                 obligation_state, updated_at)
                VALUES (:epoch,'practice',:account,'US_STANDARD',:symbol,:lifecycle,
                        :family,:severity,:qty,:price,:pnl,:hard,:soft,
                        :signal_at,:signal_at,'PENDING',now())
                ON CONFLICT (trading_epoch_id,env,account_id,strategy_owner,symbol,lifecycle_id)
                DO UPDATE SET
                    severity=GREATEST(us_protective_stop_obligations.severity,EXCLUDED.severity),
                    stop_family=CASE WHEN EXCLUDED.severity>=us_protective_stop_obligations.severity
                                     THEN EXCLUDED.stop_family ELSE us_protective_stop_obligations.stop_family END,
                    target_qty=CASE WHEN EXCLUDED.severity>=us_protective_stop_obligations.severity
                                    THEN GREATEST(us_protective_stop_obligations.target_qty,EXCLUDED.target_qty)
                                    ELSE us_protective_stop_obligations.target_qty END,
                    last_signal_at=GREATEST(us_protective_stop_obligations.last_signal_at,EXCLUDED.last_signal_at),
                    updated_at=now()
                WHERE us_protective_stop_obligations.obligation_state<>'SATISFIED'
                RETURNING obligation_state
            """), {"epoch":epoch,"account":account,"symbol":symbol,"lifecycle":lifecycle,
                   "family":exit_type,"severity":severity,"qty":int(target_qty),
                   "price":signal_price,"pnl":pnl_pct,
                   "hard":float(policy.get("hard_stop",0.06)),
                   "soft":float(policy.get("soft_stop",0.03)),
                   "signal_at":signal_time}).mappings().first()
        if not row:
            # A completed lifecycle cannot be silently reactivated by an old signal.
            logger.warning("[US_PROTECTIVE_STOP][ALREADY_TERMINAL] symbol=%s lifecycle=%s",symbol,lifecycle)
            return False
        logger.info("[US_PROTECTIVE_STOP][DURABLE] symbol=%s lifecycle=%s family=%s",symbol,lifecycle,exit_type)
        return True
    except Exception as exc:
        logger.error("[US_PROTECTIVE_STOP][DURABLE_FAIL] symbol=%s reason=%s",
                     symbol,type(exc).__name__)
        if mode == "ENFORCE":
            raise StopObligationUnavailable("STOP_OBLIGATION_WRITE_FAILED") from exc
        return False


def load_pending_stop_obligations(*, env: str="practice") -> list[dict]:
    """Return unresolved stop debt; NEVER assert these are safe to resubmit."""
    if env.lower()!="practice":
        raise StopObligationUnavailable("STOP_OBLIGATION_ENV_FORBIDDEN")
    from sqlalchemy import text
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    engine=_get_engine_or_none()
    if engine is None:
        raise StopObligationUnavailable("STOP_OBLIGATION_DATABASE_UNAVAILABLE")
    with engine.connect() as conn:
        epoch=_active_us_epoch(conn)
        if not epoch:
            raise StopObligationUnavailable("STOP_OBLIGATION_EPOCH_MISSING")
        rows=conn.execute(text("""
            SELECT account_id,symbol,lifecycle_id,stop_family,severity,target_qty,
                   confirmed_filled_qty,obligation_state,first_signal_at,last_signal_at
            FROM us_protective_stop_obligations
            WHERE trading_epoch_id=:epoch AND env='practice'
              AND obligation_state<>'SATISFIED'
            ORDER BY severity DESC,first_signal_at
        """),{"epoch":epoch}).mappings().all()
    return [dict(x) for x in rows]
