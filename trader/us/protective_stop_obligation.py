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
                   confirmed_filled_qty,obligation_state,first_signal_at,last_signal_at,
                   soft_stop_pct,hard_stop_pct
            FROM us_protective_stop_obligations
            WHERE trading_epoch_id=:epoch AND env='practice'
              AND obligation_state<>'SATISFIED'
            ORDER BY severity DESC,first_signal_at
        """),{"epoch":epoch}).mappings().all()
    return [dict(x) for x in rows]


def replay_confirmed_protective_intents(
    *, snapshots: list[dict], existing_intents: list[dict],
    pending: list[dict], now: datetime | None,
) -> list[dict]:
    """Rehydrate durable hard/persistent stop via existing PB1 -> canonical router.

    This function does not inspect, cancel, or release broker orders. _make_exit_intent
    and the normal router remain authoritative for pending TP, shared account
    reservations, live quantity, risk, and KIS submission uncertainty.
    """
    from trader.us.stop_policy_revision import scope_for_position
    from trader.us.entry_exit_contract import contract_exit_config
    from trader.us.pb1.us_exit_engine import _make_exit_intent

    existing_symbols = {
        str(i.get("symbol") or "").upper() for i in existing_intents
        if str(i.get("side") or "").upper() == "SELL"
    }
    by_scope = {}
    for snapshot in snapshots:
        symbol, life, account = scope_for_position(snapshot)
        if symbol and life and account:
            by_scope[(symbol,life,account)] = snapshot

    emitted=[]
    for debt in pending:
        if int(debt.get("severity") or 0) < 2:
            continue  # Only confirmed full-exit debt is eligible for replay.
        if str(debt.get("obligation_state") or "").upper() not in {
            "PENDING","BLOCKED","RECONCILE_REQUIRED","SUBMITTED","PARTIAL","ESCALATED",
        }:
            continue  # Never re-submit a satisfied/invalid lifecycle.
        symbol = str(debt.get("symbol") or "").upper()
        if symbol in existing_symbols:
            continue
        key=(symbol,str(debt.get("lifecycle_id") or ""),str(debt.get("account_id") or ""))
        pos=by_scope.get(key)
        if not pos:
            continue
        try:
            holding=int(pos.get("holding_qty") or pos.get("qty") or 0)
            orderable=int(pos.get("orderable_qty") or 0)
            current=float(pos.get("resolved_current_price") or pos.get("current_price") or 0)
            entry=float(pos.get("resolved_entry_price") or pos.get("entry_price") or 0)
        except (TypeError,ValueError):
            continue
        if holding<=0 or orderable<=0 or current<=0 or entry<=0:
            # A blocked live broker quantity is not terminal evidence.
            continue
        severity=int(debt["severity"])
        kind="hard_stop_loss" if severity==3 else "persistent_soft_stop_full_exit"
        cfg=contract_exit_config(pos)
        if not cfg:
            continue  # unsigned/unknown contract cannot be replay authority
        # The original confirmed stop threshold is immutable debt evidence.
        cfg={**cfg,"hard_stop":float(debt.get("hard_stop_pct") or cfg["hard_stop"]),
             "soft_stop":float(debt.get("soft_stop_pct") or cfg["soft_stop"])}
        intent=_make_exit_intent(
            symbol=symbol,exchange=str(pos.get("exchange") or "NASDAQ"),
            qty=min(holding,orderable), current_price=current,entry_price=entry,
            exit_type=kind,reason="persistent_durable_confirmed_exit",
            unrealized_pnl_usd=(current-entry)*min(holding,orderable),
            pnl_pct=(current-entry)/entry,
            holding_qty=holding,orderable_qty=orderable,now=now,
            policy_cfg=cfg,position_lifecycle_id=key[1],
            meta_extra={"stop_obligation_replay":True,
                        "stop_obligation_first_signal_at":str(debt.get("first_signal_at") or "")},
        )
        if intent:
            emitted.append(intent)
            existing_symbols.add(symbol)
    return emitted


def validate_terminal_broker_proof(debt: dict, proof: dict) -> bool:
    """Only fully reconciled KIS original-account zero position is terminal.

    KIS cancel ACK, old CCNL OPEN, a local position snapshot, and a completed
    REST request never satisfy this predicate. The caller must be the
    authoritative broker reconciliation path; this helper cannot fetch proof.
    """
    if not isinstance(proof,dict) or not isinstance(debt,dict):
        return False
    if any(str(proof.get(k) or "") != str(debt.get(k) or "") for k in
           ("account_id","symbol","lifecycle_id","trading_epoch_id")):
        return False
    if (str(proof.get("env") or "")!="practice" or
            str(proof.get("strategy_owner") or "")!="US_STANDARD"):
        return False
    if (str(proof.get("position_source") or "")!="KIS_AUTHORITATIVE" or
            str(proof.get("open_orders_source") or "")!="KIS_AUTHORITATIVE"):
        return False
    if proof.get("fills_reconciled") is not True or proof.get("claims_reconciled") is not True:
        return False
    if proof.get("broker_terminal_order_states") is not True:
        return False
    try:
        balance_qty=int(proof["broker_holding_qty"])
        open_sell_qty=int(proof["broker_open_sell_qty"])
    except (KeyError,TypeError,ValueError):
        return False
    if balance_qty!=0 or open_sell_qty!=0:
        return False
    if not str(proof.get("broker_evidence_ref") or "").strip():
        return False
    if isinstance(proof.get("broker_holding_qty"), bool) or isinstance(proof.get("broker_open_sell_qty"), bool):
        return False
    observed=proof.get("observed_at")
    try:
        when=datetime.fromisoformat(str(observed).replace("Z","+00:00"))
    except (TypeError, ValueError):
        return False
    return when.tzinfo is not None


def satisfy_stop_obligation_from_broker_proof(*, debt: dict, proof: dict) -> bool:
    """Finalize stop debt from independent authoritative KIS recon evidence.

    Never alter fills, original Claim/Attempt state, or reservations here.
    Reject if any related TP/SELL claim remains in a non-terminal state.
    """
    import json
    if not validate_terminal_broker_proof(debt,proof):
        raise StopObligationUnavailable("STOP_OBLIGATION_TERMINAL_PROOF_UNVERIFIED")
    from sqlalchemy import text
    from trader.us.db.repos import _get_engine_or_none, _active_us_epoch
    engine=_get_engine_or_none()
    if engine is None:
        raise StopObligationUnavailable("STOP_OBLIGATION_DATABASE_UNAVAILABLE")
    with engine.begin() as conn:
        epoch=_active_us_epoch(conn)
        if not epoch or str(epoch)!=str(debt["trading_epoch_id"]):
            raise StopObligationUnavailable("STOP_OBLIGATION_EPOCH_CHANGED")
        result=conn.execute(text("""
            UPDATE us_protective_stop_obligations o
               SET obligation_state='SATISFIED',
                   terminal_proof=CAST(:proof AS jsonb),updated_at=now()
             WHERE o.trading_epoch_id=:epoch
               AND o.env='practice'
               AND o.account_id=:account
               AND o.strategy_owner='US_STANDARD'
               AND o.symbol=:symbol
               AND o.lifecycle_id=:lifecycle
               AND o.obligation_state<>'SATISFIED'
               AND NOT EXISTS (
                   SELECT 1 FROM us_execution_claims c
                    WHERE c.trading_epoch_id=o.trading_epoch_id
                      AND c.env=o.env AND c.account_id=o.account_id
                      AND c.strategy_owner=o.strategy_owner
                      AND c.lifecycle_id=o.lifecycle_id
                      AND c.action_state IN ('IN_FLIGHT','UNCERTAIN','PARTIALLY_SATISFIED')
               )
            RETURNING o.lifecycle_id
        """),{
            "proof":json.dumps(proof,sort_keys=True,default=str),
            "epoch":epoch,"account":debt["account_id"],
            "symbol":debt["symbol"],"lifecycle":debt["lifecycle_id"],
        })
        committed=result.first() is not None
    if not committed:
        logger.warning("[US_PROTECTIVE_STOP][TERMINAL_BLOCKED] symbol=%s lifecycle=%s action=retain_debt",
                       debt.get("symbol"),debt.get("lifecycle_id"))
    return committed
