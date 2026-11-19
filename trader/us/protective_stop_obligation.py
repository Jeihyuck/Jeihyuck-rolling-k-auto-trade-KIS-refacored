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


def refresh_soft_stop_fill_progress(*, provider=None, trade_date: str) -> dict:
    """AM/CLOSE-only derived accounting from already committed native fills.

    A completed partial soft SELL does not close the remaining position or
    release any claim. Keep the lifecycle obligation PARTIAL and escalatable.
    Replays recompute unique order totals rather than adding the fill twice.
    """
    import hashlib
    from sqlalchemy import text
    from trader.account_state import get_account_key, resolve_env_name
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    if resolve_env_name() != "practice":
        return {"updated": 0}
    engine = _get_engine_or_none()
    if engine is None:
        raise StopObligationUnavailable("STOP_OBLIGATION_DATABASE_UNAVAILABLE")
    account = get_account_key(env="practice")
    hashed = hashlib.sha256(account.encode()).hexdigest()
    with engine.begin() as conn:
        epoch = _active_us_epoch(conn, required=True)
        result = conn.execute(text("""
            WITH proven AS (
                SELECT d.symbol,d.lifecycle_id,
                       LEAST(d.target_qty,SUM(f.qty))::integer AS filled
                FROM us_protective_stop_obligations d
                JOIN us_fills f ON f.symbol=d.symbol AND f.side='SELL'
                 AND f.trading_epoch_id=d.trading_epoch_id
                 AND f.account_id=:hashed
                 AND f.portfolio_epoch_id=d.portfolio_epoch_id
                 AND f.position_cycle_id=d.lifecycle_id
                 AND f.meta->>'position_lifecycle_id'=d.lifecycle_id
                 AND f.meta->>'fill_evidence_type'='KIS_ORDER_CUMULATIVE_ACTUAL'
                 AND f.meta->>'is_synthetic'='false'
                 AND COALESCE(f.meta->>'accounting_active','true')='true'
                 AND f.filled_at>=d.first_signal_at AND f.trade_date<=CAST(:td AS date)
                JOIN us_orders o ON o.trading_epoch_id=f.trading_epoch_id
                 AND o.env=d.env AND o.account_id=f.account_id
                 AND o.portfolio_epoch_id=f.portfolio_epoch_id
                 AND o.position_cycle_id=f.position_cycle_id
                 AND o.trade_date=f.trade_date AND o.symbol=f.symbol AND o.side='SELL'
                 AND o.client_order_key=f.client_order_key AND o.order_no=f.order_no
                 AND o.status='FILLED' AND o.qty_filled=f.qty AND o.qty_requested=f.qty
                 AND o.meta->>'stop_type' IN ('soft_stop','soft_stop_loss')
                JOIN us_execution_claims c ON c.action_key=o.meta->>'execution_action_key'
                 AND c.trading_epoch_id=o.trading_epoch_id AND c.env=o.env
                 AND c.market='US' AND c.account_id=:hashed
                 AND c.strategy_owner=d.strategy_owner AND c.lifecycle_id=d.lifecycle_id
                 AND c.portfolio_epoch_id=d.portfolio_epoch_id
                 AND c.position_cycle_id=d.lifecycle_id
                 AND c.action_state='SATISFIED' AND c.remaining_target_qty=0
                 AND c.cumulative_filled_qty>=o.qty_filled
                WHERE d.trading_epoch_id=:epoch AND d.env='practice'
                  AND d.account_id=:account AND d.strategy_owner='US_STANDARD'
                  AND d.symbol<>'TQQQ' AND d.severity=1
                  AND d.stop_family IN ('soft_stop','soft_stop_loss')
                  AND d.obligation_state<>'SATISFIED'
                  AND NOT EXISTS (
                      SELECT 1 FROM us_fills duplicate
                      WHERE duplicate.id<>f.id AND duplicate.trading_epoch_id=f.trading_epoch_id
                        AND duplicate.trade_date=f.trade_date AND duplicate.symbol=f.symbol
                        AND duplicate.side=f.side AND duplicate.order_no=f.order_no
                        AND COALESCE(duplicate.meta->>'accounting_active','true')='true'
                  )
                GROUP BY d.symbol,d.lifecycle_id,d.target_qty
            )
            UPDATE us_protective_stop_obligations d
               SET confirmed_filled_qty=proven.filled,obligation_state='PARTIAL',updated_at=now()
              FROM proven
             WHERE d.trading_epoch_id=:epoch AND d.env='practice' AND d.account_id=:account
               AND d.strategy_owner='US_STANDARD' AND d.symbol=proven.symbol
               AND d.lifecycle_id=proven.lifecycle_id AND d.severity=1
               AND d.obligation_state<>'SATISFIED'
               AND proven.filled>d.confirmed_filled_qty
        """), {"account": account, "hashed": hashed, "epoch": epoch, "td": trade_date})
    return {"updated": int(result.rowcount or 0)}


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
            strict_scope = os.getenv("WSL_RUN_MARKET", "").upper() == "US"
            portfolio_scope = ""
            if strict_scope:
                from trader.account_state import get_account_key
                from trader.us.execution.recovery_identity import current_us_portfolio_epoch
                meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
                position_epoch = str(position.get("trading_epoch_id") or meta.get("trading_epoch_id") or "")
                position_portfolio = str(position.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or "")
                position_cycle = str(position.get("position_cycle_id") or meta.get("position_cycle_id") or lifecycle)
                if (account != get_account_key(env="practice") or position_epoch != str(epoch)
                        or not position_portfolio or position_cycle != lifecycle):
                    raise StopObligationUnavailable("STOP_OBLIGATION_ORIGINAL_SCOPE_MISMATCH")
                portfolio_scope = current_us_portfolio_epoch(
                    conn, env="practice", account_key=account,
                    trading_epoch_id=str(epoch), strategy_owner="US_STANDARD",
                )
                if portfolio_scope != position_portfolio:
                    raise StopObligationUnavailable("STOP_OBLIGATION_PORTFOLIO_EPOCH_MISMATCH")
            sql = """
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
            """
            params = {"epoch":epoch,"account":account,"symbol":symbol,"lifecycle":lifecycle,
                      "family":exit_type,"severity":severity,"qty":int(target_qty),
                      "price":signal_price,"pnl":pnl_pct,
                      "hard":float(policy.get("hard_stop",0.06)),
                      "soft":float(policy.get("soft_stop",0.03)),
                      "signal_at":signal_time}
            if strict_scope:
                sql = sql.replace(
                    "symbol, lifecycle_id,",
                    "symbol, lifecycle_id, portfolio_epoch_id, position_cycle_id,",
                ).replace(
                    ":symbol,:lifecycle,",
                    ":symbol,:lifecycle,:portfolio,:cycle,",
                ).replace(
                    "WHERE us_protective_stop_obligations.obligation_state<>'SATISFIED'",
                    "WHERE us_protective_stop_obligations.obligation_state<>'SATISFIED' "
                    "AND us_protective_stop_obligations.portfolio_epoch_id=:portfolio "
                    "AND us_protective_stop_obligations.position_cycle_id=:cycle",
                )
                params.update({"portfolio":portfolio_scope,"cycle":lifecycle})
            row = conn.execute(text(sql), params).mappings().first()
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
    from trader.account_state import get_account_key, resolve_env_name
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    if resolve_env_name() != "practice":
        raise StopObligationUnavailable("STOP_OBLIGATION_ACCOUNT_ENV_MISMATCH")
    account = str(get_account_key(env="practice") or "").strip()
    if not account:
        raise StopObligationUnavailable("STOP_OBLIGATION_ACCOUNT_UNVERIFIED")
    engine=_get_engine_or_none()
    if engine is None:
        raise StopObligationUnavailable("STOP_OBLIGATION_DATABASE_UNAVAILABLE")
    with engine.connect() as conn:
        epoch=_active_us_epoch(conn)
        if not epoch:
            raise StopObligationUnavailable("STOP_OBLIGATION_EPOCH_MISSING")
        # Unresolved stop debt is ACCOUNT+LIFECYCLE durable, not today-Epoch
        # durable. An epoch rollover must not hide yesterday's stop loss.
        # Hard cap and fail closed rather than truncate some protective debts.
        select_scope = (
            ",portfolio_epoch_id,position_cycle_id"
            if os.getenv("WSL_RUN_MARKET", "").upper() == "US" else ""
        )
        rows=conn.execute(text("""
            SELECT trading_epoch_id,account_id,symbol,lifecycle_id"""
            + select_scope + """,
                   stop_family,severity,target_qty,
                   confirmed_filled_qty,obligation_state,first_signal_at,last_signal_at,
                   soft_stop_pct,hard_stop_pct
            FROM us_protective_stop_obligations
            WHERE account_id=:account AND env='practice'
              AND strategy_owner='US_STANDARD'
              AND obligation_state<>'SATISFIED'
            ORDER BY severity DESC,first_signal_at
            LIMIT 501
        """),{"account":account}).mappings().all()
    if len(rows)>500:
        raise StopObligationUnavailable("STOP_OBLIGATION_INCOMPLETE_EPOCH_SCAN")
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
    from trader.db.trading_epoch import trading_epoch_enforced

    require_epoch = trading_epoch_enforced()
    by_scope = {}
    # Only for a pending durable high-severity stop on one of the nine legacy
    # holdings: re-prove the ORIGINAL V2 BUY from the ACTIVE account/epoch DB
    # and map the snapshot to the SAME prospective cycle used on durable
    # stop creation. Never infer a cycle from a ticker or old NULL scope.
    # This is DB-only preparation, NOT authorization of broker execution.
    legacy_pending_symbols = {
        str(debt.get("symbol") or "").upper() for debt in pending
        if int(debt.get("severity") or 0) >= 2
        and str(debt.get("obligation_state") or "").upper() in {
            "PENDING", "BLOCKED", "RECONCILE_REQUIRED",
            "SUBMITTED", "PARTIAL", "ESCALATED",
        }
    }
    for snapshot in snapshots:
        original_symbol = str(snapshot.get("symbol") or "").upper()
        if (original_symbol in legacy_pending_symbols
                and os.getenv("WSL_RUN_MARKET", "").upper() == "US"
                and os.getenv("KIS_ENV") == "practice"):
            try:
                from zoneinfo import ZoneInfo
                from trader.us.execution.us_legacy_nine_active_sell import (
                    eligible_standard_symbol,
                    enrich_legacy_nine_v2_decision_position,
                    build_verified_legacy_stop_obligation_position,
                )
                if eligible_standard_symbol(original_symbol):
                    observed = now or datetime.now(timezone.utc)
                    if observed.tzinfo is None:
                        observed = observed.replace(tzinfo=timezone.utc)
                    us_trade_day = observed.astimezone(
                        ZoneInfo("America/New_York")
                    ).date().isoformat()
                    legacy = enrich_legacy_nine_v2_decision_position(
                        snapshot, trade_date=us_trade_day,
                    )
                    if legacy.get("legacy_nine_decision_only_provisional") is True:
                        snapshot = build_verified_legacy_stop_obligation_position(
                            legacy, trade_date=us_trade_day,
                        )
            except Exception as exc:
                # Failed DB proof may never fall back to a foreign account,
                # stale trading epoch or an invented KIS terminal status.
                logger.warning(
                    "[US_PROTECTIVE_STOP][LEGACY_REPLAY_PROOF_FENCED] "
                    "symbol=%s error_type=%s",
                    original_symbol, type(exc).__name__,
                )
        symbol, life, account = scope_for_position(snapshot)
        meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
        position_epoch = str(snapshot.get("trading_epoch_id") or meta.get("trading_epoch_id") or "")
        portfolio_epoch = str(snapshot.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or "")
        position_cycle = str(snapshot.get("position_cycle_id") or meta.get("position_cycle_id") or life)
        # Missing portfolio evidence is not permission to adopt a prior cycle.
        if (symbol and life and account and position_cycle == life
                and (position_epoch or not require_epoch)
                and (portfolio_epoch or not require_epoch)):
            by_scope[(symbol,life,account,position_epoch,portfolio_epoch)] = snapshot

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
        key=(symbol,str(debt.get("lifecycle_id") or ""),str(debt.get("account_id") or ""),
             str(debt.get("trading_epoch_id") or ""),
             str(debt.get("portfolio_epoch_id") or ""))
        source_cycle = str(debt.get("position_cycle_id") or "")
        if (require_epoch and (not key[3] or not key[4] or source_cycle != key[1])):
            continue
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
                        "stop_obligation_first_signal_at":str(debt.get("first_signal_at") or ""),
                        # Route legacy replays through the same DB+fresh KIS
                        # proof as natural strategy signals, even though this
                        # debt already has a complete management scope.
                        **({"legacy_nine_decision_only_provisional": True}
                           if pos.get("legacy_nine_decision_only_provisional") is True else {}),
                        **({"trading_epoch_id":key[3],
                            "portfolio_epoch_id":key[4],
                            "position_cycle_id":key[1]} if require_epoch else {})},
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
    import hashlib
    if not validate_terminal_broker_proof(debt,proof):
        raise StopObligationUnavailable("STOP_OBLIGATION_TERMINAL_PROOF_UNVERIFIED")
    # Keep the materialized confirmed quantity consistent with the exact
    # original KIS full-stop fills that the Stage4 writer has already matched
    # to the immutable claim, order number, lifecycle, account and epoch.
    # Old TP-only settlement proofs do not imply a stop execution.
    confirmed_stop_fill_qty = 0
    unique_stop_orders: set[tuple[str, str]] = set()
    original_orders = proof.get("terminal_orders")
    if isinstance(original_orders, list):
        for original in original_orders:
            if not isinstance(original, dict):
                raise StopObligationUnavailable("STOP_OBLIGATION_ORIGINAL_EVIDENCE_INVALID")
            stop_type = str(original.get("stop_type") or "").lower()
            if stop_type not in {
                "hard_stop_full_exit", "hard_stop_loss",
                "persistent_soft_stop_full_exit",
            }:
                continue
            try:
                filled = int(original["filled_qty"])
                open_qty = int(original["broker_open_qty"])
            except (ValueError, TypeError, KeyError) as exc:
                raise StopObligationUnavailable("STOP_OBLIGATION_STOP_FILL_INVALID") from exc
            status = str(original.get("status") or "").upper()
            if (filled < 0 or open_qty != 0
                    or status not in {"FILLED", "CANCELLED", "CANCELED"}):
                raise StopObligationUnavailable("STOP_OBLIGATION_STOP_FILL_UNVERIFIED")
            # A KIS-proven zero-fill cancellation is terminal but contributes
            # zero actual executions. Never convert it into a synthetic fill.
            if filled == 0:
                continue
            unique = (str(original.get("trade_date") or ""),
                      str(original.get("order_no") or "").lstrip("0"))
            if not all(unique) or unique in unique_stop_orders:
                raise StopObligationUnavailable("STOP_OBLIGATION_STOP_ORDER_DUPLICATED")
            unique_stop_orders.add(unique)
            confirmed_stop_fill_qty += filled

    from sqlalchemy import text
    from trader.us.db.repos import _get_engine_or_none, _active_us_epoch
    engine=_get_engine_or_none()
    if engine is None:
        raise StopObligationUnavailable("STOP_OBLIGATION_DATABASE_UNAVAILABLE")
    with engine.begin() as conn:
        epoch=_active_us_epoch(conn)
        if not epoch:
            raise StopObligationUnavailable("STOP_OBLIGATION_ACTIVE_EPOCH_UNAVAILABLE")
        from trader.account_state import get_account_key, resolve_env_name
        if (resolve_env_name() != "practice"
                or str(get_account_key(env="practice")) != str(debt["account_id"])):
            raise StopObligationUnavailable("STOP_OBLIGATION_ACCOUNT_SCOPE_CHANGED")
        # Original Epoch is part of the durable debt. It is expected to differ
        # after an AM/Afternoon or account-generation rollover; terminal proof
        # still must match that exact original epoch.
        debt_epoch=str(debt["trading_epoch_id"])
        result=conn.execute(text("""
            UPDATE us_protective_stop_obligations o
               SET obligation_state='SATISFIED',
                   confirmed_filled_qty=GREATEST(
                       COALESCE(o.confirmed_filled_qty,0),
                       LEAST(o.target_qty,:confirmed_stop_fill_qty)
                   ),
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
                      AND c.env=o.env AND c.account_id=:account_claim
                      AND c.market='US'
                      AND c.strategy_owner=o.strategy_owner
                      AND c.lifecycle_id=o.lifecycle_id
                      AND c.action_state IN ('IN_FLIGHT','UNCERTAIN','PARTIALLY_SATISFIED')
               )
            RETURNING o.lifecycle_id
        """),{
            "proof":json.dumps(proof,sort_keys=True,default=str),
            "confirmed_stop_fill_qty":confirmed_stop_fill_qty,
            "epoch":debt_epoch,"account":debt["account_id"],
            "account_claim":hashlib.sha256(str(debt["account_id"]).encode("utf-8")).hexdigest(),
            "symbol":debt["symbol"],"lifecycle":debt["lifecycle_id"],
        })
        committed=result.first() is not None
    if not committed:
        logger.warning("[US_PROTECTIVE_STOP][TERMINAL_BLOCKED] symbol=%s lifecycle=%s action=retain_debt",
                       debt.get("symbol"),debt.get("lifecycle_id"))
    return committed
