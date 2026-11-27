"""AM/Close-only TP repair from durable proof of an early router abort.

No absence-of-order inference. A historical status string is not a certificate.
Never asks KIS to submit/cancel, creates a claim, or changes orders/fills.
"""
from __future__ import annotations

import logging
from uuid import uuid4

from sqlalchemy import text

logger = logging.getLogger(__name__)
CERTIFICATE = "ROUTER_ABORTED_BEFORE_CLAIM"


def record_router_abort(intent: dict, *, checkpoint: str) -> bool:
    """Called only on two returns preceding intent/claim/native submission."""
    meta = intent.get("meta") or {}
    symbol = str(intent.get("symbol") or "").upper()
    stage = str(meta.get("profit_capture_stage") or "").lower()
    owner = str(intent.get("strategy_owner") or meta.get("strategy_owner") or "US_STANDARD")
    runtime_cycle = str(intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or "")
    key = str(intent.get("client_order_key") or "")
    from trader.us.profit_capture import tp_stage_lifecycle_from_order_proof
    try:
        cycle = tp_stage_lifecycle_from_order_proof(runtime_cycle, meta, client_order_key=key)
    except ValueError:
        return False
    if (intent.get("side") != "SELL" or symbol == "TQQQ" or owner != "US_STANDARD"
            or stage not in {"tp1", "tp2", "tp3"} or not cycle
            or not key.startswith(f"US_PC_{intent.get('trade_date')}_{symbol}_{cycle}_TAKE_PROFIT_{stage.upper()}")
            or checkpoint not in {"semantic_lookup_failed", "original_scope_rejected"}):
        return False
    from trader.us.db.repos import _us_execution_claim_scope
    from trader.us.execution.order_journal import append_order_event
    try:
        scope = _us_execution_claim_scope()
        if scope.get("env") != "practice" or not scope.get("trading_epoch_id") or not scope.get("account_id"):
            return False
        proof_intent = {**intent, "strategy_owner": "US_STANDARD", "submit_attempt_id": str(uuid4()),
                        "broker_account_hash": scope["account_id"], "position_lifecycle_id": cycle,
                        "trading_epoch_id": scope["trading_epoch_id"],
                        "meta": {**meta, "router_abort_checkpoint": checkpoint,
                                 "router_abort_runtime_lifecycle_id": runtime_cycle}}
        append_order_event(CERTIFICATE, proof_intent, require_db=True)
        return True
    except Exception as exc:
        logger.warning("[US_TP_RETRY][CERTIFICATE_UNAVAILABLE] symbol=%s error=%s keep_fence=1",
                       symbol, type(exc).__name__)
        return False


def _release_certificate_row(conn, row: dict, scope: dict) -> bool:
    """The caller holds the exact TP row lock in the boundary transaction."""
    from trader.us.db.repos import mark_us_profit_capture_stage
    params = {"td": str(row["trade_date"]), "sym": row["symbol"], "stage": row["stage"],
              "cycle": row["position_lifecycle_id"], "key": row["client_order_key"],
              "epoch": scope["trading_epoch_id"], "account": scope["account_id"]}
    if (row["symbol"] == "TQQQ"
            or row.get("stage_status") not in {"NOT_TRIGGERED", "PENDING"}):
        return False
    events = conn.execute(text("""
        SELECT event_type, submit_attempt_id, account_id, position_lifecycle_id,
               canonical_broker_order_no, payload, trade_date, trading_epoch_id
          FROM us_order_events WHERE client_order_key=:key
    """), params).mappings().all()
    if not events or any(str(e.get("trading_epoch_id") or "") != params["epoch"]
                         or str(e.get("trade_date") or "") != params["td"] for e in events):
        return False
    if any(e.get("event_type") in {"BROKER_SUBMIT_ABORTED_PRE_IO", "BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY"}
           for e in events):
        # Existing claim-bearing aborts retain their stronger atomic checks.
        from trader.us.db.repos import release_us_profit_capture_pre_http_abort
        return release_us_profit_capture_pre_http_abort(
            trade_date=params["td"], symbol=params["sym"], stage=params["stage"],
            position_lifecycle_id=params["cycle"], client_order_key=params["key"],
            close_intent=True, _conn=conn,
        )
    # A pre-claim certificate cannot certify partial-fill progress; only the
    # claim-bearing path above may validate a proven remainder abort.
    if row.get("canonical_broker_order_no") or int(row.get("cumulative_filled_qty") or 0) != 0:
        return False
    for e in events:
        payload = e.get("payload") or {}
        if (e.get("event_type") != CERTIFICATE or not e.get("submit_attempt_id")
                or (e.get("account_id") or payload.get("broker_account_hash")) != params["account"]
                or e.get("position_lifecycle_id") != params["cycle"]
                or e.get("canonical_broker_order_no")
                or payload.get("symbol") != params["sym"] or payload.get("side") != "SELL"
                or payload.get("profit_capture_stage") != params["stage"]
                or payload.get("strategy_owner") != "US_STANDARD"
                or (payload.get("meta") or {}).get("router_abort_checkpoint")
                    not in {"semantic_lookup_failed", "original_scope_rejected"}):
            return False
    # Check all epochs/accounts for this globally unique client key. Unknown
    # or terminal broker evidence always wins over an early-router certificate.
    for table in ("us_execution_attempts", "us_orders", "us_fills"):
        if conn.execute(text(f"SELECT 1 FROM {table} WHERE client_order_key=:key LIMIT 1"), params).scalar_one_or_none() is not None:
            return False
    # An existing intent may be missing on a pre-intent abort. If present,
    # lock and verify every recorded source field before changing its state.
    intent_row = conn.execute(text("""
        SELECT trade_date,symbol,side,status,account_id,trading_epoch_id,qty,meta
          FROM us_order_intents WHERE client_order_key=:key FOR UPDATE
    """), params).mappings().one_or_none()
    if intent_row:
        im = intent_row.get("meta") or {}
        declared_account = intent_row.get("account_id") or im.get("broker_account_hash")
        declared_cycle = str(im.get("position_lifecycle_id") or "")
        if declared_cycle:
            from trader.us.profit_capture import tp_stage_lifecycle_from_order_proof
            try:
                declared_cycle = tp_stage_lifecycle_from_order_proof(
                    declared_cycle, im, client_order_key=params["key"])
            except ValueError:
                return False
        if (str(intent_row.get("trade_date") or "") != params["td"]
                or intent_row.get("symbol") != params["sym"] or intent_row.get("side") != "SELL"
                or intent_row.get("trading_epoch_id") != params["epoch"]
                or intent_row.get("status") not in {"PENDING", "BLOCKED"}
                or (declared_account and declared_account != params["account"])
                or (declared_cycle and declared_cycle != params["cycle"])
                or im.get("strategy_owner", "US_STANDARD") != "US_STANDARD"):
            return False
    # This closes a local intent only. Never invent a historical transaction.
    conn.execute(text("""
        UPDATE us_order_intents SET status='BLOCKED',
          meta=COALESCE(meta,'{}'::jsonb) || CAST(:abort_patch AS jsonb)
        WHERE client_order_key=:key AND trade_date=:td AND symbol=:sym AND side='SELL'
          AND trading_epoch_id=:epoch AND status='PENDING'
    """), {**params, "abort_patch": '{"router_abort_boundary_verified":true}'})
    mark_us_profit_capture_stage(params["td"], params["sym"], params["stage"],
        order_key=params["key"], position_lifecycle_id=params["cycle"],
        status="FAILED", evidence_type=CERTIFICATE, _conn=conn)
    verified = conn.execute(text("""
        SELECT state FROM us_profit_capture_lifecycle WHERE trade_date=:td
          AND symbol=:sym AND stage=:stage AND position_lifecycle_id=:cycle
          AND client_order_key=:key AND trading_epoch_id=:epoch
    """), params).scalar_one()
    meta = verified.get("meta") or {}
    if (verified.get(params["stage"] + "_pending") or meta.get("last_status") != "FAILED"
            or meta.get("last_evidence_type") != CERTIFICATE):
        raise RuntimeError("TP_ROUTER_ABORT_RELEASE_POSTCONDITION_FAILED")
    return True


def repair_tp_retries_at_boundary(*, trade_date: str, phase: str,
                                   budget_sec: float = 20.0, clock=None) -> dict:
    # Only AM_START / CLOSE. No historical retry loop in the trading tick.
    if str(phase).upper() not in {"AM", "AM_START", "CLOSE"}:
        return {"status": "SKIPPED_NOT_BOUNDARY", "released": 0}
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    from datetime import date
    import time
    clock = clock or time.monotonic
    result = {"status": "OK", "released": 0, "unverified": [], "checked": 0}
    try:
        deadline = clock() + max(0.1,min(30.0,float(budget_sec)))
        engine = _get_engine_or_none()
        scope = _us_execution_claim_scope()
        if (engine is None or scope.get("env") != "practice"
                or not scope.get("trading_epoch_id") or not scope.get("account_id")):
            raise RuntimeError("TP_BOUNDARY_DURABLE_SCOPE_UNAVAILABLE")
        # Read the candidate identities without locking the whole historical
        # population. Each repair owns exactly one short transaction.
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout='1000ms'"))
            conn.execute(text("SET LOCAL statement_timeout='5000ms'"))
            rows = [dict(row) for row in conn.execute(text("""
                SELECT * FROM us_profit_capture_lifecycle
                WHERE trade_date<=:td AND trading_epoch_id=:epoch
                  AND stage_status IN ('NOT_TRIGGERED','PENDING')
                  AND client_order_key IS NOT NULL AND client_order_key<>''
                  AND symbol<>'TQQQ'
                  AND (stage_status='PENDING' OR COALESCE(state->'meta'->>'last_evidence_type','')
                      NOT IN ('ROUTER_ABORTED_BEFORE_CLAIM','BROKER_SUBMIT_ABORTED_PRE_IO'))
                ORDER BY trade_date,symbol,stage
            """), {"td": trade_date, "epoch": scope["trading_epoch_id"]}).mappings().all()]
        result["candidate_count"] = len(rows)
        # Deterministic rotation on subsequent boundaries avoids starvation.
        if rows:
            offset=(date.fromisoformat(trade_date).toordinal()*2 + int(str(phase).upper()=="CLOSE")) % len(rows)
            rows=rows[offset:]+rows[:offset]
        for row in rows:
            detail={"symbol":row["symbol"],"stage":row["stage"],
                    "source_trade_date":str(row["trade_date"])}
            if clock() >= deadline:
                result["unverified"].append({**detail,"reason":"DEFERRED_BOUNDARY_BUDGET"})
                continue
            result["checked"] += 1
            try:
                with engine.begin() as conn:
                    conn.execute(text("SET LOCAL lock_timeout='1000ms'"))
                    conn.execute(text("SET LOCAL statement_timeout='5000ms'"))
                    if _us_execution_claim_scope() != scope:
                        raise RuntimeError("TP_BOUNDARY_SCOPE_CHANGED")
                    locked=conn.execute(text("""
                        SELECT * FROM us_profit_capture_lifecycle
                        WHERE trade_date=:td AND symbol=:sym AND stage=:stage
                          AND position_lifecycle_id=:cycle
                          AND client_order_key=:key AND trading_epoch_id=:epoch
                        FOR UPDATE
                    """), {"td":str(row["trade_date"]),"sym":row["symbol"],"stage":row["stage"],
                           "cycle":row["position_lifecycle_id"],"key":row["client_order_key"],
                           "epoch":scope["trading_epoch_id"]}).mappings().one_or_none()
                    if locked is None or dict(locked) != row:
                        released=False
                        reason="CANDIDATE_CHANGED_REPROVE_NEXT_BOUNDARY"
                    else:
                        released=_release_certificate_row(conn, dict(locked), scope)
                        reason="NO_DURABLE_UNSENT_CERTIFICATE"
                # Increment only after the transaction actually commits.
                if released:
                    result["released"] += 1
                else:
                    result["unverified"].append({**detail,"reason":reason})
            except Exception as exc:
                result["unverified"].append({**detail,"reason":"REPAIR_ROLLED_BACK:"+type(exc).__name__})
        if result["unverified"]:
            result["status"] = "RETRY_FENCES_RETAINED"
    except Exception as exc:
        result.update(status="UNAVAILABLE", error=type(exc).__name__)
    logger.warning("[US_TP_RETRY][BOUNDARY] phase=%s result=%s", phase, result)
    return result
