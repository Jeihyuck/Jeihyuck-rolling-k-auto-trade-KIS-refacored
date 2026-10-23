"""Stage4 practice-only authoritative broker reconciliation process.

Owns historical replay, broker fills persistence and ACK reconciliation while
the main trading tick is in WORKER mode. Its account-wide PostgreSQL advisory
lock excludes a second Stage4 writer; runner-side startup readiness and SQL
health fencing must be checked before a trading tick trusts its evidence.
No BUY, SELL, cancel, replace, or broker submit call occurs in this module.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

logger=logging.getLogger(__name__)
_STOP=False


def _stop(_signum,_frame):
    global _STOP
    _STOP=True


def advisory_key(scope: dict) -> int:
    payload="stage4:writer:"+str(scope["env"])+":"+str(scope["account_id"])
    raw=int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8],"big")
    return raw-(1<<64) if raw>=(1<<63) else raw


def read_worker_health(*, env: str="practice", max_age_seconds: float=30.) -> dict:
    """Fail closed on stale, wrong-account, wrong-epoch or unready writer."""
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    scope=_us_execution_claim_scope()
    if env!="practice" or scope.get("env")!="practice":
        raise RuntimeError("US_STAGE4_WRITER_PRACTICE_ONLY")
    engine=_get_engine_or_none()
    if engine is None:
        raise RuntimeError("US_STAGE4_WRITER_DATABASE_UNAVAILABLE")
    with engine.connect() as conn:
        row=conn.execute(text("""
            SELECT evidence_at,worker_state,reconcile_status,replay_status,
                   pending_count,unresolved_count,filled_rows,writer_generation
            FROM us_stage4_reconcile_health
            WHERE env=:env AND account_fingerprint=:account AND trading_epoch_id=:epoch
        """),{"env":scope["env"],"account":scope["account_id"],
              "epoch":scope["trading_epoch_id"]}).mappings().first()
    if not row or str(row["worker_state"])!="READY":
        raise RuntimeError("US_STAGE4_WRITER_NOT_READY")
    at=row.get("evidence_at")
    if not isinstance(at,datetime) or at.tzinfo is None:
        raise RuntimeError("US_STAGE4_WRITER_TIMESTAMP_UNVERIFIED")
    lag=(datetime.now(timezone.utc)-at).total_seconds()
    if lag<0 or lag>max_age_seconds:
        raise RuntimeError("US_STAGE4_WRITER_STALE")
    return dict(row)


def run_reconcile_once(*, provider: Any, trade_date: str,
                       session_run_id: str, writer_generation: str,
                       conn: Any, scope: dict) -> dict:
    """One serial authoritative reconciliation cycle, no order submissions."""
    from trader.us.execution.fills import get_fills_today
    from trader.us.db.repos import save_fills_with_result
    from trader.us.execution.order_journal import replay_order_journal
    from trader.us.execution.reconcile import reconcile_ack_orders_with_balance
    fills=get_fills_today(provider=provider,signal_only=False,trade_date=trade_date)
    if fills.get("status")!="OK":
        raise RuntimeError("US_STAGE4_KIS_FILLS_UNVERIFIED")
    save=save_fills_with_result(fills.get("fills") or [],trade_date=trade_date)
    if save.get("status")!="OK":
        raise RuntimeError("US_STAGE4_FILLS_PERSIST_UNVERIFIED")
    replay=replay_order_journal(
        trade_date,session_run_id=session_run_id,
        tick_id="stage4-"+writer_generation,
        provider=provider,include_active_claims=True,
        active_claims_only=True,
    )
    ack=reconcile_ack_orders_with_balance(
        provider=provider,trade_date=trade_date,env="practice",
    )
    # Failed or partial historical recovery must remain visible as an unsafe
    # observation; an HTTP request returning is not an order terminal proof.
    replay_status=str(replay.get("status") or "ERROR").upper()
    ack_status=str(ack.get("status") or "ERROR").upper()
    if (replay_status not in {"OK","RECOVERED","NO_ACTIVE_CLAIMS"}
            or ack_status not in {"OK","WARN"}):
        raise RuntimeError("US_STAGE4_BROKER_RECOVERY_NOT_VERIFIED")
    return {
        "reconcile_status":ack_status,
        "replay_status":replay_status,
        "pending_count":int(ack.get("pending_count") or 0),
        "unresolved_count":max(
            int(ack.get("unresolved_count") or 0),
            int(replay.get("unresolved_count") or 0),
        ),
        "filled_rows":len(fills.get("fills") or []),
    }


def publish_health(conn: Any, *, scope: dict, generation: str,
                   state: str, result: dict | None=None) -> None:
    result=result or {}
    conn.execute(text("""
        INSERT INTO us_stage4_reconcile_health
        (env,account_fingerprint,trading_epoch_id,writer_generation,worker_state,
         evidence_at,reconcile_status,replay_status,pending_count,
         unresolved_count,filled_rows,updated_at)
        VALUES (:env,:account,:epoch,:generation,:state,
                CASE WHEN :state='READY' THEN NOW() ELSE NULL END,
                :reconcile,:replay,:pending,:unresolved,:fills,NOW())
        ON CONFLICT (env,account_fingerprint,trading_epoch_id)
        DO UPDATE SET
            writer_generation=EXCLUDED.writer_generation,
            worker_state=EXCLUDED.worker_state,
            evidence_at=EXCLUDED.evidence_at,
            reconcile_status=EXCLUDED.reconcile_status,
            replay_status=EXCLUDED.replay_status,
            pending_count=EXCLUDED.pending_count,
            unresolved_count=EXCLUDED.unresolved_count,
            filled_rows=EXCLUDED.filled_rows,
            updated_at=NOW()
    """),{"env":scope["env"],"account":scope["account_id"],
          "epoch":scope["trading_epoch_id"],"generation":generation,
          "state":state,"reconcile":result.get("reconcile_status","ERROR"),
          "replay":result.get("replay_status","ERROR"),
          "pending":int(result.get("pending_count") or 0),
          "unresolved":int(result.get("unresolved_count") or 1),
          "fills":int(result.get("filled_rows") or 0)})


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--trade-date",required=True)
    parser.add_argument("--session-run-id",required=True)
    parser.add_argument("--env",default="practice")
    parser.add_argument("--interval-sec",type=int,default=10)
    args=parser.parse_args()
    if (args.env!="practice"
            or os.getenv("KIS_ENV","practice").lower()!="practice"
            or os.getenv("US_RECONCILE_WORKER_MODE")!="WORKER"
            or os.getenv("US_STAGE4_WRITER_AUTHORIZED")!="1"
            or os.getenv("US_SHARED_KIS_LIMITER_ENABLED")!="1"
            or os.getenv("US_ACCOUNT_RESERVATION_MODE")!="ENFORCE"
            or os.getenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED")!="1"):
        raise RuntimeError("US_STAGE4_WORKER_NOT_AUTHORIZED")
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    from trader.us.data_provider import USDataProvider
    engine=_get_engine_or_none()
    scope=_us_execution_claim_scope()
    if engine is None or scope["env"]!="practice" or not scope["trading_epoch_id"]:
        raise RuntimeError("US_STAGE4_WORKER_SCOPE_UNVERIFIED")
    signal.signal(signal.SIGTERM,_stop)
    signal.signal(signal.SIGINT,_stop)
    generation=str(uuid.uuid4())
    # Hold only the dedicated one-writer advisory lock. It is distinct from
    # the short-lived trade reservation leases, so it does not hold KIS HTTP.
    with engine.connect() as lease:
        granted=lease.execute(text("SELECT pg_try_advisory_lock(:key)"),
                              {"key":advisory_key(scope)}).scalar_one()
        if not granted:
            raise RuntimeError("US_STAGE4_DUPLICATE_WRITER")
        try:
            provider=USDataProvider(offline=False,env="practice")
            while not _STOP:
                started=time.monotonic()
                try:
                    result=run_reconcile_once(
                        provider=provider,trade_date=args.trade_date,
                        session_run_id=args.session_run_id,
                        writer_generation=generation,conn=lease,scope=scope,
                    )
                    with engine.begin() as conn:
                        publish_health(conn,scope=scope,generation=generation,
                                       state="READY",result=result)
                except Exception as exc:
                    logger.exception("[US_STAGE4][RECONCILE_FAILURE] type=%s",
                                     type(exc).__name__)
                    with engine.begin() as conn:
                        publish_health(conn,scope=scope,generation=generation,
                                       state="FAILED")
                wait=max(0.0,max(5,args.interval_sec)-(time.monotonic()-started))
                if wait:
                    time.sleep(wait)
        finally:
            with engine.begin() as conn:
                publish_health(conn,scope=scope,generation=generation,state="STOPPED")
            lease.execute(text("SELECT pg_advisory_unlock(:key)"),
                          {"key":advisory_key(scope)})
    return 0


if __name__=="__main__":
    raise SystemExit(main())
