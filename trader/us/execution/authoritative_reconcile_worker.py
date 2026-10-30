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
_RECOVERY_CURSOR=0
_STAGE4_FILL_MEMOS: dict[str, Any] = {}


def recover_stop_tp_conflicts_once(*, provider: Any,trade_date: str,
                                   max_candidates: int=3) -> dict:
    """Independent TP repair: original-date READ-ONLY query by default.

    Cancel requests require both the existing cancel gate and explicit
    operator authorization. A local OPEN row is NEVER actionable proof.
    """
    global _RECOVERY_CURSOR
    from trader.us.protective_stop_obligation import load_pending_stop_obligations
    from trader.us.execution.protective_stop_recovery_worker import recovery_candidates
    from trader.us.protective_tp_recovery import request_protective_tp_cancel
    debts=load_pending_stop_obligations(env="practice")
    candidates=recovery_candidates(debts)
    if not candidates:
        return {"checked":0,"total":0,"cancel_authorized":False}
    limit=min(4,max(1,int(max_candidates)))
    selected=[
        candidates[(_RECOVERY_CURSOR+j)%len(candidates)]
        for j in range(min(limit,len(candidates)))
    ]
    _RECOVERY_CURSOR=(_RECOVERY_CURSOR+len(selected))%len(candidates)
    cancel_authorized=(
        os.getenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED","0")=="1"
        and os.getenv("US_STOP_RECOVERY_AUTHORIZED","0")=="1"
    )
    class _NoOrderSubmit:
        def cancel_us_order(self,**kwargs):
            raise RuntimeError("US_STAGE4_READ_ONLY_CANCEL_NOT_AUTHORIZED")
    # For non-authorized runs, the cancel reservation cannot be acquired;
    # terminal historical orders can still be authoritatively reconciled.
    results=request_protective_tp_cancel(
        intents=selected,provider=provider,
        kis_client=provider._get_client() if cancel_authorized else _NoOrderSubmit(),
        trade_date=trade_date,env="practice",
        **({} if cancel_authorized else {
            "reserve":lambda order:False,
            "read_only_terminal_reconcile":True,
        }),
    )
    return {"checked":len(selected),"total":len(candidates),
            "cancel_authorized":cancel_authorized,
            "statuses":[str(x.get("status") or "") for x in results]}




def _stop(_signum,_frame):
    global _STOP
    _STOP=True


def advisory_key(scope: dict) -> int:
    payload="stage4:writer:"+str(scope["env"])+":"+str(scope["account_id"])
    raw=int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8],"big")
    return raw-(1<<64) if raw>=(1<<63) else raw


def stage4_process_start_ticks(pid: int) -> str:
    """Read Linux process birth identity; PID alone may be reused after a crash."""
    if int(pid) <= 0:
        raise RuntimeError("US_STAGE4_WORKER_PROCESS_DEAD")
    try:
        with open(f"/proc/{int(pid)}/stat", encoding="utf-8") as fh:
            stat = fh.read()
        # The command in (...) can contain spaces/parentheses; split AFTER
        # the final ')'. Linux /proc stat field 22 is offset 19 from state.
        rest = stat.rsplit(") ", 1)[1].split()
        if len(rest) <= 19 or rest[0] in {"Z", "X", "x"}:
            raise RuntimeError("US_STAGE4_WORKER_PROCESS_DEAD")
        return rest[19]
    except (OSError, ValueError, IndexError) as exc:
        raise RuntimeError("US_STAGE4_WORKER_PROCESS_DEAD") from exc


def assert_stage4_worker_process_alive() -> None:
    """Fail closed in a Tick child *and* at the KIS pre-HTTP boundary.

    The parent pins the exact PID+birth timestamp before Tick spawning.
    This check does not trust the forked child's inherited ALIVE=1 flag.
    """
    if os.getenv("US_RECONCILE_WORKER_MODE") != "WORKER":
        return
    if os.getenv("US_STAGE4_PROCESS_PROOF_REQUIRED") != "1":
        # Backward-compatible standalone tests/non-session evidence readers.
        return
    try:
        pid = int(os.environ["US_STAGE4_WORKER_PID"])
        expected = os.environ["US_STAGE4_WORKER_START_TICKS"]
    except (KeyError, ValueError) as exc:
        raise RuntimeError("US_STAGE4_WORKER_PROCESS_IDENTITY_MISSING") from exc
    if not expected or stage4_process_start_ticks(pid) != expected:
        raise RuntimeError("US_STAGE4_WORKER_PROCESS_DEAD")


def read_worker_health(*, env: str="practice", max_age_seconds: float=30.) -> dict:
    """Fail closed on stale, wrong-account, wrong-epoch or unready writer."""
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    scope=_us_execution_claim_scope()
    assert_stage4_worker_process_alive()
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
    if os.getenv("US_STAGE4_WORKER_ALIVE") == "0":
        raise RuntimeError("US_STAGE4_WORKER_PROCESS_DEAD")
    if not row or str(row["worker_state"])!="READY":
        raise RuntimeError("US_STAGE4_WRITER_NOT_READY")
    expected_generation=os.getenv("US_STAGE4_EXPECTED_GENERATION")
    if os.getenv("US_RECONCILE_WORKER_MODE")=="WORKER":
        if (os.getenv("US_STAGE4_WRITER_AUTHORIZED")!="1"
                or os.getenv("US_PROTECTIVE_STOP_LEDGER_MODE")!="ENFORCE"):
            raise RuntimeError("US_STAGE4_WRITER_POLICY_NOT_ARMED")
    if (os.getenv("US_RECONCILE_WORKER_MODE")=="WORKER"
            and not expected_generation):
        raise RuntimeError("US_STAGE4_WORKER_GENERATION_NOT_CONFIGURED")
    # PostgreSQL UUID columns render with hyphens; older session runners
    # supplied UUID.hex without them. Compare UUID identity, not formatting.
    # Malformed or missing DB evidence must fail closed.
    if expected_generation:
        try:
            matches = uuid.UUID(str(row["writer_generation"])) == uuid.UUID(str(expected_generation))
        except (ValueError, TypeError, AttributeError):
            matches = False
        if not matches:
            raise RuntimeError("US_STAGE4_WRITER_GENERATION_MISMATCH")
    at=row.get("evidence_at")
    if not isinstance(at,datetime) or at.tzinfo is None:
        raise RuntimeError("US_STAGE4_WRITER_TIMESTAMP_UNVERIFIED")
    lag=(datetime.now(timezone.utc)-at).total_seconds()
    if lag<0 or lag>max_age_seconds:
        raise RuntimeError("US_STAGE4_WRITER_STALE")
    # The worker may die during the DB query; check the real PID again.
    assert_stage4_worker_process_alive()
    return dict(row)




def confirm_stage4_health_row_absent(*, expected_generation: str,
                                     env: str = "practice") -> bool:
    """Allow fresh writer after OS reaping when predecessor died before READY.

    An existing row (even STOPPED) or a DB error is NOT proof of absence.
    Call only *after* the old subprocess has been reaped and could not be
    generation-marked STOPPED. Never clears existing health evidence.
    """
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    scope = _us_execution_claim_scope()
    if env != "practice" or scope.get("env") != env:
        raise RuntimeError("US_STAGE4_RESTART_PRACTICE_ONLY")
    try:
        uuid.UUID(str(expected_generation))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("US_STAGE4_RESTART_GENERATION_UNVERIFIED") from exc
    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("US_STAGE4_RESTART_DATABASE_UNAVAILABLE")
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT writer_generation FROM us_stage4_reconcile_health
             WHERE env=:env AND account_fingerprint=:account
               AND trading_epoch_id=:epoch
        """), {"env":scope["env"], "account":scope["account_id"],
               "epoch":scope["trading_epoch_id"]}).mappings().first()
    return row is None


def mark_stopped_after_confirmed_exit(*, expected_generation: str, env: str = "practice") -> bool:
    """Mark only this account/epoch/generation STOPPED after the OS reaps its writer.

    A different/new writer is never modified. This is process-liveness evidence,
    not a statement that any broker order was filled, cancelled, or reconciled.
    """
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope

    scope = _us_execution_claim_scope()
    if env != "practice" or scope.get("env") != "practice":
        raise RuntimeError("US_STAGE4_WRITER_PRACTICE_ONLY")
    if not scope.get("account_id") or not scope.get("trading_epoch_id"):
        raise RuntimeError("US_STAGE4_WRITER_SCOPE_UNVERIFIED")
    try:
        generation = str(uuid.UUID(str(expected_generation)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise RuntimeError("US_STAGE4_WORKER_GENERATION_NOT_CONFIGURED") from exc
    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("US_STAGE4_WRITER_DATABASE_UNAVAILABLE")
    with engine.begin() as conn:
        updated = conn.execute(text("""
            UPDATE us_stage4_reconcile_health
            SET worker_state='STOPPED', evidence_at=NULL, updated_at=NOW()
            WHERE env=:env AND account_fingerprint=:account
              AND trading_epoch_id=:epoch
              AND writer_generation=CAST(:generation AS uuid)
        """), {
            "env": scope["env"],
            "account": scope["account_id"],
            "epoch": scope["trading_epoch_id"],
            "generation": generation,
        })
        return updated.rowcount == 1


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
    # Every cycle still fetches current authoritative KIS evidence. Skip only
    # identical, *previously persisted* cumulative fills for a bounded period.
    # This removes the repeated 18-22s DB write observed on 2026-10-27.
    # A changed quantity/price, failed save, worker restart or TTL expiry
    # always forces the original full persistence/reconciliation path.
    from trader.us.execution.stage4_fill_memo import VerifiedFillPersistenceMemo
    memo = _STAGE4_FILL_MEMOS.setdefault(trade_date, VerifiedFillPersistenceMemo())
    broker_fills = fills.get("fills") or []
    if memo.should_persist(broker_fills):
        save = save_fills_with_result(broker_fills, trade_date=trade_date)
        memo.mark_persisted(broker_fills, status=save.get("status", ""))
        if save.get("status") != "OK":
            raise RuntimeError("US_STAGE4_FILLS_PERSIST_UNVERIFIED")
    else:
        logger.info("[US_STAGE4][FILL_SAVE_MEMO_HIT] count=%d verified_prior_save=1", len(broker_fills))
    # Partial SOFT stops and trend stage acknowledgments are strategy-state
    # transitions owned by this writer when the Tick no longer writes fills.
    from trader.us.runner.trade_tick_runner import (
        _mark_trend_stages_from_records,_mark_soft_stop_stages_from_records,
    )
    if fills.get("fills"):
        _mark_trend_stages_from_records(
            fills["fills"],trade_date=trade_date,status="FILLED",
        )
        _mark_soft_stop_stages_from_records(
            fills["fills"],trade_date=trade_date,status="FILLED",
        )
    replay=replay_order_journal(
        trade_date,session_run_id=session_run_id,
        tick_id="stage4-"+writer_generation,
        provider=provider,include_active_claims=True,
        active_claims_only=True,
    )
    ack=reconcile_ack_orders_with_balance(
        provider=provider,trade_date=trade_date,env="practice",
    )
    _mark_trend_stages_from_records(ack,trade_date=trade_date,status="FILLED")
    _mark_soft_stop_stages_from_records(ack,trade_date=trade_date,status="FILLED")
    # This writer also owns previously verified account reservation cleanups;
    # the RESERVATION_ONLY child must not run in WORKER mode.
    from trader.us.execution.reservation_reconcile_worker import (
        reconcile_proven_reservations_once,
    )
    reservation=reconcile_proven_reservations_once(
        env="practice",trade_date=trade_date,
    )
    if int(reservation.get("unresolved") or 0)>0:
        logger.warning("[US_STAGE4][RESERVATION_DEFERRED] unresolved=%s",
                       reservation.get("unresolved"))
    # Failed or partial historical recovery must remain visible as an unsafe
    # observation; an HTTP request returning is not an order terminal proof.
    replay_status=str(replay.get("status") or "ERROR").upper()
    ack_status=str(ack.get("status") or "ERROR").upper()
    if (replay_status not in {"OK","RECOVERED","NO_ACTIVE_CLAIMS","WARN","PARTIAL","UNRESOLVED"}
            or ack_status not in {"OK","WARN"}):
        raise RuntimeError("US_STAGE4_BROKER_RECOVERY_NOT_VERIFIED")
    # Publish fresh critical-path proof first. Expensive original-date TP
    # lookups and stop terminal cleanup are after that publication and can
    # never delay this cycle's broker-evidence handoff to a clean SELL.
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


def run_deferred_recovery_once(*, provider: Any,trade_date: str) -> dict:
    """Only this authoritative process writes terminal claims.

    Deferred KIS original-date queries do not block publishing the already
    persisted Fill/ACK evidence. A slow deferred phase can still expire
    the ready heartbeat, which safely fences later orders until refreshed.
    """
    # A zero-debt session must not repeatedly fetch the entire KIS balance
    # or scan historical TP dates on the writer's critical reconciliation
    # cadence. An actual stop signal persists a debt before suppression.
    from trader.us.protective_stop_obligation import load_pending_stop_obligations
    if not load_pending_stop_obligations(env="practice"):
        return {"tp":{"checked":0,"total":0,"cancel_authorized":False},
                "settlement":{"checked":0,"settled":0,"blocked":0,"total_pending":0}}
    tp=recover_stop_tp_conflicts_once(provider=provider,trade_date=trade_date)
    from trader.us.execution.protective_stop_settlement_worker import (
        settle_verified_stop_debts_once,
    )
    settled=settle_verified_stop_debts_once(
        provider=provider,trade_date=trade_date,limit=2,
    )
    return {"tp":tp,"settlement":settled}


def publish_health(conn: Any, *, scope: dict, generation: str,
                   state: str, result: dict | None=None) -> None:
    # Only READY supplies fresh reconciliation numbers.  FAILED/STOPPED are
    # liveness transitions, not new KIS evidence: preserve the last verified
    # pending/unresolved/fill counters and never invent ERROR/1 on shutdown.
    # The generation predicate also prevents an old writer's finally block
    # from overwriting a newer writer's READY row.
    if state in {"FAILED", "STOPPED"}:
        conn.execute(text("""
            UPDATE us_stage4_reconcile_health
               SET worker_state=:state, evidence_at=NULL, updated_at=NOW()
             WHERE env=:env AND account_fingerprint=:account
               AND trading_epoch_id=:epoch
               AND writer_generation=CAST(:generation AS uuid)
        """), {"env":scope["env"], "account":scope["account_id"],
               "epoch":scope["trading_epoch_id"], "generation":generation,
               "state":state})
        return
    if state != "READY":
        raise ValueError("US_STAGE4_HEALTH_STATE_UNSUPPORTED")
    result=result or {}
    conn.execute(text("""
        INSERT INTO us_stage4_reconcile_health
        (env,account_fingerprint,trading_epoch_id,writer_generation,worker_state,
         evidence_at,reconcile_status,replay_status,pending_count,
         unresolved_count,filled_rows,updated_at)
        VALUES (:env,:account,:epoch,CAST(:generation AS uuid),:state,
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
          "unresolved":int(result["unresolved_count"]) if "unresolved_count" in result else 1,
          "fills":int(result.get("filled_rows") or 0)})


def main() -> int:
    # The runner redirects this subprocess stderr to a mode-0600 WSL log.
    # Without a handler, INFO readiness and deferred-repair trace is invisible.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
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
            or os.getenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED")!="1"
            or os.getenv("US_PROTECTIVE_STOP_LEDGER_MODE")!="ENFORCE"):
        raise RuntimeError("US_STAGE4_WORKER_NOT_AUTHORIZED")
    from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
    from trader.us.data_provider import USDataProvider
    engine=_get_engine_or_none()
    scope=_us_execution_claim_scope()
    if engine is None or scope["env"]!="practice" or not scope["trading_epoch_id"]:
        raise RuntimeError("US_STAGE4_WORKER_SCOPE_UNVERIFIED")
    signal.signal(signal.SIGTERM,_stop)
    signal.signal(signal.SIGINT,_stop)
    generation=str(os.getenv("US_STAGE4_EXPECTED_GENERATION") or uuid.uuid4().hex)
    # Hold only the dedicated one-writer advisory lock. It is distinct from
    # the short-lived trade reservation leases, so it does not hold KIS HTTP.
    with engine.connect() as lease:
        granted=lease.execute(text("SELECT pg_try_advisory_lock(:key)"),
                              {"key":advisory_key(scope)}).scalar_one()
        lease.commit()  # session advisory lock remains held, no idle DB TX
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
                    from trader.us.execution.reconcile_shadow_worker import (
                        observer_report_path,save_diagnostic_atomic,
                    )
                    save_diagnostic_atomic(
                        observer_report_path(args.trade_date,args.session_run_id),
                        {"sampled_at_utc":datetime.now(timezone.utc).isoformat(),
                         "read_status":"OK","worker_state":"READY",
                         "writer_generation":generation,
                         "pending_order_count":result["pending_count"],
                         "active_claim_count":result["unresolved_count"]},
                    )
                    # Slow historical broker inquiries are outside the main
                    # Tick and after the READY evidence publication.
                    try:
                        deferred=run_deferred_recovery_once(
                            provider=provider,trade_date=args.trade_date,
                        )
                        logger.info("[US_STAGE4][DEFERRED_RECOVERY] %s",deferred)
                    except Exception as recovery_exc:
                        # Keep the confirmed stop obligation and old TP fence.
                        # Do not falsify earlier *confirmed* ACK/Fill proof.
                        logger.exception("[US_STAGE4][DEFERRED_RECOVERY_FAILED] type=%s",
                                         type(recovery_exc).__name__)
                except Exception as exc:
                    logger.exception("[US_STAGE4][RECONCILE_FAILURE] type=%s",
                                     type(exc).__name__)
                    with engine.begin() as conn:
                        publish_health(conn,scope=scope,generation=generation,
                                       state="FAILED")
                wait=max(0.0,max(5,args.interval_sec)-(time.monotonic()-started))
                # Python sleep can resume after SIGTERM (PEP 475). Poll the
                # stop flag so normal shutdown reaches publish_health(STOPPED)
                # instead of waiting for the parent's five-second kill timer.
                sleep_deadline=time.monotonic()+wait
                while not _STOP:
                    remaining=sleep_deadline-time.monotonic()
                    if remaining<=0:
                        break
                    time.sleep(min(0.25,remaining))
        finally:
            with engine.begin() as conn:
                publish_health(conn,scope=scope,generation=generation,state="STOPPED")
            lease.execute(text("SELECT pg_advisory_unlock(:key)"),
                          {"key":advisory_key(scope)})
            lease.commit()
    return 0


if __name__=="__main__":
    raise SystemExit(main())
