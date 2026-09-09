# -*- coding: utf-8 -*-
"""KR PB1 session-specific runner.

This module is intentionally thin: WSL session scripts call this KR/PB1 entrypoint
instead of the old all-in-one trader entrypoint. It enforces prep artifacts,
preopen handling, and KIS balance SAFE_STOP before handing live sessions to the
PB1 runner.
"""
from __future__ import annotations

# Legacy PB1 guards intentionally load only from the Korean execution path.
from trader import install_legacy_pb1_runtime_guards

install_legacy_pb1_runtime_guards()

import argparse
import json
import logging
import os
import sys
import time as time_mod
from dataclasses import dataclass, asdict
from datetime import datetime, time
from pathlib import Path
from typing import Any

from trader.kis_wrapper import KisAPI, KisBalanceUnavailable, resolve_kr_balance_fail_soft
from trader.execution_state import BalanceRecoveryState
from trader.kr.calendar import resolve_kr_expected_as_of, resolve_kr_trade_date
from trader.kr.artifacts import (
    validate_kr_prep_artifact,
    quarantine_stale_kr_artifacts,
    rescue_kr_final30_from_db,
)
from trader.kr.diagnostics import write_kr_diagnostics_manifest
from trader.kr.runner.session_policy import exit_code_for_result, kr_prep_schedule_guard, now_kst, wait_until_kr_am_target

logger = logging.getLogger(__name__)
_LAST_GOOD_BALANCE: tuple[dict[str, Any], datetime] | None = None

ROOT = Path(__file__).resolve().parents[3]


@dataclass
class KrSessionContext:
    session: str
    trade_date: Any
    expected_as_of: Any
    env: str
    market: str
    now_kst: datetime
    artifact_valid: bool
    artifact_reason: str | None
    balance_state: str | None
    close_forced: bool


@dataclass
class BalancePrecheck:
    state: str
    source: str
    checked_at_kst: datetime
    entry_allowed: bool
    exit_allowed: bool
    close_allowed: bool
    reason: str | None
    cash: int | None = None
    holdings_count: int | None = None
    positions_summary: dict[str, Any] | None = None
    raw_snapshot_available: bool = False
    raw_snapshot: dict[str, Any] | None = None


def reconcile_kr_order_counts(*, engine_order_count: int, broker_ack_count: int,
                              db_ack_count: int, ack_db_failed_count: int,
                              balance_confirmed_count: int, filled_confirmed_count: int,
                              unresolved_ack_count: int | None = None) -> dict[str, Any]:
    """Reconcile PB1, durable ACK ledger, and close-balance evidence."""
    unresolved = int(unresolved_ack_count if unresolved_ack_count is not None else max(0, broker_ack_count - balance_confirmed_count))
    mismatch = bool(
        broker_ack_count != db_ack_count + ack_db_failed_count
        or balance_confirmed_count > broker_ack_count
        or filled_confirmed_count > balance_confirmed_count
        or unresolved < 0
    )
    resolved_ack_failure = bool(ack_db_failed_count and balance_confirmed_count >= broker_ack_count and unresolved == 0)
    return {
        "engine_order_count": int(engine_order_count),
        "broker_ack_count": int(broker_ack_count),
        "db_ack_count": int(db_ack_count),
        "ack_db_failed_count": int(ack_db_failed_count),
        "balance_confirmed_count": int(balance_confirmed_count),
        "filled_confirmed_count": int(filled_confirmed_count),
        "unresolved_ack_count": unresolved,
        "manual_reconcile_required": int(bool(ack_db_failed_count or unresolved or mismatch)),
        "reconciliation_source": "engine_order_ledger_close_balance",
        "status": "WARNING_RECONCILE_MISMATCH" if mismatch else ("BALANCE_CONFIRMED_AFTER_ACK_DB_FAILED" if resolved_ack_failure else ("OK_WITH_WARNINGS" if ack_db_failed_count or unresolved else "OK")),
        "reason": "ORDER_SOURCE_MISMATCH" if mismatch else ("BALANCE_CONFIRMED_AFTER_ACK_DB_FAILED" if resolved_ack_failure else None),
    }


def _write_session_last_stage(*, session: str, trade_date: Any, expected_as_of: Any, stage: str, status: str = "start") -> None:
    import time as _time
    started = float(os.getenv("KR_SESSION_STARTED_MONOTONIC", "0") or 0)
    payload = {"market":"KR","session":session,"trade_date":str(trade_date),"expected_as_of":str(expected_as_of),"stage":stage,"status":status,"updated_at":_now_kst().isoformat(),"elapsed_sec_from_start":round((_time.monotonic()-started),3) if started else 0.0,"pid":os.getpid()}
    for path in (ROOT/"runtime/state/kr/session_last_stage.json", ROOT/"runtime/state/kr"/f"session_last_stage_{session}.json"):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        except Exception as exc:
            logger.warning("[KR_SESSION][LAST_STAGE][WRITE_FAIL] stage=%s err=%s", stage, exc)

def _read_session_last_stage(session: str) -> str:
    path = ROOT/"runtime/state/kr"/f"session_last_stage_{session}.json"
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("stage", "")
    except Exception:
        return ""


def _stage(session: str, trade_date: Any, expected_as_of: Any, name: str, action):
    import time as _time
    started = _time.monotonic()
    _write_session_last_stage(session=session, trade_date=trade_date, expected_as_of=expected_as_of, stage=name, status="start")
    logger.info("[STAGE][START] stage=%s", name)
    try:
        return action()
    finally:
        elapsed = _time.monotonic() - started
        _write_session_last_stage(session=session, trade_date=trade_date, expected_as_of=expected_as_of, stage=name, status="done")
        logger.info("[STAGE][END] stage=%s elapsed=%.3f", name, elapsed)

def _balance_cash(snapshot: Any) -> int | None:
    if not isinstance(snapshot, dict):
        return None
    out2 = snapshot.get("output2")
    summary = out2[0] if isinstance(out2, list) and out2 else out2 if isinstance(out2, dict) else {}
    for key in ("dnca_tot_amt", "tot_evlu_amt", "cash", "cash_krw"):
        try:
            if key in summary:
                return int(float(str(summary.get(key)).replace(",", "")))
        except Exception:
            continue
    return None


def _balance_holdings_count(snapshot: Any) -> int | None:
    if not isinstance(snapshot, dict):
        return None
    rows = snapshot.get("output1")
    if not isinstance(rows, list):
        return None
    count = 0
    for row in rows:
        try:
            qty = int(float(str((row or {}).get("hldg_qty") or (row or {}).get("qty") or "0").replace(",", "")))
        except Exception:
            qty = 0
        if qty > 0:
            count += 1
    return count



@dataclass
class CloseBalanceGuardResult:
    status: str
    retryable: int
    completed: int
    reason: str


def _safe_int_value(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", "")))
    except Exception:
        return 0


def close_balance_guard(*, holdings_count: int, market_value: float, raw_balance: dict[str, Any] | None = None) -> CloseBalanceGuardResult:
    if int(holdings_count or 0) == 0 and float(market_value or 0.0) > 0:
        logger.warning(
            "[KR_CLOSE][BALANCE_INCONSISTENT] holdings=0 market_value=%s action=retry_balance_reload raw_keys=%s",
            market_value,
            sorted((raw_balance or {}).keys()) if isinstance(raw_balance, dict) else [],
        )
        return CloseBalanceGuardResult(
            status="WARN_BALANCE_INCONSISTENT",
            retryable=1,
            completed=0,
            reason="BALANCE_INCONSISTENT_HOLDINGS_UNKNOWN",
        )
    return CloseBalanceGuardResult(status="OK", retryable=0, completed=1, reason="ok")


def _balance_market_value(snapshot: Any) -> int:
    if not isinstance(snapshot, dict):
        return 0
    out2 = snapshot.get("output2")
    summary = out2[0] if isinstance(out2, list) and out2 else out2 if isinstance(out2, dict) else {}
    if not isinstance(summary, dict):
        return 0

    # Stock/equity valuation fields only. Do not treat total asset value as market value;
    # cash-only accounts can have positive tot_evlu_amt with no holdings.
    for key in ("scts_evlu_amt", "evlu_amt_smtl_amt", "stock_evlu_amt", "stk_evlu_amt"):
        val = _safe_int_value(summary.get(key))
        if val > 0:
            return val

    total_raw = summary.get("tot_evlu_amt")
    cash_raw = None
    for cash_key in ("dnca_tot_amt", "cash", "cash_krw", "ord_psbl_cash"):
        if cash_key in summary and summary.get(cash_key) not in (None, ""):
            cash_raw = summary.get(cash_key)
            break
    total = _safe_int_value(total_raw)
    cash = _safe_int_value(cash_raw)
    if total_raw not in (None, "") and cash_raw not in (None, "") and total > cash:
        return total - cash
    return 0

def _setup_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


def _now_kst() -> datetime:
    return now_kst()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rows_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("items") or []
        return rows if isinstance(rows, list) else []
    return []


def _session_context(session: str, env: str, *, artifact_valid: bool = False, artifact_reason: str | None = None, balance_state: str | None = None) -> KrSessionContext:
    td_raw = os.getenv("KR_TRADE_DATE")
    td = datetime.fromisoformat(td_raw).date() if td_raw else resolve_kr_trade_date(_now_kst())
    exp_raw = os.getenv("KR_EXPECTED_AS_OF")
    exp = datetime.fromisoformat(exp_raw).date() if exp_raw else resolve_kr_expected_as_of(td)
    os.environ["KR_TRADE_DATE"] = td.isoformat()
    os.environ["KR_EXPECTED_AS_OF"] = exp.isoformat()
    os.environ["AS_OF_OVERRIDE"] = exp.isoformat()
    ctx = KrSessionContext(session, td, exp, env, "KR", _now_kst(), artifact_valid, artifact_reason, balance_state, session == "close")
    logger.info("[KR_SESSION][CONTEXT] session=%s trade_date=%s expected_as_of=%s env=%s market=KR", session, td, exp, env)
    return ctx


def _write_prep_summary(rows: int, status: str, reason: str) -> None:
    today = _now_kst().strftime("%Y-%m-%d")
    out_dir = ROOT / "reports/kr_prep" / today
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"status": status, "reason": reason, "final30_rows": rows, "updated_at": _now_kst().isoformat()}
    (out_dir / "kr_prep_summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_prep(env: str) -> dict[str, Any]:
    ctx = _session_context("prep", env)
    guard = kr_prep_schedule_guard()
    logger.info(
        "[KR_PREP][SCHEDULE_GUARD][PY] now=%s allowed=%d reason=%s window=06:30-08:50",
        _now_kst().strftime("%H:%M"),
        int(guard.action != "BLOCK"),
        guard.reason,
    )
    if guard.action == "BLOCK":
        logger.error("[KR_PREP][BLOCKED] reason=OUTSIDE_PREP_WINDOW action=no_artifact_touch")
        logger.info("[KR_PREP][ARTIFACT_CLEAN][SKIP] reason=OUTSIDE_PREP_WINDOW")
        return {"status": "FAIL", "reason": "OUTSIDE_PREP_WINDOW", "exit_code": 2}
    quarantine_stale_kr_artifacts(trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, env=env)
    os.environ.update({
        "STRATEGY_ENV": env,
        "KIS_ENV": os.getenv("KIS_ENV") or env,
        "STRATEGY_MODE": "PREP",
        "DRY_RUN": "1",
        "DISABLE_LIVE_TRADING": "1",
        "LIVE_TRADING_ENABLED": "0",
        "PB1_SESSION": "prep",
    })
    logger.info("[KR_SESSION][START] session=prep env=%s", env)
    import trader.prep_runner as prep_runner

    run_started_ts = _now_kst().timestamp()
    exit_code = int(prep_runner.main() or 0)
    artifact = validate_kr_prep_artifact(trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, env=env, strict=True, allow_legacy_fallback=False)
    rows = artifact.rows
    source = ROOT / "signals/kr/latest_final30_scored.json"
    fresh_artifact = bool(source.exists() and source.stat().st_mtime >= run_started_ts)
    if exit_code != 0 and not (artifact.ok and fresh_artifact):
        logger.error("[KR_PREP][FAIL] reason=PREP_RUNNER_FAILED exit_code=%s fresh_artifact=%d artifact_ok=%d", exit_code, int(fresh_artifact), int(artifact.ok))
        logger.info("[RUN_SUMMARY][RESULT] session=prep status=FAIL reason=PREP_RUNNER_FAILED")
        return {"status": "FAIL", "reason": "PREP_RUNNER_FAILED", "rows": rows, "exit_code": 1}
    if not artifact.ok:
        logger.error("[KR_PREP][FAIL] reason=%s detail=%s rows=%s", artifact.reason, artifact.detail, rows)
        logger.info("[RUN_SUMMARY][RESULT] session=prep status=FAIL reason=%s", artifact.reason)
        return {"status": "FAIL", "reason": artifact.reason, "rows": rows, "exit_code": 2}
    # Do not overwrite the rich canonical prep_contract generated by mirror_kr_prep_artifacts().
    if rows != 30:
        _write_prep_summary(rows, "FAIL", "FINAL30_ROW_COUNT")
        logger.error("[KR_PREP][FAIL] reason=FINAL30_ROW_COUNT rows=%s", rows)
        logger.info("[RUN_SUMMARY][RESULT] session=prep status=FAIL reason=FINAL30_ROW_COUNT")
        return {"status": "FAIL", "reason": "FINAL30_ROW_COUNT", "rows": rows, "exit_code": exit_code}
    _write_prep_summary(rows, "OK", "KR_PREP_DONE")
    logger.info("[KR_PREP][ARTIFACT] trade_date=%s expected_as_of=%s rows=%s path=%s", ctx.trade_date, ctx.expected_as_of, rows, source)
    logger.info("[KR_PREP][CONTRACT] contract_ok=1 trade_can_proceed=1 final30_rows=%s", rows)
    logger.info("[KR_PREP][DONE] status=OK")
    reason = "CANONICAL_PREP_READY"
    logger.info("[KR_PREP][SUCCESS] trade_date=%s expected_as_of=%s rows=%s source=%s", ctx.trade_date, ctx.expected_as_of, rows, artifact.source)
    logger.info("[RUN_SUMMARY][RESULT] session=prep status=OK reason=CANONICAL_PREP_READY rows=%s", rows)
    return {"status": "OK", "reason": reason, "rows": rows, "exit_code": 0}


def _guard_trade_session(session: str, ctx: KrSessionContext) -> dict[str, Any] | None:
    import concurrent.futures
    now = _now_kst()
    tag = f"KR_{session.upper()}" if session != "afternoon" else "KR_AFTERNOON"
    if session == "am":
        target_raw = os.getenv("KR_AM_ENTRY_START_TIME", "09:00:05")
        hh, mm, ss = [int(x) for x in target_raw.split(":")]
        logger.info("[KR_AM][PREOPEN_WAIT] trade_date=%s target_time=%s", ctx.trade_date, target_raw)
        try:
            wait_until_kr_am_target(trade_date=ctx.trade_date, target_time=time(hh, mm, ss), max_wait_sec=int(os.getenv("KR_AM_MAX_WAIT_SEC", os.getenv("KR_AM_MAX_WAIT_SECONDS", "900"))))
        except RuntimeError as exc:
            if str(exc) == "KR_AM_WAIT_TOO_LONG":
                logger.error("[KR_AM][FAIL] reason=KR_AM_WAIT_TOO_LONG trade_date=%s target_time=%s", ctx.trade_date, target_raw)
                logger.info("[RUN_SUMMARY][RESULT] market=KR session=am status=FAIL reason=KR_AM_WAIT_TOO_LONG orders_intent=0 orders_ack=0 blocked=0")
                return {"status": "FAIL", "reason": "KR_AM_WAIT_TOO_LONG", "exit_code": 2}
            raise
        logger.info("[KR_AM][RUN_AFTER_TARGET] trade_date=%s", ctx.trade_date)
    if session == "close":
        logger.info("[KR_SESSION][CLOSE_CONTINUE] reason=EXIT_ONLY_DOES_NOT_REQUIRE_ENTRY_ARTIFACT")
        return None

    timeout = float(os.getenv("KR_SESSION_PRECHECK_TIMEOUT_SEC", "30"))
    logger.info("[KR_SESSION][PRECHECK_START] session=%s timeout_sec=%s", session, timeout)
    def _precheck() -> dict[str, Any] | None:
        _write_session_last_stage(session=session, trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, stage="precheck.artifact_files_validate", status="start")
        logger.info("[KR_SESSION][PRECHECK_STAGE][START] stage=artifact_files_validate")
        started = _now_kst()
        result = validate_kr_prep_artifact(trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, env=os.getenv("STRATEGY_ENV", "practice"), require_db_exact=False, strict=True, allow_legacy_fallback=False)
        logger.info("[KR_SESSION][PRECHECK_STAGE][DONE] stage=artifact_files_validate elapsed=%.3f", (_now_kst()-started).total_seconds())
        _write_session_last_stage(session=session, trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, stage="precheck.artifact_files_validate", status="done")
        if result.ok:
            logger.info("[KR_SESSION][PRECHECK_OK] session=%s source=%s rows=%s reason=%s", session, result.source, result.rows, result.reason)
            logger.info("[KR_SESSION][PROCEED] session=%s", session)
            return None
        _write_session_last_stage(session=session, trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, stage="precheck.db_rescue_load", status="start")
        logger.info("[KR_SESSION][PRECHECK_STAGE][START] stage=db_rescue_load")
        rstart = _now_kst()
        rescue = rescue_kr_final30_from_db(trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, env=os.getenv("STRATEGY_ENV", "practice"))
        logger.info("[KR_SESSION][PRECHECK_STAGE][DONE] stage=db_rescue_load elapsed=%.3f", (_now_kst()-rstart).total_seconds())
        _write_session_last_stage(session=session, trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, stage="precheck.db_rescue_quality_validate", status="done" if rescue.ok else "fail")
        logger.info("[KR_SESSION][PRECHECK_STAGE][DONE] stage=db_rescue_quality_validate elapsed=0.000")
        if rescue.ok:
            logger.warning("[KR_SESSION][PRECHECK_RESCUED] session=%s source=db_final30_scored rows=30 artifact_rebuilt=%s", session, int((rescue.details or {}).get("artifact_rebuilt", 0)))
            logger.info("[KR_SESSION][PROCEED] session=%s", session)
            return None
        logger.error("[KR_SESSION][PRECHECK_FAIL] session=%s reason=%s detail=%s db_rescue=failed", session, result.reason, result.detail)
        logger.info("[RUN_SUMMARY][RESULT] market=KR session=%s status=FAIL reason=%s orders_intent=0 orders_ack=0 blocked=0", session, result.reason)
        return {"status": "FAIL", "reason": result.reason}
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(_precheck)
    try:
        return fut.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        ex.shutdown(wait=False, cancel_futures=True)
        _write_session_last_stage(session=session, trade_date=ctx.trade_date, expected_as_of=ctx.expected_as_of, stage="precheck.timeout", status="timeout")
        logger.error("[KR_SESSION][PRECHECK_TIMEOUT] session=%s timeout_sec=%s last_stage=%s", session, timeout, _read_session_last_stage(session))
        logger.error("[KR_SESSION][PRECHECK_FAIL] session=%s reason=PRECHECK_TIMEOUT detail=timeout_sec_%s", session, timeout)
        logger.info("[RUN_SUMMARY][RESULT] market=KR session=%s status=FAIL reason=PRECHECK_TIMEOUT orders_intent=0 orders_ack=0 blocked=0", session)
        return {"status": "FAIL", "reason": "PRECHECK_TIMEOUT", "exit_code": 2}
    finally:
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

def _assert_balance_available(session: str) -> dict[str, Any] | None:
    global _LAST_GOOD_BALANCE
    td = os.getenv("KR_TRADE_DATE") or _now_kst().strftime("%Y-%m-%d")
    out = ROOT / "runtime/kr/session" / td / session / "balance_precheck.json"
    try:
        raw = KisAPI().get_balance_cached()
        snapshot = raw[0] if isinstance(raw, tuple) else raw
        snapshot = snapshot if isinstance(snapshot, dict) else None
        if snapshot:
            _LAST_GOOD_BALANCE = (snapshot, _now_kst())
        pre = BalancePrecheck(
            "OK",
            "KIS",
            _now_kst(),
            True,
            True,
            True,
            None,
            cash=_balance_cash(snapshot),
            holdings_count=_balance_holdings_count(snapshot),
            positions_summary={"holdings_count": _balance_holdings_count(snapshot)},
            raw_snapshot_available=bool(snapshot),
            raw_snapshot=snapshot,
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(asdict(pre), ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        os.environ["KR_BALANCE_PRECHECK_PATH"] = str(out)
        logger.info("[KR_SESSION][BALANCE_PRECHECK] state=OK source=KIS entry_allowed=1 exit_allowed=1 close_allowed=1 snapshot=%d", int(bool(snapshot)))
        return None
    except KisBalanceUnavailable as exc:
        env = os.getenv("STRATEGY_ENV", os.getenv("KIS_ENV", "practice"))
        try:
            fail_soft = resolve_kr_balance_fail_soft(exc, env=env)
            cached = None
            if _LAST_GOOD_BALANCE:
                candidate, saved_at = _LAST_GOOD_BALANCE
                if (_now_kst() - saved_at).total_seconds() <= float(os.getenv("KR_BALANCE_SNAPSHOT_MAX_AGE_SEC", "300")):
                    cached = {"raw_snapshot": candidate, "raw_snapshot_available": True,
                              "cash": _balance_cash(candidate), "holdings_count": _balance_holdings_count(candidate),
                              "positions_summary": {"holdings_count": _balance_holdings_count(candidate)}}
            max_age = float(os.getenv("KR_BALANCE_SNAPSHOT_MAX_AGE_SEC", "300"))
            if out.exists():
                try:
                    previous = json.loads(out.read_text(encoding="utf-8"))
                    updated = datetime.fromisoformat(str(previous.get("checked_at_kst")).replace("Z", "+00:00"))
                    age = (_now_kst() - updated.replace(tzinfo=None)).total_seconds() if updated.tzinfo is None else (_now_kst().astimezone(updated.tzinfo) - updated).total_seconds()
                    if age <= max_age and previous.get("raw_snapshot_available") and isinstance(previous.get("raw_snapshot"), dict):
                        cached = previous
                except Exception as cache_exc:
                    logger.warning("[KR_SESSION][BALANCE_CACHE][READ_WARN] err=%s", cache_exc)
            logger.warning("[KR_SESSION][BALANCE_FAIL_SOFT] session=%s reason=%s", session, fail_soft.get("reason"))
            entry_allowed = False
            exit_allowed = bool(cached)
            close_allowed = bool(cached)
            pre = BalancePrecheck("TIMEOUT", "PERSISTED_CACHE" if cached else ("CACHE" if (exit_allowed or close_allowed) else "NONE"), _now_kst(), entry_allowed, exit_allowed, close_allowed, "STALE_OR_MISSING_BALANCE" if not cached else "BALANCE_TIMEOUT_USING_FRESH_SNAPSHOT", raw_snapshot_available=bool(cached), raw_snapshot=(cached or {}).get("raw_snapshot"), cash=(cached or {}).get("cash"), holdings_count=(cached or {}).get("holdings_count"), positions_summary=(cached or {}).get("positions_summary"))
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(asdict(pre), ensure_ascii=False, default=str, indent=2), encoding="utf-8")
            os.environ["KR_BALANCE_PRECHECK_PATH"] = str(out)
            logger.info("[KR_SESSION][BALANCE_PRECHECK] state=%s source=%s entry_allowed=%d exit_allowed=%d close_allowed=%d snapshot=0", pre.state, pre.source, int(pre.entry_allowed), int(pre.exit_allowed), int(pre.close_allowed))
            return {
                "status": "WARN",
                "reason": pre.reason,
                "order_allowed": 0,
                "entry_allowed": int(entry_allowed),
                "exit_allowed": int(exit_allowed),
                "performance_reliable": 0,
            }
        except Exception:
            logger.error("[KR_SESSION][SAFE_STOP] session=%s reason=KIS_BALANCE_UNAVAILABLE err=%s", session, exc, exc_info=True)
            logger.info("[RUN_SUMMARY][RESULT] session=%s status=SAFE_STOP reason=KIS_BALANCE_UNAVAILABLE", session)
            return {
                "status": "SAFE_STOP",
                "reason": "KIS_BALANCE_UNAVAILABLE",
                "order_allowed": 0,
                "entry_allowed": 0,
                "exit_allowed": 0,
                "performance_reliable": 0,
            }


def recover_temporary_balance(
    session: str, initial: dict[str, Any], *, probe=None, sleep_fn=None,
    now_fn=None, max_attempts: int | None = None,
) -> dict[str, Any] | None:
    """Keep PM/Close alive fail-closed until a fresh broker balance succeeds."""
    if session not in {"afternoon", "close"} or str(initial.get("status") or "").upper() not in {"WARN", "SAFE_STOP"}:
        return initial
    probe = probe or (lambda: _assert_balance_available(session))
    sleep_fn = sleep_fn or time_mod.sleep
    now_fn = now_fn or _now_kst
    interval = max(1, int(os.getenv("KR_BALANCE_RECOVERY_INTERVAL_SEC", "60")))
    if max_attempts is None:
        default_attempts = "12" if session == "close" else "120"
        env_key = "KR_CLOSE_BALANCE_RECOVERY_MAX_ATTEMPTS" if session == "close" else "KR_BALANCE_RECOVERY_MAX_ATTEMPTS"
        max_attempts = max(1, int(os.getenv(env_key, default_attempts)))
    recovery = BalanceRecoveryState(retry_interval_seconds=interval)
    recovery.failed(now_fn())
    os.environ.update(ENTRY_ALLOWED="0", ORDER_ALLOWED="0", EXIT_ALLOWED="0",
                      KR_BALANCE_RECOVERY_ONLY="1")
    for attempt in range(1, max_attempts + 1):
        logger.warning("[SESSION][BALANCE_RECOVERY] session=%s state=%s retry=%s next_retry=%s entry_allowed=0 new_order_allowed=0",
                       session, recovery.state, attempt, recovery.next_retry_at)
        sleep_fn(interval)
        result = probe()
        if result is None:
            recovery.recovered()
            os.environ.update(ENTRY_ALLOWED="1", ORDER_ALLOWED="1", EXIT_ALLOWED="1",
                              KR_BALANCE_RECOVERY_ONLY="0")
            logger.info("[SESSION][BALANCE_RECOVERY] state=NORMAL retry=%s next_retry=none action=RESUME_FRESH_SNAPSHOT", attempt)
            return None
        recovery.failed(now_fn())
    return {**initial, "status": "RETRYABLE_DEGRADED", "reason": "KIS_BALANCE_TIMEOUT",
            "completed": False, "retryable": True, "exit_code": 75}





def load_pb1_session_result(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except Exception as exc:
        logger.warning("[KR_SESSION][PB1_RESULT][LOAD_WARN] path=%s err=%s", path, exc)
        return {}

def extract_sell_orders_ack(result: dict | None) -> int:
    """Extract acknowledged sell orders from actual PB1/session result; never read PB1_SELL_ORDERS_ACK."""
    if not isinstance(result, dict):
        return 0
    for key in (
        "sell_orders_ack", "orders_sell_ack", "sell_ack", "sell_accepted", "sell_filled",
        "sell_orders_accepted", "sell_orders_filled", "sell_orders",
    ):
        try:
            value = int(result.get(key) or 0)
            if value > 0:
                return value
        except Exception:
            pass
    summary = result.get("order_summary") or result.get("orders_summary") or {}
    if isinstance(summary, dict):
        for key in ("sell_ack", "sell_accepted", "sell_filled", "accepted_sell", "filled_sell", "sell_orders"):
            try:
                value = int(summary.get(key) or 0)
                if value > 0:
                    return value
            except Exception:
                pass
    orders = result.get("orders") or result.get("order_results") or []
    if isinstance(orders, list):
        count = 0
        for order in orders:
            if not isinstance(order, dict):
                continue
            side = str(order.get("side") or order.get("order_side") or "").upper()
            status = str(order.get("status") or order.get("result") or "").upper()
            if side == "SELL" and status in {"ACCEPTED", "FILLED", "OK", "SUBMITTED"}:
                count += 1
        return count
    return 0

def build_session_result(*, exit_code: int, status: str = "UNKNOWN", sell_orders_ack: int = 0, entry_status: str = "UNKNOWN", entry_abort_reason: str | None = None, fatal_error: bool = False) -> dict[str, Any]:
    return {
        "exit_code": int(exit_code), "status": str(status or "UNKNOWN"),
        "sell_orders_ack": int(sell_orders_ack or 0), "entry_status": str(entry_status or "UNKNOWN"),
        "entry_abort_reason": entry_abort_reason, "fatal_error": bool(fatal_error),
    }

def compute_session_marker(result: dict[str, Any]) -> dict[str, Any]:
    exit_code = int((result or {}).get("exit_code") or 0)
    status = str((result or {}).get("status") or "UNKNOWN").upper()
    entry_status = str((result or {}).get("entry_status") or "UNKNOWN").upper()
    entry_abort_reason = (result or {}).get("entry_abort_reason")
    fatal_error = bool((result or {}).get("fatal_error"))
    sell_orders_ack = int((result or {}).get("sell_orders_ack") or 0)
    entry_done = entry_status in {"DONE", "OK", "OK_NO_TRADE", "SKIPPED_BY_POLICY"} and not entry_abort_reason
    can_mark_completed = (
        exit_code == 0
        and status in {"OK", "OK_NO_TRADE", "PB1_SESSION_DONE"}
        and entry_done
        and not fatal_error
    )
    partial_success_retryable = sell_orders_ack > 0 and not can_mark_completed
    if partial_success_retryable:
        marker_status = "PARTIAL_SUCCESS_RETRYABLE"
    elif can_mark_completed:
        marker_status = status
    else:
        marker_status = status if status not in {"", "UNKNOWN"} else "RETRYABLE_FAILURE"
    return {
        "status": marker_status,
        "completed": int(can_mark_completed),
        "retryable": int(not can_mark_completed),
        "sell_completed": int(sell_orders_ack > 0),
        "entry_completed": int(entry_done),
        "sell_orders_ack": sell_orders_ack,
        "entry_status": entry_status,
        "entry_abort_reason": entry_abort_reason,
    }


def lock_unavailable_result_fields() -> dict[str, Any]:
    """Stable contract for a PB1 engine that never started due to its DB lock."""
    return {
        "engine_started": False,
        "pb1_result_present": False,
        "orders_intent": 0,
        "orders_ack": 0,
    }



def normalize_kr_session_completion(
    *,
    status: str,
    summary_reason: str,
    marker: dict[str, Any],
    pb1_status: str,
    pb1_exit_reason: str,
) -> tuple[str, str, int, int]:
    base_retryable_reasons = {
        "DB_EXACT_FINAL30_ZERO",
        "CLOSE_BALANCE_UNCONFIRMED",
        "BALANCE_TIMEOUT_FAIL_SOFT",
        "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT",
        "ENTRY_EXIT_PLAN_MISSING_OR_INVALID",
        "ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
        "RETRYABLE_ORDER_BUILD_ERROR",
    }
    retryable_order_build_reasons = {
        "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT",
        "ENTRY_EXIT_PLAN_MISSING_OR_INVALID",
        "ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
        "RETRYABLE_ORDER_BUILD_ERROR",
        "ORDER_CANDIDATE_BUT_ZERO_API_SUBMIT",
    }
    pb1_status_u = str(pb1_status or "").upper()
    pb1_exit_reason_s = str(pb1_exit_reason or "")
    if "FATAL_RUNTIME_REPEAT" in f"{pb1_status_u} {pb1_exit_reason_s.upper()}":
        return "FAILED", "FATAL_RUNTIME_REPEAT", 0, 1
    if pb1_status_u == "SKIP_LOCKED" or pb1_exit_reason_s == "PB1_ADVISORY_LOCK_UNAVAILABLE":
        return "FAILED", "PB1_ADVISORY_LOCK_UNAVAILABLE", 0, 1
    if pb1_exit_reason_s == "PB1_RESULT_MISSING":
        return "FAILED", "PB1_RESULT_MISSING", 0, 1
    policy_block_reasons = {
        "BUYABLE_EXISTING_HOLDING_KIS",
        "BUYABLE_TODAY_BUY_EXISTS",
        "BUYABLE_TODAY_SELL_REBUY_BLOCKED",
        "BUYABLE_COOLDOWN",
        "BUYABLE_DUPLICATE",
        "MARKET_RISK_OFF_ENTRY_BLOCK",
        "SECTOR_CAP_BLOCK",
        "GROSS_EXPOSURE_CAP",
        "CASH_INSUFFICIENT",
        "MAX_POSITIONS_REACHED",
    }

    if pb1_exit_reason_s.upper() in policy_block_reasons:
        return "OK_NO_TRADE", pb1_exit_reason_s, 1, 0

    if pb1_status_u == "RETRYABLE_ORDER_BUILD_ERROR" or pb1_exit_reason_s in retryable_order_build_reasons:
        reason = pb1_exit_reason_s if pb1_exit_reason_s in retryable_order_build_reasons else "RETRYABLE_ORDER_BUILD_ERROR"
        return "RETRYABLE_ORDER_BUILD_ERROR", reason, 0, 1

    if pb1_exit_reason_s == "phase_guard_skip_duplicate_pm_run":
        completed = int(bool((marker or {}).get("completed")))
        retryable = int(bool((marker or {}).get("retryable")))
        if completed and not retryable:
            return "SKIP_DUPLICATE_NORMAL", "phase_guard_skip_duplicate_pm_run", 1, 0
        return str(status or "SKIP_DUPLICATE_NORMAL"), "phase_guard_skip_duplicate_pm_run", completed, retryable

    completed = int((marker or {}).get("completed") or 0)
    retryable = int(bool((marker or {}).get("retryable")) or str(summary_reason or "") in base_retryable_reasons or str(status or "") == "WARN")
    if str(summary_reason or "") in base_retryable_reasons:
        completed = 0
        retryable = 1
    return str(status or "UNKNOWN"), str(summary_reason or ""), completed, retryable

def _run_pb1_session(session: str, env: str) -> dict[str, Any]:
    ctx = _session_context(session, env)
    _stage(session, ctx.trade_date, ctx.expected_as_of, "prep_contract_load", lambda: None)
    _stage(session, ctx.trade_date, ctx.expected_as_of, "regime_load", lambda: None)
    os.environ.update({
        "STRATEGY_ENV": env,
        "KIS_ENV": os.getenv("KIS_ENV") or env,
        "STRATEGY_MODE": "LIVE",
        "DRY_RUN": os.getenv("DRY_RUN", "0"),
        "DISABLE_LIVE_TRADING": os.getenv("DISABLE_LIVE_TRADING", "0"),
        "LIVE_TRADING_ENABLED": os.getenv("LIVE_TRADING_ENABLED", "1"),
        "KR_LIVE_TRADING_ENABLED": os.getenv("KR_LIVE_TRADING_ENABLED", "1"),
        "KR_ORDER_ARMED": os.getenv("KR_ORDER_ARMED", "1"),
        "PB1_SESSION": session,
    })
    logger.info("[KR_SESSION][START] session=%s env=%s", session, env)
    if session == "close":
        os.environ["FORCE_MARKET_WINDOW"] = "close"
        os.environ["FORCE_PB1_PHASE"] = "close"
        os.environ["PB1_ENTRY_ENABLED"] = "0"
        os.environ["PB1_EXIT_ENABLED"] = "1"
        os.environ["PB1_CLOSE_ENABLED"] = "1"
        os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] = os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")
        os.environ["KR_CLOSE_SESSION"] = "1"
        phase_marker = ROOT / "runtime/kr/session" / ctx.trade_date.isoformat() / "close" / "phase.json"
        phase_marker.parent.mkdir(parents=True, exist_ok=True)
        phase_marker.write_text(
            json.dumps(
                {
                    "phase": "close",
                    "phase_executed": True,
                    "force_phase": True,
                    "skip_phase_window": False,
                    "entry_enabled": False,
                    "exit_enabled": True,
                    "close_enabled": True,
                    "close_liquidation_enabled": False,
                    "created_at_kst": _now_kst().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        logger.info("[KR_CLOSE][PHASE] phase=close entry_enabled=0 exit_enabled=1 close_enabled=1 close_liquidation_enabled=0")

    # KR PR #61 권한 모델: entry block must not kill exit/close liveness.
    kr_entry_can_proceed = str(os.getenv("PB1_ENTRY_ENABLED", "1")).strip() == "1"
    kr_exit_can_proceed = str(os.getenv("PB1_EXIT_ENABLED", "1")).strip() == "1"
    kr_close_can_proceed = session == "close" or str(os.getenv("PB1_CLOSE_ENABLED", "1")).strip() == "1"
    kr_session_can_run = kr_entry_can_proceed or kr_exit_can_proceed or kr_close_can_proceed
    logger.info(
        "[KR_SESSION][PERMISSION] entry_can_proceed=%s exit_can_proceed=%s close_can_proceed=%s session_can_run=%s",
        int(kr_entry_can_proceed), int(kr_exit_can_proceed), int(kr_close_can_proceed), int(kr_session_can_run),
    )
    guarded = _guard_trade_session(session, ctx)
    if guarded is not None:
        return guarded
    infinite_allow_entry = session in {"am", "afternoon"} and kr_entry_can_proceed
    balance_state = _stage(session, ctx.trade_date, ctx.expected_as_of, "balance_precheck", lambda: _assert_balance_available(session))
    if (balance_state is not None and session in {"afternoon", "close"}
            and int(balance_state.get("exit_allowed", 0)) == 0):
        balance_state = recover_temporary_balance(session, balance_state)
    if balance_state is not None:
        if balance_state.get("status") == "WARN" and int(balance_state.get("exit_allowed", 0)) == 1:
            os.environ["ENTRY_ALLOWED"] = str(int(balance_state.get("entry_allowed", 0)))
            os.environ["EXIT_ALLOWED"] = str(int(balance_state.get("exit_allowed", 0)))
            os.environ["ORDER_ALLOWED"] = str(int(balance_state.get("order_allowed", 0)))
            os.environ["KR_BALANCE_FAIL_SOFT_ACTIVE"] = "1"
            infinite_allow_entry = infinite_allow_entry and bool(int(balance_state.get("entry_allowed", 0)))
            logger.warning("[KR_SESSION][CONTINUE_AFTER_BALANCE_FAIL_SOFT] session=%s entry_allowed=%s exit_allowed=%s order_allowed=%s", session, os.environ["ENTRY_ALLOWED"], os.environ["EXIT_ALLOWED"], os.environ["ORDER_ALLOWED"])
        else:
            return balance_state

    window = {"am": "morning", "afternoon": "day", "close": "close"}[session]
    pb1_runner = sys.modules.get("trader.pb1_runner")
    if pb1_runner is None:
        import trader.pb1_runner as pb1_runner

    old_argv = sys.argv[:]
    os.environ.pop("PB1_LAST_RESULT_STATUS", None)
    os.environ.pop("PB1_LAST_EXIT_REASON", None)
    logger.info("[KR_SESSION][PB1_ENV_CLEAR] cleared=PB1_LAST_RESULT_STATUS,PB1_LAST_EXIT_REASON")
    pb1_result_path = ROOT / "runtime/kr/session" / ctx.trade_date.isoformat() / session / "pb1_result.json"
    pb1_result_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["PB1_SESSION_RESULT_PATH"] = str(pb1_result_path)
    try:
        sys.argv = ["kr-pb1-session", "--window", window, "--phase", "auto", "--env", env]
        exit_code = int(_stage(session, ctx.trade_date, ctx.expected_as_of, "pb1_entry_eval", lambda: pb1_runner.main()) or 0)
    finally:
        sys.argv = old_argv
    _stage(session, ctx.trade_date, ctx.expected_as_of, "order_submit", lambda: None)
    _stage(session, ctx.trade_date, ctx.expected_as_of, "order_ack_persist", lambda: None)
    _stage(session, ctx.trade_date, ctx.expected_as_of, "fill_reconcile", lambda: None)
    session_end_entry_allowed = infinite_allow_entry and str(os.getenv("ENTRY_ALLOWED", "1")) == "1" and str(os.getenv("ORDER_ALLOWED", "1")) == "1"
    pb1_result_present = pb1_result_path.exists()
    if pb1_result_present:
        pb1_result = load_pb1_session_result(pb1_result_path)
        logger.info(
            "[KR_SESSION][PB1_RESULT][LOAD_OK] path=%s keys=%s sell_orders_ack=%s",
            pb1_result_path, sorted(pb1_result.keys()), extract_sell_orders_ack(pb1_result),
        )
        logger.info("[KR_SESSION][PB1_RESULT][OK] path=%s", pb1_result_path)
    else:
        pb1_result = {}
        logger.warning(
            "[KR_SESSION][RESULT][MISSING] session=%s reason=PB1_RESULT_MISSING action=mark_failed path=%s exit_code=%s",
            session,
            pb1_result_path, exit_code,
        )
    pb1_last = str(os.getenv("PB1_LAST_RESULT_STATUS") or "").upper()
    pb1_reason = str(os.getenv("PB1_LAST_EXIT_REASON") or "")
    lock_unavailable = pb1_last == "SKIP_LOCKED" or pb1_reason == "PB1_ADVISORY_LOCK_UNAVAILABLE"
    summary_reason = ""
    if lock_unavailable:
        status = "FAILED"
        exit_code = max(exit_code, 1)
        summary_reason = "PB1_ADVISORY_LOCK_UNAVAILABLE"
    elif not pb1_result_present:
        status = "FAILED"
        exit_code = max(exit_code, 1)
        summary_reason = "PB1_RESULT_MISSING"
    elif pb1_last == "FAIL_PRECHECK" or "DB_EXACT_FINAL30_ZERO" in pb1_reason:
        status = "FAILED"
        exit_code = 2
    else:
        status = "OK" if exit_code == 0 else "FAIL"
    if session == "close" and status == "OK" and str(os.getenv("PB1_LAST_RESULT_STATUS") or "").upper() == "SKIP_PHASE_WINDOW":
        status = "FAIL"
        exit_code = 2
        logger.error("[KR_CLOSE][FAIL] reason=CLOSE_PHASE_NOT_EXECUTED")
    if not summary_reason:
        summary_reason = "DB_EXACT_FINAL30_ZERO" if (pb1_last == "FAIL_PRECHECK" or "DB_EXACT_FINAL30_ZERO" in pb1_reason) else "PB1_SESSION_DONE"
    blocked = 0
    if session == "close" and balance_state is not None and balance_state.get("status") == "WARN":
        status = "WARN"
        summary_reason = "CLOSE_BALANCE_UNCONFIRMED"
        blocked = 1
        logger.warning("[KR_CLOSE][WARN] reason=BALANCE_UNCONFIRMED close_orders_blocked=1")
    entry_status = "ABORT" if exit_code != 0 or pb1_last in {"FAIL_PRECHECK", "ERROR", "SKIP_LOCKED"} or not pb1_result_present else "DONE"
    entry_reason = (pb1_reason or summary_reason) if entry_status == "ABORT" else None
    sell_orders_ack = extract_sell_orders_ack(pb1_result)
    logger.info("[KR_SESSION][SELL_ACK] session=%s sell_orders_ack=%s source=pb1_result", session, sell_orders_ack)
    marker = compute_session_marker(build_session_result(
        exit_code=exit_code, status=status, sell_orders_ack=sell_orders_ack,
        entry_status=entry_status, entry_abort_reason=entry_reason, fatal_error=(status in {"FAIL", "FAILED"} and exit_code != 0),
    ))
    pb1_status = str(pb1_result.get("status") or pb1_last or "").upper()
    pb1_exit_reason = str(pb1_result.get("exit_reason") or pb1_reason or "")
    status, summary_reason, completed, retryable = normalize_kr_session_completion(
        status=status,
        summary_reason=summary_reason,
        marker=marker,
        pb1_status=pb1_status,
        pb1_exit_reason=pb1_exit_reason,
    )
    if retryable and not completed and int(marker.get("sell_completed", 0)):
        status = "PARTIAL_SUCCESS_RETRYABLE"
    if session == "close":
        raw_balance = None
        try:
            raw_balance = KisAPI().get_balance()
        except Exception as exc:
            logger.warning("[KR_CLOSE][BALANCE_INCONSISTENT][RELOAD_WARN] err=%s", exc)
        guard = close_balance_guard(
            holdings_count=_balance_holdings_count(raw_balance) or 0,
            market_value=_balance_market_value(raw_balance),
            raw_balance=raw_balance if isinstance(raw_balance, dict) else {},
        )
        if guard.status == "WARN_BALANCE_INCONSISTENT":
            status = guard.status
            summary_reason = guard.reason
            completed = guard.completed
            retryable = guard.retryable
    logger.info("[KR_SESSION][DONE] session=%s status=%s exit_code=%s reason=%s completed=%s retryable=%s sell_orders_ack=%s entry_status=%s entry_reason=%s", session, status, exit_code, summary_reason, completed, retryable, sell_orders_ack, entry_status, entry_reason)
    orders_intent = int(pb1_result.get("order_intents_created", pb1_result.get("order_candidates", 0)) or 0)
    orders_submitted = int(pb1_result.get("broker_submitted", pb1_result.get("api_submitted", 0)) or 0)
    orders_ack = int(pb1_result.get("broker_acked", pb1_result.get("accepted", 0)) or 0)
    ack_db_failed = int(pb1_result.get("ack_db_failed_count", pb1_result.get("ack_db_failed", 0)) or 0)
    balance_confirmed = int(pb1_result.get("balance_confirmed_count", pb1_result.get("balance_reconcile_count", 0)) or 0)
    filled_confirmed = int(pb1_result.get("fills_confirmed", pb1_result.get("filled_confirmed_count", pb1_result.get("filled_confirmed", 0))) or 0)
    ack_without_fill = int(pb1_result.get("ack_without_confirmed_fill", 0) or 0)
    count_reconcile = reconcile_kr_order_counts(
        engine_order_count=orders_intent,
        broker_ack_count=orders_ack,
        db_ack_count=max(0, orders_ack - ack_db_failed),
        ack_db_failed_count=ack_db_failed,
        balance_confirmed_count=balance_confirmed,
        filled_confirmed_count=filled_confirmed,
        unresolved_ack_count=pb1_result.get("unresolved_acks", pb1_result.get("unresolved_ack_count")),
    )
    _stage(session, ctx.trade_date, ctx.expected_as_of, "daily_report_build", lambda: None)
    if summary_reason == "CLOSE_BALANCE_UNCONFIRMED":
        logger.info("[RUN_SUMMARY][RESULT] market=KR session=%s status=%s reason=%s orders_intent=%s orders_submitted=%s orders_ack=%s fills_confirmed=%s ack_without_fill=%s unresolved_ack=%s blocked=%s balance_state=TIMEOUT", session, status, summary_reason, orders_intent, orders_submitted, orders_ack, filled_confirmed, ack_without_fill, pb1_result.get("unresolved_acks", 0), blocked)
    else:
        logger.info("[RUN_SUMMARY][RESULT] market=KR session=%s status=%s reason=%s orders_intent=%s orders_submitted=%s orders_ack=%s fills_confirmed=%s ack_without_fill=%s unresolved_ack=%s blocked=%s", session, status, summary_reason, orders_intent, orders_submitted, orders_ack, filled_confirmed, ack_without_fill, pb1_result.get("unresolved_acks", 0), blocked)
    result = {"status": status, "final_status": status, "reason": summary_reason, "exit_code": exit_code, "completed": bool(completed), "retryable": bool(retryable), "engine_started": bool(pb1_result_present and not lock_unavailable), "pb1_result_present": bool(pb1_result_present), "orders_intent": orders_intent, "orders_submitted": orders_submitted, "orders_ack": orders_ack, "fills_confirmed": filled_confirmed, "ack_without_confirmed_fill": ack_without_fill, "unresolved_ack": int(pb1_result.get("unresolved_acks", 0) or 0), **count_reconcile}
    if lock_unavailable:
        result.update(lock_unavailable_result_fields())
    if session == "close":
        result.update({
            "phase": "close",
            "phase_executed": status != "FAIL",
            "force_phase": True,
            "skip_phase_window": status == "FAIL" and exit_code == 2,
            "entry_enabled": False,
            "exit_enabled": True,
            "close_enabled": True,
            "close_liquidation_enabled": True,
        })
    if balance_state is not None and balance_state.get("status") == "WARN":
        result["balance_fail_soft"] = balance_state
        result["balance_state"] = "TIMEOUT"
    return result


def _run_infinite_session_hook(*, session: str, env: str, checkpoint: str, allow_entry: bool) -> None:
    """Fail-soft orchestration hook; all sleeve policy remains isolated."""
    try:
        from trader.kr.infinite.runner import run_canonical_session
        result = run_canonical_session(session=session, env=env, allow_entry=allow_entry)
        logger.info("[KR_SESSION][INFINITE] session=%s checkpoint=%s allow_entry=%s decision=%s reason=%s",
                    session, checkpoint, int(allow_entry), result.decision.action.value, result.decision.reason)
    except Exception as exc:
        logger.exception("[KR_SESSION][INFINITE][FAIL_SOFT] session=%s checkpoint=%s err=%s",
                         session, checkpoint, exc)


def run_session(session: str, env: str = "practice") -> dict[str, Any]:
    session = session.strip().lower()
    if session == "prep":
        result = _run_prep(env)
    elif session in {"am", "afternoon", "close"}:
        result = _run_pb1_session(session, env)
    else:
        raise ValueError(f"unsupported session={session!r}")
    try:
        td = resolve_kr_trade_date(_now_kst())
        exp = resolve_kr_expected_as_of(td)
        write_kr_diagnostics_manifest(trade_date=td, expected_as_of=exp, session=session, result=result, env=env)
    except Exception as exc:
        logger.warning("[KR_DIAGNOSTICS][MANIFEST][WARN] err=%s", exc)
    return result


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(description="KR PB1 session runner")
    parser.add_argument("--session", required=True, choices=["prep", "am", "afternoon", "close"])
    parser.add_argument("--env", default="practice")
    parser.add_argument("--max-wait-seconds", type=int, default=None)
    args = parser.parse_args(argv)
    if args.max_wait_seconds is not None:
        os.environ["MAX_WAIT_SECONDS"] = str(args.max_wait_seconds)
    result = run_session(args.session, args.env)
    return exit_code_for_result(result)


if __name__ == "__main__":
    raise SystemExit(main())
