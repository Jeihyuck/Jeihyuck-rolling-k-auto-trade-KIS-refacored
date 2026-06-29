# -*- coding: utf-8 -*-
"""KR PB1 session-specific runner.

This module is intentionally thin: WSL session scripts call this KR/PB1 entrypoint
instead of the old all-in-one trader entrypoint. It enforces prep artifacts,
preopen handling, and KIS balance SAFE_STOP before handing live sessions to the
PB1 runner.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, time
from pathlib import Path
from typing import Any

from trader.kis_wrapper import KisAPI, KisBalanceUnavailable, resolve_kr_balance_fail_soft
from trader.kr.calendar import resolve_kr_expected_as_of, resolve_kr_trade_date
from trader.kr.artifacts import (
    validate_kr_prep_artifact,
    quarantine_stale_kr_artifacts,
    rescue_kr_final30_from_db,
)
from trader.kr.diagnostics import write_kr_diagnostics_manifest
from trader.kr.runner.session_policy import exit_code_for_result, kr_prep_schedule_guard, now_kst, wait_until_kr_am_target

logger = logging.getLogger(__name__)

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
    td = os.getenv("KR_TRADE_DATE") or _now_kst().strftime("%Y-%m-%d")
    out = ROOT / "runtime/kr/session" / td / session / "balance_precheck.json"
    try:
        raw = KisAPI().get_balance_cached()
        snapshot = raw[0] if isinstance(raw, tuple) else raw
        snapshot = snapshot if isinstance(snapshot, dict) else None
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
            logger.warning("[KR_SESSION][BALANCE_FAIL_SOFT] session=%s reason=%s", session, fail_soft.get("reason"))
            entry_allowed = False
            exit_allowed = bool(fail_soft.get("exit_allowed")) or os.getenv("KR_ALLOW_BALANCE_CACHE_FOR_EXIT", "1") == "1"
            close_allowed = session == "close" or os.getenv("KR_ALLOW_BALANCE_CACHE_FOR_CLOSE", "1") == "1"
            pre = BalancePrecheck("TIMEOUT", "CACHE" if (exit_allowed or close_allowed) else "NONE", _now_kst(), entry_allowed, exit_allowed, close_allowed, fail_soft.get("reason", "BALANCE_TIMEOUT_FAIL_SOFT"), raw_snapshot_available=False)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(asdict(pre), ensure_ascii=False, default=str, indent=2), encoding="utf-8")
            os.environ["KR_BALANCE_PRECHECK_PATH"] = str(out)
            logger.info("[KR_SESSION][BALANCE_PRECHECK] state=%s source=%s entry_allowed=%d exit_allowed=%d close_allowed=%d snapshot=0", pre.state, pre.source, int(pre.entry_allowed), int(pre.exit_allowed), int(pre.close_allowed))
            return {
                "status": "WARN",
                "reason": fail_soft.get("reason", "BALANCE_TIMEOUT_FAIL_SOFT"),
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


def _run_pb1_session(session: str, env: str) -> dict[str, Any]:
    ctx = _session_context(session, env)
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
        os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] = "1"
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
                    "close_liquidation_enabled": True,
                    "created_at_kst": _now_kst().isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        logger.info("[KR_CLOSE][PHASE] phase=close entry_enabled=0 exit_enabled=1 close_enabled=1 close_liquidation_enabled=1")
    guarded = _guard_trade_session(session, ctx)
    if guarded is not None:
        return guarded
    balance_state = _assert_balance_available(session)
    if balance_state is not None:
        if balance_state.get("status") == "WARN" and int(balance_state.get("exit_allowed", 0)) == 1:
            os.environ["ENTRY_ALLOWED"] = str(int(balance_state.get("entry_allowed", 0)))
            os.environ["EXIT_ALLOWED"] = str(int(balance_state.get("exit_allowed", 0)))
            os.environ["ORDER_ALLOWED"] = str(int(balance_state.get("order_allowed", 0)))
            os.environ["KR_BALANCE_FAIL_SOFT_ACTIVE"] = "1"
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
        exit_code = int(pb1_runner.main() or 0)
    finally:
        sys.argv = old_argv
    if pb1_result_path.exists():
        pb1_result = load_pb1_session_result(pb1_result_path)
        logger.info(
            "[KR_SESSION][PB1_RESULT][LOAD_OK] path=%s keys=%s sell_orders_ack=%s",
            pb1_result_path, sorted(pb1_result.keys()), extract_sell_orders_ack(pb1_result),
        )
    else:
        pb1_result = {}
        logger.warning(
            "[KR_SESSION][PB1_RESULT][MISSING] path=%s exit_code=%s action=continue_with_zero_sell_ack",
            pb1_result_path, exit_code,
        )
    pb1_last = str(os.getenv("PB1_LAST_RESULT_STATUS") or "").upper()
    pb1_reason = str(os.getenv("PB1_LAST_EXIT_REASON") or "")
    if pb1_last == "FAIL_PRECHECK" or "DB_EXACT_FINAL30_ZERO" in pb1_reason:
        status = "FAILED"
        exit_code = 2
    else:
        status = "OK" if exit_code == 0 else "FAIL"
    if session == "close" and status == "OK" and str(os.getenv("PB1_LAST_RESULT_STATUS") or "").upper() == "SKIP_PHASE_WINDOW":
        status = "FAIL"
        exit_code = 2
        logger.error("[KR_CLOSE][FAIL] reason=CLOSE_PHASE_NOT_EXECUTED")
    summary_reason = "DB_EXACT_FINAL30_ZERO" if (pb1_last == "FAIL_PRECHECK" or "DB_EXACT_FINAL30_ZERO" in pb1_reason) else "PB1_SESSION_DONE"
    blocked = 0
    if session == "close" and balance_state is not None and balance_state.get("status") == "WARN":
        status = "WARN"
        summary_reason = "CLOSE_BALANCE_UNCONFIRMED"
        blocked = 1
        logger.warning("[KR_CLOSE][WARN] reason=BALANCE_UNCONFIRMED close_orders_blocked=1")
    entry_status = "ABORT" if exit_code != 0 or pb1_last in {"FAIL_PRECHECK", "ERROR"} else "DONE"
    entry_reason = pb1_reason if entry_status == "ABORT" else None
    sell_orders_ack = extract_sell_orders_ack(pb1_result)
    logger.info("[KR_SESSION][SELL_ACK] session=%s sell_orders_ack=%s source=pb1_result", session, sell_orders_ack)
    marker = compute_session_marker(build_session_result(
        exit_code=exit_code, status=status, sell_orders_ack=sell_orders_ack,
        entry_status=entry_status, entry_abort_reason=entry_reason, fatal_error=(status in {"FAIL", "FAILED"} and exit_code != 0),
    ))
    retryable_reasons = {"DB_EXACT_FINAL30_ZERO", "CLOSE_BALANCE_UNCONFIRMED", "BALANCE_TIMEOUT_FAIL_SOFT"}
    completed = int(marker["completed"])
    retryable = int(marker["retryable"] or summary_reason in retryable_reasons or status == "WARN")
    if retryable and not completed and int(marker.get("sell_completed", 0)):
        status = "PARTIAL_SUCCESS_RETRYABLE"
    if summary_reason in retryable_reasons:
        completed = 0
        retryable = 1
    logger.info("[KR_SESSION][DONE] session=%s status=%s exit_code=%s reason=%s completed=%s retryable=%s sell_orders_ack=%s entry_status=%s entry_reason=%s", session, status, exit_code, summary_reason, completed, retryable, sell_orders_ack, entry_status, entry_reason)
    if summary_reason == "CLOSE_BALANCE_UNCONFIRMED":
        logger.info("[RUN_SUMMARY][RESULT] market=KR session=%s status=%s reason=%s orders_intent=0 orders_ack=0 blocked=%s balance_state=TIMEOUT", session, status, summary_reason, blocked)
    else:
        logger.info("[RUN_SUMMARY][RESULT] market=KR session=%s status=%s reason=%s orders_intent=0 orders_ack=0 blocked=%s", session, status, summary_reason, blocked)
    result = {"status": status, "reason": summary_reason, "exit_code": exit_code, "completed": bool(completed), "retryable": bool(retryable)}
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
