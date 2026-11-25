"""Deterministic KR log-mail execution truth, independent of PREP generation.

Only read evidence already packaged in the mail staging directory. This tool
never contacts KIS, a DB, or a trading runner and never authorizes an order.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def _json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _latest_session(entry: dict) -> dict:
    if not isinstance(entry, dict):
        return {}
    attempts = entry.get("attempts")
    if not isinstance(attempts, list):
        return {}
    effective_id = entry.get("effective_run_id")
    candidates = [x for x in attempts if isinstance(x, dict)]
    for attempt in reversed(candidates):
        if attempt.get("run_id") == effective_id and effective_id:
            return attempt
    return candidates[-1] if candidates else {}


def _failure_reason(stage: Path, trade: str, purpose: str, attempt: dict) -> str:
    """Prefer the actual precheck reason over generic EXIT_CODE_1."""
    raw_rel = attempt.get("log_file")
    if not isinstance(raw_rel, str):
        return str(attempt.get("reason") or "MISSING_LOG")
    expected_prefix = f"runtime/logs/kr/{trade}/{purpose}/"
    if not raw_rel.startswith(expected_prefix) or "/../" in raw_rel or "\\" in raw_rel:
        return "LOG_PATH_UNVERIFIED"
    log_path = stage / raw_rel
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return str(attempt.get("reason") or "MISSING_LOG")
    for line in reversed(lines):
        if "[KR_SESSION][PRECHECK_FAIL]" in line:
            match = re.search(r"\breason=([A-Z][A-Z_0-9]+)", line)
            if match:
                return match.group(1)
        if "[RUN_SUMMARY][RESULT]" in line and "status=FAIL" in line:
            match = re.search(r"\breason=([A-Z][A-Z_0-9]+)", line)
            if match:
                return match.group(1)
    return str(attempt.get("reason") or "UNSPECIFIED")


def _session_order_activity(stage: Path, trade: str, purpose: str, attempt: dict) -> dict:
    """Count distinct terminal tick markers and owner-specific outcomes, not HTTP retries."""
    raw = attempt.get("log_file")
    prefix = f"runtime/logs/kr/{trade}/{purpose}/"
    unknown = {"evidence": "MISSING_OR_UNVERIFIED", "market_data_error_ticks": None,
               "infinite_order_attempts": None, "infinite_explicit_rejections": None,
               "infinite_unresolved_attempts": None, "infinite_acknowledged_attempts": None}
    if not isinstance(raw, str) or not raw.startswith(prefix) or "\\" in raw:
        return unknown
    try:
        path = (stage / raw).resolve(strict=True)
        path.relative_to((stage / prefix).resolve())
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except (OSError, ValueError):
        return unknown
    states = []
    legacy_block = 0
    data_errors = 0
    for line in lines:
        if "[RUN_SUMMARY][RESULT]" in line and "status=RETRYABLE_MARKET_DATA_ERROR" in line:
            data_errors += 1
        if "[KR_INF][TICK][AFTER_PROTECTIVE_SELL]" in line:
            match = re.search(r" attempted=([01]) submission_state=([A-Z_]+)(?:\s|$)", line)
            if match:
                states.append((match.group(1) == "1", match.group(2)))
            elif "decision=BLOCK" in line:
                legacy_block += 1
    return {"evidence": "LOG_READ", "market_data_error_ticks": data_errors,
            "infinite_order_attempts": sum(attempted for attempted, _ in states) if states else None,
            "infinite_explicit_rejections": sum(s == "REJECTED_EXPLICIT" for _, s in states) if states else None,
            "infinite_unresolved_attempts": sum(s == "UNRESOLVED" for _, s in states) if states else None,
            "infinite_acknowledged_attempts": sum(s == "ACKED" for _, s in states) if states else None,
            "infinite_legacy_block_decisions": legacy_block}


def collect_kr_execution_status(stage: Path, trade: str) -> dict[str, Any]:
    stage = Path(stage)
    manifest = _json(stage / "runtime" / "logs" / "kr" / trade / "session-manifest.json")
    sessions = manifest.get("sessions") if isinstance(manifest.get("sessions"), dict) else {}
    summary: dict[str, dict[str, str]] = {}
    any_failed = False
    any_missing = False
    watchdog_alerts: dict[str, list[str]] = {}
    order_activity: dict[str, dict] = {}
    for purpose in ("prep", "am", "afternoon", "close"):
        attempt = _latest_session(sessions.get(purpose, {}))
        if not attempt:
            summary[purpose] = {"status": "MISSING", "reason": "SESSION_LOG_MISSING"}
            any_missing = True
            continue
        status = str(attempt.get("status") or "UNKNOWN").upper()
        reason = str(attempt.get("reason") or "")
        if status in {"FAILED", "FAIL", "TIMEOUT", "RETRYABLE_DEGRADED"}:
            any_failed = True
            reason = _failure_reason(stage, trade, purpose, attempt)
        elif status not in {"OK", "OK_WITH_WARNINGS", "WARN", "DEGRADED", "SKIPPED_NON_TRADING_DAY"}:
            any_missing = True
        summary[purpose] = {"status": status, "reason": reason}
        if purpose in {"am", "afternoon", "close"}:
            order_activity[purpose] = _session_order_activity(stage, trade, purpose, attempt)
        if purpose in {"am", "afternoon"}:
            # Exact effective attempt only. Report historical watchdog alerts
            # even if later recovered; they are not fabricated broker faults.
            run_id = str(attempt.get("run_id") or "")
            if re.fullmatch(r"[A-Za-z0-9_.-]+", run_id) and run_id not in {".", ".."}:
                monitor = stage / "runtime" / "logs" / "kr" / trade / purpose / (run_id+".watchdog.log")
                observed = set()
                try:
                    with monitor.open(encoding="utf-8") as file:
                        for line in file:
                            prefix = "[KR][SESSION_WATCHDOG] "
                            if not line.startswith(prefix):
                                continue
                            try:
                                row = json.loads(line[len(prefix):])
                            except (ValueError, TypeError):
                                continue
                            if (isinstance(row, dict) and row.get("date") == trade
                                    and row.get("session") == purpose and row.get("run_id") == run_id):
                                observed.update(str(x) for x in row.get("alerts", [])
                                    if isinstance(x, str) and re.fullmatch(r"[A-Z_]+", x))
                except OSError:
                    pass
                if observed:
                    watchdog_alerts[purpose] = sorted(observed)
    pb1 = _json(stage / "runtime" / "kr" / "session" / trade / "close" / "pb1_result.json")
    economic = str(pb1.get("economic_settlement_state") or "UNKNOWN")
    today_economic = str(pb1.get("today_economic_settlement_state") or "NOT_REPORTED")
    historical_economic = str(pb1.get("historical_economic_settlement_state") or "NOT_REPORTED")
    try:
        native_gap = int(pb1.get("native_priced_sell_gap_count") or 0)
    except (TypeError, ValueError):
        native_gap = None
    if pb1.get("economic_settlement_pending") in (True, 1) or (native_gap is not None and native_gap > 0):
        economic = "ECONOMIC_SETTLEMENT_PENDING"
    activity_degraded = any(
        any((row.get(key) or 0) > 0 for key in ("market_data_error_ticks",
            "infinite_explicit_rejections", "infinite_unresolved_attempts", "infinite_legacy_block_decisions"))
        for row in order_activity.values()
    )
    if any_failed:
        daily = "FAILED"
    elif any_missing or activity_degraded or economic in {"UNKNOWN", "ECONOMIC_SETTLEMENT_PENDING"}:
        daily = "DEGRADED"
    else:
        daily = "COMPLETED"
    summary_tokens = [
        f"{name.upper()}={item['status']}" + (
            f":{item['reason']}" if item["status"] in {"FAILED","FAIL","TIMEOUT","RETRYABLE_DEGRADED"} else ""
        )
        for name, item in summary.items()
    ]
    for purpose, alerts in watchdog_alerts.items():
        summary_tokens.append(f"{purpose.upper()}_WATCHDOG=" + ",".join(alerts))
    for purpose, row in order_activity.items():
        summary_tokens.append(f"{purpose.upper()}_MARKET_DATA_ERROR_TICKS={row.get('market_data_error_ticks')}")
        summary_tokens.append(f"{purpose.upper()}_INFINITE_ATTEMPTS={row.get('infinite_order_attempts')}")
        summary_tokens.append(f"{purpose.upper()}_INFINITE_REJECTED={row.get('infinite_explicit_rejections')}")
        summary_tokens.append(f"{purpose.upper()}_INFINITE_UNRESOLVED={row.get('infinite_unresolved_attempts')}")
    summary_tokens.append(f"CLOSE_SETTLEMENT={economic}")
    summary_tokens.append(f"TODAY_SETTLEMENT={today_economic}")
    summary_tokens.append(f"HISTORICAL_SETTLEMENT={historical_economic}")
    if native_gap is not None:
        summary_tokens.append(f"SELL_PRICING_GAPS={native_gap}")
    # Never claim an all-session zero-order day from CLOSE counters alone.
    if "broker_submitted" in pb1:
        summary_tokens.append(f"CLOSE_ORDERS_SUBMITTED={pb1['broker_submitted']}")
    return {
        "daily_execution_status": daily,
        "execution_sessions": summary,
        "watchdog_alerts": watchdog_alerts,
        "order_activity_by_session": order_activity,
        "close_economic_settlement_state": economic,
        "close_today_economic_settlement_state": today_economic,
        "close_historical_economic_settlement_state": historical_economic,
        "close_today_priced_sell_gap_count": pb1.get("today_priced_sell_gap_count"),
        "close_historical_priced_sell_gap_count": pb1.get("historical_priced_sell_gap_count"),
        "close_native_priced_sell_gap_count": native_gap,
        "execution_summary": "; ".join(summary_tokens),
    }
