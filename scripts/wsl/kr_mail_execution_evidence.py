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
    if not raw_rel.startswith(expected_prefix) or "/../" in raw_rel or "\\\\" in raw_rel:
        return "LOG_PATH_UNVERIFIED"
    log_path = stage / raw_rel
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return str(attempt.get("reason") or "MISSING_LOG")
    for line in reversed(lines):
        if "[KR_SESSION][PRECHECK_FAIL]" in line:
            match = re.search(r"\\breason=([A-Z][A-Z_0-9]+)", line)
            if match:
                return match.group(1)
        if "[RUN_SUMMARY][RESULT]" in line and "status=FAIL" in line:
            match = re.search(r"\\breason=([A-Z][A-Z_0-9]+)", line)
            if match:
                return match.group(1)
    return str(attempt.get("reason") or "UNSPECIFIED")


def collect_kr_execution_status(stage: Path, trade: str) -> dict[str, Any]:
    stage = Path(stage)
    manifest = _json(stage / "runtime" / "logs" / "kr" / trade / "session-manifest.json")
    sessions = manifest.get("sessions") if isinstance(manifest.get("sessions"), dict) else {}
    summary: dict[str, dict[str, str]] = {}
    any_failed = False
    any_missing = False
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
    pb1 = _json(stage / "runtime" / "kr" / "session" / trade / "close" / "pb1_result.json")
    economic = str(pb1.get("economic_settlement_state") or "UNKNOWN")
    try:
        native_gap = int(pb1.get("native_priced_sell_gap_count") or 0)
    except (TypeError, ValueError):
        native_gap = None
    if pb1.get("economic_settlement_pending") in (True, 1) or (native_gap is not None and native_gap > 0):
        economic = "ECONOMIC_SETTLEMENT_PENDING"
    if any_failed:
        daily = "FAILED"
    elif any_missing or economic in {"UNKNOWN", "ECONOMIC_SETTLEMENT_PENDING"}:
        daily = "DEGRADED"
    else:
        daily = "COMPLETED"
    summary_tokens = [
        f"{name.upper()}={item['status']}" + (
            f":{item['reason']}" if item["status"] in {"FAILED","FAIL","TIMEOUT","RETRYABLE_DEGRADED"} else ""
        )
        for name, item in summary.items()
    ]
    summary_tokens.append(f"CLOSE_SETTLEMENT={economic}")
    if native_gap is not None:
        summary_tokens.append(f"SELL_PRICING_GAPS={native_gap}")
    # Never claim an all-session zero-order day from CLOSE counters alone.
    if "broker_submitted" in pb1:
        summary_tokens.append(f"CLOSE_ORDERS_SUBMITTED={pb1['broker_submitted']}")
    return {
        "daily_execution_status": daily,
        "execution_sessions": summary,
        "close_economic_settlement_state": economic,
        "close_native_priced_sell_gap_count": native_gap,
        "execution_summary": "; ".join(summary_tokens),
    }
