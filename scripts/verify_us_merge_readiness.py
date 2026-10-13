#!/usr/bin/env python3
"""Evidence-based US A+B+C operational merge gate; read-only.

Usage (deployed WSL after the *same SHA* has completed an actual US session):
  python scripts/verify_us_merge_readiness.py --root . --date 2026-10-13 \
    --expected-sha <exact-40-character-Git-SHA>

No orders, KIS REST calls, or DB writes are performed.
A CI/mock run alone can never satisfy this WSL evidence gate.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import re
import sys

_TICK_RE = re.compile(r"\[US_TICK\]\[LATENCY\]\s+tick=(\d+)\s+total_ms=([0-9.]+)")
_SELL_RE = re.compile(
    r"\[US_EXIT\]\[SIGNAL_TO_BROKER_BOUNDARY\].*?delay_ms=([0-9.]+)"
)
_FALLBACK_RE = re.compile(r"\[US_KIS\]\[WS_COLLECTOR_FALLBACK\]")
_ACCEPT_CLOSE = frozenset(("OK", "SUCCESS", "COMPLETED"))


def nearest_rank(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def evaluate(root: Path, trade_date: str, expected_sha: str,
             *, min_ticks: int = 30) -> dict:
    """Fail closed when a source, code revision or submitted SELL is unverified."""
    reasons = []
    metrics: dict = {"trade_date": trade_date, "expected_sha": expected_sha}
    daily = root / "reports" / "us_daily" / trade_date
    required_files = [daily / f"{s}_summary.json" for s in ("am", "afternoon")]
    summaries = []
    for path in required_files:
        try:
            summaries.append(json.loads(path.read_text(encoding="utf-8")))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            reasons.append(f"MISSING_OR_INVALID_SESSION_SUMMARY:{path.name}")
    try:
        close = json.loads((daily / "close_summary.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        close = {}
        reasons.append("MISSING_OR_INVALID_CLOSE_SUMMARY")
    # The collector is opt-in. Passing mock tests or a fast tick does not
    # demonstrate that production WSL can maintain fresh KIS websocket data.
    # The read-only smoke tool records a separate same-revision, same-day proof.
    smoke_path = daily / "ws_collector_smoke.json"
    try:
        ws_smoke = json.loads(smoke_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        ws_smoke = {}
        reasons.append("MISSING_OR_INVALID_LIVE_WS_COLLECTOR_EVIDENCE")
    metrics["ws_collector_smoke"] = {
        "evidence_at_utc": ws_smoke.get("evidence_at_utc"),
        "commit_sha": ws_smoke.get("commit_sha"),
        "verified_fresh_quote_for_all": ws_smoke.get("verified_fresh_quote_for_all"),
        "collector_generation": ws_smoke.get("collector_generation"),
        "mode": ws_smoke.get("mode"),
        "read_only": ws_smoke.get("no_order_submit"),
    }
    if ws_smoke and not (
        ws_smoke.get("commit_sha") == expected_sha
        and ws_smoke.get("mode") == "READ_ONLY_PRACTICE_MARKET_DATA"
        and ws_smoke.get("no_order_submit") is True
        and ws_smoke.get("verified_fresh_quote_for_all") is True
        and bool(ws_smoke.get("collector_generation"))
        and bool(ws_smoke.get("evidence_at_utc"))
        and bool(ws_smoke.get("symbols"))
        and all(
            int(v.get("hit") or 0) >= 1
            for v in (ws_smoke.get("symbols") or {}).values()
            if isinstance(v, dict)
        )
    ):
        reasons.append("LIVE_WS_COLLECTOR_EVIDENCE_FAILED_OR_SHA_MISMATCH")
    if len(expected_sha) != 40 or not all(ch in "0123456789abcdef" for ch in expected_sha.lower()):
        reasons.append("EXPECTED_SHA_MUST_BE_FULL_40_HEX_DIGITS")
    all_documents = summaries + ([close] if close else [])
    observed_shas = sorted(set(str(d.get("commit_sha") or d.get("sha") or "") for d in all_documents))
    metrics["reported_shas"] = observed_shas
    if not all_documents or any(sha != expected_sha for sha in observed_shas):
        reasons.append("EXECUTION_SHA_MISMATCH_OR_UNVERIFIED")

    tick_samples: list[float] = []
    sell_samples: list[float] = []
    fallback_count = 0
    for session in ("am", "afternoon"):
        folder = root / "runtime" / "logs" / "us" / trade_date / session
        paths = list(folder.glob("*.log"))
        if not paths:
            reasons.append(f"MISSING_RUNTIME_LOG:{session}")
        for path in paths:
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                if match := _TICK_RE.search(line):
                    tick_samples.append(float(match.group(2)) / 1000.0)
                if match := _SELL_RE.search(line):
                    sell_samples.append(float(match.group(1)) / 1000.0)
                if _FALLBACK_RE.search(line):
                    fallback_count += 1
    metrics.update({
        "measured_ticks": len(tick_samples),
        "tick_p95_sec": nearest_rank(tick_samples, 0.95),
        "tick_max_sec": max(tick_samples) if tick_samples else None,
        "sell_submits_measured": len(sell_samples),
        "sell_signal_to_broker_boundary_p95_sec": nearest_rank(sell_samples, 0.95),
        "sell_signal_to_broker_boundary_max_sec": max(sell_samples) if sell_samples else None,
        "ws_collector_fallback_log_count": fallback_count,
        "buy_entry_deferred_count": sum(
            int((d.get("tick_latency") or {}).get("entry_defer_count") or 0)
            for d in summaries
        ),
        "kis_quote_rest_calls": sum(
            int(((d.get("tick_latency") or {}).get("kis_http_calls") or {}).get("quote_http_calls") or 0)
            for d in summaries
        ),
        "close_status": str(close.get("status") or ""),
        "close_integrity_status": str(close.get("close_integrity_status") or ""),
        "close_claim_unresolved": int(
            (close.get("execution_claim_health") or {}).get("unresolved_execution_actions") or 0
        ),
        "close_claim_conflicts": int(
            (close.get("execution_claim_health") or {}).get("execution_claim_conflicts") or 0
        ),
        "close_broker_recovery_errors": len(close.get("broker_recovery_integrity_errors") or []),
        "close_runtime_liveness_failures": len(close.get("runtime_liveness_failures") or []),
        "close_broker_orders_unresolved": int(close.get("broker_orders_unresolved") or 0),
        "close_unresolved_submit_attempts": int(close.get("unresolved_submit_attempt_count") or 0),
        "close_unresolved_actions": len([
            err for err in (close.get("errors") or [])
            if str(err).startswith("UNRESOLVED_EXECUTION_ACTIONS")
        ]),
        "close_open_order_pending_total": int(close.get("open_order_pending_total") or 0),
        "close_manual_reconcile_required": int(close.get("manual_reconcile_required") or 0),
    })
    if len(tick_samples) < min_ticks:
        reasons.append("INSUFFICIENT_LIVE_TICK_SAMPLES")
    if not tick_samples or nearest_rank(tick_samples, 0.95) > 60.0:
        reasons.append("TICK_P95_ABOVE_60_SEC_OR_UNMEASURED")
    if not sell_samples:
        reasons.append("NO_ACTUAL_SELL_BROKER_SUBMISSION_LATENCY_EVIDENCE")
    elif nearest_rank(sell_samples, 0.95) > 15.0 or max(sell_samples) > 30.0:
        reasons.append("PROTECTIVE_SELL_LATENCY_LIMIT_FAILED")
    if metrics["buy_entry_deferred_count"] > 0:
        reasons.append("BUY_EVALUATION_DEFERRED_FOR_TIME")
    if metrics["close_status"] not in _ACCEPT_CLOSE:
        reasons.append("CLOSE_RECONCILIATION_NOT_OK")
    if metrics["close_unresolved_actions"] or metrics["close_manual_reconcile_required"]:
        reasons.append("UNRESOLVED_EXECUTION_OR_MANUAL_RECONCILIATION")
    if (
        metrics["close_claim_unresolved"] or metrics["close_claim_conflicts"]
        or metrics["close_broker_recovery_errors"]
        or metrics["close_runtime_liveness_failures"]
        or metrics["close_broker_orders_unresolved"]
        or metrics["close_unresolved_submit_attempts"]
        or metrics["close_integrity_status"] in {"RECONCILE_REQUIRED", "FAILED", "ERROR"}
    ):
        reasons.append("CLOSE_BROKER_CLAIM_OR_LIVENESS_NOT_CLEAN")
    # A broker-open order is not always a Close failure: it must be explained
    # by authoritative broker evidence, never auto-cleared to satisfy the gate.
    if metrics["close_open_order_pending_total"]:
        reasons.append("BROKER_OPEN_ORDERS_REQUIRE_CLOSE_TRUTH_REVIEW")
    metrics["reasons"] = sorted(set(reasons))
    metrics["result"] = "PASS" if not reasons else "NOT_MERGE_READY"
    metrics["note"] = "Live WSL session evidence only; CI and mocks must be reported separately"
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--date", required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--min-ticks", type=int, default=30)
    args = parser.parse_args()
    report = evaluate(args.root, args.date, args.expected_sha,
                      min_ticks=max(1, args.min_ticks))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["result"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
