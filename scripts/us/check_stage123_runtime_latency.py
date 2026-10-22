#!/usr/bin/env python3
"""Read-only WSL Stage 1/2/3 runtime performance gate.

Run against ORIGINAL WSL session logs only, AFTER separately confirming
source KIS practice native HTTP order evidence. This script cannot attest
native broker identity by itself, and CI must not call this a live KIS gate.

Examples:
 python scripts/us/check_stage123_runtime_latency.py runtime/us/logs/am.log
 python scripts/us/check_stage123_runtime_latency.py archive-am.log archive-pm.log

PASS means N>=20 unique selected SELL attempts AND N>=20 distinct completed
Ticks, selected-signal -> HTTP p95<=10s and full Tick p95<=60s. Missing,
unobserved, or zero actual attempts never passes.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

SIGNAL = re.compile(
    r"\[US_EXIT\]\[SIGNAL_TO_BROKER_BOUNDARY\].*?"
    r"attempt=(?P<attempt>\S+).*?signal_id=(?P<signal_id>\S+).*?"
    r"source=selected_intent_exact\s+delay_ms=(?P<latency>\d+(?:\.\d+)?)"
)
TICK = re.compile(
    r"\[US_TICK\]\[LATENCY\]\s+tick=(?P<tick>\S+)\s+"
    r"total_ms=(?P<latency>\d+(?:\.\d+)?)"
)
TIMESTAMP = re.compile(r"^(?P<time>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)")
NATIVE = re.compile(r"\[US_ORDER\]\[KIS_NATIVE_PERMISSION_PRECHECK\]\[BLOCK\]")


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * 0.95
    low = int(idx)
    high = min(len(ordered) - 1, low + 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (idx - low)


def evaluate_logs(text: str, *, min_samples: int = 20,
                  max_signal_sec: float = 10.0,
                  max_tick_sec: float = 60.0) -> dict:
    sells = {}
    ticks = {}
    invalid_signals = 0
    native_permission_blocks = 0
    tick_failures = 0
    tick_deadline_events = 0
    for line in text.splitlines():
        if "[US_TICK][DONE]" in line and ("status=FAILED" in line or "status=ERROR" in line):
            tick_failures += 1
        if "[US_TICK][TIMEOUT]" in line or "ORDER_ROUTE_BUDGET_EXHAUSTED" in line:
            tick_deadline_events += 1
        if NATIVE.search(line):
            native_permission_blocks += 1
        match = SIGNAL.search(line)
        if match:
            ident = match.group("attempt")
            source = match.group("signal_id")
            delay = float(match.group("latency")) / 1000.0
            if not ident or source in {"", "UNOBSERVED", "NONE"}:
                invalid_signals += 1
            else:
                if ident in sells and sells[ident] != delay:
                    invalid_signals += 1
                sells[ident] = delay
        match = TICK.search(line)
        if match:
            stamp = TIMESTAMP.search(line)
            if not stamp:
                continue
            key = (stamp.group("time"), match.group("tick"))
            ticks[key] = float(match.group("latency")) / 1000.0
    sell_values = list(sells.values())
    tick_values = list(ticks.values())
    sell_p95 = _p95(sell_values)
    tick_p95 = _p95(tick_values)
    reasons = []
    if len(sell_values) < min_samples:
        reasons.append("insufficient_independent_selected_sell_attempts")
    if len(tick_values) < min_samples:
        reasons.append("insufficient_completed_ticks")
    if invalid_signals:
        reasons.append("missing_or_conflicting_selected_signal_identity")
    if native_permission_blocks:
        reasons.append("native_KIS_permission_blocked_in_checked_logs")
    if tick_failures:
        reasons.append("tick_failures_present_in_evidence_window")
    if tick_deadline_events:
        reasons.append("tick_deadline_or_route_budget_exhausted")
    if sell_p95 is not None and sell_p95 > max_signal_sec:
        reasons.append("selected_sell_p95_above_target")
    if tick_p95 is not None and tick_p95 > max_tick_sec:
        reasons.append("full_tick_p95_above_target")
    return {
        "status": "PASS_LOG_TIMINGS_ONLY" if not reasons else "FAIL",
        "unique_selected_sell_attempts": len(sell_values),
        "completed_tick_samples": len(tick_values),
        "selected_sell_signal_to_http_p95_sec": sell_p95,
        "tick_total_p95_sec": tick_p95,
        "native_KIS_proven": False,
        "issues": reasons,
    }


def main(argv: list[str] | None = None) -> int:
    import json
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("logs", nargs="+", type=Path)
    p.add_argument("--min-samples", type=int, default=20)
    opts = p.parse_args(argv)
    if opts.min_samples < 20:
        p.error("minimum 20 samples required for production acceptance")
    try:
        text = "\n".join(path.read_text(encoding="utf-8", errors="replace")
                         for path in opts.logs)
    except OSError as exc:
        print(json.dumps({"status": "FAIL", "reason": "log_read_failed", "error": str(exc)}))
        return 2
    result = evaluate_logs(text, min_samples=opts.min_samples)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result["status"] == "PASS_LOG_TIMINGS_ONLY" else 1


if __name__ == "__main__":
    sys.exit(main())
