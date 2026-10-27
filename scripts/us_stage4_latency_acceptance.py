#!/usr/bin/env python3
"""Read-only Stage4 native-boundary latency acceptance from actual WSL logs.

Never invent missing signal IDs, HTTP submissions, fills or latency samples.
A native HTTP *boundary* is not a KIS ACK/fill. Unknown and blocked attempts
are counted separately, never treated as fast orders.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

SELECTED = re.compile(
    r"\[US_EXIT\]\[SIGNAL_TO_BROKER_BOUNDARY\]\s+"
    r"symbol=(?P<symbol>\S+)\s+attempt=(?P<attempt>\S+)\s+"
    r"signal_id=(?P<signal>\S+)\s+source=selected_intent_exact\s+"
    r"delay_ms=(?P<ms>[\d.]+)\s+owner=(?P<owner>\S+)\s+"
    r"exit_type=(?P<exit>\S+)\s+order_key=(?P<key>\S+)"
)
TICK = re.compile(
    r"\[US_TICK\]\[LATENCY\]\s+"
    r"tick=(?P<tick>\d+)\s+total_ms=(?P<ms>[\d.]+).*"
    r"session=(?P<session>\S+)\s+run=(?P<run>\S+)"
)
PROTECTIVE = frozenset({
    "hard_stop", "hard_stop_loss", "hard_stop_full_exit",
    "soft_stop", "soft_stop_loss",
})


STAGE4_READY_INTERVAL = re.compile(
    r"\[US_STAGE4\]\[READY_INTERVAL\]\s+interval_ms=(?P<ms>[\d.]+)"
)


def evaluate_stage4_ready_intervals(lines, *, min_samples: int = 20,
                                    p95_limit_ms: float = 25000.,
                                    p99_limit_ms: float = 30000.) -> dict:
    """Stage4 continuity includes deferred work, not just reconciliation body.

    A missing log, SQL failure, 30s+ interval, or too few samples never passes.
    """
    # Standalone WSL CLI must work with `python scripts/...` even if the
    # repository root is not included in sys.path; only stdlib is imported.
    def interval_percentiles(values_ms: list[float]) -> dict:
        sorted_ms = sorted(v for v in values_ms if math.isfinite(v) and v >= 0)
        if not sorted_ms:
            return {"count": 0, "p50_ms": None, "p95_ms": None,
                    "p99_ms": None, "max_ms": None}
        def rank(q: float) -> float:
            return round(sorted_ms[math.ceil(q * len(sorted_ms)) - 1], 3)
        return {"count": len(sorted_ms), "p50_ms": rank(.50),
                "p95_ms": rank(.95), "p99_ms": rank(.99),
                "max_ms": round(sorted_ms[-1], 3)}
    values = []
    reconcile_failures = 0
    stale_events = 0
    for line in lines:
        if "[US_STAGE4][RECONCILE_FAILURE]" in line:
            reconcile_failures += 1
        if "[US_STAGE4][READY_INTERVAL_STALE_RISK]" in line:
            stale_events += 1
        match = STAGE4_READY_INTERVAL.search(line)
        if match:
            values.append(float(match.group("ms")))
    pct = interval_percentiles(values)
    blockers = []
    if pct["count"] < min_samples:
        blockers.append("STAGE4_READY_INTERVAL_SAMPLES_INSUFFICIENT")
    if reconcile_failures:
        blockers.append("STAGE4_RECONCILE_FAILURE_OBSERVED")
    if stale_events or any(v >= 30000. for v in values):
        blockers.append("STAGE4_WRITER_STALE_RISK")
    if pct["p95_ms"] is not None and pct["p95_ms"] > p95_limit_ms:
        blockers.append("STAGE4_READY_INTERVAL_P95_TOO_HIGH")
    if pct["p99_ms"] is not None and pct["p99_ms"] > p99_limit_ms:
        blockers.append("STAGE4_READY_INTERVAL_P99_TOO_HIGH")
    return {
        "status": "PASS" if not blockers else "NOT_PROVEN",
        "source": "stage4_ready_interval_wsl_log",
        "broker_ack_fill_proven": False,
        "minimum_interval_samples": min_samples,
        "reconcile_failure_count": reconcile_failures,
        "stale_warning_count": stale_events,
        "p95_limit_ms": p95_limit_ms,
        "p99_limit_ms": p99_limit_ms,
        "blockers": blockers,
        **pct,
    }


def nearest_rank_p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3)


def evaluate_logs(lines, *, min_orders: int = 20, min_ticks: int = 20) -> dict:
    selected: dict[tuple[str, str], float] = {}
    ticks: dict[tuple[str, str, str], float] = {}
    other_sells = 0
    pre_io_failures = 0
    unknown_result = 0
    missing_identity = 0
    for line in lines:
        if "[US_ORDER][PRE_IO_FAILURE]" in line:
            pre_io_failures += 1
        if "[US_ORDER][BROKER_CALL_EXCEPTION]" in line:
            unknown_result += 1
        signal = SELECTED.search(line)
        if signal:
            row = signal.groupdict()
            if (row["owner"] != "US_STANDARD" or
                    row["exit"].lower() not in PROTECTIVE or
                    row["symbol"].upper() == "TQQQ"):
                other_sells += 1
                continue
            if row["signal"] in {"UNOBSERVED", "UNKNOWN", ""} or row["key"] in {"UNKNOWN", "", "UNOBSERVED"}:
                missing_identity += 1
                continue
            duration = float(row["ms"])
            if not math.isfinite(duration) or duration < 0:
                missing_identity += 1
                continue
            # Distinct original order key, not repeated callback/attempt logs.
            identity = (row["symbol"].upper(), row["key"])
            selected[identity] = max(duration, selected.get(identity, 0))
        tick = TICK.search(line)
        if tick:
            row = tick.groupdict()
            duration = float(row["ms"])
            if math.isfinite(duration) and duration >= 0:
                identity = (row["run"], row["session"], row["tick"])
                ticks[identity] = max(duration, ticks.get(identity, 0))
    sell_values = list(selected.values())
    tick_values = list(ticks.values())
    sell_p95 = nearest_rank_p95(sell_values)
    tick_p95 = nearest_rank_p95(tick_values)
    sell_pass = len(sell_values) >= min_orders and sell_p95 is not None and sell_p95 <= 10_000
    tick_pass = len(tick_values) >= min_ticks and tick_p95 is not None and tick_p95 <= 60_000
    return {
        "status": "PASS" if (sell_pass and tick_pass) else "NOT_PROVEN",
        "native_boundary_is_not_kis_ack_or_fill": True,
        "protective_owner": "US_STANDARD",
        "protected_exit_types": sorted(PROTECTIVE),
        "selected_protective_unique_order_count": len(sell_values),
        "selected_protective_signal_to_http_p95_ms": sell_p95,
        "signal_to_http_p95_target_ms": 10_000,
        "signal_to_http_gate": "PASS" if sell_pass else "NOT_PROVEN",
        "comparable_unique_tick_count": len(tick_values),
        "whole_tick_p95_ms": tick_p95,
        "whole_tick_p95_target_ms": 60_000,
        "whole_tick_gate": "PASS" if tick_pass else "NOT_PROVEN",
        "minimum_distinct_protective_orders": min_orders,
        "minimum_comparable_ticks": min_ticks,
        "excluded_nonprotective_or_other_owner_signals": other_sells,
        "excluded_missing_selected_identity": missing_identity,
        "pre_http_failures_not_counted_as_http": pre_io_failures,
        "broker_call_exceptions_separate_from_success": unknown_result,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_paths", nargs="+", type=Path)
    parser.add_argument("--require-stage4-ready", action="store_true",
                        help="Also fail unless >=20 verified Stage4 READY intervals meet 30s freshness")
    args = parser.parse_args()
    def rows():
        for path in args.log_paths:
            if not path.is_file():
                raise FileNotFoundError(str(path))
            with path.open("r", encoding="utf-8", errors="replace") as f:
                yield from f
    lines = list(rows())
    result = evaluate_logs(lines)
    stage4_result = evaluate_stage4_ready_intervals(lines)
    result["stage4_writer_ready_continuity"] = stage4_result
    if args.require_stage4_ready and stage4_result["status"] != "PASS":
        result["status"] = "NOT_PROVEN"
        result["stage4_writer_acceptance_required"] = True
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
