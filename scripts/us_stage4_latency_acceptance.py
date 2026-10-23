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
    args = parser.parse_args()
    def rows():
        for path in args.log_paths:
            if not path.is_file():
                raise FileNotFoundError(str(path))
            with path.open("r", encoding="utf-8", errors="replace") as f:
                yield from f
    result = evaluate_logs(rows())
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
