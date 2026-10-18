#!/usr/bin/env python3
"""Read-only US tick latency report; never infers unobserved signal times.

Example:
  python scripts/us_latency_compare.py /path/to/baseline/logs /path/to/new/logs

The script has no DB, WS or KIS dependency and never submits orders.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Iterable

_FIELDS = {
    "tick_total_sec": ("[US_TICK][PHASE_BUDGET]", "total_ms"),
    "exit_engine_sec": ("[US_TICK][PHASE_BUDGET]", "exit_engine_ms"),
    "order_reconcile_sec": ("[US_TICK][PHASE_BUDGET]", "order_reconcile_ms"),
    "market_state_sec": ("[US_TICK][PHASE_BUDGET]", "market_state_ms"),
    "active_claim_replay_sec": ("[US_RECONCILE][ACTIVE_CLAIM_LATENCY]", "duration_ms"),
    "ack_reconcile_sec": ("[US_RECONCILE][ACK_LATENCY]", "duration_ms"),
    "exact_selected_sell_to_http_sec": (
        "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY]", "delay_ms"),
}
_PATTERN = re.compile(r"(?<![A-Za-z_])(\w+)=([0-9]+(?:\.[0-9]+)?)")


def _paths(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
    elif path.is_dir():
        yield from sorted(path.rglob("*.log"))


def _dist(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    def pct(p: float) -> float:
        pos = (len(ordered) - 1) * p
        low = int(pos)
        high = min(low + 1, len(ordered) - 1)
        return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)
    return {
        "count": len(ordered),
        "p50": round(pct(.5), 3),
        "p95": round(pct(.95), 3),
        "max": round(ordered[-1], 3),
        "underpowered_p95": len(ordered) < 20,
    }


def summarize(path: Path) -> dict:
    values = {key: [] for key in _FIELDS}
    counters = {
        "ws_hit": 0, "ws_fallback": 0,
        "sell_http_exact": 0, "sell_http_legacy_aggregate": 0,
        "early_pilot_submissions": 0,
        "snapshot_failure": 0,
        "files_scanned": 0,
    }
    for source in _paths(path):
        counters["files_scanned"] += 1
        for line in source.read_text(encoding="utf-8", errors="replace").splitlines():
            counters["ws_hit"] += int("[US_KIS][WS_COLLECTOR_HIT]" in line)
            counters["ws_fallback"] += int("[US_KIS][WS_COLLECTOR_FALLBACK]" in line)
            counters["early_pilot_submissions"] += int(
                "[US_EXIT][EARLY_HARD_STOP_PILOT]" in line
                and "action=route_via_canonical_router" in line
            )
            counters["snapshot_failure"] += int(
                "[US_EXIT][SNAPSHOT_OUTCOME]" in line and "outcome=SKIP" in line
            )
            if "[US_EXIT][SIGNAL_TO_BROKER_BOUNDARY]" in line:
                if "source=selected_intent_exact" in line:
                    counters["sell_http_exact"] += 1
                else:
                    counters["sell_http_legacy_aggregate"] += 1
            for key, (marker, field) in _FIELDS.items():
                if marker not in line:
                    continue
                if key == "exact_selected_sell_to_http_sec" and (
                    "source=selected_intent_exact" not in line
                ):
                    continue
                match = re.search(r"(?<![A-Za-z_])" + field + r"=([0-9]+(?:\.[0-9]+)?)", line)
                if match:
                    values[key].append(float(match.group(1)) / 1000.0)
    return {
        "path": str(path),
        "counters": counters,
        "distributions_seconds": {k: _dist(v) for k, v in values.items()},
        "measurement_policy": {
            "legacy_aggregate_signal_times_excluded": True,
            "unobserved_signals_are_not_assigned_times": True,
            "low_sample_p95_is_not_operational_proof": True,
            "broker_submit_is_not_fill": True,
        },
    }


def compare(baseline: dict, candidate: dict) -> dict:
    comparison = {}
    for key in _FIELDS:
        before = baseline["distributions_seconds"][key]
        after = candidate["distributions_seconds"][key]
        comparable = (
            before["count"] >= 20 and after["count"] >= 20
            and before["p95"] is not None and after["p95"] is not None
        )
        comparison[key] = {
            "baseline_p95_sec": before["p95"],
            "candidate_p95_sec": after["p95"],
            "p95_delta_sec": (
                round(after["p95"] - before["p95"], 3)
                if comparable else None
            ),
            "comparison_valid": comparable,
            "reason": (
                "same log metric with at least 20 observations per side"
                if comparable else "missing or insufficient comparable observations"
            ),
        }
    return comparison


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--output", type=Path)
    a = p.parse_args()
    baseline = summarize(a.baseline)
    candidate = summarize(a.candidate)
    output = {
        "baseline": baseline, "candidate": candidate,
        "p95_comparison": compare(baseline, candidate),
    }
    serialized = json.dumps(output, ensure_ascii=False, indent=2)
    if a.output:
        a.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)


if __name__ == "__main__":
    main()
