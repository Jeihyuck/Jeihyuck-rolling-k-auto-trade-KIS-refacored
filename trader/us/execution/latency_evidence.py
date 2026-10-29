"""Side-effect-free latency aggregation for US order audit. No broker path changes."""
from __future__ import annotations
import math
from statistics import median

PHASES = ("signal_to_intent", "intent_to_risk", "risk_to_submit",
          "submit_to_ack", "ack_to_fill", "fill_to_persist", "stage4_ready_interval")

def percentile(values: list[float], fraction: float) -> float | None:
    vals = sorted(float(x) for x in values)
    if not vals:
        return None
    index = (len(vals) - 1) * fraction
    lo = int(index)
    hi = min(lo + 1, len(vals) - 1)
    return round(vals[lo] + (vals[hi] - vals[lo]) * (index - lo), 4)

def latency_summary(samples: list[dict], *, ready_stale_seconds: float = 30.0) -> dict:
    """Analyze elapsed *monotonic* phase durations in seconds; never count missing as zero."""
    out = {"sample_count": len(samples), "phases": {}, "invalid_count": 0}
    for phase in PHASES:
        numbers = []
        missing = 0
        for sample in samples:
            val = sample.get(phase)
            if val is None:
                missing += 1
                continue
            try:
                value = float(val)
            except (ValueError, TypeError):
                out["invalid_count"] += 1
                continue
            if not math.isfinite(value) or value < 0:
                out["invalid_count"] += 1
                continue
            numbers.append(value)
        out["phases"][phase] = {
            "count": len(numbers), "missing_count": missing,
            "p50_sec": percentile(numbers, 0.50),
            "p95_sec": percentile(numbers, 0.95),
            "p99_sec": percentile(numbers, 0.99),
            "max_sec": max(numbers) if numbers else None,
            "stale_risk_count": (sum(x >= ready_stale_seconds for x in numbers)
                                 if phase == "stage4_ready_interval" else None),
        }
    return out
