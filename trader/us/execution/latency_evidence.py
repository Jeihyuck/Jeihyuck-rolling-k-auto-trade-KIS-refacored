"""Side-effect-free latency aggregation for US order audit. No broker path changes."""
from __future__ import annotations
import math
from statistics import median

PHASES = ("signal_to_intent", "intent_to_risk", "risk_to_submit",
          "submit_to_ack", "ack_to_fill", "fill_to_persist", "stage4_ready_interval",
          "broker_http")

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
    for phase in PHASES + TICK_PHASES:
        numbers = []
        missing = 0
        for sample in samples:
            if phase == "broker_http":
                nested = sample.get("broker_http_by_attempt_sec")
                if isinstance(nested, dict):
                    for val in nested.values():
                        try:
                            duration = float(val)
                        except (ValueError, TypeError):
                            out["invalid_count"] += 1
                            continue
                        if math.isfinite(duration) and duration >= 0:
                            numbers.append(duration)
                        else:
                            out["invalid_count"] += 1
                    continue
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

# Tick-stage metrics already exist in trade_tick_runner; preserve their own
# non-overlapping names instead of pretending they are per-order timestamps.
TICK_PHASE_MAP = {
    "tick_total": "tick_total_ms",
    "cash_fetch": "cash_fetch_ms",
    "fill_fetch": "fill_fetch_stage_ms",
    "fill_persist": "fill_persist_ms",
    "exit_eval": "exit_engine_ms",
    "entry_eval": "entry_engine_ms",
    "order_route": "order_route_ms",
    "post_route_reconcile": "post_route_reconcile_ms",
}
TICK_PHASES = tuple(TICK_PHASE_MAP)

def observed_tick_sample(metrics: dict) -> dict:
    """Create a real metrics-based sample; absent stages remain absent."""
    out = {}
    for target, source in TICK_PHASE_MAP.items():
        value = metrics.get(source)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (ValueError, TypeError):
            continue
        if math.isfinite(numeric) and numeric >= 0:
            out[target] = round(numeric / 1000.0, 6)
    # KIS call returns are not broker ACKs/fills. Keep separate timing field.
    call_attempts = metrics.get("broker_http_by_attempt_ms")
    if isinstance(call_attempts, dict):
        out["broker_http_by_attempt_sec"] = {
            str(k): round(float(v) / 1000.0, 6)
            for k, v in list(call_attempts.items())[:128]
            if isinstance(v, (int, float)) and math.isfinite(float(v)) and v >= 0
        }
    return out
