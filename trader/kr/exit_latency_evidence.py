"""Pure timing classification; never alters SELL policy or order quantity."""
from __future__ import annotations

from math import isfinite


def classify_exit_evaluation_latency(elapsed_sec: float) -> str:
    try:
        elapsed = float(elapsed_sec)
    except (ValueError, TypeError):
        return "UNKNOWN"
    if not isfinite(elapsed) or elapsed < 0:
        return "UNKNOWN"
    if elapsed > 30.0:
        return "CRITICAL"
    if elapsed > 10.0:
        return "SLOW"
    return "OK"
