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


def timed_ordered_exit_history(records, *, clock=None):
    """Observe wait for serial exit-history delivery without reordering it.

    This measures delays BEFORE exit policy evaluation, which evaluation-only
    timing misses. No broker calls, timeout changes or parallel SELL execution.
    """
    from time import monotonic
    now = clock or monotonic
    iterator = iter(records)
    while True:
        started = now()
        try:
            record = next(iterator)
        except StopIteration:
            return
        waited = max(0.0, now() - started)
        yield waited, record
