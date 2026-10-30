"""Pure phase metrics for the independent US Stage4 reconciliation worker.

Monotonic phase durations make the 2026-10-27 >30s writer-stale
incident attributable without relying on broker query timestamps.
No broker order authority or health threshold is changed here.
"""
from __future__ import annotations

import time
from typing import Callable


class Stage4PhaseTimer:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self._started = clock()
        self._previous = self._started
        self.phases_ms: dict[str, float] = {}

    def mark(self, label: str) -> float:
        current = self.clock()
        elapsed = max(0.0, (current - self._previous) * 1000.0)
        self.phases_ms[str(label)] = round(elapsed, 3)
        self._previous = current
        return elapsed

    def summary(self) -> dict:
        return {"total_ms": round(max(0.0, (self.clock() - self._started) * 1000.0), 3),
                "phases_ms": dict(self.phases_ms)}


def latency_percentiles(samples_ms: list[float]) -> dict:
    """Nearest-rank p50/p95/p99; intentionally no interpolation below max."""
    import math
    values = sorted(float(x) for x in samples_ms if math.isfinite(float(x)) and float(x) >= 0)
    if not values:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None,
                "max_ms": None}
    def rank(q: float) -> float:
        return round(values[max(0, math.ceil(len(values) * q) - 1)], 3)
    return {
        "count": len(values),
        "p50_ms": rank(0.50), "p95_ms": rank(0.95),
        "p99_ms": rank(0.99), "max_ms": round(values[-1], 3),
    }


class RollingStage4Latency:
    """Bounded rolling window of ready-to-ready gaps (not just KIS query)."""
    def __init__(self, size: int = 128):
        from collections import deque
        self._samples = deque(maxlen=max(1, int(size)))

    def observe(self, duration_ms: float) -> dict:
        import math
        if math.isfinite(float(duration_ms)) and float(duration_ms) >= 0:
            self._samples.append(float(duration_ms))
        return latency_percentiles(list(self._samples))
