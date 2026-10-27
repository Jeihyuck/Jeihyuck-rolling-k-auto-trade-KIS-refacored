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
