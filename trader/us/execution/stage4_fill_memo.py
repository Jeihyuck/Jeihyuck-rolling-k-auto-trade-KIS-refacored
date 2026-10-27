"""Bounded memo for Stage4's repeat KIS cumulative-fill persistence.

Only a *successful* DB save of identical broker evidence may be reused.
Every worker restart or change in broker evidence forces a new write; a
periodic refresh protects against out-of-band DB changes. This memo never
caches the broker HTTP response and never acts as ACK/claim proof.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from typing import Any, Callable


_VOLATILE = frozenset({"observed_at", "save_status"})


def _stable_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _stable_value(v) for k, v in sorted(value.items())
                if str(k) not in _VOLATILE}
    if isinstance(value, (list, tuple)):
        return [_stable_value(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        # Never silently coerce non-finite evidence into apparently valid data.
        return {"nonfinite_float": str(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def fills_digest(fills: list[dict]) -> str:
    payload = [_stable_value(row) for row in fills]
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class VerifiedFillPersistenceMemo:
    """Avoid repeated 10-20s identical writes, preserving fail-closed behavior."""

    def __init__(self, *, refresh_sec: float = 120.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.refresh_sec = max(1.0, float(refresh_sec))
        self.clock = clock
        self._digest: str | None = None
        self._saved_at: float | None = None

    def should_persist(self, fills: list[dict]) -> bool:
        # Empty evidence is never memoized: reconciliation must still inspect it.
        if not fills or self._digest is None or self._saved_at is None:
            return True
        if self.clock() - self._saved_at >= self.refresh_sec:
            return True
        return fills_digest(fills) != self._digest

    def mark_persisted(self, fills: list[dict], *, status: str) -> None:
        if str(status).upper() != "OK":
            self._digest = None
            self._saved_at = None
            return
        self._digest = fills_digest(fills)
        self._saved_at = self.clock()

    def invalidate(self) -> None:
        self._digest = None
        self._saved_at = None
