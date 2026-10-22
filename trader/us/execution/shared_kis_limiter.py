"""WSL/Linux account-scoped KIS GET/POST pacing across Tick and Worker PIDs.

Entirely opt-in. The lock file stores only a monotonic timestamp under a
SHA-256 account-scope filename (never the actual account, keys or KIS tokens).
When filesystem locks/budget are unavailable, fail closed before HTTP.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import time
from typing import Callable


class SharedLimiterUnavailable(RuntimeError):
    pass


def wait_for_shared_slot(
    account_scope: str, *, min_interval_sec: float,
    sleep_with_budget: Callable[[float], bool],
    root: str | None = None,
) -> float:
    """Reserve the next KIS HTTP start window across OS processes.

    Caller still enforces its endpoint-level limiter. Holding this flock
    covers waiting and writing the timestamp, never the actual HTTP call.
    """
    if not account_scope or not callable(sleep_with_budget):
        raise SharedLimiterUnavailable("shared_KIS_limiter_scope_missing")
    interval = float(min_interval_sec)
    if not 0.05 <= interval <= 5.0:
        raise SharedLimiterUnavailable("shared_KIS_limiter_interval_invalid")
    try:
        import fcntl
    except ImportError as exc:
        raise SharedLimiterUnavailable("shared_KIS_limiter_requires_WSL_linux") from exc
    directory = Path(root or os.getenv("US_SHARED_KIS_LIMITER_DIR", "runtime/us/shared_kis_limiter"))
    directory.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(account_scope.encode("utf-8")).hexdigest()
    path = directory / (key + ".slot")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        # Nonblocking acquire allows the Tick deadline to cancel instead of
        # hanging behind a dead or unexpectedly slow process.
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not sleep_with_budget(0.03):
                    raise SharedLimiterUnavailable("shared_KIS_limiter_lock_budget_exceeded")
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            raw = os.read(fd, 64).decode("ascii", "replace").strip()
            try:
                previous = float(raw)
            except (ValueError, OverflowError):
                previous = 0.0
            now = time.monotonic()
            if not (0 < previous <= now):
                previous = 0.0
            needed = max(0.0, interval - (now - previous)) if previous else 0.0
            if needed and not sleep_with_budget(needed):
                raise SharedLimiterUnavailable("shared_KIS_limiter_tick_budget_exceeded")
            slot = time.monotonic()
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            os.write(fd, format(slot, ".9f").encode("ascii"))
            os.fsync(fd)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
    # This is the under-lock reserved native HTTP START slot, not the
    # caller's completion time (which can be reordered by OS scheduling).
    return slot
