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
    defer_if_busy: bool = False,
    urgent: bool = False,
) -> float:
    """Reserve the next KIS HTTP start window across OS processes.

    Caller still enforces its endpoint-level limiter. Holding this flock
    covers only checking and writing the timestamp, never HTTP or sleeping.
    Opportunistic market-data readers must not hold the lock while waiting.
    They defer without consuming a slot when the account is busy.
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
    if urgent and defer_if_busy:
        raise SharedLimiterUnavailable("shared_KIS_limiter_priority_invalid")
    foreground_fd = os.open(directory / (key + ".foreground"), flags, 0o600)
    urgent_fd = fd = None

    def lock(handle, mode, *, defer=False):
        try:
            fcntl.flock(handle, mode | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            if defer:
                raise SharedLimiterUnavailable("shared_KIS_background_deferred")
            return False

    def pause(seconds, reason):
        if defer_if_busy:
            raise SharedLimiterUnavailable("shared_KIS_background_deferred")
        if not sleep_with_budget(max(0.001, seconds)):
            raise SharedLimiterUnavailable(reason)

    try:
        # Urgent SELL/cancel/fill waiters announce themselves before slot
        # competition. Ordinary waiters never hold the urgent marker while
        # sleeping, so they cannot reserve a future slot ahead of a SELL.
        urgent_fd = os.open(directory / (key + ".urgent"), flags, 0o600)
        if urgent:
            while not lock(urgent_fd, fcntl.LOCK_SH):
                pause(.03, "shared_KIS_limiter_urgent_budget_exceeded")
        # Foreground marker remains held across waits to keep opportunistic
        # minute collectors out. Acquisition order cannot deadlock: ordinary
        # foreground marker is shared, background takes it exclusively.
        while not lock(foreground_fd, fcntl.LOCK_EX if defer_if_busy else fcntl.LOCK_SH,
                       defer=defer_if_busy):
            pause(.03, "shared_KIS_limiter_priority_budget_exceeded")
        fd = os.open(path, flags, 0o600)
        while True:
            ordinary_marker = False
            slot_locked = False
            needed = .03
            reason = "shared_KIS_limiter_lock_budget_exceeded"
            try:
                if not urgent:
                    ordinary_marker = lock(urgent_fd, fcntl.LOCK_EX, defer=defer_if_busy)
                    if not ordinary_marker:
                        reason = "shared_KIS_limiter_urgent_wait_budget_exceeded"
                    else:
                        slot_locked = lock(fd, fcntl.LOCK_EX, defer=defer_if_busy)
                else:
                    slot_locked = lock(fd, fcntl.LOCK_EX)
                if slot_locked:
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
                    if not needed:
                        slot = time.monotonic()
                        os.lseek(fd, 0, os.SEEK_SET)
                        os.ftruncate(fd, 0)
                        os.write(fd, format(slot, ".9f").encode("ascii"))
                        os.fsync(fd)
                        break
                    reason = "shared_KIS_limiter_tick_budget_exceeded"
            finally:
                if slot_locked:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                if ordinary_marker:
                    fcntl.flock(urgent_fd, fcntl.LOCK_UN)
            # Re-read the shared clock after each wait. Never sleep with
            # either an ordinary urgent-marker lock or the slot lock held.
            pause(min(.03, needed), reason)
    finally:
        if fd is not None:
            os.close(fd)
        if urgent_fd is not None:
            os.close(urgent_fd)
        os.close(foreground_fd)
    # This is the under-lock reserved native HTTP START slot, not the
    # caller's completion time (which can be reordered by OS scheduling).
    return slot
