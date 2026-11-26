"""Practice-only KR cross-process HTTP admission at the real transport boundary.

Existing endpoint limits and observer budget remain in force. No lock is
held during network I/O or sleep. This is not a broker quota certificate:
the operator must supply an independently verified RPS and test it in WSL.
It covers participating KR wrapper attempts, not arbitrary external clients.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import time


def enabled(environment: str) -> bool:
    return str(environment).lower() == "practice" and os.getenv("KR_SHARED_HTTP_BUDGET_ENABLED") == "1"


def _rate() -> float:
    rate = float(os.getenv("KR_SHARED_HTTP_RPS_PRACTICE", "0"))
    if not math.isfinite(rate) or not 0 < rate <= 10:
        raise ValueError("KR_SHARED_HTTP_RPS_UNVERIFIED")
    return rate


def _path(environment: str, appkey: str) -> Path:
    if not appkey:
        raise ValueError("KR_SHARED_HTTP_CREDENTIAL_MISSING")
    root = Path("/tmp") / f"kr-kis-budget-{os.getuid()}"
    root.mkdir(mode=0o700, exist_ok=True)
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise ValueError("KR_SHARED_HTTP_DIRECTORY_UNSAFE")
    digest = hashlib.sha256((environment.lower() + "\0" + appkey).encode()).hexdigest()
    return root / (digest + ".json")


def _locked_file(path: Path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077):
            raise ValueError("KR_SHARED_HTTP_FILE_UNSAFE")
        return os.fdopen(fd, "r+", encoding="utf-8")
    except BaseException:
        os.close(fd)
        raise


def _read(file, now, rate):
    file.seek(0)
    raw = file.read(4097)
    if not raw:
        return {"rate": rate, "next_at": 0.0, "cooldown_until": 0.0, "priority_until": 0.0}
    state = json.loads(raw)
    if not isinstance(state, dict) or state.get("rate") != rate:
        raise ValueError("KR_SHARED_HTTP_CONFIG_MISMATCH")
    for key in ("next_at", "cooldown_until", "priority_until"):
        v = state.get(key)
        if not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            raise ValueError("KR_SHARED_HTTP_CORRUPT_STATE")
        # A monotonic clock reset (host reboot) invalidates previous leases.
        if v > now + 120:
            raise ValueError("KR_SHARED_HTTP_CLOCK_RESET_REVIEW_REQUIRED")
    return state


def _write(file, state):
    file.seek(0)
    file.write(json.dumps(state, separators=(",", ":")))
    file.truncate()
    file.flush()


def acquire(*, environment: str, appkey: str, priority: bool,
            observer: bool, max_wait_sec: float) -> tuple[bool, str, float]:
    """Reserve one actual HTTP attempt. No future slots or unbounded wait.

    Priority traffic announces a short lease while waiting; the observer
    defers instead of queuing ahead of it. Ordinary trading reads and
    critical calls still obey the same aggregate gap and cooldown.
    """
    start = time.monotonic()
    deadline = start + max(0.0, min(float(max_wait_sec), 8.0))
    try:
        rate = _rate()
        path = _path(environment, appkey)
        while True:
            now = time.monotonic()
            if now >= deadline:
                return False, "DEADLINE", now-start
            wait = 0.02
            with _locked_file(path) as file:
                try:
                    fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    state = _read(file, now, rate)
                    if priority:
                        # Never reserve a long queue; allow only one
                        # short-lived notification of imminent critical work.
                        state["priority_until"] = max(state["priority_until"], min(deadline, now+1.0))
                        _write(file, state)
                    if observer and not priority and state["priority_until"] > now:
                        return False, "PRIORITY_TRAFFIC", time.monotonic()-start
                    earliest = max(state["next_at"], state["cooldown_until"])
                    if earliest <= now:
                        state["next_at"] = now + 1.0 / rate
                        _write(file, state)
                        return True, "GRANTED", time.monotonic()-start
                    wait = min(0.05, earliest-now)
            # File closes/releases flock before wait or any HTTP.
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                return False, "DEADLINE", time.monotonic()-start
            time.sleep(min(remaining, max(0.005, wait)))
    except (OSError, ValueError, TypeError, OverflowError):
        return False, "STATE_OR_CONFIG_UNAVAILABLE", time.monotonic()-start


def penalize(*, environment: str, appkey: str, seconds: float) -> bool:
    """Publish broker quota cooldown without changing any order outcome."""
    try:
        rate = _rate()
        now = time.monotonic()
        delay = min(60.0, max(0.1, float(seconds)))
        with _locked_file(_path(environment, appkey)) as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            state = _read(file, now, rate)
            state["cooldown_until"] = max(state["cooldown_until"], now + delay)
            _write(file, state)
        return True
    except (OSError, ValueError, TypeError, OverflowError):
        return False
