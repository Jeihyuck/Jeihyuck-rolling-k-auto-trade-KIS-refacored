"""Bounded, practice-only Stage4 writer startup and shutdown handshakes.

No broker order path is opened by this module. Every Tick and every pre-HTTP
order still performs the authoritative Stage4 health check independently.
"""
from __future__ import annotations

import logging
import subprocess
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

_RETRYABLE_STARTUP_CODES = frozenset({
    "US_STAGE4_WRITER_NOT_READY",
    "US_STAGE4_WRITER_STALE",
    "US_STAGE4_WRITER_GENERATION_MISMATCH",
    "US_STAGE4_WRITER_DATABASE_UNAVAILABLE",
    "US_STAGE4_WRITER_TIMESTAMP_UNVERIFIED",
    "US_STAGE4_RECONCILIATION_UNVERIFIED",
})
_OK_RECONCILE = frozenset({"OK", "WARN"})
_OK_REPLAY = frozenset({
    "OK", "RECOVERED", "NO_ACTIVE_CLAIMS", "WARN", "PARTIAL", "UNRESOLVED",
})


def stage4_error_code(exc: BaseException) -> str:
    """Expose only structured error codes, never DB/HTTP text or credentials."""
    value = str(exc).strip()
    if (isinstance(exc, RuntimeError)
            and value.startswith("US_STAGE4_")
            and all(c.isupper() or c.isdigit() or c == "_" for c in value)):
        return value
    return type(exc).__name__


def wait_for_stage4_ready(
    child: Any, *, env: str = "practice", timeout_sec: float = 75.0,
    poll_sec: float = 0.5, health_reader: Callable[..., dict] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict:
    """Require a *current-generation* READY before the first order-capable Tick.

    read_worker_health enforces account, trading epoch, UUID generation and
    heartbeat freshness. A prior session's READY cannot satisfy the handshake.
    The child must also still be alive after the successful SQL read.
    """
    if env != "practice":
        raise RuntimeError("US_STAGE4_STARTUP_PRACTICE_ONLY")
    if health_reader is None:
        from trader.us.execution.authoritative_reconcile_worker import read_worker_health
        health_reader = read_worker_health

    deadline = clock() + max(0.1, float(timeout_sec))
    delay = max(0.05, float(poll_sec))
    last_code = "US_STAGE4_WRITER_NOT_READY"
    while True:
        if child.poll() is not None:
            raise RuntimeError("US_STAGE4_STARTUP_WORKER_EXITED")
        try:
            evidence = health_reader(env=env)
            if (evidence.get("reconcile_status") not in _OK_RECONCILE
                    or evidence.get("replay_status") not in _OK_REPLAY):
                raise RuntimeError("US_STAGE4_RECONCILIATION_UNVERIFIED")
            if child.poll() is not None:
                raise RuntimeError("US_STAGE4_STARTUP_WORKER_EXITED")
            return evidence
        except Exception as exc:
            code = stage4_error_code(exc)
            if code not in _RETRYABLE_STARTUP_CODES:
                raise RuntimeError("US_STAGE4_STARTUP_FATAL") from exc
            last_code = code

        remaining = deadline - clock()
        if remaining <= 0:
            logger.error("[US_STAGE4][STARTUP_TIMEOUT] last_code=%s", last_code)
            raise RuntimeError("US_STAGE4_STARTUP_TIMEOUT")
        sleeper(min(delay, remaining))


def stop_stage4_worker(
    child: Any, *, expected_generation: str,
    terminate_timeout_sec: float = 5.0, kill_timeout_sec: float = 5.0,
    marker: Callable[..., bool] | None = None,
) -> bool:
    """Reap the writer first; only then conditionally mark its generation STOPPED.

    A process that cannot be reaped is *not* certified STOPPED. The database
    update itself requires account, epoch and generation equality.
    """
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=max(0.1, terminate_timeout_sec))
        except subprocess.TimeoutExpired:
            child.kill()
            try:
                child.wait(timeout=max(0.1, kill_timeout_sec))
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("US_STAGE4_WORKER_STOP_UNVERIFIED") from exc
    if child.poll() is None:
        raise RuntimeError("US_STAGE4_WORKER_STOP_UNVERIFIED")
    if marker is None:
        from trader.us.execution.authoritative_reconcile_worker import (
            mark_stopped_after_confirmed_exit,
        )
        marker = mark_stopped_after_confirmed_exit
    return bool(marker(expected_generation=expected_generation, env="practice"))
