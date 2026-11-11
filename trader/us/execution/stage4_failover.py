"""Stage4 -> legacy EXIT_ONLY handoff requiring proven writer termination.

A missing or unhealthy worker is NOT evidence of a safe handoff. The original
process must be reaped and its matching generation marked STOPPED or shown
absent before any other executor can assume reconciliation authority.
"""
from __future__ import annotations

from typing import Any, Callable


def verified_stage4_exit_only_handoff(
    *,
    child: Any,
    expected_generation: str,
    env: str = "practice",
    stop_worker: Callable[..., bool] | None = None,
    confirm_row_absent: Callable[..., bool] | None = None,
) -> tuple[bool, str]:
    if env != "practice":
        return False, "PRACTICE_ONLY"
    if not child or not str(expected_generation or "").strip():
        return False, "WRITER_IDENTITY_MISSING"
    if stop_worker is None:
        from trader.us.execution.stage4_lifecycle import stop_stage4_worker
        stop_worker = stop_stage4_worker
    if confirm_row_absent is None:
        from trader.us.execution.authoritative_reconcile_worker import (
            confirm_stage4_health_row_absent,
        )
        confirm_row_absent = confirm_stage4_health_row_absent
    try:
        # Stop + generation-fenced SQL update. A failed stop or unknown
        # generation cannot be treated as a successful handoff.
        marked_stopped = bool(stop_worker(
            child, expected_generation=expected_generation,
        ))
        if child.poll() is None:
            return False, "WRITER_PROCESS_NOT_REAPED"
        if not marked_stopped:
            if not bool(confirm_row_absent(
                expected_generation=expected_generation, env=env,
            )):
                return False, "WRITER_DB_STOP_UNVERIFIED"
        return True, "WRITER_STOP_PROVEN"
    except Exception as exc:
        return False, "WRITER_HANDOFF_ERROR_" + type(exc).__name__.upper()
