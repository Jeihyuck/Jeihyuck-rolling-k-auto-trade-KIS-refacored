"""Shared session-to-tick BUY permission for US trading.

The shared ``entry_can_proceed`` boolean is authoritative for US_STANDARD/PB1.
It must never be promoted merely so another strategy owner can evaluate a BUY.
TQQQ_INFINITE owner-specific policy isolation is handled downstream inside the
Infinite sleeve using its own overlay/owner contract.

This helper therefore applies only global/operational fences on top of the
PREP guard's own entry permission.  If the authoritative PREP guard blocks PB1,
the shared tick permission stays False even when the block reason is a PB1-only
policy such as a sector/cluster cap.
"""
from __future__ import annotations

from typing import Any


_OPERATIONAL_GUARD_STATES = frozenset({
    "PREFLIGHT_EXIT_ONLY",
    "PREP_MISSING_EXIT_ONLY",
    "PREP_STALE_EXIT_ONLY",
    "PREP_GUARD_EXCEPTION_EXIT_ONLY",
})


def resolve_shared_tick_entry_evaluation_permission(
    prep_guard_result: dict[str, Any] | None,
    *,
    timeout_entry_block: bool,
    session_execution_mode: str,
) -> bool:
    """Return the shared PB1/session BUY permission without owner promotion.

    ``False`` is fail-closed.  In particular, a policy-only PREP block remains
    False here; the caller must not raise the common permission for TQQQ.  This
    prevents a stale independently-loaded DB PREP cache from re-enabling normal
    PB1 buys after the authoritative artifact-backed guard has blocked them.
    """
    guard = dict(prep_guard_result or {})

    if bool(timeout_entry_block):
        return False
    if str(session_execution_mode or "NORMAL").upper() != "NORMAL":
        return False
    if not bool(guard.get("exit_can_proceed", True)):
        return False

    guard_state = str(guard.get("guard_state") or "").upper()
    if guard_state in _OPERATIONAL_GUARD_STATES:
        return False

    revision_mismatch = bool(guard.get("run_revision_mismatch") or guard.get("version_mismatch"))
    revision_recovered = bool(guard.get("revision_mismatch_recovery_allowed"))
    if revision_mismatch and not revision_recovered:
        return False

    return bool(guard.get("entry_can_proceed", False))
