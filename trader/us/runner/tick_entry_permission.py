"""Session-to-tick entry permission split for US strategy-owner isolation.

The session-level boolean historically mixed two different concepts:

* US_STANDARD/PB1 policy blocks from the PREP contract (cluster caps, FinalN
  quality, risk-off policy), and
* operational safety blocks (preflight EXIT_ONLY, revision mismatch,
  SAFE_DEGRADED/timeouts, or exit liveness failure).

The trade tick is allowed to *evaluate* entries when the only upstream block is
PB1-owned policy.  The standard PB1 path re-validates that policy from the PREP
contract before it can create an order, while the dedicated TQQQ_INFINITE sleeve
must not inherit PB1 policy.  Operational safety remains fail-closed for every
BUY owner.
"""
from __future__ import annotations

from typing import Any

from trader.us.infinite.risk_adapter import TQQQ_PB1_ONLY_ENTRY_BLOCK_REASONS


_OPERATIONAL_GUARD_STATES = frozenset({
    "PREFLIGHT_EXIT_ONLY",
    "PREP_MISSING_EXIT_ONLY",
    "PREP_STALE_EXIT_ONLY",
    "PREP_GUARD_EXCEPTION_EXIT_ONLY",
})


def _reason(guard: dict[str, Any]) -> str:
    return str(
        guard.get("trade_block_reason")
        or guard.get("primary_entry_block_reason")
        or guard.get("reason")
        or ""
    ).strip().lower()


def resolve_shared_tick_entry_evaluation_permission(
    prep_guard_result: dict[str, Any] | None,
    *,
    timeout_entry_block: bool,
    session_execution_mode: str,
) -> bool:
    """Return whether the tick may reach owner-specific BUY evaluation.

    This is intentionally *not* a PB1 order permission.  A False result is an
    operational/global fail-closed fence.  A True result can mean either that
    PB1 entry is allowed or that the only upstream block is PB1-owned policy;
    in the latter case the standard path still blocks later from the same PREP
    contract while TQQQ_INFINITE evaluates its own policy.
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

    if bool(guard.get("entry_can_proceed", False)):
        return True

    # Only a canonical completed PREP whose entry side is degraded by a
    # strategy/policy reason may be promoted to evaluation-only.  Unknown or
    # operational reasons remain fail-closed.
    if guard_state != "PREP_DEGRADED_ENTRY_BLOCKED":
        return False
    return _reason(guard) in TQQQ_PB1_ONLY_ENTRY_BLOCK_REASONS
