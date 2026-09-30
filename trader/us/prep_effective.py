"""Canonical helpers for deciding whether a US PREP result is operationally usable.

Entry quality and session/exit liveness are intentionally separate.  A PREP that
completed successfully but blocks new BUYs (cluster cap, underfill, risk/data
quality) is still an effective same-day contract and must not be overwritten by
recovery with a newer STARTED row.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def is_effective_prep_status(status: object) -> bool:
    """Return True for completed non-fatal PREP statuses.

    Current production statuses intentionally include several
    ``OK_WITH_WARNINGS_*`` variants.  Treat the OK prefix as the stable status
    family instead of enumerating every suffix in scheduler/control-plane code.
    """
    text = str(status or "").strip().upper()
    return bool(text) and text.startswith("OK")


def is_effective_prep_contract(
    contract: dict[str, Any] | None,
    rows: Sequence[Any] | None,
    *,
    trade_date: str | None = None,
    min_rows: int = 10,
) -> bool:
    """Return whether a same-day PREP artifact is safe to reuse.

    This answers only whether recovery should regard PREP as completed.  It does
    *not* re-enable entries.  ``entry_can_proceed`` may legitimately be false;
    existing-position exit/close liveness remains authoritative.
    """
    if not isinstance(contract, dict):
        return False
    if trade_date and str(contract.get("trade_date") or "") not in {"", str(trade_date)}:
        return False
    if not is_effective_prep_status(contract.get("status")):
        return False
    if len(rows or ()) < max(1, int(min_rows)):
        return False

    trade_can_proceed = bool(int(contract.get("trade_can_proceed", 0) or 0))
    contract_ok = bool(contract.get("contract_ok", trade_can_proceed))
    exit_can_proceed = bool(int(contract.get("exit_can_proceed", trade_can_proceed) or 0))
    if not contract_ok or not exit_can_proceed:
        return False

    row_count = len(rows or ())
    declared_count = int(contract.get("final30_scored_count", row_count) or row_count)
    score_nonzero_count = int(contract.get("score_nonzero_count", declared_count) or 0)
    if declared_count > 0 and declared_count != row_count:
        return False
    if score_nonzero_count != declared_count:
        return False
    return True
