"""US effective_max_new_positions: describe the implementation, not a new policy.

The 2026-10-27 regime cap is consumed as the upper bound on *one Tick's*
available current-position slots. It is NOT an atomic daily cumulative cap.
Report this distinction without silently changing the trading strategy.
"""
from __future__ import annotations

US_NEW_POSITION_LIMIT_APPLIED_SCOPE = "PER_TICK_AVAILABLE_CURRENT_POSITION_SLOTS"


def explain_effective_new_position_limit(
    *, available_current_position_slots: int, effective_max_new_positions: int | None,
) -> dict:
    remaining = max(0, int(available_current_position_slots))
    if effective_max_new_positions is None:
        return {
            "applied_scope": US_NEW_POSITION_LIMIT_APPLIED_SCOPE,
            "configured_cap": None,
            "tick_new_slots_after_cap": remaining,
            "daily_cumulative_new_positions_proven": False,
        }
    cap = max(0, int(effective_max_new_positions))
    return {
        "applied_scope": US_NEW_POSITION_LIMIT_APPLIED_SCOPE,
        "configured_cap": cap,
        "tick_new_slots_after_cap": min(remaining, cap),
        "daily_cumulative_new_positions_proven": False,
    }
