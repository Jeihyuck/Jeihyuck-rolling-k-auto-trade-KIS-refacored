"""KR entry/exit timing guards that do not alter strategy signal generation."""
from __future__ import annotations

import os
from datetime import datetime


SOFT_EXIT_REASONS = {
    "TRAIL_STOP_HIT", "EXIT_TRAIL", "EXIT_TRAIL_STOP_HIT",
    "SWING_PROFIT_PROTECT_GIVEBACK", "EXIT_SWING_PROFIT_PROTECT_GIVEBACK",
}


def same_day_soft_exit_block(
    *, now_kst: datetime, bought_at: datetime | None, exit_reason: str
) -> tuple[bool, int, int]:
    """Return a same-day minimum-hold decision; hard/emergency reasons fail open."""
    minimum = max(0, int(os.getenv("KR_SWING_MIN_HOLD_MINUTES", "240")))
    enabled = os.getenv("KR_SWING_BLOCK_SAME_DAY_SOFT_EXIT", "1").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled or bought_at is None or str(exit_reason).upper() not in SOFT_EXIT_REASONS:
        return False, 0, minimum
    if bought_at.tzinfo is None and now_kst.tzinfo is not None:
        bought_at = bought_at.replace(tzinfo=now_kst.tzinfo)
    holding_minutes = max(0, int((now_kst - bought_at).total_seconds() // 60))
    return bought_at.date() == now_kst.date() and holding_minutes < minimum, holding_minutes, minimum
