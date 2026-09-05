from __future__ import annotations

from typing import Any

from trader.config import MIN_EXIT_BARS, MIN_TRAIL_BARS


def resolve_exit_policy(
    *,
    days_held: int,
    holding_bars: int,
    stop_hit: bool,
    trail_stop_price: float | None,
    mark: float,
    ma20: float | None,
    ma50: float | None,
    time_stop_hit: bool,
    risk_off_signal: bool,
) -> dict[str, Any]:
    same_day_entry = int(days_held or 0) == 0
    trail_eligible = int(days_held or 0) >= 1 and int(holding_bars or 0) >= int(MIN_TRAIL_BARS)
    soft_exit_eligible = int(days_held or 0) >= 1 and int(holding_bars or 0) >= int(MIN_EXIT_BARS)
    trail_hit = bool(trail_eligible and trail_stop_price is not None and mark <= float(trail_stop_price))
    ma20_break = bool(soft_exit_eligible and ma20 is not None and mark < ma20)
    ma50_break = bool(soft_exit_eligible and ma50 is not None and mark < ma50)
    soft_exit_hit = bool(soft_exit_eligible and (risk_off_signal or ma50_break or ma20_break))

    triggered: list[str] = []
    final_reason = "NO_EXIT_SIGNAL"
    family = "SKIP"
    exit_ok = False
    if stop_hit:
        triggered.append("EXIT_HARD_STOP")
        final_reason = "EXIT_HARD_STOP"
        family = "EXIT_STOP"
        exit_ok = True
    elif trail_hit:
        triggered.append("EXIT_TRAIL")
        final_reason = "EXIT_TRAIL"
        family = "EXIT_TRAIL"
        exit_ok = True
    elif soft_exit_hit:
        triggered.append("EXIT_SOFT_RISK_OFF")
        final_reason = "EXIT_SOFT_RISK_OFF"
        family = "EXIT_RISK_OFF"
        exit_ok = True
    elif time_stop_hit:
        triggered.append("EXIT_TIME_BASED")
        final_reason = "EXIT_TIME_BASED"
        family = "EXIT_TIME"
        exit_ok = True

    return {
        "same_day_entry": same_day_entry,
        "trail_eligible": trail_eligible,
        "soft_exit_eligible": soft_exit_eligible,
        "trail_hit": trail_hit,
        "ma20_break": ma20_break,
        "ma50_break": ma50_break,
        "risk_off_hit": bool(soft_exit_eligible and risk_off_signal),
        "soft_exit_hit": soft_exit_hit,
        "triggered": triggered,
        "final_reason": final_reason,
        "family": family,
        "exit_ok": exit_ok,
    }
