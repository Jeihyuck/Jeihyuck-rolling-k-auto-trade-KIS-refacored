from __future__ import annotations

from datetime import datetime, timedelta


def resolve_exit_cooldown_until(*, now_kst: datetime, exit_primary_reason: str, reentry_cooldown_days: int) -> str | None:
    if exit_primary_reason not in {"EXIT_HARD_STOP", "EXIT_SOFT_RISK_OFF"}:
        return None
    if reentry_cooldown_days <= 0:
        return now_kst.date().isoformat()
    return (now_kst + timedelta(days=reentry_cooldown_days)).date().isoformat()
