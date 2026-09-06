from __future__ import annotations

from datetime import datetime, timezone

from trader.kr.pb1.exit_cooldown import resolve_exit_cooldown_until


def test_resolve_exit_cooldown_until_hard_stop_advances_by_days():
    now_kst = datetime(2026, 9, 5, 13, 0, tzinfo=timezone.utc)

    assert resolve_exit_cooldown_until(
        now_kst=now_kst,
        exit_primary_reason="EXIT_HARD_STOP",
        reentry_cooldown_days=5,
    ) == "2026-09-10"


def test_resolve_exit_cooldown_until_zero_days_uses_today_for_soft_risk_off():
    now_kst = datetime(2026, 9, 5, 13, 0, tzinfo=timezone.utc)

    assert resolve_exit_cooldown_until(
        now_kst=now_kst,
        exit_primary_reason="EXIT_SOFT_RISK_OFF",
        reentry_cooldown_days=0,
    ) == "2026-09-05"


def test_resolve_exit_cooldown_until_non_cooldown_reason_returns_none():
    now_kst = datetime(2026, 9, 5, 13, 0, tzinfo=timezone.utc)

    assert (
        resolve_exit_cooldown_until(
            now_kst=now_kst,
            exit_primary_reason="EXIT_TP1",
            reentry_cooldown_days=5,
        )
        is None
    )
