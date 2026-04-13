from __future__ import annotations

from trader.pb1_engine import PB1Engine


def test_resolve_buy_cooldown_state_ignores_stale_metadata() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-04-13"

    result = engine._resolve_buy_cooldown_state(
        code="005930",
        cooldown_until="2026-04-14",
        today_buy_exists=False,
        today_fill_exists=False,
        cooldown_source_events_count=0,
        last_fill_event_at=None,
    )

    assert result["cooldown_active"] is False
    assert result["stale_ignored"] is True


def test_resolve_buy_cooldown_state_keeps_real_fill_block() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-04-13"

    result = engine._resolve_buy_cooldown_state(
        code="005930",
        cooldown_until="2026-04-14",
        today_buy_exists=False,
        today_fill_exists=True,
        cooldown_source_events_count=0,
        last_fill_event_at=None,
    )

    assert result["cooldown_active"] is True
    assert result["stale_ignored"] is False