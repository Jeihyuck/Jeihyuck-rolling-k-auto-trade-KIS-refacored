from __future__ import annotations

from trader.pb1_engine import PB1Engine
from trader.kr.pb1.buy_cooldown import resolve_buy_cooldown_state


def test_resolve_buy_cooldown_state_ignores_stale_metadata() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-04-13"

    result = engine._resolve_buy_cooldown_state(
        code="005930",
        cooldown_until="2026-04-14",
        holding_qty=0,
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
        holding_qty=1,
        today_buy_exists=False,
        today_fill_exists=True,
        cooldown_source_events_count=0,
        last_fill_event_at=None,
        cooldown_source="completed_trade_cooldown",
        recent_valid_exit_event=True,
        recent_exit_reason="EXIT_TAKE_PROFIT",
    )

    assert result["cooldown_active"] is True
    assert result["stale_ignored"] is False


def test_resolve_buy_cooldown_state_same_day_duplicate_only() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-04-13"

    result = engine._resolve_buy_cooldown_state(
        code="005930",
        cooldown_until="2026-04-25",
        holding_qty=0,
        today_buy_exists=True,
        today_fill_exists=True,
        cooldown_source_events_count=0,
        last_fill_event_at="2026-04-13T09:05:00+09:00",
        cooldown_source="same_day_duplicate_prevention",
        recent_valid_exit_event=False,
        recent_exit_reason=None,
    )

    assert result["cooldown_active"] is True
    assert result["final_cooldown_policy"] == "same_day_only"
    assert result["cooldown_source"] == "same_day_duplicate_prevention"


def test_resolve_buy_cooldown_state_helper_matches_engine_wrapper() -> None:
    helper_result = resolve_buy_cooldown_state(
        today="2026-04-13",
        code="005930",
        cooldown_until="2026-04-14",
        holding_qty=0,
        today_buy_exists=False,
        today_fill_exists=False,
        cooldown_source_events_count=0,
        last_fill_event_at=None,
    )

    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-04-13"
    wrapper_result = engine._resolve_buy_cooldown_state(
        code="005930",
        cooldown_until="2026-04-14",
        holding_qty=0,
        today_buy_exists=False,
        today_fill_exists=False,
        cooldown_source_events_count=0,
        last_fill_event_at=None,
    )

    assert helper_result == wrapper_result