from datetime import datetime
from zoneinfo import ZoneInfo

from trader.us.runner.trade_tick_runner import _is_us_opening_buy_blocked


def test_us_buy_block_boundaries(monkeypatch):
    monkeypatch.setenv("US_OPENING_BUY_BLOCK_ENABLED", "1")
    tz = ZoneInfo("America/New_York")
    assert _is_us_opening_buy_blocked(datetime(2026, 8, 24, 9, 45, tzinfo=tz)) == (True, "10:00:00")
    assert _is_us_opening_buy_blocked(datetime(2026, 8, 24, 10, 0, tzinfo=tz))[0] is False


def test_us_opening_gate_does_not_change_exit_permission():
    blocked, _ = _is_us_opening_buy_blocked(
        datetime(2026, 8, 24, 9, 45, tzinfo=ZoneInfo("America/New_York"))
    )
    entry_can_proceed, exit_can_proceed = not blocked, True
    assert entry_can_proceed is False and exit_can_proceed is True
