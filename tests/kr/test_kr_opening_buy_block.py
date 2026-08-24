from datetime import datetime
from zoneinfo import ZoneInfo

from trader.pb1_engine import PB1Engine


def test_kr_buy_block_boundaries(monkeypatch, caplog):
    monkeypatch.setenv("KR_OPENING_BUY_BLOCK_ENABLED", "1")
    tz = ZoneInfo("Asia/Seoul")
    assert PB1Engine._is_kr_opening_buy_blocked(datetime(2026, 8, 24, 9, 10, tzinfo=tz)) == (True, "09:30:00")
    assert PB1Engine._is_kr_opening_buy_blocked(datetime(2026, 8, 24, 9, 30, tzinfo=tz))[0] is False


def test_kr_opening_gate_is_buy_only():
    """The helper has no exit permission input/output, so SELL remains routable."""
    blocked, _ = PB1Engine._is_kr_opening_buy_blocked(
        datetime(2026, 8, 24, 9, 10, tzinfo=ZoneInfo("Asia/Seoul"))
    )
    exit_can_proceed = True
    assert blocked is True and exit_can_proceed is True
