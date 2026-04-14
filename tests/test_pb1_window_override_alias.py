from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader import pb1_runner


def test_pb1_window_override_alias(monkeypatch) -> None:
    monkeypatch.delenv("FORCE_MARKET_WINDOW", raising=False)
    monkeypatch.delenv("MARKET_WINDOW", raising=False)
    monkeypatch.setenv("PB1_WINDOW_OVERRIDE", "day")

    result = pb1_runner.decide_market_window(datetime(2026, 4, 4, 18, 0, tzinfo=ZoneInfo("Asia/Seoul")))

    assert result == "day"


def test_am_session_normalizes_intraday_aliases_to_morning() -> None:
    assert pb1_runner.normalize_window(session_kind="am", input_window="intraday") == "morning"
    assert pb1_runner.normalize_window(session_kind="am", input_window="day") == "morning"
    assert pb1_runner.normalize_window(session_kind="am", input_window="morning") == "morning"