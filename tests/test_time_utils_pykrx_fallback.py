from __future__ import annotations

from datetime import date

from trader import time_utils


def test_resolve_prev_trading_day_falls_back_to_last_weekday(monkeypatch) -> None:
    monkeypatch.setattr(time_utils, "_resolve_pykrx_previous_or_same", lambda _d: (_ for _ in ()).throw(ValueError("bad json")))

    resolved = time_utils.resolve_prev_trading_day(date(2026, 3, 16))

    assert resolved == date(2026, 3, 13)


def test_is_trading_date_uses_weekday_heuristic_when_pykrx_fails(monkeypatch) -> None:
    monkeypatch.setattr(time_utils, "_resolve_pykrx_previous_or_same", lambda _d: (_ for _ in ()).throw(ValueError("bad json")))

    assert time_utils.is_trading_date(date(2026, 3, 16)) is True
    assert time_utils.is_trading_date(date(2026, 3, 15)) is False