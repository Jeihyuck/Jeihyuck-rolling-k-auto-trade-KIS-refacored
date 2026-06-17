from __future__ import annotations

from datetime import date

from trader.kr import calendar


def test_calendar_uses_krx_resolver_when_available(monkeypatch):
    monkeypatch.setattr(calendar, "_krx_nearest_business_day", lambda candidate, prev: date(2026, 6, 16) if prev else date(2026, 6, 17))
    assert calendar.resolve_kr_trade_date(calendar.datetime(2026, 6, 17, tzinfo=calendar.KST)) == date(2026, 6, 17)
    assert calendar.resolve_kr_expected_as_of(date(2026, 6, 17)) == date(2026, 6, 16)


def test_calendar_weekday_fallback_logs_warning(monkeypatch, caplog):
    monkeypatch.setattr(calendar, "_krx_nearest_business_day", lambda candidate, prev: None)
    assert calendar.resolve_kr_expected_as_of(date(2026, 6, 22)) == date(2026, 6, 19)
    assert "FALLBACK_WEEKDAY_ONLY" in caplog.text
