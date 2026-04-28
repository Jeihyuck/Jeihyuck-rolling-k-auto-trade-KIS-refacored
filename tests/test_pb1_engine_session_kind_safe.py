from __future__ import annotations

from trader.pb1_engine import PB1Engine


def test_safe_session_kind_from_am_env(monkeypatch):
    monkeypatch.setenv("PB1_SESSION_KIND", "am")
    monkeypatch.delenv("PB1_FORCE_TRADE_SESSION", raising=False)
    monkeypatch.delenv("FORCE_MARKET_WINDOW", raising=False)

    engine = PB1Engine.__new__(PB1Engine)
    assert engine._safe_session_kind() == "am"


def test_safe_session_kind_from_afternoon_env(monkeypatch):
    monkeypatch.setenv("PB1_SESSION_KIND", "afternoon")
    monkeypatch.delenv("PB1_FORCE_TRADE_SESSION", raising=False)
    monkeypatch.delenv("FORCE_MARKET_WINDOW", raising=False)

    engine = PB1Engine.__new__(PB1Engine)
    assert engine._safe_session_kind() == "afternoon"


def test_safe_session_kind_maps_pm_close_day_to_afternoon(monkeypatch):
    engine = PB1Engine.__new__(PB1Engine)

    for raw in ["pm", "close", "trade-pm", "trade-close", "day", "intraday"]:
        monkeypatch.setenv("PB1_SESSION_KIND", raw)
        monkeypatch.delenv("PB1_FORCE_TRADE_SESSION", raising=False)
        monkeypatch.delenv("FORCE_MARKET_WINDOW", raising=False)
        assert engine._safe_session_kind() == "afternoon", f"expected afternoon for {raw!r}"


def test_safe_session_kind_maps_morning_to_am(monkeypatch):
    engine = PB1Engine.__new__(PB1Engine)

    for raw in ["am", "morning", "trade-am"]:
        monkeypatch.setenv("PB1_SESSION_KIND", raw)
        monkeypatch.delenv("PB1_FORCE_TRADE_SESSION", raising=False)
        monkeypatch.delenv("FORCE_MARKET_WINDOW", raising=False)
        assert engine._safe_session_kind() == "am", f"expected am for {raw!r}"


def test_safe_session_kind_never_raises_without_env(monkeypatch):
    monkeypatch.delenv("PB1_SESSION_KIND", raising=False)
    monkeypatch.delenv("PB1_FORCE_TRADE_SESSION", raising=False)
    monkeypatch.delenv("FORCE_MARKET_WINDOW", raising=False)

    engine = PB1Engine.__new__(PB1Engine)
    value = engine._safe_session_kind()
    assert isinstance(value, str)
    assert value
