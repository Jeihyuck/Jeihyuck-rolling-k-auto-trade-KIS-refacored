# -*- coding: utf-8 -*-
"""US entry engine regression tests for local import shadowing issues."""

from __future__ import annotations


class _DummyProvider:
    def get_daily_prices(self, symbol: str, exchange: str, count: int = 120):
        return [{"clos": "100", "tvol": "100000"}] * max(count, 60)

    def get_current_price(self, symbol: str, exchange: str):
        return {"last": "101.5"}


def test_generate_entry_intents_watchlist_no_os_shadowing(monkeypatch):
    """watchlist 기반 진입 평가에서 os.getenv 접근이 shadowing 없이 동작해야 한다."""
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "2")
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")

    # DB gate는 모두 통과하도록 고정
    monkeypatch.setattr("trader.us.db.repos.has_pending_order", lambda _symbol: False)
    monkeypatch.setattr("trader.us.db.repos.has_position", lambda _symbol: False)

    watchlist_entries = [
        {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.82, "rank": 1, "meta": {}},
        {"symbol": "MSFT", "exchange": "NASDAQ", "score": 0.81, "rank": 2, "meta": {}},
    ]

    intents = generate_entry_intents(
        tickers=None,
        provider=_DummyProvider(),
        sold_today=set(),
        available_cash_usd=20000.0,
        position_count=0,
        capital_usd_cap=20000.0,
        watchlist_entries=watchlist_entries,
    )

    assert isinstance(intents, list)
    assert len(intents) >= 1


def test_generate_entry_intents_respects_env_max_entries(monkeypatch):
    """US_MAX_NEW_ENTRIES_PER_TICK 환경변수가 정상 반영되어야 한다."""
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")

    monkeypatch.setattr("trader.us.db.repos.has_pending_order", lambda _symbol: False)
    monkeypatch.setattr("trader.us.db.repos.has_position", lambda _symbol: False)

    watchlist_entries = [
        {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.85, "rank": 1, "meta": {}},
        {"symbol": "NVDA", "exchange": "NASDAQ", "score": 0.84, "rank": 2, "meta": {}},
    ]

    intents = generate_entry_intents(
        tickers=None,
        provider=_DummyProvider(),
        sold_today=set(),
        available_cash_usd=20000.0,
        position_count=0,
        capital_usd_cap=20000.0,
        watchlist_entries=watchlist_entries,
    )

    assert len(intents) == 1
