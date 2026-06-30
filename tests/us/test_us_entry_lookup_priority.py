from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def test_entry_lookup_prioritizes_new_candidates_over_held_candidates(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setenv("US_ENTRY_MIN_NEW_PRICE_LOOKUP", "8")
    monkeypatch.setenv("US_ENTRY_MAX_TOTAL_PRICE_LOOKUP", "20")
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")

    held = {f"HELD{i}" for i in range(8)}
    rows = []
    for i in range(8):
        rows.append({"symbol": f"HELD{i}", "exchange": "NASDAQ", "score": 1.00 - i * 0.01})
    for i in range(22):
        rows.append({"symbol": f"NEW{i}", "exchange": "NASDAQ", "score": 0.80 - i * 0.01})

    class Provider:
        def __init__(self):
            self.lookups: list[str] = []

        def get_current_price(self, symbol, exchange):
            self.lookups.append(symbol)
            return {"last": 100.0}

    provider = Provider()
    generate_entry_intents(
        tickers=None,
        provider=provider,
        sold_today=set(),
        available_cash_usd=100000.0,
        position_count=0,
        capital_usd_cap=100000.0,
        now=datetime(2026, 6, 29, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=rows,
        current_position_symbols=held,
    )

    looked_new = [s for s in provider.lookups if s.startswith("NEW")]
    assert len(set(looked_new)) >= 8
    assert provider.lookups[:8] == [f"NEW{i}" for i in range(8)]
    assert len(provider.lookups) <= 20
