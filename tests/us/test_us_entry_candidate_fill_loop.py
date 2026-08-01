from datetime import datetime
from zoneinfo import ZoneInfo


def test_candidate_fill_loop_exhausts_blocked_leaders_and_fills_target(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr(repos, "has_position", lambda symbol: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda trade_date: set())
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")

    rows = [
        {"symbol": f"BAD{i}", "exchange": "NASDAQ", "score": 1 - i * .01, "rank_final30": i + 1}
        for i in range(10)
    ] + [
        {"symbol": f"GOOD{i}", "exchange": "NASDAQ", "score": .8 - i * .01, "rank_final30": i + 11}
        for i in range(3)
    ]

    class Provider:
        def get_current_price(self, symbol, exchange):
            if symbol.startswith("BAD"):
                return {"last": 0}
            return {"last": 100}

    diagnostics = {}
    intents = generate_entry_intents(
        None, Provider(), set(), 10_000, 0, 10_000,
        now=datetime(2026, 7, 31, 10, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=3, watchlist_entries=rows,
        current_position_symbols=set(), available_new_slots=3,
        diagnostics=diagnostics,
    )

    assert [intent["symbol"] for intent in intents] == ["GOOD0", "GOOD1", "GOOD2"]
    assert diagnostics["accepted"] == 3
    assert diagnostics["backfill_attempt_count"] >= 10
    assert diagnostics["candidate_pool_exhausted"] is False


def test_candidate_fill_count_equals_available_candidates(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr(repos, "has_position", lambda symbol: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda trade_date: set())
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    rows = [{"symbol": f"S{i}", "exchange": "NASDAQ", "score": 1 - i * .01} for i in range(7)]

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 0 if symbol in {"S0", "S2", "S4"} else 100}

    intents = generate_entry_intents(
        None, Provider(), set(), 10_000, 0, 10_000,
        max_new_entries=3, watchlist_entries=rows, current_position_symbols=set(),
    )
    assert len(intents) == min(3, 4)
