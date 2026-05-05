# -*- coding: utf-8 -*-
"""US trade watchlist dedupe contract tests."""


def test_dedupe_watchlist_best_by_symbol():
    """_dedupe_watchlist_best_by_symbol should keep only best score per symbol."""
    from trader.us.runner.trade_tick_runner import _dedupe_watchlist_best_by_symbol
    
    rows = [
        {"symbol": "AAOI", "exchange": "NASDAQ", "score": 0.1, "strategy": "a"},
        {"symbol": "AAOI", "exchange": "NASDAQ", "score": 0.9, "strategy": "b"},
        {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.5, "strategy": "a"},
    ]

    out = _dedupe_watchlist_best_by_symbol(rows)

    assert len(out) == 2
    aaoi = [r for r in out if r["symbol"] == "AAOI"][0]
    assert aaoi["score"] == 0.9


def test_dedupe_watchlist_empty_input():
    """_dedupe_watchlist_best_by_symbol should handle empty input."""
    from trader.us.runner.trade_tick_runner import _dedupe_watchlist_best_by_symbol
    
    out = _dedupe_watchlist_best_by_symbol([])
    assert len(out) == 0


def test_dedupe_watchlist_single_row():
    """_dedupe_watchlist_best_by_symbol should handle single row."""
    from trader.us.runner.trade_tick_runner import _dedupe_watchlist_best_by_symbol
    
    rows = [{"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.7}]
    out = _dedupe_watchlist_best_by_symbol(rows)
    
    assert len(out) == 1
    assert out[0]["symbol"] == "AAPL"
    assert out[0]["score"] == 0.7
