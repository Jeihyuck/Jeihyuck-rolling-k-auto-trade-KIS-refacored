# -*- coding: utf-8 -*-
"""US PB1 exchange resolution contract tests.

PB1 prep scoring must resolve exchange from trader.us.symbols registry.
It must not default all string tickers to NASDAQ.
"""

from trader.us.symbols import resolve_exchange


def test_known_non_nasdaq_symbols_resolve_correctly():
    """NYSE symbols must resolve to NYSE, not NASDAQ."""
    assert resolve_exchange("SPY") == "NYSE"
    assert resolve_exchange("TSM") == "NYSE"
    assert resolve_exchange("VRT") == "NYSE"
    assert resolve_exchange("CIEN") == "NYSE"
    assert resolve_exchange("COHR") == "NYSE"


def test_pb1_must_not_default_all_string_tickers_to_nasdaq():
    """PB1 prep must not use NASDAQ as default exchange for all string tickers."""
    symbols = ["SPY", "TSM", "VRT", "CIEN", "COHR"]
    exchanges = {sym: resolve_exchange(sym) for sym in symbols}

    for sym in symbols:
        assert exchanges[sym] != "NASDAQ", f"{sym} must not resolve to NASDAQ"


def test_nasdaq_symbols_resolve_correctly():
    """NASDAQ symbols must resolve to NASDAQ."""
    assert resolve_exchange("QQQ") == "NASDAQ"
    assert resolve_exchange("NVDA") == "NASDAQ"
    assert resolve_exchange("SMH") == "NASDAQ"
    assert resolve_exchange("MSFT") == "NASDAQ"
