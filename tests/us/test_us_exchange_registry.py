# -*- coding: utf-8 -*-
"""Tests: US exchange registry — symbols.py."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# NYSE 심볼 검증
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("symbol", [
    "IBM", "MA", "JPM", "WMT", "DELL", "HPE",
    "ACN", "V", "HD", "CAT", "LLY", "NVO",
    "UBER", "BE", "GEV", "ETN", "HUBB", "PWR",
    "VST", "NRG", "GLW", "TEL", "APH",
    "SPY", "TSM", "VRT", "CIEN", "COHR",
])
def test_nyse_symbols(symbol: str):
    """지정된 NYSE 심볼들이 'NYSE'로 resolve되어야 한다."""
    from trader.us.symbols import resolve_exchange

    exchange = resolve_exchange(symbol)
    assert exchange == "NYSE", f"{symbol} expected NYSE, got {exchange}"


# ---------------------------------------------------------------------------
# NASDAQ 심볼 검증
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("symbol", [
    "NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "AVGO",
    "QQQ", "QQQM", "SMH", "SOXX",
    "COST", "NFLX", "ABNB", "COIN", "HOOD", "MSTR", "CEG",
    "CRDO", "MRVL", "NVTS", "VIAVI", "LITE", "AAOI",
])
def test_nasdaq_symbols(symbol: str):
    """지정된 NASDAQ 심볼들이 'NASDAQ'으로 resolve되어야 한다."""
    from trader.us.symbols import resolve_exchange

    exchange = resolve_exchange(symbol)
    assert exchange == "NASDAQ", f"{symbol} expected NASDAQ, got {exchange}"


# ---------------------------------------------------------------------------
# 미등록 심볼 에러
# ---------------------------------------------------------------------------

def test_unknown_symbol_raises_value_error():
    """미등록 심볼은 ValueError를 발생시켜야 한다."""
    from trader.us.symbols import resolve_exchange

    # ZZZZ = 4글자 대문자 (형식 유효), but 미등록 심볼
    with pytest.raises(ValueError):
        resolve_exchange("ZZZZ")


# ---------------------------------------------------------------------------
# get_quote_exchange_code (NAS / NYS 코드)
# ---------------------------------------------------------------------------

def test_nasdaq_quote_excd():
    from trader.us.symbols import get_quote_exchange_code

    assert get_quote_exchange_code("NASDAQ") == "NAS"


def test_nyse_quote_excd():
    from trader.us.symbols import get_quote_exchange_code

    assert get_quote_exchange_code("NYSE") == "NYS"


# ---------------------------------------------------------------------------
# normalize_us_exchange
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("NAS", "NASDAQ"),
    ("NASD", "NASDAQ"),
    ("NASDAQ", "NASDAQ"),
    ("NYS", "NYSE"),
    ("NYSE", "NYSE"),
    ("AMS", "AMEX"),
    ("ASE", "AMEX"),
    ("AMEX", "AMEX"),
])
def test_normalize_us_exchange(raw: str, expected: str):
    from trader.us.symbols import normalize_us_exchange

    assert normalize_us_exchange(raw) == expected


def test_normalize_us_exchange_unknown_raises():
    from trader.us.symbols import normalize_us_exchange

    with pytest.raises(ValueError):
        normalize_us_exchange("UNKNOWN")
