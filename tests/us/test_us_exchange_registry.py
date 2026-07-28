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


def test_prepare_registers_dynamic_universe_source_exchange(monkeypatch):
    from trader.us import symbols
    from trader.us.exchange_registry import prepare_exchange_registry

    monkeypatch.delitem(symbols._SYMBOL_EXCHANGE_MAP, "ZZZZ", raising=False)
    with pytest.raises(ValueError, match="unknown symbol"):
        symbols.resolve_exchange("ZZZZ")

    result = prepare_exchange_registry(
        dynamic_universe_result={"symbols": [{"symbol": "ZZZZ", "exchange_code": "NYS"}]},
        benchmark_symbols=(),
    )
    assert result["failed_count"] == 0
    assert symbols.resolve_exchange("ZZZZ") == "NYSE"


def test_prepare_does_not_guess_missing_exchange(monkeypatch):
    from trader.us import symbols
    from trader.us.exchange_registry import prepare_exchange_registry

    monkeypatch.delitem(symbols._SYMBOL_EXCHANGE_MAP, "ZZZZ", raising=False)
    result = prepare_exchange_registry(
        dynamic_universe_result={"symbols": [{"symbol": "ZZZZ"}]},
        benchmark_symbols=(),
    )
    assert result["failed_symbols"] == ["ZZZZ"]
    with pytest.raises(ValueError, match="unknown symbol"):
        symbols.resolve_exchange("ZZZZ")


def test_prepare_isolates_invalid_symbol_without_aborting(monkeypatch):
    from trader.us import symbols
    from trader.us.exchange_registry import prepare_exchange_registry

    monkeypatch.delitem(symbols._SYMBOL_EXCHANGE_MAP, "ZZZZ", raising=False)
    result = prepare_exchange_registry(
        dynamic_universe_result={"symbols": [
            {"symbol": "BRK.B", "exchange_code": "NYS"},
            {"symbol": "ZZZZ", "exchange_code": "NYS"},
        ]},
        benchmark_symbols=(),
    )
    assert result["failed_symbols"] == ["BRK.B"]
    assert result["registered_symbols"] == ["ZZZZ"]
    assert symbols.resolve_exchange("ZZZZ") == "NYSE"


def test_prepare_preserves_metadata_when_lifecycle_bare_symbol_duplicates(monkeypatch):
    from trader.us import symbols
    from trader.us.exchange_registry import prepare_exchange_registry

    monkeypatch.delitem(symbols._SYMBOL_EXCHANGE_MAP, "ZZZZ", raising=False)
    result = prepare_exchange_registry(
        dynamic_universe_result={"symbols": []},
        benchmark_symbols=(),
        open_positions=[{"symbol": "ZZZZ", "exchange": "NYS"}, "ZZZZ"],
    )
    assert result["failed_count"] == 0
    assert result["registered_symbols"] == ["ZZZZ"]
    assert symbols.resolve_exchange("ZZZZ") == "NYSE"


def test_all_prep_benchmarks_resolve():
    from trader.us.symbols import resolve_exchange

    benchmarks = {"SPY", "QQQ", "QQQM", "SMH", "SOXX", "DIA", "IWM", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"}
    assert {resolve_exchange(symbol) for symbol in benchmarks} <= {"NASDAQ", "NYSE", "AMEX"}
