# -*- coding: utf-8 -*-
"""US Entry Input Contract 테스트.

tickers를 dict list로 변환해서 unhashable type: 'dict' 발생하는 문제 방지.
"""
import pytest
from trader.us.pb1.us_entry_engine import normalize_us_entry_input


def test_normalize_string_list():
    """list[str]이면 그대로 반환."""
    symbols, entries = normalize_us_entry_input(["AAPL", "MSFT"], None)
    assert symbols == ["AAPL", "MSFT"]
    assert entries == []


def test_normalize_dict_list():
    """list[dict]이면 symbol 추출 후 warning."""
    tickers = [
        {"symbol": "AAPL", "exchange": "NASDAQ"},
        {"symbol": "MSFT", "exchange": "NASDAQ"},
    ]
    symbols, entries = normalize_us_entry_input(tickers, None)
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert len(entries) == 2


def test_normalize_watchlist_entries_priority():
    """watchlist_entries가 있으면 우선 사용."""
    watchlist = [
        {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.9, "rank": 1},
        {"symbol": "GOOGL", "exchange": "NASDAQ", "score": 0.85, "rank": 2},
    ]
    symbols, entries = normalize_us_entry_input(None, watchlist)
    assert symbols == ["AAPL", "GOOGL"]
    assert len(entries) == 2
    assert entries[0]["score"] == 0.9


def test_sold_today_with_string_symbols_only():
    """sold_today 검사는 반드시 str symbol로만 수행.
    
    dict가 sold_today에 들어가면 unhashable type: 'dict' 발생.
    """
    sold = {"AAPL", "MSFT"}
    
    # 정상: str
    assert "AAPL" in sold
    
    # 금지: dict
    # {"symbol": "AAPL"} in sold → 에러
    # 이를 방지하기 위해 normalize_us_entry_input이 list[str]을 반환해야 함
    symbols, _ = normalize_us_entry_input(["AAPL", "MSFT"], None)
    for sym in symbols:
        assert isinstance(sym, str)
        # sold_today 검사 시뮬레이션
        if sym in sold:
            pass  # OK
