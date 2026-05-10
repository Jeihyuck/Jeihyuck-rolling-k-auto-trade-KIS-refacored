# -*- coding: utf-8 -*-
"""US no duplicate buy when holding tests."""
import pytest
from unittest.mock import MagicMock
from trader.us.pb1.us_entry_engine import generate_entry_intents


def test_us_no_duplicate_buy_when_kis_holding():
    """positions에 AAPL, AAOI, AMZN이 있으면 watchlist에 있어도 BUY intent 생성 안 됨."""
    provider = MagicMock()
    provider._offline = True
    
    # Watchlist에는 AAOI, AAPL, AMZN이 있음
    watchlist_entries = [
        {"symbol": "AAOI", "exchange": "NASDAQ", "score": 0.8},
        {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.75},
        {"symbol": "AMZN", "exchange": "NASDAQ", "score": 0.7},
        {"symbol": "GOOGL", "exchange": "NASDAQ", "score": 0.65},  # 보유 안 함
    ]
    
    # KIS balance 기반 current_position_symbols
    current_position_symbols = {"AAPL", "AAOI", "AMZN", "AVGO", "CIEN"}
    
    intents = generate_entry_intents(
        tickers=None,
        provider=provider,
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=5,
        capital_usd_cap=50000.0,
        watchlist_entries=watchlist_entries,
        current_position_symbols=current_position_symbols,
    )
    
    # AAOI, AAPL, AMZN은 has_kis_position으로 차단되어야 함
    # GOOGL만 평가 대상
    intent_symbols = [i["symbol"] for i in intents]
    assert "AAOI" not in intent_symbols
    assert "AAPL" not in intent_symbols
    assert "AMZN" not in intent_symbols
    # GOOGL은 포함될 수 있음 (score/price 조건 만족 시)


def test_us_skip_reason_has_kis_position():
    """KIS position 보유 종목은 has_kis_position으로 skip."""
    provider = MagicMock()
    provider._offline = True
    
    watchlist_entries = [
        {"symbol": "AAPL", "exchange": "NASDAQ", "score": 0.8},
    ]
    
    current_position_symbols = {"AAPL"}
    
    intents = generate_entry_intents(
        tickers=None,
        provider=provider,
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=1,
        capital_usd_cap=50000.0,
        watchlist_entries=watchlist_entries,
        current_position_symbols=current_position_symbols,
    )
    
    # AAPL은 차단되어야 함
    assert len([i for i in intents if i["symbol"] == "AAPL"]) == 0


def test_us_sold_today_also_blocks():
    """sold_today에 있는 종목도 차단."""
    provider = MagicMock()
    provider._offline = True
    
    watchlist_entries = [
        {"symbol": "TSLA", "exchange": "NASDAQ", "score": 0.8},
    ]
    
    intents = generate_entry_intents(
        tickers=None,
        provider=provider,
        sold_today={"TSLA"},
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=50000.0,
        watchlist_entries=watchlist_entries,
        current_position_symbols=set(),
    )
    
    # TSLA는 sold_today로 차단
    assert len([i for i in intents if i["symbol"] == "TSLA"]) == 0
