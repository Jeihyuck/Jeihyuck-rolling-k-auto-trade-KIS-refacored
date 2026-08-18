# -*- coding: utf-8 -*-
"""US exit engine exchange normalization tests.

Position with exchange='NASD' should successfully fetch price without 'unknown exchange' error.
"""
import logging
from trader.us.pb1.us_exit_engine import generate_exit_intents


class MockProvider:
    """Mock USDataProvider for testing."""
    
    def __init__(self, prices: dict[tuple[str, str], dict]):
        """
        Args:
            prices: {(symbol, exchange): {"last": "100.00", ...}}
        """
        self.prices = prices
        self.calls = []  # Track all get_current_price calls
    
    def get_current_price(self, symbol: str, exchange: str) -> dict:
        """Mock get_current_price."""
        self.calls.append((symbol, exchange))
        key = (symbol, exchange)
        if key not in self.prices:
            raise ValueError(f"Mock: price not available for {symbol} on {exchange}")
        return self.prices[key]


def test_exit_normalizes_nasd_to_nasdaq():
    """Position with exchange='NASD' should normalize to NASDAQ for price lookup."""
    provider = MockProvider({
        ("AAPL", "NASDAQ"): {"last": "150.00"},
    })
    
    positions = [
        {
            "symbol": "AAPL",
            "exchange": "NASD",  # KIS API raw value
            "qty": 10,
            "entry_price": 100.0,
            "entry_date": "2024-01-01",
            "entry_time": "2024-01-01T14:30:00+00:00",
            "max_price": 160.0,
        }
    ]
    
    intents = generate_exit_intents(positions, provider)
    
    # Should successfully fetch price (no exception)
    assert len(provider.calls) == 1
    called_symbol, called_exchange = provider.calls[0]
    assert called_symbol == "AAPL"
    assert called_exchange == "NASDAQ", f"Expected NASDAQ, got {called_exchange}"
    
    # Should generate exit intent (trailing stop: 150 < 160 * 0.95)
    assert len(intents) == 1
    assert intents[0]["symbol"] == "AAPL"
    assert intents[0]["exit_type"] == "trailing_stop"


def test_exit_normalizes_nys_to_nyse():
    """Position with exchange='NYS' should normalize to NYSE for price lookup."""
    provider = MockProvider({
        ("TSM", "NYSE"): {"last": "90.00"},
    })
    
    positions = [
        {
            "symbol": "TSM",
            "exchange": "NYS",  # KIS API raw value
            "qty": 10,
            "entry_price": 100.0,
            "entry_date": "2024-01-01",
            "max_price": 100.0,
        }
    ]
    
    intents = generate_exit_intents(positions, provider)
    
    # Should successfully fetch price (no exception)
    assert len(provider.calls) == 1
    called_symbol, called_exchange = provider.calls[0]
    assert called_symbol == "TSM"
    assert called_exchange == "NYSE", f"Expected NYSE, got {called_exchange}"
    
    # Should generate exit intent (hard stop: 90 < 100 * 0.93 for 7% stop)
    assert len(intents) == 1
    assert intents[0]["symbol"] == "TSM"
    assert intents[0]["exit_type"] == "hard_stop"


def test_exit_handles_unknown_exchange_gracefully():
    """Position with unknown exchange should log warning and skip (not crash)."""
    provider = MockProvider({})
    
    positions = [
        {
            "symbol": "AAPL",
            "exchange": "UNKNOWN_EXCH",  # Invalid exchange
            "qty": 10,
            "entry_price": 100.0,
            "entry_date": "2024-01-01",
            "max_price": 100.0,
        }
    ]
    
    # Should not raise exception
    intents = generate_exit_intents(positions, provider)
    
    # Should skip position with unknown exchange (after warning log)
    assert len(intents) == 0


def test_exit_handles_multiple_exchanges():
    """Multiple positions with different exchanges should all be normalized."""
    provider = MockProvider({
        ("AAPL", "NASDAQ"): {"last": "80.00"},
        ("TSM", "NYSE"): {"last": "80.00"},
    })
    
    positions = [
        {
            "symbol": "AAPL",
            "exchange": "NASD",
            "qty": 10,
            "entry_price": 100.0,
            "entry_date": "2024-01-01",
            "max_price": 100.0,
        },
        {
            "symbol": "TSM",
            "exchange": "NYS",
            "qty": 10,
            "entry_price": 100.0,
            "entry_date": "2024-01-01",
            "max_price": 100.0,
        },
    ]
    
    intents = generate_exit_intents(positions, provider)
    
    # Both should generate exit intents (hard stop: -20%)
    assert len(intents) == 2
    assert {intent["symbol"] for intent in intents} == {"AAPL", "TSM"}
    
    # Both should have normalized exchanges in provider calls
    assert len(provider.calls) == 2
    exchanges_called = {exchange for _, exchange in provider.calls}
    assert exchanges_called == {"NASDAQ", "NYSE"}


def test_exit_no_warning_for_already_normalized():
    """Position with already-normalized exchange should not trigger warning."""
    provider = MockProvider({
        ("AAPL", "NASDAQ"): {"last": "150.00"},
    })
    
    positions = [
        {
            "symbol": "AAPL",
            "exchange": "NASDAQ",  # Already normalized
            "qty": 10,
            "entry_price": 100.0,
            "entry_date": "2024-01-01",
            "max_price": 100.0,
        }
    ]
    
    # Should successfully fetch price without any normalization warning
    intents = generate_exit_intents(positions, provider)
    
    # Should successfully fetch price
    assert len(provider.calls) == 1
    assert provider.calls[0] == ("AAPL", "NASDAQ")
