# -*- coding: utf-8 -*-
"""USDataProvider prep cache contract tests.

Prep data provider must cache daily/current price calls to avoid
repeated KIS API calls across multiple strategies.
"""

from trader.us.data_provider import USDataProvider


class FakeKisUSClient:
    """Fake KIS client for testing cache behavior."""
    
    def __init__(self):
        self.daily_calls = 0
        self.price_calls = 0
        self.stats = {
            "get_retry_count": 0,
            "post_retry_count": 0,
            "http_fail_final_count": 0,
        }

    def get_us_daily_price(self, symbol, exchange, count=120):
        self.daily_calls += 1
        return [{"xymd": "20260101", "clos": "100", "open": "99", "high": "101", "low": "98", "tvol": "1000"}]

    def get_us_price(self, symbol, exchange):
        self.price_calls += 1
        return {"output": {"last": "100", "open": "99", "high": "101", "low": "98", "tvol": "1000"}}


def test_daily_cache_reuses_same_symbol():
    """Daily price cache should reuse data for same symbol/exchange/count."""
    provider = USDataProvider(offline=False, cache_enabled=True)
    fake = FakeKisUSClient()
    provider._client = fake

    # First call - cache miss
    data1 = provider.get_daily_prices("AAPL", "NASDAQ", 120)
    assert fake.daily_calls == 1
    assert provider.stats["daily_miss"] == 1
    assert provider.stats["daily_hit"] == 0
    
    # Second call - cache hit
    data2 = provider.get_daily_prices("AAPL", "NASDAQ", 120)
    assert fake.daily_calls == 1  # No additional call
    assert provider.stats["daily_miss"] == 1
    assert provider.stats["daily_hit"] == 1
    
    # Same data returned
    assert data1 == data2


def test_price_cache_reuses_same_symbol():
    """Current price cache should reuse data for same symbol/exchange."""
    provider = USDataProvider(offline=False, cache_enabled=True)
    fake = FakeKisUSClient()
    provider._client = fake

    # First call - cache miss
    price1 = provider.get_current_price("AAPL", "NASDAQ")
    assert fake.price_calls == 1
    assert provider.stats["price_miss"] == 1
    assert provider.stats["price_hit"] == 0
    
    # Second call - cache hit
    price2 = provider.get_current_price("AAPL", "NASDAQ")
    assert fake.price_calls == 1  # No additional call
    assert provider.stats["price_miss"] == 1
    assert provider.stats["price_hit"] == 1
    
    # Same data returned
    assert price1 == price2


def test_cache_separates_different_symbols():
    """Cache should maintain separate entries for different symbols."""
    provider = USDataProvider(offline=False, cache_enabled=True)
    fake = FakeKisUSClient()
    provider._client = fake

    provider.get_daily_prices("AAPL", "NASDAQ")
    provider.get_daily_prices("MSFT", "NASDAQ")
    
    assert fake.daily_calls == 2  # Both symbols called
    assert provider.stats["daily_miss"] == 2
    assert provider.stats["daily_hit"] == 0


def test_cache_separates_different_counts():
    """Cache should maintain separate entries for different count parameters."""
    provider = USDataProvider(offline=False, cache_enabled=True)
    fake = FakeKisUSClient()
    provider._client = fake

    provider.get_daily_prices("AAPL", "NASDAQ", 60)
    provider.get_daily_prices("AAPL", "NASDAQ", 120)
    
    assert fake.daily_calls == 2  # Both counts called
    assert provider.stats["daily_miss"] == 2


def test_cache_disabled_always_calls_client():
    """When cache is disabled, every call should go to client."""
    provider = USDataProvider(offline=False, cache_enabled=False)
    fake = FakeKisUSClient()
    provider._client = fake

    provider.get_daily_prices("AAPL", "NASDAQ")
    provider.get_daily_prices("AAPL", "NASDAQ")
    
    assert fake.daily_calls == 2  # Both calls made
    assert provider.stats["daily_hit"] == 0
    assert provider.stats["daily_miss"] == 0  # No cache tracking


def test_cache_tracks_ok_and_fail_symbols():
    """Cache should track which symbols succeeded or failed."""
    provider = USDataProvider(offline=False, cache_enabled=True)
    fake = FakeKisUSClient()
    provider._client = fake

    provider.get_daily_prices("AAPL", "NASDAQ")
    provider.get_current_price("MSFT", "NASDAQ")
    
    assert "AAPL" in provider.stats["daily_ok_symbols"]
    assert "MSFT" in provider.stats["price_ok_symbols"]
    assert len(provider.stats["daily_fail_symbols"]) == 0
    assert len(provider.stats["price_fail_symbols"]) == 0


def test_offline_mode_with_cache():
    """Cache should work in offline mode with stub data."""
    provider = USDataProvider(offline=True, cache_enabled=True)

    # First call
    data1 = provider.get_daily_prices("AAPL", "NASDAQ")
    assert provider.stats["daily_miss"] == 1
    assert "AAPL" in provider.stats["daily_ok_symbols"]
    
    # Second call - cache hit
    data2 = provider.get_daily_prices("AAPL", "NASDAQ")
    assert provider.stats["daily_hit"] == 1
    assert data1 == data2


def test_client_stats_exposed():
    """Provider should expose client stats."""
    provider = USDataProvider(offline=False, cache_enabled=True)
    fake = FakeKisUSClient()
    fake.stats["get_retry_count"] = 5
    provider._client = fake

    stats = provider.get_client_stats()
    assert stats["get_retry_count"] == 5
