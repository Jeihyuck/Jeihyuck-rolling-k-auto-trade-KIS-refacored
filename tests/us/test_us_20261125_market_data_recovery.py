"""11/25 incident regression: quote venue, empty HTTP, and recovery reason."""
import pytest
from trader.us.symbols import resolve_exchange, resolve_quote_exchange, resolve_quote_exchange_code, resolve_order_exchange
from trader.us.prep_effective import benchmark_recovery_required
from trader.us.data_provider import USDataProvider
from trader.us.db.price_daily_repo import reset_us_daily_memory

@pytest.mark.parametrize("symbol", "SPY DIA IWM RSP XLE XLF XLI XLK XLP XLU XLV".split())
def test_etf_quote_route_separate_from_order(symbol):
    assert resolve_exchange(symbol) == "NYSE"
    assert resolve_quote_exchange(symbol) == "AMEX"
    assert resolve_quote_exchange_code(symbol) == "AMS"
    assert resolve_order_exchange(symbol) == "NYSE"

@pytest.mark.parametrize("symbol", "QQQ SMH QQQM SOXX TQQQ KHC".split())
def test_nasdaq_quote_route_unchanged(symbol):
    assert resolve_quote_exchange_code(symbol) == "NAS"
    assert resolve_order_exchange(symbol) == "NASDAQ"

def test_empty_http_is_attempted_but_failed():
    reset_us_daily_memory()
    class EmptyClient:
        def get_us_daily_price_history(self, *args, **kwargs):
            return []
    p = USDataProvider(offline=True)
    p._client = EmptyClient()
    p._offline = False
    result = p.get_completed_daily_prices_result("SPY", "AMEX", trade_date="2026-11-24", required_bars=260, allow_http_sync=True)
    assert result["quality"] == "STALE"
    assert result["http_sync_attempted"] is True
    assert result["http_sync_succeeded"] is False
    assert result["http_sync_call_count"] == 1

def test_benchmark_recovery_does_not_override_risk_off():
    assert benchmark_recovery_required({"recoverable_contract": True, "benchmark_daily_failed": True})
    assert not benchmark_recovery_required({"recoverable_contract": True, "trade_block_reason": "RISK_OFF"})
    assert not benchmark_recovery_required({"benchmark_daily_failed": True})
