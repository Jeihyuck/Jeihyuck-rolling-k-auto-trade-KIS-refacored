from trader.us.execution.order_router import enrich_sell_exchange


def test_sell_exchange_empty_but_holding_exchange_exists():
    intent = {"symbol": "AAPL", "side": "SELL", "exchange": "", "current_holding": {"exchange": "NASDAQ"}}
    assert enrich_sell_exchange(intent) == "NASDAQ"
    assert intent["exchange"] == "NASDAQ"
