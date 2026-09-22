from __future__ import annotations

import time

from trader.marketdata.kis_ws_price import KisWebSocketPriceService


def test_kr_trade_parser_extracts_fresh_price_and_top_of_book():
    fields = ["005930", "090001", "70100", "2", "100", "0.14", "70000", "69900", "70200", "69800", "70200", "70100"]
    parsed = KisWebSocketPriceService.parse_kr_trade("^".join(fields))
    assert parsed == {
        "market": "KR",
        "symbol": "005930",
        "last": 70100.0,
        "ask": 70200.0,
        "bid": 70100.0,
    }


def test_us_trade_parser_extracts_symbol_price_bid_ask():
    fields = [
        "DNASAAPL", "4", "20260922", "20260922", "101500", "20260922", "231500",
        "200.0", "205.0", "198.0", "203.5", "2", "1.0", "0.5", "203.4", "203.6",
    ]
    parsed = KisWebSocketPriceService.parse_us_trade("^".join(fields))
    assert parsed["market"] == "US"
    assert parsed["symbol"] == "AAPL"
    assert parsed["exchange"] == "NASDAQ"
    assert parsed["last"] == 203.5
    assert parsed["bid"] == 203.4
    assert parsed["ask"] == 203.6


def test_us_subscription_keys_are_exchange_specific():
    assert KisWebSocketPriceService.us_subscription_key("AAPL", "NASDAQ") == "DNASAAPL"
    assert KisWebSocketPriceService.us_subscription_key("IBM", "NYSE") == "DNYSIBM"
    assert KisWebSocketPriceService.us_subscription_key("SPY", "AMEX") == "DAMSSPY"


def test_freshness_cache_rejects_stale_quote():
    svc = KisWebSocketPriceService()
    svc.put_quote(market="KR", symbol="005930", last=70000, received_at=time.time() - 30)
    assert svc.get_fresh_quote("KR", "005930", max_age_sec=5) is None
    svc.put_quote(market="KR", symbol="005930", last=70100, received_at=time.time())
    quote = svc.get_fresh_quote("KR", "005930", max_age_sec=5)
    assert quote and quote["last"] == 70100
    assert quote["source"] == "KIS_WEBSOCKET"
