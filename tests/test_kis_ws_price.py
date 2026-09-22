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


def test_batched_kr_records_update_every_symbol():
    svc = KisWebSocketPriceService()
    rec1 = ["005930", "090001", "70100", "2", "100", "0.14", "70000", "69900", "70200", "69800", "70200", "70100"]
    rec2 = ["000660", "090001", "1900000", "2", "1000", "0.05", "1899000", "1890000", "1910000", "1880000", "1901000", "1900000"]
    rec1 += ["0"] * (46 - len(rec1))
    rec2 += ["0"] * (46 - len(rec2))
    svc._handle_trade_records("^".join(rec1 + rec2), 2, fields_per_record=46, parser=svc.parse_kr_trade)
    assert svc.get_fresh_quote("KR", "005930", max_age_sec=5)["last"] == 70100
    assert svc.get_fresh_quote("KR", "000660", max_age_sec=5)["last"] == 1900000


def test_us_process_uses_existing_us_specific_credentials(monkeypatch):
    svc = KisWebSocketPriceService()
    monkeypatch.setenv("MARKET_SCOPE", "us")
    monkeypatch.setenv("KIS_US_APP_KEY", "us-key")
    monkeypatch.setenv("KIS_US_APP_SECRET", "us-secret")
    monkeypatch.setenv("KIS_APP_KEY", "kr-key")
    monkeypatch.setenv("KIS_APP_SECRET", "kr-secret")
    assert svc._credentials() == ("us-key", "us-secret")


def test_disabled_service_does_not_wait(monkeypatch):
    svc = KisWebSocketPriceService()
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
    monkeypatch.delenv("KIS_WS_PRICE_TEST_ENABLE", raising=False)
    started = time.monotonic()
    assert svc.wait_for_fresh_quote("KR", "005930", max_age_sec=5, wait_sec=2.0) is None
    assert time.monotonic() - started < 0.2


def test_us_trade_parser_accepts_legacy_26_field_layout():
    fields = [
        "DNASAAPL", "AAPL", "4", "20260922", "20260922", "101500", "20260922", "231500",
        "200.0", "205.0", "198.0", "203.5", "2", "1.0", "0.5", "203.4", "203.6",
        "10", "11", "12", "13", "14", "15", "16", "17", "18",
    ]
    parsed = KisWebSocketPriceService.parse_us_trade("^".join(fields))
    assert parsed["market"] == "US"
    assert parsed["symbol"] == "AAPL"
    assert parsed["exchange"] == "NASDAQ"
    assert parsed["last"] == 203.5
    assert parsed["bid"] == 203.4
    assert parsed["ask"] == 203.6
