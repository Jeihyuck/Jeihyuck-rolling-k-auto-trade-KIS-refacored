"""2026-11-18 exact 122630 ETF tick contract, strictly NO_TRADE offline."""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import logging

import pytest

from trader.kr_price_utils import (
    normalize_kr_order_price, normalize_kr_etf_order_price,
)


@pytest.mark.parametrize(("raw", "expected", "tick"), [
    (1_999, 1_999, 1),
    (2_000, 2_000, 5),
    (55_215, 55_215, 5),
    (55_216, 55_220, 5),
    (55_300, 55_300, 5),
])
def test_etf_tick_never_uses_equity_price_bands(raw, expected, tick):
    assert normalize_kr_etf_order_price(raw, side="BUY") == (expected, tick)


def test_other_stocks_retain_original_100krw_tick_at_55215():
    assert normalize_kr_order_price(55_215, side="BUY") == (55_300, 100)


def test_kis_wrapper_never_upsizes_122630_55215_in_no_trade(monkeypatch, caplog):
    """Follow the real final KIS wrapper without placing a single order."""
    import trader.kis_wrapper as kw
    import trader.config as cf

    monkeypatch.setenv("NO_TRADE", "1")
    monkeypatch.setattr(kw, "_assert_orders_allowed", lambda *a, **kw: None)
    monkeypatch.setattr(cf, "get_live_gate_status_fresh", lambda **kw: SimpleNamespace(
        allow_live_gate=True, force_block_live=False, reason="test-only",
        window="OFFLINE", now_kst=datetime(2026, 11, 18, 9, 35,
                                           tzinfo=ZoneInfo("Asia/Seoul")),
    ))
    broker = kw.KisAPI.__new__(kw.KisAPI)
    with caplog.at_level(logging.INFO):
        response = broker.buy_stock_limit("122630", 13, 55_215)
    assert response["reason"] == "NO_TRADE"
    assert "qty=13 price=55215" in caplog.text
    assert "normalized=55300" not in caplog.text


def test_kis_wrapper_keeps_equity_normalization_separate(monkeypatch, caplog):
    import trader.kis_wrapper as kw
    import trader.config as cf
    monkeypatch.setenv("NO_TRADE", "1")
    monkeypatch.setattr(kw, "_assert_orders_allowed", lambda *a, **kw: None)
    monkeypatch.setattr(cf, "get_live_gate_status_fresh", lambda **kw: SimpleNamespace(
        allow_live_gate=True, force_block_live=False, reason="test-only",
        window="OFFLINE", now_kst=datetime(2026, 11, 18, 9, 35,
                                           tzinfo=ZoneInfo("Asia/Seoul")),
    ))
    broker = kw.KisAPI.__new__(kw.KisAPI)
    with caplog.at_level(logging.INFO):
        response = broker.buy_stock_limit("005930", 1, 55_215)
    assert response["reason"] == "NO_TRADE"
    assert "normalized=55300" in caplog.text
