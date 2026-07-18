from trader.us.data_provider import USDataProvider


def test_get_fills_by_order_no_uses_supplied_us_trade_date(monkeypatch):
    calls=[]
    provider=USDataProvider(offline=False)
    provider._offline=False
    def fake_orders(trade_date):
        calls.append(trade_date)
        return [{"order_no":"O1","symbol":"AMD","side":"SELL","filled_qty":3,"avg_price":100,"status":"PARTIALLY_FILLED"}]
    monkeypatch.setattr(provider,"get_today_orders",fake_orders)
    result=provider.get_fills_by_order_no(order_no="O1",symbol="AMD",trade_date="2026-07-17")
    assert result["filled_qty"] == 3
    assert calls == ["2026-07-17"]
