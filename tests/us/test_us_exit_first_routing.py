from trader.us.runner.trade_tick_runner import route_exit_orders_immediately


def test_exit_route_immediate_sell_does_not_use_watchlist_allowed_symbols(monkeypatch):
    calls = []
    def fake_route(intent, **kwargs):
        calls.append((intent, kwargs))
        return {"status": "ACK", "side": "SELL", "symbol": intent["symbol"], "intent": intent}
    monkeypatch.setattr("trader.us.execution.order_router.route_order", fake_route)
    orders, _ = route_exit_orders_immediately(
        [{"symbol": "BE", "side": "SELL", "qty": 1, "notional_usd": 10}],
        daily_notional=0.0,
        position_count=1,
        effective_budget=1000.0,
        signal_only=False,
        kis_order_allowed=True,
        current_position_symbols={"BE"},
    )
    assert len(orders) == 1 and orders[0]["status"] == "ACK"
    assert calls[0][1]["allowed_symbols"] is None
    assert calls[0][1]["current_position_symbols"] == {"BE"}
