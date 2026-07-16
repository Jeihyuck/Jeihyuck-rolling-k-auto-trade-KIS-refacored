from __future__ import annotations


def test_sell_empty_exchange_enriched_from_holding_before_risk_gate(monkeypatch):
    from trader.us.execution import order_router

    captured = {}
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_AGENT_ENABLED", "1")
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setattr(order_router, "save_order_intent", lambda intent: "intent-1")
    monkeypatch.setattr(order_router, "save_dry_run_order", lambda **kwargs: None)
    monkeypatch.setattr(order_router, "mark_order_intent_dry_run", lambda *a, **k: None)
    monkeypatch.setattr(order_router, "load_today_order_keys", lambda *a, **k: set())

    def fake_assert(intent, **kwargs):
        captured.update(intent)
    monkeypatch.setattr(order_router, "assert_order_allowed", fake_assert)

    result = order_router.route_order(
        {"symbol": "AAPL", "side": "SELL", "qty": 1, "limit_price": 100, "notional_usd": 100, "exchange": "", "current_holding": {"exchange": "NASDAQ"}},
        current_position_symbols={"AAPL"},
        signal_only=False,
    )
    assert captured["exchange"] == "NASDAQ"
    assert result["status"] in {"DRY_RUN", "SIGNAL_ONLY"}
