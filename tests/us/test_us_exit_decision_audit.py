def test_exit_intent_contains_separate_pnl_and_trailing_audit(monkeypatch):
    from trader.us.pb1.us_exit_engine import evaluate_exit
    monkeypatch.setattr("trader.us.pb1.us_exit_engine.should_skip_exit_due_to_pending_sell", lambda *a, **k: (False, None, None))
    intent = evaluate_exit({"symbol": "BE", "qty": 1, "entry_price": 100, "max_price": 120, "orderable_qty": 1}, 113)
    meta = intent["meta"]
    assert meta["stop_type"] == "trailing_stop"
    assert "trail_high_price" in meta and "trail_drawdown_pct" in meta
    assert "pnl_pct_from_avg_cost" in meta
    assert meta["pnl_pct_from_avg_cost"] != meta["trail_drawdown_pct"]
