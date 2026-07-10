def test_entry_degraded_status_is_warning_contract():
    from trader.us.runner.status_contract import classify_tick_status
    assert classify_tick_status({"status": "OK_NO_TRADE_ENTRY_DEGRADED", "entry_error_type": "watchlist_load_timeout"}) == "warning"
    assert classify_tick_status({"status": "OK_EXIT_SENT_ENTRY_DEGRADED", "entry_error_type": "watchlist_load_timeout"}) == "warning"


def test_run_trade_tick_exit_survives_missing_fallback_artifact(monkeypatch):
    from tests.us.test_us_exit_first_routing import _patch_tick_basics
    from trader.us.runner.trade_tick_runner import run_trade_tick

    calls = []
    _patch_tick_basics(monkeypatch, calls)
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent})

    result = run_trade_tick(session="am", env="practice", offline=False, force_now="2026-06-05T10:00:00-04:00", kis_order_allowed=False)

    assert result["status"] != "FAILED"
    assert result["entry_eval_status"] == "DEGRADED"
    assert result["entry_error_type"] == "watchlist_load_timeout"
    assert result["watchlist_fallback_used"] == 0
    assert result["entry_intents"] == 0
    assert result["orders_ack"] == 1
    assert result["exit_routed_before_entry"] == 1
