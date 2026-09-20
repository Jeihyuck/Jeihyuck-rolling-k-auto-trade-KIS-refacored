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


def test_many_entry_contract_errors_block_buys_but_keep_sell_tick_alive(monkeypatch):
    from tests.us.test_us_exit_first_routing import _patch_tick_basics
    from trader.us.runner.trade_tick_runner import run_trade_tick

    calls = []
    _patch_tick_basics(monkeypatch, calls)

    class _Engine:
        def __init__(self):
            self.last_entry_diagnostics = {}
        def evaluate_exits(self, positions, provider, now):
            return [{
                "symbol": "BE", "exchange": "NASDAQ", "side": "SELL",
                "qty": 1, "available_qty": 1, "limit_price": 100,
                "notional_usd": 2000, "client_order_key": "sell-be",
                "trade_date": "2026-06-05",
            }]
        def evaluate_entries(self, *args, **kwargs):
            self.last_entry_diagnostics = {
                "blocked": [
                    {"symbol": f"BAD{i}", "reason": "ENTRY_EXPLAIN_CONTRACT_ERROR"}
                    for i in range(18)
                ]
            }
            return []

    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner._get_strategy_engine",
        lambda env, offline: _Engine(),
    )
    monkeypatch.setattr(
        "trader.us.execution.order_router.route_order",
        lambda intent, **kwargs: {
            "status": "ACK", "side": intent["side"],
            "symbol": intent["symbol"], "intent": intent,
        },
    )

    prep = {
        "status": "OK",
        "result": {
            "status": "OK",
            "trade_can_proceed": 1,
            "allow_new_buy": True,
            "allow_add_to_existing": True,
            "force_entry_block": False,
            "market_regime": "RISK_ON",
            "capital_scale": 1.0,
            "effective_capital_scale": 1.0,
            "effective_max_new_positions": 15,
        },
    }
    watchlist = [
        {
            "symbol": f"BAD{i}", "exchange": "NASDAQ",
            "score": 1.0, "score_final": 1.0,
            "entry_style_selected": "ENTRY_PULLBACK",
            "pullback_score": 1.0,
            "meta": {"entry_style_selected": "ENTRY_PULLBACK"},
        }
        for i in range(18)
    ]

    result = run_trade_tick(
        session="am",
        env="practice",
        offline=False,
        force_now="2026-06-05T10:00:00-04:00",
        kis_order_allowed=False,
        prep_status_cache=prep,
        locked_watchlist_cache=watchlist,
    )

    assert result["orders_ack"] == 1
    assert result["entry_intents"] == 0
    assert result["entry_eval_status"] == "DEGRADED"
    assert result["entry_degraded_reason"] == "entry_contract_integrity_fail"
    assert result["entry_contract_integrity_block_count"] == 18
    assert result["status"] == "OK_EXIT_SENT_ENTRY_DEGRADED"
