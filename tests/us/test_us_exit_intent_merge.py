from trader.us.runner.trade_tick_runner import merge_exit_intents_by_symbol

def test_soft_stop_absorbs_trend_trim_for_symbol():
    rows = merge_exit_intents_by_symbol([
        {"symbol": "GE", "side": "SELL", "qty": 3, "reason": "trend_trim"},
        {"symbol": "GE", "side": "SELL", "qty": 4, "reason": "soft_stop"},
    ])
    assert len(rows) == 1
    assert rows[0]["selected_exit_reason"] == "soft_stop"
    assert rows[0]["absorbed_exit_reasons"] == ["trend_trim"]
    assert rows[0]["final_qty"] == 4
