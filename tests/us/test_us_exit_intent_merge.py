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


def test_ge_log_shaped_soft_stop_structured_fields_beat_human_reason():
    rows = merge_exit_intents_by_symbol([
        {"symbol": "GE", "side": "SELL", "qty": 3, "reason": "trend_trim"},
        {"symbol": "GE", "side": "SELL", "qty": 4,
         "reason": "pnl_pct=-0.054 <= -0.05 confirmed_ticks=2/2",
         "exit_type": "soft_stop_loss",
         "meta": {"exit_reason": "soft_stop_loss", "stop_type": "soft_stop_loss"}},
    ])
    assert len(rows) == 1
    assert rows[0]["selected_exit_family"] == "SOFT_STOP"
    assert rows[0]["selected_exit_reason"] == "pnl_pct=-0.054 <= -0.05 confirmed_ticks=2/2"
    assert "trend_trim" in rows[0]["absorbed_exit_reasons"]
    assert rows[0]["final_qty"] == 4
