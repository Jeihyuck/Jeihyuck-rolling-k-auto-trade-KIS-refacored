from trader.us.pb1.us_exit_engine import evaluate_exit

def test_hard_stop_beats_bad_trend_and_missing_daily():
    pos={"symbol":"AMD","qty":10,"orderable_qty":10,"entry_price":100,"holding_trade_days":3,"trend_state":"EXIT","final30_absent_streak":3,"weakness_signals":["X","Y","Z"]}
    assert evaluate_exit(pos, 90)["exit_reason"] == "hard_stop_full_exit"

def test_trailing_beats_trend_warning():
    pos={"symbol":"AMD","qty":10,"orderable_qty":10,"entry_price":100,"max_price":120,"trend_state":"WARNING","weakness_signals":["FINAL30_ABSENT"]}
    assert evaluate_exit(pos, 113)["exit_type"] == "profit_trailing_stop"
