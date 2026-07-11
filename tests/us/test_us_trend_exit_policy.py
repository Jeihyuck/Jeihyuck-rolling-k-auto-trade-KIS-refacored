from trader.us.pb1.us_exit_engine import evaluate_exit

def test_one_day_warning_no_sell_but_two_day_trim_and_exit_priority():
    base={"symbol":"AMD","qty":10,"orderable_qty":10,"entry_price":100,"holding_trade_days":2}
    assert evaluate_exit({**base,"trend_state":"WARNING","final30_absent_streak":1,"weakness_signals":["FINAL30_ABSENT"]}, 101) is None
    trim=evaluate_exit({**base,"trend_state":"TRIM","final30_absent_streak":2,"below_ma20_streak":2,"weakness_signals":["FINAL30_ABSENT_2D","BELOW_MA20_2D"]}, 99)
    assert trim["exit_type"] == "trend_deterioration_trim" and trim["qty"] == 3
    hard=evaluate_exit({**base,"trend_state":"TRIM","weakness_signals":["X"]}, 90)
    assert hard["exit_reason"] == "hard_stop_full_exit"

def test_trend_exit_full_and_min_hold_blocks():
    pos={"symbol":"AMD","qty":5,"orderable_qty":5,"entry_price":100,"holding_trade_days":1,"trend_state":"EXIT","below_ma50_streak":2,"weakness_signals":["BELOW_MA50_2D"]}
    assert evaluate_exit(pos, 99) is None
    pos["holding_trade_days"] = 2
    assert evaluate_exit(pos, 99)["exit_type"] == "trend_deterioration_exit"
