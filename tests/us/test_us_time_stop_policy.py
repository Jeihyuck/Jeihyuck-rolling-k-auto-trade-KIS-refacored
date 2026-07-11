from trader.us.pb1.us_exit_engine import evaluate_exit

def test_time_stop_trim_exit_and_profit_protection():
    pos={"symbol":"IBM","qty":10,"orderable_qty":10,"entry_price":100,"holding_trade_days":20,"trend_state":"WARNING","final30_absent_streak":2,"weakness_signals":["FINAL30_ABSENT_2D"]}
    assert evaluate_exit(pos, 101)["exit_type"] == "time_stop_trim"
    assert evaluate_exit({**pos,"trend_state":"HEALTHY","current_price":107,"ma20":100},107) is None
    done={**pos,"holding_trade_days":25,"trend_trim_done":True}
    assert evaluate_exit(done,101)["exit_type"] == "time_stop_exit"
