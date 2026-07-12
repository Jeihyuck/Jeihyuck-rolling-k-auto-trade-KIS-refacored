from trader.us.db import repos
from trader.us.runner.trade_tick_runner import _mark_trend_stages_from_records


def setup_function(): repos.reset_memory_stores()

def test_fill_recovers_trend_stage_from_order_no():
    repos.save_order_ack({"client_order_key":"c1","symbol":"AMD","side":"SELL","qty_requested":10,"order_no":"o1","status":"ACK","meta":{"trend_stage":"trend_trim","position_lifecycle_id":"life1"}}, trade_date="2026-07-10")
    repos.save_us_position_risk_state("AMD","2026-07-10",{"state":{"lifecycle":{"lifecycle_id":"life1"}}})
    _mark_trend_stages_from_records([{"symbol":"AMD","side":"SELL","qty":10,"order_no":"o1"}], trade_date="2026-07-10", status="FILLED")
    tr=repos.load_us_position_risk_state("AMD","2026-07-10")["state"]["trend"]
    assert tr["trend_trim_pending"] is False and tr["trend_trim_done"] is True

def test_partial_and_stale_lifecycle():
    repos.save_us_position_risk_state("AMD","2026-07-10",{"state":{"lifecycle":{"lifecycle_id":"current"},"trend":{"lifecycle_id":"current"}}})
    repos.mark_us_position_exit_stage("2026-07-10","AMD","trend_trim","k","PARTIALLY_FILLED","current")
    assert repos.load_us_position_risk_state("AMD","2026-07-10")["state"]["trend"]["trend_trim_pending"] is True
    repos.mark_us_position_exit_stage("2026-07-10","AMD","trend_trim","old","FILLED","oldlife")
    tr=repos.load_us_position_risk_state("AMD","2026-07-10")["state"]["trend"]
    assert tr["lifecycle_id"] == "current" and tr.get("trend_trim_done") is not True
