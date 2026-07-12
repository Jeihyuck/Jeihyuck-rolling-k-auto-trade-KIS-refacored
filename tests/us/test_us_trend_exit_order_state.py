from trader.us.db import repos

def setup_function(): repos.reset_memory_stores()

def test_trend_stage_transitions_and_lifecycle_isolation():
    repos.mark_us_position_exit_stage("2026-07-10","AMD","trend_trim","k1","PENDING","life1")
    tr=repos.load_us_position_risk_state("AMD","2026-07-10")["state"]["trend"]
    assert tr["trend_trim_pending"] is True and not tr.get("trend_trim_done", False)
    repos.mark_us_position_exit_stage("2026-07-10","AMD","trend_trim","k1","ACK","life1")
    assert repos.load_us_position_risk_state("AMD","2026-07-10")["state"]["trend"]["trend_trim_pending"] is True
    repos.mark_us_position_exit_stage("2026-07-10","AMD","trend_trim","k1","FILLED","life1")
    tr=repos.load_us_position_risk_state("AMD","2026-07-10")["state"]["trend"]
    assert tr["trend_trim_done"] is True and tr["trend_trim_pending"] is False
    repos.mark_us_position_exit_stage("2026-07-11","AMD","trend_trim","k2","REJECTED","life2")
    tr=repos.load_us_position_risk_state("AMD","2026-07-11")["state"]["trend"]
    assert tr["lifecycle_id"] == "life2" and tr["trend_trim_done"] is False
