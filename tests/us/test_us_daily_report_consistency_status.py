from trader.us.runner.daily_report_runner import worsen_consistency


def test_failed_consistency_maps_to_failed_reconcile(monkeypatch):
    from trader.us.runner import daily_report_runner as d
    r=d.run_daily_report(env="practice",session="close",trade_date="2026-07-16",final_balance={"positions":[{"symbol":"AMD","qty":1,"current_px":0}],"total_pvs":0},final_positions=[{"symbol":"AMD","qty":1,"current_px":0}],kis_fills=[])
    assert r["report"]["report_consistency"] == "REPORT_INCONSISTENT_POSITION_VALUE"
    assert r["status"] == "FAILED_RECONCILE"


def test_worsen_consistency_never_downgrades_failed():
    assert worsen_consistency("FAILED","OK") == "FAILED"
