from trader.us.runner.daily_report_runner import load_us_fills_breakdown

class Engine:
    pass

def test_superseded_synthetic_excluded_from_fills_count(monkeypatch):
    rows=[{"side":"SELL","order_no":"O1","evidence_type":"BALANCE_DELTA_SYNTHETIC","is_synthetic":True,"accounting_active":False,"fill_source":"balance","n":1},
          {"side":"SELL","order_no":"O1","evidence_type":"KIS_ORDER_DETAIL_ACTUAL","is_synthetic":False,"accounting_active":True,"fill_source":"fills","n":1}]
    monkeypatch.setattr('trader.us.db.repos._get_engine_or_none', lambda: Engine())
    monkeypatch.setattr('trader.us.runner.daily_report_runner._read_autocommit', lambda *a,**k: rows)
    r=load_us_fills_breakdown("2026-07-16")
    assert r["physical_fill_row_count"] == 2
    assert r["fills_count"] == 1
    assert r["superseded_synthetic_row_count"] == 1
    assert r["accounting_confirmed_order_count"] == 1
