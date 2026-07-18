import trader.us.runner.daily_report_runner as d

def test_daily_report_excludes_synthetic_from_kis_actual_count(monkeypatch):
    monkeypatch.setattr('trader.us.db.repos._get_engine_or_none',lambda:object())
    monkeypatch.setattr(d,'_read_autocommit',lambda *a,**k:[
      {'side':'SELL','order_no':'A','evidence_type':'KIS_EXECUTION_ACTUAL','is_synthetic':False,'fill_source':'kis','n':1},
      {'side':'SELL','order_no':'A','evidence_type':'BALANCE_DELTA_SYNTHETIC','is_synthetic':True,'fill_source':'balance','n':1}])
    r=d.load_us_fills_breakdown('2026-07-16'); assert r['kis_actual_fill_execution_count']==1 and r['balance_synthetic_confirmation_count']==1 and r['accounting_confirmed_order_count']==1

def test_consistency_never_improves_severe_error():
    assert d.worsen_consistency('REPORT_INCONSISTENT_POSITION_VALUE','OK')=='REPORT_INCONSISTENT_POSITION_VALUE'
