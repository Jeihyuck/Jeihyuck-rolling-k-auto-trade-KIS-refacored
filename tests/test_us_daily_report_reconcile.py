from trader.us.runner import daily_report_runner as drr

def test_daily_report_reconcile_sources():
    r=drr.reconcile_order_sources(db_orders=7, fills=4, balance_confirmed=7, router_summary=7)
    assert r['orders_ack']==7
    assert r['fill_api_count']==4
    assert r['balance_confirmed_count']==7
    assert 'FILL_API_LESS_THAN_ACK' in r['warnings']

def test_daily_report_uses_real_sources_without_env(monkeypatch, tmp_path):
    monkeypatch.delenv('US_DAILY_BALANCE_CONFIRMED_COUNT', raising=False)
    monkeypatch.delenv('US_DAILY_ROUTER_ACK_COUNT', raising=False)
    monkeypatch.setattr(drr, 'load_us_orders', lambda td: [{'status':'ACK'} for _ in range(7)])
    monkeypatch.setattr(drr, 'load_us_fills_count', lambda td: 4)
    monkeypatch.setattr(drr, 'load_balance_confirmed_count', lambda td: 7)
    monkeypatch.setattr(drr, 'load_router_summary_ack_count', lambda td, session=None: 7)
    monkeypatch.setattr(drr, 'load_us_prep_status', lambda td: None, raising=False)
    monkeypatch.chdir(tmp_path)
    result=drr.run_daily_report(env='practice', session='close', trade_date='2026-06-16', offline=False)
    report=result['report']
    assert report['orders_ack']==7
    assert report['fill_api_count']==4
    assert report['balance_confirmed_count']==7
    assert 'FILL_API_LESS_THAN_ACK' in report['warnings']
