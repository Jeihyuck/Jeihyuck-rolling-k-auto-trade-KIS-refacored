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

def test_load_us_orders_uses_timestamp_fallback(monkeypatch):
    from trader.us.db import repos
    calls=[]
    class Conn:
        def execution_options(self, **kwargs): return self
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, params):
            text=str(sql)
            calls.append((text, params))
            if 'created_at >=' in text and 'us_orders' in text:
                return [{'status':'ACK','symbol':'AAPL'}]
            return []
    class Engine:
        def connect(self): return Conn()
    monkeypatch.setattr(repos, '_get_engine_or_none', lambda: Engine())
    rows=drr.load_us_orders('2026-06-16')
    assert len(rows)==1
    assert any('created_at >=' in sql for sql,_ in calls)
    assert any('start_ts' in params and 'end_ts' in params for _,params in calls)

def test_daily_report_order_source_empty_warning(monkeypatch, tmp_path):
    monkeypatch.setattr(drr, 'load_us_orders', lambda td: [])
    monkeypatch.setattr(drr, 'load_us_fills_count', lambda td: 0)
    monkeypatch.setattr(drr, 'load_balance_confirmed_count', lambda td: 0)
    monkeypatch.setattr(drr, 'load_router_summary_ack_count', lambda td, session=None: 0)
    monkeypatch.chdir(tmp_path)
    result=drr.run_daily_report(env='practice', session='close', trade_date='2026-06-16', offline=False)
    assert 'ORDER_SOURCE_EMPTY' in result['report']['warnings']

def test_daily_report_does_not_count_balance_confirmed_as_broker_sent(monkeypatch, tmp_path):
    monkeypatch.setattr(drr, 'load_us_orders', lambda td: [
        {'status': 'ACK', 'side': 'BUY'},
        {'status': 'ACK', 'side': 'SELL'},
        {'status': 'BALANCE_CONFIRMED', 'side': 'BUY'},
        {'status': 'BALANCE_CONFIRMED', 'side': 'SELL'},
    ])
    monkeypatch.setattr(drr, 'load_us_fills_breakdown', lambda td: {
        'fills_count': 2,
        'real_broker_buys': 1,
        'real_broker_sells': 1,
        'synthetic_reconcile_buys': 0,
        'synthetic_reconcile_sells': 0,
    })
    monkeypatch.setattr(drr, 'load_balance_confirmed_count', lambda td: 2)
    monkeypatch.setattr(drr, 'load_router_summary_ack_count', lambda td, session=None: 2)
    monkeypatch.chdir(tmp_path)

    report = drr.run_daily_report(env='practice', session='close', trade_date='2026-06-29', offline=False)['report']

    assert report['orders_ack_total'] == 2
    assert report['orders_balance_confirmed_total'] == 2
    assert report['orders_sent_total'] == 2
    assert report['orders_ack'] == 2
