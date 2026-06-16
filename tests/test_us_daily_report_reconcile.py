from trader.us.runner.daily_report_runner import reconcile_order_sources

def test_daily_report_reconcile_sources():
    r=reconcile_order_sources(db_orders=7, fills=4, balance_confirmed=7, router_summary=7)
    assert r['orders_ack']==7
    assert r['fill_api_count']==4
    assert r['balance_confirmed_count']==7
    assert 'FILL_API_LESS_THAN_ACK' in r['warnings']
