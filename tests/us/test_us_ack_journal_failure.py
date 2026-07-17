from unittest.mock import patch, MagicMock
from trader.us.execution.order_router import route_order

def test_ack_journal_failure_is_not_reject(monkeypatch,tmp_path):
    i={'trade_date':'2026-07-16','client_order_key':'k','symbol':'AAPL','exchange':'NASDAQ','side':'BUY','qty':1,'limit_price':10,'notional_usd':10}
    k=MagicMock(); k.place_us_buy_order.return_value={'ok':1}
    with patch('trader.us.execution.order_router.resolve_dry_run_for_us_order',return_value=False), patch('trader.us.execution.order_router.assert_order_allowed'), patch('trader.us.db.repos.save_order_intent',return_value=True), patch('trader.us.db.repos.load_today_order_keys',return_value=set()), patch('trader.us.execution.kis_us_response_parser.extract_order_no',return_value='O1'), patch('trader.us.execution.order_journal.append_order_event',side_effect=[{},OSError('fsync')]):
        r=route_order(i,kis_client=k)
    assert r['status']=='ACK_JOURNAL_FAILED_RECONCILE_REQUIRED' and r['kis_ack'] and not r['retry_order']
