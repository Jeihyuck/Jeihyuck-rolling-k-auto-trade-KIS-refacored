import trader.us.db.repos as repos

def setup_function(): repos.reset_memory_stores()

def _order():
    repos.save_order_ack({'client_order_key':'K','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty_requested':10,'order_no':'O1','status':'ACK'}, trade_date='2026-07-16')

def test_cumulative_partial_updates_do_not_collide_or_double_count():
    _order()
    repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='K',symbol='AMD',side='SELL',filled_qty=3,requested_qty=10,cumulative_filled_qty=3,avg_price_usd=100,trade_date='2026-07-16',evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL',source='fills_by_order_no')
    repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='K',symbol='AMD',side='SELL',filled_qty=7,requested_qty=10,cumulative_filled_qty=7,avg_price_usd=100,trade_date='2026-07-16',evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL',source='fills_by_order_no')
    assert len(repos._MEM_FILLS)==1
    assert repos._MEM_FILLS[0]['qty']==7
    assert repos._MEM_ORDERS[0]['qty_filled']==7
    assert repos.verify_order_fill_accounting(trade_date='2026-07-16', order_no='O1')['status']=='OK'

def test_cumulative_key_excludes_quantity_and_price():
    a={'symbol':'AMD','side':'SELL','order_no':'O1','client_order_key':'K','qty':3,'price_usd':100,'meta':{'fill_evidence_type':'KIS_ORDER_CUMULATIVE_ACTUAL','cumulative_filled_qty':3}}
    b={**a,'qty':7,'price_usd':101,'meta':{'fill_evidence_type':'KIS_ORDER_CUMULATIVE_ACTUAL','cumulative_filled_qty':7}}
    assert repos._us_fill_idempotency_key_text(a,'2026-07-16') == repos._us_fill_idempotency_key_text(b,'2026-07-16')
    assert repos._us_fill_idempotency_key_text(a,'2026-07-16').endswith('|KIS_ORDER_CUMULATIVE_ACTUAL')
