import pytest
import trader.us.db.repos as repos

@pytest.fixture(autouse=True)
def memory(monkeypatch):
    monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None); repos.reset_memory_stores()
    repos._MEM_ORDERS.append({'trade_date':'2026-07-16','order_no':'O1','client_order_key':'K1','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty_requested':10,'qty_filled':0,'status':'ACK','meta':{}})

def execute(cumulative,evidence='KIS_ORDER_CUMULATIVE_ACTUAL'):
    return repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='K1',symbol='AMD',side='SELL',filled_qty=cumulative,requested_qty=10,cumulative_filled_qty=cumulative,avg_price_usd=100,source='fills_by_order_no',evidence_type=evidence,trade_date='2026-07-16')

def test_kis_partial_fill_keeps_order_partially_filled():
    r=execute(3); assert r['order_status']=='PARTIALLY_FILLED' and r['remaining_qty']==7 and repos._MEM_ORDERS[0]['status']=='PARTIALLY_FILLED'
def test_balance_partial_fill_keeps_remaining_qty():
    r=execute(4,'BALANCE_DELTA_SYNTHETIC'); assert r['remaining_qty']==6

def test_partial_fill_remains_pending_for_reconcile():
    execute(3); assert repos.load_pending_ack_orders('2026-07-16')[0]['qty_filled']==3

def test_second_fill_completes_partially_filled_order():
    execute(3); r=execute(10); assert r['order_status']=='FILLED' and r['remaining_qty']==0 and sum(f['qty'] for f in repos._MEM_FILLS)==10

def test_partial_fill_does_not_create_full_sold_today():
    execute(3); assert 'AMD' not in repos.load_today_symbols_sold('2026-07-16')
