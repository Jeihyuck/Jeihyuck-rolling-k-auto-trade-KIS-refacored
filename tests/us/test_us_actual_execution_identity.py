from trader.us.execution.fills import get_fills_today
import trader.us.db.repos as repos

class Provider:
    _offline=False
    def __init__(self, raw): self.raw=raw
    def _get_client(self): return self
    def get_us_fills_today(self, trade_date=None): return self.raw

def _raw(order='O1', cumulative='3', remaining='7', price='100', t='093001'):
    return {'odno':order,'pdno':'AMD','sll_buy_dvsn_cd':'01','ft_ccld_qty':cumulative,'ft_ccld_unpr3':price,'ord_dt':'20260717','ord_tmd':t,'ft_ord_qty':'10','nccs_qty':remaining,'ovrs_excg_cd':'NASDAQ'}

def _seed(monkeypatch):
    monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None)
    repos.reset_memory_stores()
    repos.save_order_ack({'client_order_key':'REAL_KEY','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty_requested':10,'order_no':'O1','status':'ACK'}, trade_date='2026-07-17')

def test_get_fills_today_returns_order_cumulative_snapshot_without_execution_identity():
    fill=get_fills_today(provider=Provider([_raw(cumulative='7', remaining='3')]), trade_date='2026-07-17')['fills'][0]
    assert fill['fill_evidence_type']=='KIS_ORDER_CUMULATIVE_ACTUAL'
    assert fill['order_timestamp']=='2026-07-17T09:30:01'
    assert fill['requested_qty']==10 and fill['cumulative_filled_qty']==7 and fill['remaining_qty']==3
    assert 'broker_execution_id' not in fill and 'execution_sequence' not in fill and 'execution_timestamp' not in fill

def test_cumulative_progress_updates_one_active_row(monkeypatch):
    _seed(monkeypatch)
    first=get_fills_today(provider=Provider([_raw(cumulative='3', remaining='7')]), trade_date='2026-07-17')['fills']
    second=get_fills_today(provider=Provider([_raw(cumulative='7', remaining='3')]), trade_date='2026-07-17')['fills']
    assert repos.save_fills(first, trade_date='2026-07-17') == 1
    assert repos.save_fills(second, trade_date='2026-07-17') == 0
    r=repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='REAL_KEY',symbol='AMD',side='SELL',filled_qty=7,requested_qty=10,cumulative_filled_qty=7,avg_price_usd=100,trade_date='2026-07-17',evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL',source='fills_by_order_no')
    assert r['status']=='OK'
    active=[f for f in repos._MEM_FILLS if not repos.is_synthetic_fill_meta(f.get('meta'))]
    assert len(active) == 1 and active[0]['qty'] == 7
    assert repos.verify_order_fill_accounting(trade_date='2026-07-17', order_no='O1')['status']=='OK'

def test_repeated_close_same_cumulative_is_idempotent(monkeypatch):
    _seed(monkeypatch)
    fills=get_fills_today(provider=Provider([_raw(cumulative='7', remaining='3')]), trade_date='2026-07-17')['fills']
    repos.save_fills(fills, trade_date='2026-07-17')
    repos.save_fills(fills, trade_date='2026-07-17')
    assert len(repos._MEM_FILLS)==1
    assert repos._MEM_FILLS[0]['qty']==7 and repos._MEM_FILLS[0]['client_order_key']=='REAL_KEY'

def test_temporary_kis_key_promotes_to_real_order_key_during_reconcile(monkeypatch):
    monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None)
    repos.reset_memory_stores()
    f=get_fills_today(provider=Provider([_raw(cumulative='7', remaining='3')]), trade_date='2026-07-17')['fills'][0]
    repos.save_fills([f], trade_date='2026-07-17')
    assert repos._MEM_FILLS[0]['client_order_key'].startswith('KIS_')
    repos.save_order_ack({'client_order_key':'REAL_KEY','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty_requested':10,'order_no':'O1','status':'ACK'}, trade_date='2026-07-17')
    r=repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='REAL_KEY',symbol='AMD',side='SELL',filled_qty=7,requested_qty=10,cumulative_filled_qty=7,avg_price_usd=100,trade_date='2026-07-17',evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL',source='fills_by_order_no')
    assert r['status']=='OK'
    assert len(repos._MEM_FILLS)==1
    assert repos._MEM_FILLS[0]['client_order_key']=='REAL_KEY'

def test_cumulative_regression_is_not_applied(monkeypatch):
    _seed(monkeypatch)
    repos.save_fills(get_fills_today(provider=Provider([_raw(cumulative='7', remaining='3')]), trade_date='2026-07-17')['fills'], trade_date='2026-07-17')
    lower=get_fills_today(provider=Provider([_raw(cumulative='3', remaining='7')]), trade_date='2026-07-17')['fills']
    repos.save_fills(lower, trade_date='2026-07-17')
    assert repos._MEM_FILLS[0]['qty']==7
    r=repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='REAL_KEY',symbol='AMD',side='SELL',filled_qty=3,requested_qty=10,cumulative_filled_qty=3,avg_price_usd=100,trade_date='2026-07-17',evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL',source='fills_by_order_no')
    assert r['status']=='EVIDENCE_QUANTITY_REGRESSION' and r['entry_fence'] is True
