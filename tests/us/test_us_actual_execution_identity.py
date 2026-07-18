from trader.us.execution.fills import get_fills_today
import trader.us.db.repos as repos

class Provider:
    _offline=False
    def __init__(self, raw): self.raw=raw
    def _get_client(self): return self
    def get_us_fills_today(self, trade_date=None): return self.raw

def _raw(order='O1', seq='1', qty='3', price='100', t='093001'):
    return {'odno':order,'pdno':'AMD','sll_buy_dvsn_cd':'01','ft_ccld_qty':qty,'ft_ccld_unpr3':price,'ord_dt':'20260717','ccld_tmd':t,'ccld_seq':seq,'ft_ord_qty':'10','nccs_qty':'7','ovrs_excg_cd':'NASDAQ'}

def _seed(monkeypatch):
    monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None)
    repos.reset_memory_stores()
    repos.save_order_ack({'client_order_key':'REAL_KEY','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty_requested':10,'order_no':'O1','status':'ACK'}, trade_date='2026-07-17')

def test_get_fills_today_preserves_execution_identity():
    fills=get_fills_today(provider=Provider([_raw(seq='9', t='101112')]), trade_date='2026-07-17')['fills']
    assert fills[0]['broker_execution_id']=='O1-9'
    assert fills[0]['execution_sequence']=='9'
    assert fills[0]['execution_timestamp']=='2026-07-17T10:11:12'
    assert fills[0]['requested_qty']==10 and fills[0]['filled_qty']==3

def test_close_save_then_reconcile_does_not_add_cumulative_actual(monkeypatch):
    _seed(monkeypatch)
    fills=get_fills_today(provider=Provider([_raw(seq='1', qty='3'), _raw(seq='2', qty='4', t='093002')]), trade_date='2026-07-17')['fills']
    assert repos.save_fills(fills, trade_date='2026-07-17') == 2
    r=repos.mark_order_filled_by_reconcile(order_no='O1',client_order_key='REAL_KEY',symbol='AMD',side='SELL',filled_qty=7,requested_qty=10,cumulative_filled_qty=7,avg_price_usd=100,trade_date='2026-07-17',evidence_type='KIS_ORDER_DETAIL_ACTUAL',source='fills_by_order_no')
    assert r['status']=='OK'
    assert len([f for f in repos._MEM_FILLS if not repos.is_synthetic_fill_meta(f.get('meta'))]) == 2
    repos._MEM_ORDERS[0]['qty_filled']=7
    assert repos.verify_order_fill_accounting(trade_date='2026-07-17', order_no='O1')['status']=='OK'

def test_repeated_close_same_raw_is_idempotent_and_uses_order_key(monkeypatch):
    _seed(monkeypatch)
    fills=get_fills_today(provider=Provider([_raw(seq='1', qty='3'), _raw(seq='2', qty='4', t='093002')]), trade_date='2026-07-17')['fills']
    repos.save_fills(fills, trade_date='2026-07-17')
    repos.save_fills(fills, trade_date='2026-07-17')
    assert len(repos._MEM_FILLS)==2
    assert {f['client_order_key'] for f in repos._MEM_FILLS} == {'REAL_KEY'}

def test_temporary_kis_key_promotes_to_real_order_key(monkeypatch):
    monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None)
    repos.reset_memory_stores()
    f=get_fills_today(provider=Provider([_raw(seq='1', qty='3')]), trade_date='2026-07-17')['fills'][0]
    repos.save_fills([f], trade_date='2026-07-17')
    assert repos._MEM_FILLS[0]['client_order_key'].startswith('KIS_')
    repos.save_order_ack({'client_order_key':'REAL_KEY','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty_requested':10,'order_no':'O1','status':'ACK'}, trade_date='2026-07-17')
    repos.save_fills([f], trade_date='2026-07-17')
    assert len(repos._MEM_FILLS)==1
    assert repos._MEM_FILLS[0]['client_order_key']=='REAL_KEY'
