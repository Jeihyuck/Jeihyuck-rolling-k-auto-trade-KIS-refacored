"""Connected replay of the accounting-critical portions of the 2026-07-16 incident."""
import time
from unittest.mock import patch
import pytest

from trader.us.runner.tick_process import run_tick_in_process,TickProcessTimeout
from trader.us.execution.order_journal import append_order_event,replay_order_journal
from trader.us.execution.order_router import route_order
from trader.us.runner.daily_report_runner import run_daily_report
import trader.us.db.repos as repos

class KIS:
    _offline=True
    def __init__(self): self.submits=0; self.calls=[]
    def get_balance(self,**kw): self.calls.append('balance'); return {'positions':POSITIONS,'balance_parse_status':'OK','total_pvs':37429.49}
    def get_today_orders(self,**kw): self.calls.append('orders'); return [{'order_no':'TIMEOUT1','symbol':'AMD','side':'SELL','status':'ACK'}]
    def get_fills_by_order_no(self,order_no,symbol,trade_date):
        self.calls.append(('fill',order_no)); return {'filled_qty':3,'avg_price':100,'symbol':symbol,'side':'SELL'}

def hanging(tick_cancellation_event=None):
    while not tick_cancellation_event.is_set(): time.sleep(.01)
    time.sleep(10)

POSITIONS=[{'symbol':f'P{i}','qty':1,'current_px':1000+(i*10.0),'exchange':'NASDAQ'} for i in range(21)]
ACTUAL=[{'symbol':f'P{i%21}','side':'SELL','qty':1,'price_usd':100,'order_no':f'F{i}','client_order_key':f'K{i}','meta':{'is_synthetic':False,'fill_evidence_type':'KIS_EXECUTION_ACTUAL'}} for i in range(15)]

def test_20260716_connected_timeout_replay_and_close_accounting(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path); monkeypatch.setenv('US_ORDER_JOURNAL_DIR',str(tmp_path/'journal'))
    monkeypatch.setattr(repos,'_get_engine_or_none',lambda:None); repos.reset_memory_stores()
    # Generation-1 remains distinct from recovery generation-2.
    repos._MEM_ORDERS.append({'trade_date':'2026-07-16','client_order_key':'GEN1','order_no':'OLD1','symbol':'GOOGL','exchange':'NASDAQ','side':'SELL','qty_requested':1,'qty_filled':1,'status':'FILLED','meta':{'session_generation':1}})
    intent={'trade_date':'2026-07-16','session':'am','session_run_id':'generation-2','session_generation':2,'tick_id':'timeout-tick','prep_run_id':'9e11e2d0-6b3d-4fb1-af0a-e46d19aaa8d1','client_order_key':'TIMEOUT-K','symbol':'AMD','exchange':'NASDAQ','side':'SELL','qty':10}
    append_order_event('BROKER_SUBMIT_STARTED',intent); append_order_event('BROKER_ACK_RECEIVED',intent,broker_order_no='TIMEOUT1')
    with pytest.raises(TickProcessTimeout) as timeout: run_tick_in_process(hanging,kwargs={},timeout_sec=.05,terminate_grace_sec=.05)
    assert timeout.value.result['process_alive'] is False
    kis=KIS(); replay=replay_order_journal('2026-07-16',session_run_id='generation-2',tick_id='timeout-tick',provider=kis)
    assert replay['broker_partial_fill_count']==1 and repos._MEM_ORDERS[-1]['status']=='PARTIALLY_FILLED'
    assert repos._MEM_ORDERS[-1]['meta']['remaining_qty']==7 and kis.submits==0
    # An unresolved symbol/side fence prevents duplicate routing.
    from trader.us.execution.tick_context import TickExecutionContext
    ctx=TickExecutionContext('2026-07-16','am','generation-2',2,'next',blocked_symbol_sides={('AMD','SELL')})
    blocked=route_order({**intent,'tick_id':'next','client_order_key':'NEXT-K','qty':1,'limit_price':100,'notional_usd':100},context=ctx,kis_client=kis)
    assert blocked['status']=='ORDER_FENCED_BEFORE_BROKER_SUBMIT'
    # Actual fills remain 15; synthetic balance evidence is accounted separately.
    report=run_daily_report(env='practice',session='close',trade_date='2026-07-16',offline=True,
        final_balance={'total_pvs':37429.49},final_positions=POSITIONS,kis_fills=ACTUAL,
        close_order_classification={'orders':[],'counts':{},'pending_order_count':1},close_run_id='close-1')['report']
    assert report['position_count']==21 and report['kis_actual_fill_execution_count']==15
    assert abs(report['account_equity_usd']-37429.49)<.01
    assert all(o['symbol']!='AMZN' for o in repos._MEM_ORDERS if o.get('client_order_key') in {'GEN1','TIMEOUT-K'})
