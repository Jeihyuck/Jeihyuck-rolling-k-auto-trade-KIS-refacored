from datetime import date
from dataclasses import replace
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
from trader.kr.infinite.accounting import apply_confirmed_fill
from trader.kr.infinite.models import BrokerOrderState, OrderIntent, State
def test_partial_remaining_never_exceeds_cap(default_config):
 s=active(units_used=39,core_units_used=30,reserve_units_used=9,reserve_unlocked=True,core_filled_notional=3_000_000,reserve_filled_notional=990_000);d=evaluate(config=default_config,state=s,position=pos(),trade_date=date(2026,8,14),market_state="KR_NORMAL",trading_days_since_last_buy=10,orderable_cash=1e6);assert d.notional<=10_000
def test_40_units_wait(default_config):
 s=active(units_used=40,core_units_used=30,reserve_units_used=10,reserve_unlocked=True,core_filled_notional=3e6,reserve_filled_notional=1e6);assert evaluate(config=default_config,state=s,position=pos(),trade_date=date(2026,8,14),market_state="KR_RISK_ON").action==Action.WAIT

def test_partial_fill_adds_notional_but_consumes_unit_only_once():
 intent=OrderIntent(1,"cycle",date(2026,8,14),"BUY","key",100,unit_sequence=1)
 state=State(cycle_id="cycle",allocated_capital_krw=1_000_000,unit_krw=25_000)
 state,_,_=apply_confirmed_fill(state,intent,BrokerOrderState("PARTIALLY_FILLED",40,4_000,100),date(2026,8,14))
 intent=replace(intent,filled_qty=40,filled_notional_krw=4_000)
 state,_,_=apply_confirmed_fill(state,intent,BrokerOrderState("FILLED",100,10_000,100),date(2026,8,14))
 assert state.units_used==1 and state.core_units_used==1 and state.core_filled_notional==10_000
