from datetime import date
import pytest
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
D=date(2026,8,14)
@pytest.mark.parametrize("regime,price,gap,action,reason",[("KR_RISK_ON",101,1,Action.BUY,"ADAPTIVE_ADD_BUY"),("KR_RISK_ON",103,1,Action.WAIT,"PRICE_ABOVE_RISK_ON_PREMIUM"),("KR_NORMAL",99,2,Action.BUY,"ADAPTIVE_ADD_BUY"),("KR_NORMAL",101,2,Action.WAIT,"PRICE_ABOVE_NORMAL_AVG"),("KR_DEFENSE_CAUTION",97,3,Action.BUY,"ADAPTIVE_ADD_BUY"),("KR_DEFENSE_RISK_OFF",95,7,Action.BUY,"ADAPTIVE_ADD_BUY"),("KR_DEFENSE_RISK_OFF",97,7,Action.WAIT,"RISK_OFF_PRICE_STEP"),("KR_DEFENSE_RISK_OFF",94,3,Action.WAIT,"RISK_OFF_CADENCE"),("KR_DEFENSE_CRASH",70,30,Action.WAIT,"BUY_PAUSED_BY_REGIME"),("KR_SHOCK_REBOUND_PENDING",90,30,Action.WAIT,"BUY_PAUSED_BY_REGIME")])
def test_active_policy(default_config,regime,price,gap,action,reason):
 d=evaluate(config=default_config,state=active(),position=pos(price),trade_date=D,market_state=regime,trading_days_since_last_buy=gap,orderable_cash=1_000_000);assert (d.action,d.reason)==(action,reason)
def test_caution_blocks_new_cycle(default_config):assert evaluate(config=default_config,state=None,position=pos(qty=0,avg=0),trade_date=D,market_state="KR_DEFENSE_CAUTION").reason=="NEW_CYCLE_REGIME_BLOCK"
