from datetime import date,timedelta
from trader.kr.infinite.models import State,Status,Action,BrokerPosition
from trader.kr.infinite.strategy import evaluate
def test_same_day_block_next_day_allowed(default_config):
 d=date(2026,8,14);s=State(last_exit_date=d,status=Status.COMPLETE,allocated_capital_krw=4e6,unit_krw=1e5);p=BrokerPosition(0,0,0,100)
 assert evaluate(config=default_config,state=s,position=p,trade_date=d,market_state="KR_STRONG_RISK_ON").reason=="SAME_DAY_CYCLE_RESTART_BLOCK"
 assert evaluate(config=default_config,state=s,position=p,trade_date=d+timedelta(days=1),market_state="KR_NORMAL",orderable_cash=1e6).action==Action.BUY
