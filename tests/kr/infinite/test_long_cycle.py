from datetime import date
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
def test_long_cycle_requires_seven_percent_step(default_config):
 s=active(cycle_age_trading_days=120);assert evaluate(config=default_config,state=s,position=pos(95),trade_date=date(2026,8,14),market_state="KR_NORMAL",trading_days_since_last_buy=10,orderable_cash=1e6).action==Action.WAIT
 assert evaluate(config=default_config,state=s,position=pos(93),trade_date=date(2026,8,14),market_state="KR_NORMAL",trading_days_since_last_buy=10,orderable_cash=1e6).action==Action.BUY
