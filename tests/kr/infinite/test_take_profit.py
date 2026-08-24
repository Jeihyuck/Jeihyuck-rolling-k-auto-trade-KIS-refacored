from datetime import date
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
def test_broker_average_take_profit_sells_orderable_all(default_config):
 d=evaluate(config=default_config,state=active(),position=pos(110,100,150,150),trade_date=date(2026,8,14),market_state="KR_DEFENSE_CRASH");assert d.action==Action.SELL_PARTIAL and d.qty==127
def test_below_target_does_not_sell(default_config):assert evaluate(config=default_config,state=active(),position=pos(104.99),trade_date=date(2026,8,14),market_state="KR_DEFENSE_CRASH").action==Action.WAIT
