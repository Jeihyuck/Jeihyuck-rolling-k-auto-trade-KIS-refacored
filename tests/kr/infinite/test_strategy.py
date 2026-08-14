from datetime import date
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
def test_safety_gates_prevent_orders():
 from trader.kr.infinite.config import InfiniteConfig
 assert evaluate(config=InfiniteConfig(),state=active(),position=pos(),trade_date=date.today(),market_state="KR_NORMAL").action==Action.WAIT
def test_bad_average_fails_closed(default_config):assert evaluate(config=default_config,state=active(),position=pos(avg=0),trade_date=date.today(),market_state="KR_NORMAL").reason=="KR_INF_AVG_PRICE_INVALID"
