from datetime import date
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
def test_partial_remaining_never_exceeds_cap(default_config):
 s=active(units_used=39,core_units_used=30,reserve_units_used=9,reserve_unlocked=True,core_filled_notional=3_000_000,reserve_filled_notional=990_000);d=evaluate(config=default_config,state=s,position=pos(),trade_date=date(2026,8,14),market_state="KR_NORMAL",trading_days_since_last_buy=10,orderable_cash=1e6);assert d.notional<=10_000
def test_40_units_wait(default_config):
 s=active(units_used=40,core_units_used=30,reserve_units_used=10,reserve_unlocked=True,core_filled_notional=3e6,reserve_filled_notional=1e6);assert evaluate(config=default_config,state=s,position=pos(),trade_date=date(2026,8,14),market_state="KR_RISK_ON").action==Action.WAIT
