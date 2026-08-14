from datetime import date
from trader.kr.infinite.strategy import evaluate
from trader.kr.infinite.models import Action
from .conftest import active,pos
def test_recovery_is_exactly_one_idempotent_probe(default_config):
 s=active(crash_seen=True);day=date(2026,8,14);one=evaluate(config=default_config,state=s,position=pos(90),trade_date=day,market_state="KR_SHOCK_REBOUND_CONFIRMED",orderable_cash=1e6);assert one.action==Action.RECOVERY and one.qty==1111
 two=evaluate(config=default_config,state=s,position=pos(90),trade_date=day,market_state="KR_SHOCK_REBOUND_CONFIRMED",orderable_cash=1e6,existing_intent_keys=frozenset({one.idempotency_key}));assert two.reason=="DUPLICATE_INTENT"
