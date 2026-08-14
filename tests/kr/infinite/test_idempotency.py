from datetime import date
from trader.kr.infinite.policy_state import idempotency_key
def test_keys_are_deterministic_and_action_specific():
 d=date(2026,8,14);a=idempotency_key("c",d,"BUY",1);assert a==idempotency_key("c",d,"BUY",1) and a!=idempotency_key("c",d,"SELL_ALL")
