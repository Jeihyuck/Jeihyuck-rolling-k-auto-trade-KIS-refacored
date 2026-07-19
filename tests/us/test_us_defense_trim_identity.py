import pytest
from trader.us.market_state_overlay import build_defense_trim_intents
from trader.us.execution.order_identity import InvalidOrderIdentity

def test_defense_requires_date_and_never_uses_na():
    p=[{'symbol':'AMD','qty':10,'current_price':10,'position_lifecycle_id':'l'}]; o={'market_state':'DEFENSE_CRASH'}
    with pytest.raises(InvalidOrderIdentity): build_defense_trim_intents(p,o)
    i=build_defense_trim_intents(p,o,trade_date='2026-07-16')[0]
    assert i['client_order_key'].startswith('US_DEF_2026-07-16_') and 'US_DEF_NA_' not in i['client_order_key']
