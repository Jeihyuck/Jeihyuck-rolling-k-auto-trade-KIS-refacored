import pytest

from trader.us.execution.order_economics import order_intent_economics_valid
from trader.us.market_state_overlay import filter_entry_intents_for_market_state


def test_jnj_risk_off_scale_recomputes_notional():
    intent = {"symbol": "JNJ", "side": "BUY", "qty": 12, "quantity": 12,
              "limit_price": 266.66, "notional_usd": 3199.92,
              "strategy_owner": "US_STANDARD", "theme_cluster": "HEALTHCARE", "meta": {}}
    overlay = {"market_state": "NORMAL", "market_regime": "NEUTRAL", "allow_new_buy": True,
               "allow_ai_tech_buy": True, "intraday_market_overlay": "INTRADAY_RISK_OFF"}
    kept, blocked = filter_entry_intents_for_market_state([intent], overlay, [])
    assert not blocked
    assert kept[0]["qty"] == 6
    assert kept[0]["notional_usd"] == pytest.approx(1599.96)
    assert order_intent_economics_valid(kept[0])
