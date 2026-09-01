from trader.us.market_state_overlay import filter_entry_intents_for_market_state


def test_fast_dip_tqqq_infinite_bypasses_pb1_intraday_overlay():
    intent = {"symbol": "TQQQ", "side": "BUY", "qty": 3, "limit_price": 71.34,
              "notional_usd": 214.02, "strategy_owner": "TQQQ_INFINITE", "meta": {"reason": "FAST_DIP_ADD_BUY"}}
    kept, blocked = filter_entry_intents_for_market_state([intent], {"intraday_market_overlay": "INTRADAY_RISK_OFF"}, [])
    assert not blocked and kept[0]["qty"] == 3
    assert kept[0]["meta"]["strategy_owner"] == "TQQQ_INFINITE"
