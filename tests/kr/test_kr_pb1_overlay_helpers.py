from trader.kr.market_state_overlay import filter_kr_entry_intent


def test_forbidden_product_sell_allowed_buy_blocked():
    assert filter_kr_entry_intent({"side": "SELL", "symbol": "252670"}, {"market_state": "KR_DEFENSE_CRASH"}).get("status") != "BLOCKED"
    assert filter_kr_entry_intent({"side": "BUY", "symbol": "252670"}, {"market_state": "KR_NORMAL"}).get("status") == "BLOCKED"
