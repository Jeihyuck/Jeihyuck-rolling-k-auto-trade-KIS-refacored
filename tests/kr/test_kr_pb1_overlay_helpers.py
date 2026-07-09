from trader.kr.market_state_overlay import apply_kr_market_state_to_budget, filter_kr_entry_intent, pre_api_kr_buy_block


def test_forbidden_product_sell_allowed_buy_blocked():
    assert filter_kr_entry_intent({"side": "SELL", "symbol": "252670"}, {"market_state": "KR_DEFENSE_CRASH"}).get("status") != "BLOCKED"
    assert filter_kr_entry_intent({"side": "BUY", "symbol": "252670"}, {"market_state": "KR_NORMAL"}).get("status") == "BLOCKED"


def test_budget_multiplier_and_pre_api_buy_block():
    adjusted, meta = apply_kr_market_state_to_budget(1000, {"market_state": "KR_DEFENSE_CAUTION", "exposure_multiplier": 0.35})
    assert adjusted == 350
    assert meta["market_state"] == "KR_DEFENSE_CAUTION"
    blocked, reason = pre_api_kr_buy_block({"side": "BUY", "code": "005930"}, {"force_entry_block": True})
    assert blocked
    assert reason == "market_state_entry_block"
