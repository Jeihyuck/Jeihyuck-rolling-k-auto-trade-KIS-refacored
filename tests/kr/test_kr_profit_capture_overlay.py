import trader.kr.market_state_overlay as overlay
from trader.kr.market_state_overlay import filter_kr_entry_intent, generate_kr_profit_capture_intents


def test_legacy_market_state_evaluator_is_not_importable_from_production():
    assert not hasattr(overlay, "evaluate_kr_market_state")
    assert not hasattr(overlay, "_index_returns")
    assert not hasattr(overlay, "apply_kr_market_state_to_budget")


def test_sell_helpers_remain_available_without_regime_evaluator():
    base={"market_state":"KR_NORMAL","data_quality":"OK","force_entry_block":False,"sector_exposure_pct":{},"portfolio_equity_krw":1_000_000,"gross_exposure_pct":0.0,"high_beta_exposure_pct":0.0}
    assert filter_kr_entry_intent({"side":"SELL","code":"005930"},base)["side"] == "SELL"
    assert isinstance(generate_kr_profit_capture_intents([],base),list)
