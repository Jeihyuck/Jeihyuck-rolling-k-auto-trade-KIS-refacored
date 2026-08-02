from trader.kr.regime import build_kr_regime_snapshot, calculate_market_budgets

def obs(up=True):
 return {"close":120 if up else 80,"ma20":110,"ma50":105,"ma200":100,"ma20_slope_5d":1 if up else -1,"breadth_ma20":.7 if up else .3,"breadth_ma50":.65 if up else .35,"advance_ratio":.7 if up else .3,"median_return_5d":.02 if up else -.02,"return_5d":.03 if up else -.03,"return_20d":.08 if up else -.08}

def test_two_risk_on_markets_never_double_total_tick_budget():
 s=build_kr_regime_snapshot({"KOSPI":obs(),"KOSDAQ":obs()}); b=calculate_market_budgets(s,27_000_000,100_000_000,{"KOSPI":2,"KOSDAQ":1})
 assert sum(b.values()) <= 27_000_000*s.execution_policy.budget_multiplier + .01
 assert b["KOSPI"] > b["KOSDAQ"] > 0

def test_market_budget_sum_never_exceeds_available_cash():
 s=build_kr_regime_snapshot({"KOSPI":obs(),"KOSDAQ":obs()}); b=calculate_market_budgets(s,27_000_000,5_000_000,{"KOSPI":2,"KOSDAQ":2})
 assert sum(b.values()) <= 5_000_000

def test_single_active_market_receives_available_total_cap():
 s=build_kr_regime_snapshot({"KOSPI":obs()}); b=calculate_market_budgets(s,10_000_000,100_000_000,{"KOSPI":2,"KOSDAQ":0})
 assert b["KOSPI"] > 0 and b["KOSDAQ"] == 0 and sum(b.values()) <= 10_000_000*s.execution_policy.budget_multiplier

def test_unused_market_budget_is_redistributed():
 s=build_kr_regime_snapshot({"KOSPI":obs(),"KOSDAQ":obs()}); b=calculate_market_budgets(s,10_000_000,100_000_000,{"KOSPI":3,"KOSDAQ":0})
 assert b["KOSPI"] == min(10_000_000*s.execution_policy.budget_multiplier,10_000_000*s.market_policies["KOSPI"].budget_multiplier)
