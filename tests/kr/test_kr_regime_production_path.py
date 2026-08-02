import inspect
from trader.kr.regime import KRMarketState, calculate_global_market_state
from trader.pb1_engine import CandidateFeature, PB1Engine, _enforce_kr_final_order_invariants
import trader.kr.market_state_overlay as execution_helpers


def state(market,name,quality): return KRMarketState(market,0,name,quality,{})

def test_blocked_kosdaq_is_excluded_from_stabilized_global_state():
 states={"KOSPI":state("KOSPI","KR_RISK_ON","OK"),"KOSDAQ":state("KOSDAQ","KR_DEFENSE_CRASH","BLOCKED")}
 assert calculate_global_market_state(states) == "KR_RISK_ON"

def test_blocked_kospi_is_excluded_from_stabilized_global_state():
 states={"KOSPI":state("KOSPI","KR_DEFENSE_CRASH","BLOCKED"),"KOSDAQ":state("KOSDAQ","KR_NORMAL","OK")}
 assert calculate_global_market_state(states) == "KR_NORMAL"

def test_pb1_uses_shared_global_state_helper_after_stabilizer():
 source=inspect.getsource(PB1Engine._build_kr_regime_snapshot_for_tick)
 assert "calculate_global_market_state(stable_states)" in source
 assert "min((v.state for v in stable_states.values())" not in source

def test_pb1_has_single_kr_regime_authority():
 assert not hasattr(execution_helpers,"evaluate_kr_market_state")
 assert not hasattr(execution_helpers,"_index_returns")
 assert not hasattr(execution_helpers,"apply_kr_market_state_to_budget")
 assert "_build_kr_regime_snapshot_for_tick" in inspect.getsource(PB1Engine._evaluate_kr_market_state_overlay_for_tick)

def test_production_path_enforces_budget_invariant_and_writes_artifact():
 snapshot = __import__("trader.kr.regime", fromlist=["build_kr_regime_snapshot"]).build_kr_regime_snapshot({
  "KOSPI":{"close":120,"ma20":110,"ma50":105,"ma200":100,"ma20_slope_5d":1,"breadth_ma20":.7,"breadth_ma50":.65,"advance_ratio":.7,"median_return_5d":.02,"return_5d":.03,"return_20d":.08}
 })
 candidate=CandidateFeature("005930","KOSPI",{"order_price":100_000,"stop_price":95_000,"vol20":100_000},True,[],1,[],planned_qty=10,planned_value=float("nan"))
 orderable, meta = _enforce_kr_final_order_invariants(
  [candidate], snapshot=snapshot,
  base_overlay={"portfolio_equity_krw":100_000_000,"gross_exposure_pct":0,"sector_exposure_pct":{},"high_beta_exposure_pct":0},
  existing_positions=[],market_budgets={"KOSPI":10_000_000,"KOSDAQ":0},total_tick_cap=10_000_000,
  slots_remaining_at_tick_start=1,max_positions=10,existing_positions_count=0,available_cash=100_000_000,
 )
 submitted=[]
 for order in orderable: submitted.append(order)
 assert meta["invariant_valid"] is False and orderable == [] and submitted == []
