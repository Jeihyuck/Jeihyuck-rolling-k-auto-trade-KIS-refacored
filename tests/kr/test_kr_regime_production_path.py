import inspect
from trader.kr.regime import KRMarketState, calculate_global_market_state
from trader.pb1_engine import PB1Engine
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
 source=inspect.getsource(PB1Engine.run)
 assert "[KR_REGIME][BUDGET_INVARIANT]" in source
 assert 'artifacts/kr_market_budget.json' in source
 assert "_enforce_kr_final_order_invariants(" in source
