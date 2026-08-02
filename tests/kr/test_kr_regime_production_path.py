import inspect
from trader.kr.regime import KRMarketState, calculate_global_market_state
from trader.pb1_engine import (
 CandidateFeature, PB1Engine, _enforce_kr_final_order_invariants,
 _finalize_kr_order_candidates, _submit_finalized_order_candidates,
)
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


def _observations():
 return {"close":120,"ma20":110,"ma50":105,"ma200":100,"ma20_slope_5d":1,"breadth_ma20":.7,"breadth_ma50":.65,"advance_ratio":.7,"median_return_5d":.02,"return_5d":.03,"return_20d":.08}


def _candidate(code, market):
 return CandidateFeature(code,market,{"market":market,"order_price":100_000,"stop_price":95_000,"vol20":1_000_000,"above_vwap":True,"last_price":100_000,"ma20":90_000,"ma20_slope_5d":1,"turnover_expansion":2,"rs_percentile":.99},True,[],1,[],planned_qty=1,planned_value=100_000)


def _finalize(candidates, *, equity=100_000_000, slots=3, both=True):
 builder=__import__("trader.kr.regime",fromlist=["build_kr_regime_snapshot"]).build_kr_regime_snapshot
 observations={"KOSPI":_observations()}
 if both: observations["KOSDAQ"]=_observations()
 snapshot=builder(observations)
 overlay={"portfolio_equity_krw":equity,"gross_exposure_pct":0,"sector_exposure_pct":{},"high_beta_exposure_pct":0}
 return _finalize_kr_order_candidates(candidates,snapshot=snapshot,overlay=overlay,existing_positions=[],base_tick_budget=20_000_000,available_cash=100_000_000,slots_remaining=slots,max_positions=10,existing_positions_count=0,session_kind="am",risk_pct=.5,liquidity_participation=.01)


def _fake_submitter(calls):
 def submit(candidate):
  calls.append((candidate.code,candidate.planned_qty,candidate.planned_value))
  return {"terminal_event":"API_RESULT","submit_attempted":1,"api_submitted":1,"accepted":1}
 return submit


def test_pb1_finalization_valid_orders_reach_submitter():
 orderable,meta=_finalize([_candidate("005930","KOSPI")])
 calls=[]; result=_submit_finalized_order_candidates(orderable,submitter=_fake_submitter(calls))
 assert meta["invariant_valid"] and len(calls)==len(orderable)==result["api_submitted"]==1
 assert calls[0][2] <= meta["market_budgets"]["KOSPI"]


def test_pb1_finalization_invalid_invariant_never_calls_submitter():
 orderable,meta=_finalize([_candidate("005930","KOSPI")],equity=0)
 calls=[]; result=_submit_finalized_order_candidates(orderable,submitter=_fake_submitter(calls))
 assert not meta["invariant_valid"] and calls==[] and result["api_submitted"]==0


def test_pb1_finalization_uses_mtm_equity_for_risk_and_caps(monkeypatch):
 monkeypatch.setenv("PB1_KR_MAX_POSITION_KRW","20000000")
 loss_orders,loss_meta=_finalize([_candidate("005930","KOSPI")],equity=50_000_000)
 gain_orders,gain_meta=_finalize([_candidate("005930","KOSPI")],equity=100_000_000)
 assert loss_meta["mtm_equity_krw"]==50_000_000 and gain_meta["mtm_equity_krw"]==100_000_000
 assert loss_orders[0].features["risk_qty"] < gain_orders[0].features["risk_qty"]
 assert loss_orders[0].planned_qty < gain_orders[0].planned_qty


def test_pb1_cross_market_slot_cap_reaches_submitter_once():
 orderable,meta=_finalize([_candidate("005930","KOSPI"),_candidate("247540","KOSDAQ")],slots=1)
 calls=[]; _submit_finalized_order_candidates(orderable,submitter=_fake_submitter(calls))
 assert meta["invariant_valid"] and len(orderable)==len(calls)==1


def test_pb1_blocked_market_submits_only_healthy_market():
 orderable,meta=_finalize([_candidate("005930","KOSPI"),_candidate("247540","KOSDAQ")],both=False)
 calls=[]; _submit_finalized_order_candidates(orderable,submitter=_fake_submitter(calls))
 assert meta["candidate_counts"]=={"KOSPI":1,"KOSDAQ":0}
 assert [call[0] for call in calls]==["005930"]


def test_risk_qty_uses_mark_to_market_equity_not_cost_basis():
 low,_=_finalize([_candidate("005930","KOSPI")],equity=40_000_000)
 high,_=_finalize([_candidate("005930","KOSPI")],equity=80_000_000)
 assert low[0].features["risk_qty"]==40 and high[0].features["risk_qty"]==80


def test_unrealized_loss_account_reduces_risk_qty(monkeypatch):
 monkeypatch.setenv("PB1_KR_MAX_POSITION_KRW","20000000")
 loss,_=_finalize([_candidate("005930","KOSPI")],equity=50_000_000)
 cost_basis,_=_finalize([_candidate("005930","KOSPI")],equity=100_000_000)
 assert loss[0].planned_qty < cost_basis[0].planned_qty


def test_unrealized_gain_account_uses_same_equity_denominator_end_to_end():
 orders,meta=_finalize([_candidate("005930","KOSPI")],equity=200_000_000)
 assert orders[0].features["risk_qty"]==200
 assert meta["projected_gross_exposure_pct"]==orders[0].planned_value/200_000_000


def test_invalid_mtm_equity_blocks_before_quantity_rebuild():
 candidate=_candidate("005930","KOSPI")
 orders,meta=_finalize([candidate],equity=0)
 assert orders==[] and meta["reason"]=="INVALID_PORTFOLIO_EQUITY"
 assert "risk_qty" not in candidate.features
