from trader.kr.regime import build_kr_regime_snapshot, build_market_local_overlay
from trader.kr.market_state_overlay import filter_kr_entry_intent

def obs(up=True):
 return {"close":120 if up else 80,"ma20":110,"ma50":105,"ma200":100,"ma20_slope_5d":1 if up else -1,"breadth_ma20":.7 if up else .3,"breadth_ma50":.65 if up else .35,"advance_ratio":.7 if up else .3,"median_return_5d":.02 if up else -.02,"return_5d":.03 if up else -.03,"return_20d":.08 if up else -.08}

def test_local_market_state_is_passed_to_final_entry_filter():
 s=build_kr_regime_snapshot({"KOSPI":obs(True),"KOSDAQ":obs(False)}); base={"market_state":s.global_state,"portfolio_equity_krw":1_000_000,"gross_exposure_pct":0,"sector_exposure_pct":{},"high_beta_exposure_pct":0}
 k=build_market_local_overlay(base,s,"KOSPI"); q=build_market_local_overlay(base,s,"KOSDAQ")
 assert k["market_state"] == s.market_states["KOSPI"].state and q["market_state"] == s.market_states["KOSDAQ"].state
 assert k["market_state"] != q["market_state"]

def test_global_gross_exposure_cap_still_blocks_local_market():
 s=build_kr_regime_snapshot({"KOSPI":obs(True),"KOSDAQ":obs(False)}); local=build_market_local_overlay({"portfolio_equity_krw":1_000_000,"gross_exposure_pct":.95,"sector_exposure_pct":{},"high_beta_exposure_pct":0},s,"KOSPI")
 out=filter_kr_entry_intent({"side":"BUY","code":"005930","market":"KOSPI","planned_value":1000},local)
 assert out["status"] == "BLOCKED" and out["reason"] == "KR_MAX_GROSS_EXPOSURE_BLOCK"

def test_global_account_kill_switch_still_blocks_both_markets():
 s=build_kr_regime_snapshot({"KOSPI":obs(True),"KOSDAQ":obs(True)})
 base={"account_loss_kill_switch_triggered":True,"portfolio_equity_krw":1_000_000,"gross_exposure_pct":0,"sector_exposure_pct":{},"high_beta_exposure_pct":0}
 for market in ("KOSPI","KOSDAQ"):
  local=build_market_local_overlay(base,s,market)
  out=filter_kr_entry_intent({"side":"BUY","code":"005930","market":market,"planned_value":1000},local)
  assert out["status"] == "BLOCKED"
