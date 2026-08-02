from trader.kr.market_state_overlay import generate_kr_defense_trim_intents
from trader.kr.regime import KRMarketState, build_kr_regime_snapshot


def obs(up=True):
    return {"close":120 if up else 80,"ma20":110,"ma50":105,"ma200":100,"ma20_slope_5d":1 if up else -1,
    "breadth_ma20":.7 if up else .3,"breadth_ma50":.65 if up else .35,"advance_ratio":.7 if up else .3,
    "median_return_5d":.02 if up else -.02,"return_5d":.03 if up else -.03,"return_20d":.08 if up else -.08}


def snapshot(): return build_kr_regime_snapshot({"KOSPI":obs(True),"KOSDAQ":obs(False)})


def test_kosdaq_risk_off_trims_only_kosdaq_positions():
    out=generate_kr_defense_trim_intents([{"code":"A","market":"KOSDAQ","qty":10,"unrealized_pnl_pct":-.01},{"code":"B","market":"KOSPI","qty":10}],snapshot())
    assert [x["code"] for x in out] == ["A"]

def test_blocked_market_does_not_trigger_regime_trim():
    snap=build_kr_regime_snapshot({"KOSPI":obs(True)})
    assert generate_kr_defense_trim_intents([{"code":"A","market":"KOSDAQ","qty":10}],snap) == []

def test_account_kill_switch_can_trim_both_markets():
    out=generate_kr_defense_trim_intents([{"code":"A","market":"KOSPI","qty":10},{"code":"B","market":"KOSDAQ","qty":10}],snapshot(),account_kill_switch=True)
    assert {x["code"] for x in out} == {"A","B"}

def test_unknown_position_market_is_not_regime_trimmed():
    assert generate_kr_defense_trim_intents([{"code":"A","qty":10}],snapshot()) == []
