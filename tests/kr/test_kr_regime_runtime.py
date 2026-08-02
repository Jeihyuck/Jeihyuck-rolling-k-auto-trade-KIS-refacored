from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from trader.kr.regime_runtime import calculate_market_breadth, load_runtime_state, time_adjusted_turnover
from trader.pb1_engine import PB1Engine

KST = ZoneInfo("Asia/Seoul")

def rows():
    out=[]
    for market in ("KOSPI", "KOSDAQ"):
        out += [{"code":f"{i:06d}","market":market,"close":100,"ma20":90,"ma50":80,"return_1d":.03,"return_5d":.01} for i in range(30)]
    return out

def shock():
    return {"close":90,"ma20":100,"ma50":110,"ma200":120,"ma20_slope_5d":-1,
      "breadth_ma20":.82,"breadth_ma50":.7,"advance_ratio":.82,"advance_ratio_intraday":.82,
      "median_return_5d":-.01,"return_5d":-.02,"return_20d":-.1,"intraday_return":.08,
      "intraday_return_positive":.08,"gap_up_return":.04,"above_open":True,"above_vwap":True,
      "turnover_expansion":1.8,"distance_from_intraday_high":-.01}

def leader():
    value=shock(); value.update({"close":120,"ma20":100,"ma50":90}); return value

def engine(now):
    e=PB1Engine.__new__(PB1Engine); e._today="2026-07-31"; e._now_kst=now
    e._kr_broader_scored_universe=rows()
    e._kr_etf_observation=lambda symbol,breadth: leader() if symbol in {"226490","091160","005930","000660"} else shock()
    return e

def test_breadth_artifact_and_process_recreation_persist_shock(monkeypatch,tmp_path):
    monkeypatch.chdir(tmp_path)
    start=datetime(2026,7,31,10,0,tzinfo=KST)
    first=engine(start)._build_kr_regime_snapshot_for_tick([], {})
    assert first.market_states["KOSPI"].state == "KR_SHOCK_REBOUND_PENDING"
    second=engine(start+timedelta(minutes=5))._build_kr_regime_snapshot_for_tick([], {})
    assert second.market_states["KOSPI"].state == "KR_SHOCK_REBOUND_PENDING"
    third=engine(start+timedelta(minutes=11))._build_kr_regime_snapshot_for_tick([], {})
    assert third.market_states["KOSPI"].state == "KR_SHOCK_REBOUND_CONFIRMED"
    state=load_runtime_state("2026-07-31")
    assert state["markets"]["KOSPI"]["shock_consecutive_ticks"] == 3
    assert (tmp_path/"artifacts/kr_market_breadth.json").exists()
    assert (tmp_path/"runtime/state/kr/kr_regime_runtime_state.json").exists()

def test_time_adjusted_turnover_uses_expected_same_time_volume():
    ratio,source=time_adjusted_turnover(accumulated_volume=180,avg_daily_volume=1000,now=datetime(2026,7,31,10,0,tzinfo=KST),expected_volume_until_now=100)
    assert ratio == 1.8 and source == "same_time_history"

def test_breadth_missing_fields_is_explicitly_blocked(monkeypatch,tmp_path,caplog):
    monkeypatch.chdir(tmp_path)
    result=calculate_market_breadth([{"code":"1","market":"KOSPI","close":100}],as_of="2026-07-31",source="final30_fallback")
    assert result["markets"]["KOSPI"]["data_quality"] == "BLOCKED"
    assert "BREADTH_INPUT_BLOCKED" in caplog.text

def test_all_regime_symbols_allow_long_fetch_in_trade_precomputed_mode():
    import pandas as pd
    from types import SimpleNamespace
    from trader.kr.regime import KR_REGIME_REQUIRED_SYMBOLS
    calls=[]
    class Provider:
        def get_ohlcv(self,code,count,**kwargs):
            calls.append((code,count,kwargs))
            df=pd.DataFrame({"date":pd.date_range("2025-01-01",periods=260),"close":range(260),"volume":[100]*260})
            return SimpleNamespace(df=df,meta={"source":"db"})
    e=PB1Engine.__new__(PB1Engine); e.phase="entry"; e.trade_use_precomputed_features=True
    e._precomputed_final30_map={"x":{}}; e.window_internal="morning"; e.env="trade"; e.daily_fetch_count=0; e.ohlcv_provider=Provider(); e._data_metrics={}
    for symbol in KR_REGIME_REQUIRED_SYMBOLS:
        df,_=e._fetch_daily(symbol,count=260)
        assert len(df) == 260
    assert all(kwargs["purpose"] == "kr_regime" and kwargs["allow_long_fetch"] for _,_,kwargs in calls)
