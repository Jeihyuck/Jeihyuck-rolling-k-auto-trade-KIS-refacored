from datetime import datetime
from zoneinfo import ZoneInfo
from trader.marketdata.opening_range_intelligence import analyze_opening_range

def test_opening_observation_never_authorizes_and_excludes_future_and_conflict():
    now=datetime(2026,11,25,9,32,tzinfo=ZoneInfo("America/New_York"))
    row={"time_et":"2026-11-25T09:30:00-05:00","open":100,"high":102,"low":99,"close":101,"volume":10,"turnover":1010}
    second={**row,"time_et":"2026-11-25T09:31:00-05:00"}
    future={**row,"time_et":"2026-11-25T09:32:00-05:00","high":200}
    result=analyze_opening_range(market="US",trade_date="2026-11-25",now=now,bars=[row,second,future,{**second,"high":105}])
    assert result["opening_observed_count"]==1 and result["opening_missing_count"]==1
    assert result["opening_high"]==102 and result["opening_vwap"]==101
    assert result["provisional_entry_ceiling"] is None
    assert result["order_authorization"] is False

def test_advisory_price_uses_explicit_strategy_support_target_and_tick():
    now=datetime(2026,11,25,9,31,tzinfo=ZoneInfo("Asia/Seoul"))
    row={"time_kst":"2026-11-25T09:00:00+09:00","open":100,"high":105,"low":98,"close":101,"volume":10,"turnover":1010}
    result=analyze_opening_range(market="KR",trade_date="2026-11-25",now=now,bars=[row],support=99,target=105,min_rr=1.5,max_risk=.025,tick_size=1)
    assert result["provisional_entry_ceiling"]==101
    assert result["order_authorization"] is False
