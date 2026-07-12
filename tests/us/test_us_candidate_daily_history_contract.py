from datetime import date, timedelta
from trader.us.candidate_pool_builder import _score_symbol_candidate


def _rows(n):
    end=date(2026,7,10)
    return [{"xymd":(end-timedelta(days=n-i)).strftime("%Y%m%d"),"clos":str(100+i*.1),"high":str(101+i*.1),"low":str(99+i*.1),"tvol":"1000"} for i in range(n)]

def test_candidate_computes_ma200_diagnostics_without_score_weight_change():
    r=_score_symbol_candidate({"symbol":"NVDA","price":130,"avg_dollar_volume_20d":1_000_000}, _rows(260), [], [], [])
    assert r["ma20"] and r["ma50"] and r["ma150"] and r["ma200"]
    assert r["daily_bar_count"] == 260 and r["daily_history_quality"] == "OK"
    short=_score_symbol_candidate({"symbol":"IPO","price":100}, _rows(120), [], [], [])
    assert short["ma150"] is None and short["daily_history_quality"] == "DEGRADED_SHORT_HISTORY"
