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


def test_candidate_excludes_stale_db_error_and_keeps_ok(monkeypatch):
    from trader.us.candidate_pool_builder import build_us_candidate_pool
    monkeypatch.setenv("US_CANDIDATE_POOL_MIN", "1")
    monkeypatch.setenv("US_CANDIDATE_POOL_TARGET", "1")
    monkeypatch.setenv("US_CANDIDATE_POOL_MAX", "10")
    rows=_rows(260)
    class P:
        def get_completed_daily_prices_result(self, symbol, exchange, **kwargs):
            q={"OK":"OK","STALE":"STALE","DBERR":"DB_ERROR","KISFAIL":"KIS_SYNC_FAILED"}[symbol]
            return {"rows": rows if q=="OK" else [], "quality": q, "valid_bar_count": 260 if q=="OK" else 0, "db_latest":"2026-07-10", "expected_latest":"2026-07-10"}
    universe=[{"symbol":s,"exchange":"NASDAQ","price":130,"avg_dollar_volume_20d":1_000_000} for s in ["OK","STALE","DBERR","KISFAIL"]]
    res=build_us_candidate_pool(trade_date="2026-07-13", as_of_date="20260710", env="practice", dynamic_universe=universe, provider=P(), force_rebuild=True)
    assert [r["symbol"] for r in res["rows"]] == ["OK"]
    assert res["rows"][0]["daily_history_quality"] == "OK"
