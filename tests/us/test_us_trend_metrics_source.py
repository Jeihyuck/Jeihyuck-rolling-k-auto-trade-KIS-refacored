from datetime import datetime, timezone, date, timedelta
from trader.us.db import repos
from trader.us.db.price_daily_repo import upsert_us_daily_bars
from trader.us.runner.trade_tick_runner import _update_position_trends_for_tick

class P:
    def get_current_price(self,*a): return {"last":101}

def setup_function(): repos.reset_memory_stores()

def _bars(n=260):
    end=date(2026,7,10)
    return [{"date":(end-timedelta(days=n-i)).isoformat(),"close":100,"open":100,"high":101,"low":99,"volume":1} for i in range(n)]

def test_trend_source_persisted_final30_price_daily_unknown():
    repos.save_us_position_risk_state("PERSIST","2026-07-13", {"state":{"trend":{"daily_metrics_trade_date":"2026-07-13","ma20":100,"ma50":90,"trend_state":"HEALTHY"}}})
    final=[{"symbol":"FINAL","trade_date":"2026-07-13","score":1,"score_final":1,"daily_metrics_as_of":"2026-07-10","daily_bar_count":260,"ma20":100,"ma50":90,"ma150":80}]
    upsert_us_daily_bars(symbol="DROP", bars=_bars(), source="TEST")
    positions=[{"symbol":"PERSIST","qty":1,"current_price_usd":101},{"symbol":"FINAL","qty":1,"current_price_usd":101},{"symbol":"DROP","qty":1,"current_price_usd":101},{"symbol":"MISS","qty":1,"current_price_usd":101}]
    positions, *_ = _update_position_trends_for_tick(positions=positions, provider=P(), trade_date="2026-07-13", now=datetime(2026,7,13,tzinfo=timezone.utc), locked_watchlist_cache=final, watchlist_cache_source="test")
    src={p["symbol"]:(p.get("trend") or {}).get("daily_metrics_source") for p in positions}
    assert src == {"PERSIST":"persisted_trend_metrics","FINAL":"final30_prep_metrics","DROP":"price_daily","MISS":"insufficient_history"}
