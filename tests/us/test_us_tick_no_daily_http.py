from datetime import datetime, timezone, date, timedelta
from trader.us.db import repos
from trader.us.db.price_daily_repo import upsert_us_daily_bars
from trader.us.runner.trade_tick_runner import _update_position_trends_for_tick

class P:
    def __init__(self): self.daily_calls=0; self.price_calls=0
    def get_daily_prices(self,*a,**k): self.daily_calls += 1; raise AssertionError("daily HTTP forbidden")
    def get_current_price(self,*a): self.price_calls += 1; return {"last":101}

def setup_function(): repos.reset_memory_stores()

def test_tick_trend_never_calls_daily_provider():
    end=date(2026,7,10)
    upsert_us_daily_bars(symbol="AMD", bars=[{"date":(end-timedelta(days=260-i)).isoformat(),"close":100,"open":100,"high":101,"low":99,"volume":1} for i in range(260)], source="TEST")
    p=P()
    for _ in range(10):
        _update_position_trends_for_tick(positions=[{"symbol":"AMD","qty":1,"current_price_usd":101}], provider=p, trade_date="2026-07-13", now=datetime(2026,7,13,tzinfo=timezone.utc), locked_watchlist_cache=[], watchlist_cache_source="test")
    assert p.daily_calls == 0
