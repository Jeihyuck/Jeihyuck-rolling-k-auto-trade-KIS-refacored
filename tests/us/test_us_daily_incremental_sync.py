from datetime import date, timedelta
from trader.us.db.price_daily_repo import reset_us_daily_memory, upsert_us_daily_bars
from trader.us.data_provider import USDataProvider


def setup_function(): reset_us_daily_memory()

def _bars(n, end=date(2026,7,10), close=100):
    return [{"date": (end-timedelta(days=n-i)).isoformat(), "close": close+i, "open": close+i, "high": close+i, "low": close+i, "volume": 1} for i in range(n)]

class Client:
    def __init__(self): self.calls=0
    def get_us_daily_price_history(self, *a, **k): self.calls += 1; return _bars(k.get("required_bars",260))


def test_db_latest_enough_calls_kis_zero():
    upsert_us_daily_bars(symbol="AMD", bars=_bars(260), source="TEST")
    p=USDataProvider(offline=True); c=Client(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("AMD","NASDAQ",trade_date="2026-07-10",required_bars=260,allow_http_sync=True)
    assert len(rows)==260 and c.calls==0

def test_empty_db_bootstraps_and_upserts():
    p=USDataProvider(offline=True); c=Client(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("ARM","NASDAQ",trade_date="2026-07-10",required_bars=260,allow_http_sync=True)
    assert len(rows)==260 and c.calls==1


def test_stale_260_bars_syncs_missing_session():
    # trade_date Monday 2026-07-13 expects Friday 2026-07-10; DB latest Thursday is stale.
    upsert_us_daily_bars(symbol="AMD", bars=_bars(260, end=date(2026,7,10)), source="TEST")
    class OneDay(Client):
        def get_us_daily_price_history(self, *a, **k):
            self.calls += 1
            return [{"date":"2026-07-10","close":999,"open":999,"high":999,"low":999,"volume":1}]
    p=USDataProvider(offline=True); c=OneDay(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("AMD","NASDAQ",trade_date="2026-07-13",required_bars=260,allow_http_sync=True)
    assert c.calls == 1
    assert rows[-1]["xymd"] == "20260710"


def test_weekend_trade_date_friday_latest_calls_kis_zero():
    upsert_us_daily_bars(symbol="NVDA", bars=_bars(260, end=date(2026,7,11)), source="TEST")
    p=USDataProvider(offline=True); c=Client(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("NVDA","NASDAQ",trade_date="2026-07-12",required_bars=260,allow_http_sync=True)
    assert len(rows) == 260 and c.calls == 0 and rows[-1]["xymd"] == "20260710"


def test_us_holiday_trade_date_previous_session_latest_calls_kis_zero():
    # 2026-07-03 is Independence Day observed; Monday 2026-07-06 expects Thu 2026-07-02.
    upsert_us_daily_bars(symbol="QQQ", bars=_bars(260, end=date(2026,7,3)), source="TEST")
    p=USDataProvider(offline=True); c=Client(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("QQQ","NASDAQ",trade_date="2026-07-06",required_bars=260,allow_http_sync=True)
    assert len(rows) == 260 and c.calls == 0 and rows[-1]["xymd"] == "20260702"


def test_current_but_short_history_backfills_older_bars():
    # Latest matches expected previous session, but only 100 valid bars exist.
    upsert_us_daily_bars(symbol="MSFT", bars=_bars(100, end=date(2026,7,11)), source="TEST")
    class Older(Client):
        def __init__(self): super().__init__(); self.asofs=[]
        def get_us_daily_price_history(self, *a, **k):
            self.calls += 1; self.asofs.append(k.get("as_of_date"))
            return _bars(200, end=date(2026,3,23), close=10)
    p=USDataProvider(offline=True); c=Older(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("MSFT","NASDAQ",trade_date="2026-07-13",required_bars=260,allow_http_sync=True)
    assert c.calls == 1
    assert c.asofs[0] < "20260710"
    assert len(rows) == 260


def test_stale_and_short_history_fills_both_directions():
    upsert_us_daily_bars(symbol="META", bars=_bars(100, end=date(2026,7,10)), source="TEST")
    class Both(Client):
        def __init__(self): super().__init__(); self.asofs=[]; self.stops=[]
        def get_us_daily_price_history(self, *a, **k):
            self.calls += 1; self.asofs.append(k.get("as_of_date")); self.stops.append(k.get("stop_at_date"))
            if self.calls == 1:
                return [{"date":"2026-07-10","close":999,"open":999,"high":999,"low":999,"volume":1}]
            return _bars(200, end=date(2026,3,23), close=10)
    p=USDataProvider(offline=True); c=Both(); p._client=c; p._offline=False
    rows=p.get_completed_daily_prices("META","NASDAQ",trade_date="2026-07-13",required_bars=260,allow_http_sync=True)
    assert c.calls == 2
    assert c.stops[0] == "2026-07-09"
    assert c.stops[1] is None
    assert len(rows) == 260


def test_completed_daily_prices_result_quality_contract_ok_and_stale():
    upsert_us_daily_bars(symbol="AAPL", bars=_bars(260, end=date(2026,7,11)), source="TEST")
    p=USDataProvider(offline=True); c=Client(); p._client=c; p._offline=False
    ok=p.get_completed_daily_prices_result("AAPL","NASDAQ",trade_date="2026-07-13",required_bars=260,allow_http_sync=False)
    assert ok["quality"] == "OK" and ok["valid_bar_count"] == 260 and ok["http_sync_attempted"] is False
    stale=p.get_completed_daily_prices_result("AAPL","NASDAQ",trade_date="2026-07-14",required_bars=260,allow_http_sync=False)
    assert stale["quality"] == "STALE" and stale["db_latest"] == "2026-07-10"


def test_invalid_latest_close_is_stale_and_zero_close_not_upserted():
    # A close=0 latest row must not be accepted as a valid completed session.
    bars = _bars(260, end=date(2026, 7, 10))
    assert upsert_us_daily_bars(symbol="BAD", bars=bars + [{"date":"2026-07-10","close":0,"open":0,"high":0,"low":0,"volume":1}], source="TEST") == 260
    class Fix(Client):
        def get_us_daily_price_history(self, *a, **k):
            self.calls += 1
            return [{"date":"2026-07-10","close":123,"open":123,"high":123,"low":123,"volume":1}]
    p=USDataProvider(offline=True); c=Fix(); p._client=c; p._offline=False
    stale=p.get_completed_daily_prices_result("BAD","NASDAQ",trade_date="2026-07-13",required_bars=260,allow_http_sync=False)
    assert stale["quality"] == "STALE"
    synced=p.get_completed_daily_prices_result("BAD","NASDAQ",trade_date="2026-07-13",required_bars=260,allow_http_sync=True)
    assert c.calls == 1 and synced["quality"] == "OK" and synced["db_latest"] == "2026-07-10"
