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
