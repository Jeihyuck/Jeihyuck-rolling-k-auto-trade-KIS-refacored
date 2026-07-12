from datetime import date, datetime, timedelta
from trader.us.db.price_daily_repo import *
from trader.us.dates import canonical_us_bar_date


def setup_function(): reset_us_daily_memory()

def bars(n=270, end=date(2026,7,10), close=100):
    return [{"date": (end-timedelta(days=n-i)).isoformat(), "open": close, "high": close+1, "low": close-1, "close": close+i*0.01, "volume": 1000+i} for i in range(n)]

def test_us_symbol_normalization_no_zfill_and_dates():
    assert [normalize_us_price_symbol(s) for s in ["AMD","nvda"," ARM ","BRK.B","BF.B"]] == ["AMD","NVDA","ARM","BRK.B","BF.B"]
    assert normalize_us_price_symbol("AMD") != "000AMD"
    assert canonical_us_bar_date("20260710") == canonical_us_bar_date("2026-07-10") == canonical_us_bar_date("2026-07-10T00:00:00") == date(2026,7,10)

def test_recent_us_daily_returns_limit_ascending_excludes_trade_date():
    upsert_us_daily_bars(symbol="NVDA", bars=bars(270) + [{"date":"2026-07-10","close":999}], source="TEST")
    rows = load_recent_us_daily_bars(symbol="NVDA", before_date="2026-07-10", limit=260)
    assert len(rows) == 260
    assert rows == sorted(rows, key=lambda r: r["date"])
    assert rows[-1]["date"] < "2026-07-10"
    audit = audit_us_daily_history(symbol="NVDA", before_date="2026-07-10", required_bars=260)
    assert audit["enough_history"] is True and audit["invalid_close_count"] == 0
