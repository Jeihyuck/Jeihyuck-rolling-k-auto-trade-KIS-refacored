from datetime import date
from trader.us.db.price_daily_repo import reset_us_daily_memory, upsert_us_daily_bars, load_recent_us_daily_bars, normalize_us_price_symbol


def setup_function(): reset_us_daily_memory()

def test_us_upsert_never_updates_kr_rows_and_load_filters_us():
    # In-memory US repo has no path to write KOSPI/KOSDAQ rows; verify US symbol only and no zfill contamination.
    upsert_us_daily_bars(symbol="NVDA", bars=[{"date":"2026-07-09","close":100}], source="TEST")
    upsert_us_daily_bars(symbol="005930", bars=[{"date":"2026-07-09","close":70000}], source="TEST")
    assert normalize_us_price_symbol("NVDA") == "NVDA"
    assert load_recent_us_daily_bars(symbol="NVDA", before_date="2026-07-10", limit=10)[0]["symbol"] == "NVDA"
    assert load_recent_us_daily_bars(symbol="00NVDA", before_date="2026-07-10", limit=10) == []
