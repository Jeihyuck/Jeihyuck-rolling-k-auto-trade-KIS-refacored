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


def test_us_upsert_never_updates_kr_rows_sqlite(monkeypatch, tmp_path):
    from sqlalchemy import create_engine, text
    import trader.us.db.price_daily_repo as repo
    engine = create_engine(f"sqlite:///{tmp_path/'px.db'}")
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE price_daily (
            market TEXT NOT NULL, code TEXT NOT NULL, date DATE NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume INTEGER, value REAL, source TEXT,
            PRIMARY KEY (market, code, date)
        )"""))
        conn.execute(text("INSERT INTO price_daily (market,code,date,open,high,low,close,volume,value,source) VALUES ('KOSPI','005930','2026-07-09',1,2,0.5,70000,10,1,'KR')"))
        conn.execute(text("INSERT INTO price_daily (market,code,date,open,high,low,close,volume,value,source) VALUES ('KOSDAQ','035420','2026-07-09',1,2,0.5,100000,10,1,'KR')"))
        conn.execute(text("INSERT INTO price_daily (market,code,date,open,high,low,close,volume,value,source) VALUES ('US','NVDA','2026-07-09',1,2,0.5,100,10,1,'OLD')"))
    monkeypatch.setattr(repo, 'get_engine', lambda: engine)
    repo.upsert_us_daily_bars(symbol='NVDA', bars=[{'date':'2026-07-09','open':10,'high':20,'low':5,'close':123,'volume':99}], source='TEST')
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT market, code, close, volume, source FROM price_daily ORDER BY market, code")).fetchall()
    assert ('KOSPI','005930',70000,10,'KR') in rows
    assert ('KOSDAQ','035420',100000,10,'KR') in rows
    assert ('US','NVDA',123,99,'TEST') in rows
    loaded = repo.load_recent_us_daily_bars(symbol='NVDA', before_date='2026-07-10', limit=10)
    assert [r['symbol'] for r in loaded] == ['NVDA']
    assert repo.load_recent_us_daily_bars(symbol='000AMD', before_date='2026-07-10', limit=10) == []
