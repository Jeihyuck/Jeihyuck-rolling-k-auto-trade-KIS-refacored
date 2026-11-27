"""P0 native 1-minute KIS source conflict, offline SQLite only."""
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.us.entry_timing.opening_observer import (
    load_bars, open_store, persist_bars, store_path,
)
from trader.us.entry_timing.opportunity import evaluate_price_opportunity, normalize_bars

DAY = "2026-11-20"
NY = ZoneInfo("America/New_York")


def _native(close="100.50", minute="093000"):
    return {
        "xymd": "20261120", "xhms": minute,
        "open": "100", "high": "102", "low": "99",
        "last": close, "evol": "150", "eamt": "15075",
    }


def _db(tmp_path):
    return open_store(store_path(env="practice", account_scope="fixture-epoch",
                                 trade_date=DAY, base=tmp_path))


def _ingest(db, native):
    return persist_bars(
        db, trade_date=DAY, symbol="KO", owner="US_STANDARD",
        native_rows=native,
        now_et=datetime(2026, 11, 20, 9, 35, tzinfo=NY),
    )


def test_same_batch_conflicting_completed_minute_is_quarantined_no_authoritative_bar(tmp_path):
    db = _db(tmp_path)
    original, different = _native("100.50"), _native("100.75")
    assert _ingest(db, [original, different]) == 0
    assert load_bars(db, trade_date=DAY, symbol="KO") == []
    conflicts = db.execute(
        "SELECT original_row_json,incoming_row_json,status FROM bar_conflicts"
    ).fetchall()
    assert len(conflicts) == 1
    assert conflicts[0][0] != conflicts[0][1]
    assert conflicts[0][2] == "UNRESOLVED"
    # Subsequent identical native output cannot simply overwrite/clear a
    # previously conflicted completed minute.
    # V2 repair requires three independent spaced native observations;
    # an un-attested re-read cannot publish an authoritative row.
    assert _ingest(db, [original]) == 0
    assert db.execute("SELECT COUNT(*) FROM verified_bars").fetchone()[0] == 0
    assert load_bars(db, trade_date=DAY, symbol="KO") == []
    db.close()


def test_duplicate_identical_completed_minute_is_idempotent(tmp_path):
    db = _db(tmp_path)
    rows = [_native(), _native()]
    assert _ingest(db, rows) == 1
    assert len(load_bars(db, trade_date=DAY, symbol="KO")) == 1
    assert db.execute("SELECT COUNT(*) FROM bar_conflicts").fetchone()[0] == 0
    db.close()


def test_conflict_is_minute_scoped_other_completed_minute_remains_usable(tmp_path):
    db = _db(tmp_path)
    rows = [
        _native("100.50", "093000"),
        _native("100.75", "093000"),
        _native("100.60", "093100"),
    ]
    assert _ingest(db, rows) == 1
    visible = load_bars(db, trade_date=DAY, symbol="KO")
    assert len(visible) == 1
    assert visible[0]["time_et"][11:16] == "09:31"
    assert db.execute("SELECT COUNT(*) FROM bar_conflicts").fetchone()[0] == 1
    db.close()


def test_tqqq_owner_cannot_write_conflicting_standard_bars(tmp_path):
    db = _db(tmp_path)
    assert persist_bars(
        db, trade_date=DAY, symbol="TQQQ", owner="TQQQ_INFINITE",
        native_rows=[_native("100.50"), _native("100.75")],
        now_et=datetime(2026, 11, 20, 9, 35, tzinfo=NY),
    ) == 0
    assert db.execute("SELECT COUNT(*) FROM bar_conflicts").fetchone()[0] == 0
    db.close()
