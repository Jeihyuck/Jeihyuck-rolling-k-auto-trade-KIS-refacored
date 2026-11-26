"""US_STANDARD pre-minute collector priorities and immutable minute conflicts.

No KIS, network, trading account or real order. Synthetic offline evidence.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.us.entry_timing.opening_observer import (
    load_bars, load_preminute_priority, next_priority_pair, open_store,
    persist_bars, priority_manifest_path, publish_preminute_candidates,
    store_path,
)

DAY = "2026-10-28"
NY = ZoneInfo("America/New_York")


def _row(last="100.5", minute="093000"):
    return {
        "xymd": "20261028", "xhms": minute,
        "open": "100", "high": "101", "low": "99",
        "last": last, "evol": "500", "eamt": "50250",
    }


def test_finished_minute_conflict_quarantined_without_rewriting_original(tmp_path):
    path = store_path(env="practice", account_scope="scope:epoch",
                      trade_date=DAY, base=tmp_path)
    db = open_store(path)
    now = datetime(2026, 10, 28, 10, 5, tzinfo=NY)
    assert persist_bars(db, trade_date=DAY, symbol="CL", owner="US_STANDARD",
                        native_rows=[_row()], now_et=now) == 1
    before = db.execute("SELECT row_json FROM verified_bars").fetchone()[0]
    altered = _row(last="100.6")
    assert persist_bars(db, trade_date=DAY, symbol="CL", owner="US_STANDARD",
                        native_rows=[altered], now_et=now) == 0
    after = db.execute("SELECT row_json FROM verified_bars").fetchone()[0]
    assert after == before
    history = db.execute(
        "SELECT original_row_json,incoming_row_json,status FROM bar_conflicts"
    ).fetchall()
    assert len(history) == 1
    assert "100.5" in history[0][0] and "100.6" in history[0][1]
    assert history[0][2] == "UNRESOLVED"
    assert load_bars(db, trade_date=DAY, symbol="CL") == []
    db.close()

    # Durable even after process restart; repeated same correction deduplicates.
    db = open_store(path)
    assert persist_bars(db, trade_date=DAY, symbol="CL", owner="US_STANDARD",
                        native_rows=[altered], now_et=now) == 0
    assert db.execute("SELECT count(*) FROM bar_conflicts").fetchone()[0] == 1
    assert load_bars(db, trade_date=DAY, symbol="CL") == []
    db.close()


def test_unrelated_minute_conflict_does_not_globally_block_other_evidence(tmp_path):
    db = open_store(store_path(env="practice", account_scope="scope:epoch",
                               trade_date=DAY, base=tmp_path))
    now = datetime(2026, 10, 28, 10, 5, tzinfo=NY)
    assert persist_bars(db, trade_date=DAY, symbol="CL", owner="US_STANDARD",
                        native_rows=[_row(minute="093000"), _row(minute="093100")],
                        now_et=now) == 2
    persist_bars(db, trade_date=DAY, symbol="CL", owner="US_STANDARD",
                 native_rows=[_row(last="100.6", minute="093000")], now_et=now)
    bars = load_bars(db, trade_date=DAY, symbol="CL")
    assert len(bars) == 1
    assert bars[0]["time_et"][11:16] == "09:31"
    db.close()


def _pairs(count=18):
    return [(f"P{i:02d}", "NYSE") for i in range(count)]


def _candidates(pairs):
    return [(1.0 - i / 100, sym, exch, None, {"position_state": "NOT_HELD"})
            for i, (sym, exch) in enumerate(pairs)]


def test_manifest_candidate_pub_consume_fairness_and_ttl(tmp_path):
    pairs = _pairs()
    published = publish_preminute_candidates(
        env="practice", scope="scope:epoch", trade_date=DAY,
        candidates=_candidates(pairs), now_epoch=1000.0,
        base=tmp_path,
    )
    assert published["hot"] == 5
    assert published["warm"] == 5
    assert priority_manifest_path(
        env="practice", scope="scope:epoch", trade_date=DAY,
        base=tmp_path,
    ).is_file()
    lanes, state = load_preminute_priority(
        env="practice", scope="scope:epoch", trade_date=DAY,
        pairs=pairs, now_epoch=1002.0, base=tmp_path,
    )
    assert state == "VALID"
    assert tuple(map(len, (lanes["HOT"], lanes["WARM"], lanes["COLD"]))) == (5, 5, 8)
    offsets = {"HOT": 0, "WARM": 0, "COLD": 0}
    counts = {"HOT": 0, "WARM": 0, "COLD": 0}
    for slot in range(60):
        pair, tier = next_priority_pair(lanes, slot=slot, offsets=offsets)
        assert pair in pairs
        counts[tier] += 1
    assert counts == {"HOT": 60, "WARM": 0, "COLD": 0}
    assert len({p[0] for p in lanes["HOT"]}) == 5
    stale_lanes, stale = load_preminute_priority(
        env="practice", scope="scope:epoch", trade_date=DAY,
        pairs=pairs, now_epoch=1181, base=tmp_path,
    )
    assert stale == "STALE"
    assert next_priority_pair(stale_lanes, slot=0, offsets={}) is None
    _, cross_scope = load_preminute_priority(
        env="practice", scope="other:epoch", trade_date=DAY,
        pairs=pairs, now_epoch=1002, base=tmp_path,
    )
    assert cross_scope == "MISSING"


def test_malformed_manifest_falls_back_without_buy_authority(tmp_path):
    pairs = _pairs(4)
    path = priority_manifest_path(env="practice", scope="epoch",
                                  trade_date=DAY, base=tmp_path)
    path.write_text("{broken", encoding="utf-8")
    lanes, status = load_preminute_priority(
        env="practice", scope="epoch", trade_date=DAY,
        pairs=pairs, now_epoch=1002, base=tmp_path,
    )
    assert status == "INVALID"
    assert lanes["HOT"] == [] and lanes["WARM"] == []
    assert lanes["COLD"] == pairs
    assert next_priority_pair(lanes, slot=0, offsets={}) is None


def test_zero_candidates_does_not_publish_fake_coverage_or_cross_owner(tmp_path):
    pairs = [("CL", "NYSE"), ("PG", "NYSE")]
    results = publish_preminute_candidates(
        env="practice", scope="scope", trade_date=DAY, now_epoch=1000,
        candidates=[(1.0, "TQQQ", "NASDAQ", None,
                     {"strategy_owner": "TQQQ_INFINITE"})],
        base=tmp_path,
    )
    assert results["hot"] == results["warm"] == 0
    lanes, state = load_preminute_priority(
        env="practice", scope="scope", trade_date=DAY,
        pairs=pairs, now_epoch=1002, base=tmp_path,
    )
    assert state == "VALID"
    assert lanes["COLD"] == pairs
    assert next_priority_pair(lanes, slot=0, offsets={}) is None


def test_native_priority_never_fetches_warm_cold_or_overcap():
    assert next_priority_pair({"HOT": [], "WARM": _pairs(5), "COLD": _pairs(30)}, slot=0, offsets={}) is None
    assert next_priority_pair({"HOT": _pairs(6)}, slot=0, offsets={}) is None


def test_request_clock_excludes_in_progress_minute(tmp_path):
    db = open_store(store_path(env="practice", account_scope="scope", trade_date=DAY, base=tmp_path))
    request_time = datetime(2026, 10, 28, 9, 31, 59, tzinfo=NY)
    try:
        assert persist_bars(db, trade_date=DAY, symbol="CL", owner="US_STANDARD", native_rows=[_row(minute="093000"), _row(minute="093100")], now_et=request_time) == 1
        assert len(load_bars(db, trade_date=DAY, symbol="CL")) == 1
    finally:
        db.close()
