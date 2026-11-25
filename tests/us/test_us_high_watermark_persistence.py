from datetime import datetime, timezone
from trader.us.db import repos
from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles, update_us_position_high_watermark


def setup_function(): repos.reset_memory_stores()

def test_high_watermark_persists_across_ticks_days_and_resets_new_lifecycle():
    now=datetime(2026,7,10,tzinfo=timezone.utc)
    identity={"env":"practice","account_id":"practice:test-account","trading_epoch_id":"epoch-a","strategy_owner":"US_STANDARD"}
    lc=reconcile_us_position_lifecycles(positions=[{"symbol":"SAMPLE","qty":2,"entry_price":100,**identity}], trade_date="2026-07-10", now=now, authoritative=True)["SAMPLE"]
    lid=lc["lifecycle_id"]
    assert update_us_position_high_watermark(symbol="SAMPLE", trade_date="2026-07-10", lifecycle_id=lid, current_price=105, entry_price=100, now=now)["high_watermark"] == 105
    assert update_us_position_high_watermark(symbol="SAMPLE", trade_date="2026-07-10", lifecycle_id=lid, current_price=103, entry_price=100, now=now)["high_watermark"] == 105
    assert update_us_position_high_watermark(symbol="SAMPLE", trade_date="2026-07-11", lifecycle_id=lid, current_price=104, entry_price=99, now=datetime(2026,7,11,tzinfo=timezone.utc))["high_watermark"] == 105
    assert update_us_position_high_watermark(symbol="SAMPLE", trade_date="2026-07-11", lifecycle_id=lid, current_price=108, entry_price=99, now=datetime(2026,7,11,tzinfo=timezone.utc))["high_watermark"] == 108


def test_hwm_atomic_pair_preserved_under_delayed_writer(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    base = {"lifecycle_id": "hwm-pair-cycle", "is_open": True,
            "env": "practice", "strategy_owner": "US_STANDARD",
            "account_id": "practice:fixture", "trading_epoch_id": "hwm-pair-epoch",
            "high_watermark_source": "us_position_risk_state"}
    day = "2026-11-24"
    peak = {**base, "high_watermark": 130.0, "high_watermark_at": "2026-11-24T15:00:00+00:00"}
    assert repos.save_us_position_lifecycle_atomic("HWMPAIR", day, peak)["high_watermark"] == 130
    stale = {**base, "high_watermark": 120.0, "high_watermark_at": "2026-11-24T16:00:00+00:00",
             "high_watermark_source": "stale_writer"}
    for price in (120.0, 130.0):
        result = repos.save_us_position_lifecycle_atomic("HWMPAIR", day, {**stale, "high_watermark": price})
        assert result["high_watermark"] == 130
        assert result["high_watermark_at"] == peak["high_watermark_at"]
        assert result["high_watermark_source"] == peak["high_watermark_source"]
    newer = {**base, "high_watermark": 140.0, "high_watermark_at": "2026-11-24T17:00:00+00:00"}
    result = repos.save_us_position_lifecycle_atomic("HWMPAIR", day, newer)
    assert result["high_watermark"] == 140
    assert result["high_watermark_at"] == newer["high_watermark_at"]
