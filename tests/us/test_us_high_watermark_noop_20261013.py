"""PR-A: do not write the identical confirmed same-day high-watermark every tick."""
from datetime import datetime, timezone

from trader.us import position_lifecycle_state as state


NOW = datetime(2026, 10, 12, 17, 30, tzinfo=timezone.utc)
TODAY = "2026-10-12"
ID = "lifecycle-hw-1"


def _run(monkeypatch, *, saved_date=TODAY, high=125.0, current=120.0,
         same=True, source="us_position_risk_state", closed=False):
    previous = {
        "lifecycle_id": ID if same else "old-lifecycle",
        "high_watermark": high,
        "high_watermark_at": "2026-10-12T16:00:00+00:00",
        "high_watermark_source": source,
        "is_open": not closed,
        "strategy_owner": "US_STANDARD",
        "account_id": "practice:test",
        "env": "practice",
        "trading_epoch_id": "epoch-test",
    }
    writes = []
    monkeypatch.setattr(
        state, "load_latest_us_position_risk_state",
        lambda symbol, date: {
            "trade_date": saved_date,
            "state": {"lifecycle": dict(previous)},
        },
    )
    monkeypatch.setattr(
        state, "_save_lifecycle",
        lambda symbol, date, lifecycle: writes.append(dict(lifecycle)) or dict(lifecycle),
    )
    result = state.update_us_position_high_watermark(
        symbol="MSFT", trade_date=TODAY, lifecycle_id=ID,
        current_price=current, entry_price=100.0, now=NOW,
    )
    return result, writes


def test_confirmed_same_day_high_watermark_below_peak_is_read_only(monkeypatch):
    for _ in range(12):
        result, writes = _run(monkeypatch)
        assert writes == []
        assert result["high_watermark"] == 125.0
        assert result["high_watermark_at"] == "2026-10-12T16:00:00+00:00"


def test_new_peak_writes_atomic_lifecycle_and_moves_high_watermark(monkeypatch):
    result, writes = _run(monkeypatch, current=136.0)
    assert len(writes) == 1
    assert writes[0]["high_watermark"] == 136.0
    assert result["high_watermark"] == 136.0


def test_previous_day_row_is_not_treated_as_saved_today(monkeypatch):
    result, writes = _run(monkeypatch, saved_date="2026-10-09")
    assert len(writes) == 1
    assert result["high_watermark"] == 125.0


def test_new_lifecycle_and_missing_provenance_must_write(monkeypatch):
    _, new_life_writes = _run(monkeypatch, same=False)
    _, missing_source_writes = _run(monkeypatch, source="")
    assert len(new_life_writes) == len(missing_source_writes) == 1


def test_closed_lifecycle_never_short_circuits_atomic_validation(monkeypatch):
    _, writes = _run(monkeypatch, closed=True)
    assert len(writes) == 1
    assert writes[0]["is_open"] is False
