"""An actual session start/end event owns UTC, ET and KST provenance.

2026-11-02 is US standard time; 2026-10-30 was daylight time.
No KIS network or broker orders are used in these tests.
"""
from __future__ import annotations

import pytest

from trader.us.runner.trade_session_runner import _runtime_provenance


@pytest.mark.parametrize(
    ("start_et", "end_et", "start_utc", "start_kst", "end_utc", "end_kst"),
    [
        (
            "2026-11-02T09:30:00-05:00", "2026-11-02T12:35:00-05:00",
            "2026-11-02T14:30:00Z", "2026-11-02T23:30:00+09:00",
            "2026-11-02T17:35:00Z", "2026-11-03T02:35:00+09:00",
        ),
        (
            "2026-10-30T09:30:00-04:00", "2026-10-30T12:35:00-04:00",
            "2026-10-30T13:30:00Z", "2026-10-30T22:30:00+09:00",
            "2026-10-30T16:35:00Z", "2026-10-31T01:35:00+09:00",
        ),
    ],
)
def test_actual_start_and_end_event_are_preserved_across_all_timezones(
    monkeypatch, start_et, end_et, start_utc, start_kst, end_utc, end_kst
):
    # The scheduler's environment can be absent or wrong. An explicitly
    # recorded ET session-start event must not turn into report-end time.
    monkeypatch.setenv("US_STARTED_AT_UTC", "2026-11-03T06:05:00Z")
    monkeypatch.setenv("US_STARTED_AT_KST", "2026-11-03T15:05:00+09:00")
    result = _runtime_provenance(
        session="am", run_id="fixture",
        started_at_et=start_et, ended_at_et=end_et,
        wall_elapsed_sec=11100.0,
    )
    assert result["started_at_utc"] == start_utc
    assert result["started_at_et"] == start_et
    assert result["started_at_kst"] == start_kst
    assert result["ended_at_utc"] == end_utc
    assert result["ended_at_et"] == end_et
    assert result["ended_at_kst"] == end_kst
    assert result["started_at_utc"] != result["ended_at_utc"]
    assert result["wall_elapsed_sec"] == 11100.0


def test_provenance_rejects_naive_session_event():
    with pytest.raises(ValueError, match="NAIVE_TIMESTAMP"):
        _runtime_provenance(
            session="am", run_id="bad",
            started_at_et="2026-11-02T09:30:00",
            ended_at_et="2026-11-02T12:35:00-05:00",
        )


def test_daily_report_provenance_uses_one_event_source_for_three_timezones(monkeypatch):
    from trader.us.runner.daily_report_runner import _report_provenance
    monkeypatch.setenv("US_STARTED_AT_ET", "2026-11-02T09:30:00-05:00")
    monkeypatch.setenv("US_STARTED_AT_UTC", "2026-11-03T06:05:00Z")  # stale
    monkeypatch.setenv("US_STARTED_AT_KST", "2026-11-03T15:05:00+09:00")  # stale
    monkeypatch.setenv("US_ENDED_AT_ET", "2026-11-02T12:35:00-05:00")
    report = _report_provenance("am")
    assert report["timestamp_provenance_status"] == "OK"
    assert report["started_at_source"] == "supplied_event"
    assert report["started_at_utc"] == "2026-11-02T14:30:00Z"
    assert report["started_at_kst"] == "2026-11-02T23:30:00+09:00"
    assert report["ended_at_utc"] == "2026-11-02T17:35:00Z"
    assert report["ended_at_kst"] == "2026-11-03T02:35:00+09:00"


def test_daily_report_detects_invalid_time_provenance(monkeypatch):
    from trader.us.runner.daily_report_runner import _report_provenance
    monkeypatch.setenv("US_STARTED_AT_ET", "not-a-time")
    monkeypatch.delenv("US_STARTED_AT_UTC", raising=False)
    monkeypatch.delenv("US_STARTED_AT_KST", raising=False)
    report = _report_provenance("close")
    assert report["timestamp_provenance_status"] == "INVALID_OR_UNVERIFIED_EVENT_TIME"
    assert report["started_at_source"] == "report_generation_fallback"
