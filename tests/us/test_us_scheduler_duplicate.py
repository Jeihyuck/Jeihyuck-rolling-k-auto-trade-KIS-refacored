from trader.us.session_lock import acquire_session_lock


def test_us_am_duplicate_run_blocked_within_60_seconds(tmp_path):
    first = acquire_session_lock("us", "am", trade_date="2026-07-13", lock_dir=tmp_path)
    second = acquire_session_lock("us", "am", trade_date="2026-07-13", lock_dir=tmp_path)
    assert first.ok is True
    assert second.ok is False
    assert second.reason == "duplicate_recent_run"
