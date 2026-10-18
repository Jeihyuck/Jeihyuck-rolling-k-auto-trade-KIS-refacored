"""Start-aligned KR cadence: no overlapping or replayed missed tick slots."""
from datetime import datetime, timedelta, timezone

from trader.pb1_runner import _sleep_until_start_aligned_tick


NOW = datetime(2026, 10, 13, 9, 0, tzinfo=timezone(timedelta(hours=9)))


def _sleep(tick_elapsed, *, end_sec=600, interval=60):
    return _sleep_until_start_aligned_tick(
        tick_started_monotonic=1000.0,
        current_monotonic=1000.0 + tick_elapsed,
        current_kst=NOW + timedelta(seconds=tick_elapsed),
        session_end_dt=NOW + timedelta(seconds=end_sec),
        loop_interval=interval,
    )


def test_40s_tick_waits_only_20s_not_another_full_minute():
    assert _sleep(40) == 20


def test_80s_tick_skips_missed_60s_slot_and_targets_120s():
    assert _sleep(80) == 40


def test_130s_tick_skips_60s_and_120s_starts():
    assert _sleep(130) == 50


def test_boundary_deadline_has_no_new_tick():
    assert _sleep(100, end_sec=100) == 0
    assert _sleep(95, end_sec=100) == 5


def test_start_to_start_60s_not_finish_plus_60():
    assert _sleep(41) + 41 == 60


def test_single_runner_remains_serial_and_timeout_recovery_fence_untouched():
    import inspect
    from trader import pb1_runner

    source = inspect.getsource(pb1_runner._run_loop)
    assert "tick_started_monotonic = time_mod.monotonic()" in source
    assert "_sleep_until_start_aligned_tick(" in source
    assert "except TickTimeoutError" in source
    assert "set_kr_tick_deadline(tick_deadline_monotonic)" in source
