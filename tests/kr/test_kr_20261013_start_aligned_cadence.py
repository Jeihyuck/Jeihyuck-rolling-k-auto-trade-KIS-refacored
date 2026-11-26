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


def test_clean_policy_wait_can_resume_after_bounded_idle_without_catchup():
    for elapsed in (60, 80, 130, 300):
        sleep = _sleep_until_start_aligned_tick(
            tick_started_monotonic=1000, current_monotonic=1000+elapsed,
            current_kst=NOW+timedelta(seconds=elapsed),
            session_end_dt=NOW+timedelta(seconds=600),
            loop_interval=60, fresh_policy_wait_resume=True,
        )
        assert sleep == 5
    assert _sleep_until_start_aligned_tick(
        tick_started_monotonic=1000, current_monotonic=1040,
        current_kst=NOW+timedelta(seconds=40),
        session_end_dt=NOW+timedelta(seconds=600),
        loop_interval=60, fresh_policy_wait_resume=True,
    ) == 20
    assert _sleep_until_start_aligned_tick(
        tick_started_monotonic=1000, current_monotonic=1100,
        current_kst=NOW+timedelta(seconds=100),
        session_end_dt=NOW+timedelta(seconds=100),
        loop_interval=60, fresh_policy_wait_resume=True,
    ) == 0


def test_fast_resume_only_clean_owner_wait(monkeypatch):
    from trader.pb1_runner import _kr_fresh_policy_wait_resume
    monkeypatch.delenv("PB1_PENDING_RECONCILE_ONLY", raising=False)
    monkeypatch.setenv("KR_PB1_FRESH_POLICY_WAIT_RESUME", "1")
    args = dict(env="practice", strategy="pb1_pullback_close",
                session_kind="am", status="OK_NO_TRADE")
    metrics = dict(tick_result_reason="KR_ENTRY_TIMING_POLICY_WAIT",
                   api_submitted=0, buy_orders=0, sell_orders=0,
                   kr_infinite_decision="WAIT", warning_counts={})
    assert _kr_fresh_policy_wait_resume(**args, metrics=metrics)
    for override in ({"env":"real"}, {"strategy":"KR_INFINITE"},
                     {"session_kind":"close"}, {"status":"RETRYABLE_MARKET_DATA_ERROR"}):
        assert not _kr_fresh_policy_wait_resume(**{**args, **override}, metrics=metrics)
    for override in ({"api_submitted":1}, {"buy_orders":1}, {"sell_orders":1},
                     {"kr_infinite_decision":"BUY"}, {"degraded":True},
                     {"warning_counts":{"timeout_count":1}},
                     {"tick_result_reason":"OPENING_30MIN_BUY_BLOCK"}):
        assert not _kr_fresh_policy_wait_resume(**args, metrics={**metrics, **override})
    assert not _kr_fresh_policy_wait_resume(**args, metrics={k:v for k,v in metrics.items() if k!="api_submitted"})
    monkeypatch.setenv("PB1_PENDING_RECONCILE_ONLY", "1")
    assert not _kr_fresh_policy_wait_resume(**args, metrics=metrics)
