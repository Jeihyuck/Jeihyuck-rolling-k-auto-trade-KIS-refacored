"""An unreaped order-capable child must fence later sessions, not just raise."""
from __future__ import annotations

import json

import pytest


def test_durable_fence_outlives_dead_parent_and_refuses_atexit_release(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import trader.us.utils.session_guard as guard
    td, session, run_id = "2026-10-13", "am", "unreaped-run"
    acquired = guard.acquire_us_session_running_lock(td, session, run_id)
    assert acquired["acquired"]
    fence = guard.mark_us_session_child_unverified_fence(
        td, session, run_id=run_id, pid=123456, reason="forced_unreaped_child",
    )
    assert fence.exists()
    assert guard.release_us_session_running_lock(td, session, run_id)["released"] is False
    assert (tmp_path / "runtime/session_guard/us" / td / "am.running").exists()
    # Even a stale lock or bypass flag must not override authoritative UNVERIFIED.
    monkeypatch.setenv("US_BYPASS_RUNNING_LOCK_FOR_TEST", "1")
    next_run = guard.acquire_us_session_running_lock(td, session, "second-run", stale_minutes=1)
    assert next_run["acquired"] is False
    assert next_run["reason"] == "CHILD_UNVERIFIED_FENCE"


def test_runner_integrates_unreaped_child_into_session_finally(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "fake-unreaped")
    monkeypatch.setenv("GITHUB_SHA", "testsha")
    monkeypatch.setenv("GITHUB_WORKFLOW", "unreaped-fence-test")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "0")

    import trader.us.runner.trade_session_runner as runner
    import trader.us.runner.tick_process as ticks
    import trader.us.utils.session_guard as guard

    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _d: True)
    monkeypatch.setattr("trader.us.prep_contract.check_us_prep_guard",
                        lambda date: {"ok": True, "status": "OK"})
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget",
                        lambda cash: {"capital_usd_cap": 10000.0})
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda **kwargs: 0)
    monkeypatch.setattr("trader.us.db.repos.load_today_committed_buy_notional",
                        lambda *a, **kw: 0.0)
    monkeypatch.setattr(guard, "check_us_session_file_guard",
                        lambda *a, **kw: {"already_ran": False, "guard_status": "OK"})

    def _order_capable_target(**_kwargs):
        pytest.fail("fake process adapter must raise before running child")

    _order_capable_target.__module__ = "trader.us.runner.trade_tick_runner"
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.run_trade_tick",
                        _order_capable_target)

    def _unreaped(*_a, **kwargs):
        path = kwargs["kwargs"]["active_session_state_path"]
        guard.mark_us_session_child_unverified_fence(
            "2026-06-05", "am", run_id="am-2026-06-05-fake-unreaped",
            pid=2147483, reason="mock child refuses SIGTERM and SIGKILL")
        raise ticks.TickChildUnverifiedError("mock unreaped child pid=2147483")

    monkeypatch.setattr(ticks, "run_tick_in_process", _unreaped)
    try:
        runner.run_trade_session(
            session="am", env="practice", offline=False,
            max_minutes=1, interval_sec=1, max_ticks=1,
            force_now="2026-06-05T09:35:00-04:00",
        )
    except ticks.TickChildUnverifiedError:
        pass
    state_files = list((tmp_path / "runtime/us/session_state/2026-06-05").glob("*.json"))
    assert state_files, "real session runner must write active session state"
    assert all(json.loads(path.read_text())["state"] == "CHILD_UNVERIFIED"
               for path in state_files)
    assert (tmp_path / "runtime/session_guard/us/2026-06-05/am.running").exists()
    assert (tmp_path / "runtime/session_guard/us/2026-06-05/am.child_unverified.fence").exists()
    assert guard.acquire_us_session_running_lock("2026-06-05", "am", "replacement")["acquired"] is False
