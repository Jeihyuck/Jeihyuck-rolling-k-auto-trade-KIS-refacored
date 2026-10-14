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
    # The full runner must refuse starting even its ACTIVE state file.
    import trader.us.runner.trade_session_runner as runner
    result = runner.run_trade_session(
        session=session, env="practice", offline=True,
        force_now="2026-10-13T09:35:00-04:00", max_ticks=1,
    )
    assert result["status"] == "SKIP"
    assert result["reason"] == "duplicate_session_running"
    assert not list((tmp_path / "runtime/us/session_state" / td).glob("*.json"))


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
    with pytest.raises((ticks.TickChildUnverifiedError, RuntimeError)):
        runner.run_trade_session(
            session="am", env="practice", offline=False,
            max_minutes=1, interval_sec=1, max_ticks=1,
            force_now="2026-06-05T09:35:00-04:00",
        )
    state_files = list((tmp_path / "runtime/us/session_state/2026-06-05").glob("*.json"))
    assert state_files, "real session runner must write active session state"
    assert all(json.loads(path.read_text())["state"] == "CHILD_UNVERIFIED"
               for path in state_files)
    assert (tmp_path / "runtime/session_guard/us/2026-06-05/am.running").exists()
    assert (tmp_path / "runtime/session_guard/us/2026-06-05/am.child_unverified.fence").exists()
    assert guard.acquire_us_session_running_lock("2026-06-05", "am", "replacement")["acquired"] is False


def test_unreaped_child_blocks_all_us_sessions_and_later_trade_dates_for_same_account(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")
    from trader.us import config as us_cfg
    monkeypatch.setattr(us_cfg, "CANO", "12345678")
    monkeypatch.setattr(us_cfg, "ACNT_PRDT_CD", "01")
    monkeypatch.setattr(us_cfg, "KIS_ENV", "practice")
    import trader.us.utils.session_guard as guard
    source_date = "2026-10-13"
    assert guard.acquire_us_session_running_lock(source_date, "am", "source", env="practice")["acquired"]
    guard.mark_us_session_child_unverified_fence(
        source_date, "am", run_id="source", pid=54321,
        reason="simulated_orphan", env="practice",
    )
    monkeypatch.setenv("US_BYPASS_RUNNING_LOCK_FOR_TEST", "1")
    for trade_date, session in ((source_date, "am"), (source_date, "afternoon"),
                                ("2026-10-14", "am"), ("2026-10-15", "close"),
                                ("2026-10-20", "trader")):
        denied = guard.acquire_us_session_running_lock(
            trade_date, session, "new-run", env="practice",
        )
        assert denied["acquired"] is False, (trade_date, session, denied)
        assert denied["reason"] == "CHILD_UNVERIFIED_FENCE"
    assert guard.release_us_session_running_lock(source_date, "am", "source", env="practice")["released"] is False

    # The precise account+environment is the fence scope, not all US accounts.
    # Merely changing the shell cannot impersonate another KIS account:
    # it is a configuration mismatch and must fail before lock admission.
    monkeypatch.setenv("CANO", "87654321")
    mismatch = guard.acquire_us_session_running_lock(
        "2026-10-14", "am", "shell-conflict", env="practice",
    )
    assert mismatch["acquired"] is False
    assert mismatch["reason"] == "BROKER_ACCOUNT_CONFIG_MISMATCH"
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setattr(us_cfg, "CANO", "87654321")
    monkeypatch.setenv("CANO", "87654321")
    assert guard.acquire_us_session_running_lock(
        "2026-10-14", "am", "real-other-account", env="practice",
    )["acquired"] is True
    monkeypatch.setattr(us_cfg, "KIS_ENV", "prod")
    assert guard.acquire_us_session_running_lock(
        "2026-10-14", "am", "real-other-env", env="prod",
    )["acquired"] is True
    monkeypatch.setattr(us_cfg, "KIS_ENV", "practice")
    monkeypatch.setattr(us_cfg, "CANO", "12345678")
    monkeypatch.setenv("CANO", "12345678")
    assert guard.acquire_us_session_running_lock(
        "2026-10-14", "am", "same-again", env="practice",
    )["acquired"] is False

    scope_files = list((tmp_path / "runtime/session_guard/us/.child_scope").glob("*.fence"))
    assert len(scope_files) == 1
    assert "12345678" not in str(scope_files[0])
    assert "12345678" not in scope_files[0].read_text()


def test_child_account_scope_is_pinned_to_broker_identity_not_shell(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from trader.us import config as us_cfg
    import trader.us.utils.session_guard as guard
    monkeypatch.setattr(us_cfg, "KIS_ENV", "practice")
    monkeypatch.setattr(us_cfg, "CANO", "11223344")
    monkeypatch.setattr(us_cfg, "ACNT_PRDT_CD", "01")
    monkeypatch.setenv("CANO", "11223344")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")
    pinned = guard._child_fence_scope("practice")
    assert guard.acquire_us_session_running_lock(
        "2026-10-13", "am", "initial", env="practice", account_scope=pinned,
    )["acquired"]
    monkeypatch.setenv("CANO", "99887766")
    denied = guard.acquire_us_session_running_lock(
        "2026-10-14", "afternoon", "should-not-run",
        env="practice", account_scope=pinned,
    )
    assert denied["acquired"] is False
    assert denied["reason"] == "BROKER_ACCOUNT_CONFIG_MISMATCH"
    # A later child that did not exit writes to the original pinned scope,
    # regardless of what the shell environment has become.
    guard.mark_us_session_child_unverified_fence(
        "2026-10-13", "am", run_id="initial", pid=9876,
        reason="simulated_child_alive", env="practice", account_scope=pinned,
    )
    monkeypatch.setenv("CANO", "11223344")
    assert guard.acquire_us_session_running_lock(
        "2026-10-15", "close", "blocked", env="practice",
    )["reason"] == "CHILD_UNVERIFIED_FENCE"
