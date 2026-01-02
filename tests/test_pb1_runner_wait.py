from datetime import datetime
from zoneinfo import ZoneInfo
from types import SimpleNamespace

import trader.pb1_runner as pb1_runner
from trader.pb1_runner import _next_window_start, _parse_hhmm_to_time


def test_next_window_start_returns_next_in_day() -> None:
    now = datetime(2024, 1, 2, 8, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    starts = [_parse_hhmm_to_time("08:50"), _parse_hhmm_to_time("14:00"), _parse_hhmm_to_time("15:20")]

    target = _next_window_start(now, starts)

    assert target is not None
    assert target.hour == 8 and target.minute == 50
    assert target.tzinfo == now.tzinfo


def test_next_window_start_none_when_past_all_windows() -> None:
    now = datetime(2024, 1, 2, 16, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    starts = [_parse_hhmm_to_time("08:50"), _parse_hhmm_to_time("14:00"), _parse_hhmm_to_time("15:20")]

    target = _next_window_start(now, starts)

    assert target is None


def test_wait_branch_does_not_crash(monkeypatch) -> None:
    # Arrange times to enter wait then proceed
    times = [
        datetime(2024, 1, 2, 8, 40, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2024, 1, 2, 8, 45, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2024, 1, 2, 8, 50, tzinfo=ZoneInfo("Asia/Seoul")),
    ]

    def fake_now():
        return times.pop(0) if times else datetime(2024, 1, 2, 8, 50, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(pb1_runner, "now_kst", fake_now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.delenv("EXPECT_LIVE_TRADING", raising=False)
    monkeypatch.setattr(pb1_runner, "acquire_lock", lambda *_, **__: False)
    monkeypatch.setattr(pb1_runner, "KisAPI", lambda *_, **__: SimpleNamespace(env="practice", get_balance=lambda: {}))
    monkeypatch.setattr(
        pb1_runner,
        "parse_args",
        lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state"),
    )

    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(pb1_runner.time_mod, "sleep", fake_sleep)
    monkeypatch.setattr(pb1_runner, "setup_worktree", lambda *_, **__: None)
    monkeypatch.setattr(pb1_runner, "resolve_botstate_worktree_dir", lambda: pb1_runner.Path("/tmp"))
    monkeypatch.setattr(pb1_runner, "is_trading_day", lambda _: True)

    # Act / Assert: should exit cleanly without raising
    pb1_runner.main()
    assert slept, "wait path should invoke sleep"


def test_expect_live_guard_skipped_in_diag(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 16, 10, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setattr(pb1_runner, "now_kst", lambda: now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("EXPECT_LIVE_TRADING", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("API_BASE_URL", "https://openapivts.koreainvestment.com:29443")
    monkeypatch.setattr(pb1_runner, "acquire_lock", lambda *_, **__: False)
    monkeypatch.setattr(pb1_runner, "setup_worktree", lambda *_, **__: None)
    monkeypatch.setattr(pb1_runner, "resolve_botstate_worktree_dir", lambda: pb1_runner.Path("/tmp"))
    monkeypatch.setattr(
        pb1_runner,
        "parse_args",
        lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state"),
    )

    pb1_runner.main()  # should not raise even with EXPECT_LIVE_TRADING=1 in diag path


def test_schedule_event_does_not_wait(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 8, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setattr(pb1_runner, "now_kst", lambda: now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setattr(pb1_runner, "acquire_lock", lambda *_, **__: False)
    monkeypatch.setattr(pb1_runner, "setup_worktree", lambda *_, **__: None)
    monkeypatch.setattr(pb1_runner, "resolve_botstate_worktree_dir", lambda: pb1_runner.Path("/tmp"))
    monkeypatch.setattr(
        pb1_runner,
        "parse_args",
        lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state"),
    )

    def sleep_fail(_):
        raise AssertionError("schedule run should not sleep")

    monkeypatch.setattr(pb1_runner.time_mod, "sleep", sleep_fail)

    pb1_runner.main()
