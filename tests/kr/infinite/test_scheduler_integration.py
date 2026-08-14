from pathlib import Path


def test_windows_scheduler_remains_single_owner_and_times_unchanged():
    scheduler = Path("scripts/windows/update-nullim-scheduler.ps1").read_text()
    assert "WINDOWS_TASK_SCHEDULER" in scheduler
    expected = {
        "PB1 KR Prep WSL": "06:30", "PB1 KR AM WSL": "08:55",
        "PB1 KR Afternoon WSL": "13:00", "PB1 KR Close WSL": "15:15",
        "PB1 KR Mail WSL": "16:00", "PB1 KR Health WSL": "16:10",
    }
    for task, time in expected.items():
        assert f'Name="{task}"' in scheduler and f'Time="{time}"' in scheduler
    assert "KR Infinite" not in scheduler
    assert "-MultipleInstances IgnoreNew" in scheduler


def test_existing_kr_sessions_own_infinite_hook_without_new_scheduler():
    runner = Path("trader/kr/runner/trade_session_runner.py").read_text()
    for session in ('"am"', '"afternoon"', '"close"'):
        assert session in runner
    assert "_run_infinite_session_hook(session=session" in runner
    for script in ("run-kr-am.sh", "run-kr-afternoon.sh", "run-kr-close.sh"):
        text = Path("scripts/wsl", script).read_text()
        assert "trader.kr.runner.trade_session_runner" in text
        assert "trader.kr.infinite" not in text


def test_no_infinite_workflow_cron_task_or_wsl_cron():
    assert not Path(".github/workflows/kr-infinite.yml").exists()
    cron = Path("scripts/wsl/install-nullim-cron.sh").read_text()
    assert "WINDOWS_TASK_SCHEDULER_ONLY" in cron
    assert "KR Infinite" not in cron


def test_canonical_regime_and_migration_paths_are_reused():
    pb1_engine = Path("trader/pb1_engine.py").read_text()
    pb1_runner = Path("trader/pb1_runner.py").read_text()
    adapter = Path("trader/kr/infinite/regime_adapter.py").read_text()
    assert "write_snapshot(snapshot)" in pb1_engine
    assert "run_migrations(engine)" in pb1_runner
    assert "calculate_market_state" not in adapter
    assert Path("migrations/0049_add_kr_infinite_state.sql").exists()


def test_session_hook_is_fail_soft(monkeypatch):
    from trader.kr.runner import trade_session_runner
    import trader.kr.infinite.runner as infinite_runner
    monkeypatch.setattr(infinite_runner, "run_canonical_session", lambda **_: (_ for _ in ()).throw(RuntimeError("isolated")))
    trade_session_runner._run_infinite_session_hook(session="am", env="practice", checkpoint="test")
