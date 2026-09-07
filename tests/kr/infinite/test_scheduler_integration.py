from pathlib import Path


def test_windows_scheduler_times_remain_unchanged_without_new_task():
    scheduler = Path("scripts/windows/update-nullim-scheduler.ps1").read_text()
    assert "WINDOWS_TASK_SCHEDULER" in scheduler
    expected = {
        "PB1 KR Prep WSL": "06:30", "PB1 KR AM WSL": "08:55",
        "PB1 KR Afternoon WSL": "13:00", "PB1 KR Close WSL": "15:15",
        "PB1 KR Mail WSL": "16:00", "PB1 KR Health WSL": "16:10",
    }
    for task, at in expected.items():
        assert f'Name="{task}"' in scheduler and f'Time="{at}"' in scheduler
    assert "KR Infinite" not in scheduler
    assert "-MultipleInstances IgnoreNew" in scheduler


def test_kr_infinite_is_a_sibling_runtime_not_owned_by_pb1():
    pb1_runner = Path("trader/pb1_runner.py").read_text()
    independent = Path("trader/kr/infinite/session_runner.py").read_text()
    sidecar = Path("scripts/wsl/kr-infinite-sidecar.sh").read_text()

    assert "run_kr_infinite_sleeve_tick(" not in pb1_runner
    assert "trader.pb1_runner" not in independent
    assert "run_canonical_session" in independent
    assert "KR_INFINITE_LOOP_INTERVAL_SEC" in independent
    assert "kr-infinite-runtime.lock" in independent
    assert "trader.kr.infinite.session_runner" in sidecar

    for script in ("run-kr-am.sh", "run-kr-afternoon.sh", "run-kr-close.sh"):
        text = Path("scripts/wsl", script).read_text()
        assert "trader.kr.runner.trade_session_runner" in text
        assert 'source "$SCRIPT_DIR/kr-infinite-sidecar.sh"' in text
        assert 'nullim_start_kr_infinite_sidecar "$WSL_RUN_SESSION" "$STRATEGY_ENV"' in text
        assert "nullim_wait_kr_infinite_sidecar" in text


def test_no_infinite_workflow_cron_task_or_wsl_cron():
    assert not Path(".github/workflows/kr-infinite.yml").exists()
    cron = Path("scripts/wsl/install-nullim-cron.sh").read_text()
    assert "WINDOWS_TASK_SCHEDULER_ONLY" in cron
    assert "KR Infinite" not in cron


def test_infinite_owns_schema_bootstrap_and_reuses_canonical_regime_contract():
    import inspect

    from trader.kr.infinite.regime_adapter import load_canonical_snapshot
    from trader.kr.regime import write_snapshot

    independent = Path("trader/kr/infinite/session_runner.py").read_text()
    adapter = Path("trader/kr/infinite/regime_adapter.py").read_text()

    assert "_ensure_infinite_schema" in independent
    assert "run_migrations" in independent
    assert "calculate_market_state" not in adapter
    assert Path("migrations/0049_add_kr_infinite_state.sql").exists()
    assert inspect.signature(write_snapshot).parameters["path"].default == "artifacts/kr_regime_snapshot.json"
    assert inspect.signature(load_canonical_snapshot).parameters["path"].default is None
