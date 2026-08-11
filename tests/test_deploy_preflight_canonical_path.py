from pathlib import Path


def test_preflight_declares_canonical_path_and_nonblocking_dirty_policy():
    text = Path("scripts/wsl/deploy-preflight.sh").read_text()
    for marker in ("PATH_MISMATCH", "STALE_CODE", "DIRTY_CODE", "DIRTY_GENERATED", "ALLOW_STALE_CODE", "action=NON_BLOCKING", "CODE_VERSION"):
        assert marker in text
    assert "reason=dirty_code" not in text
    assert "ALLOW_DIRTY_CODE" not in text


def test_controlled_sync_never_cleans_runtime_and_honors_active_locks():
    text = Path("scripts/wsl/sync-market-code.sh").read_text()
    assert "git fetch origin dual-agent" in text
    assert "git checkout -f -B dual-agent origin/dual-agent" in text
    assert "git reset --hard origin/dual-agent" in text
    assert "git clean" not in text
    assert "ACTIVE_TRADING_PROCESS" in text
    assert "SESSION_ALREADY_PINNED" in text


def test_existing_scheduler_chain_automatically_enters_sync():
    preflight = Path("scripts/wsl/deploy-preflight.sh").read_text()
    assert 'bash scripts/wsl/sync-market-code.sh "${market^^}" "$trade_date"' in preflight
    assert "source=existing_scheduler_chain" in preflight
    assert "[DEPLOY][SYNC][REEXEC]" in preflight
    for market in ("kr", "us"):
        for session in ("prep", "am", "afternoon", "close"):
            wrapper = Path(f"scripts/wsl/run-{market}-{session}.sh").read_text()
            assert "deploy_preflight" in wrapper


def test_scheduler_registration_does_not_reference_sync_as_a_job():
    scheduler_files = list(Path("scripts/windows").glob("*.ps1"))
    scheduler_files += list(Path("scripts/wsl").glob("*cron*.sh"))
    assert scheduler_files
    assert all("sync-market-code.sh" not in path.read_text() for path in scheduler_files)
