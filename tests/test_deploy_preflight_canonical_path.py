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
    assert "git reset --hard origin/dual-agent" in text
    assert "git clean" not in text
    assert "ACTIVE_TRADING_PROCESS" in text
    assert "SESSION_ALREADY_PINNED" in text
