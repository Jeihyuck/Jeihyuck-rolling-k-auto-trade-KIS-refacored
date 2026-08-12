from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).parents[1]
HELPER = ROOT / "scripts/wsl/session-lock.sh"


def run_lock(lock: Path, log: Path, session: str = "prep", hold: float = 0) -> subprocess.Popen:
    command = (
        f'source "{HELPER}"; '
        f'nullim_session_lock_acquire "{lock}" KR "{session}" 2026-08-13 "{log}" || exit $?; '
        f'sleep {hold}; nullim_session_lock_release'
    )
    # Set bash -c's $0 to a production-shaped runner name so active-owner
    # command validation exercises the same contract as scheduled wrappers.
    return subprocess.Popen(["bash", "-c", command, "run-kr-prep.sh"])


def test_empty_mail_lock_is_not_a_trading_preflight_blocker(tmp_path: Path):
    mail = tmp_path / "kr-mail-snapshot.lock"
    mail.touch()
    text = (ROOT / "scripts/wsl/sync-market-code.sh").read_text()
    assert "*-mail-snapshot.lock" in text
    assert "[LOCK][IGNORED][OUT_OF_SCOPE]" in text


def test_dead_pid_malformed_and_previous_day_locks_are_recovered(tmp_path: Path):
    cases = [
        "{broken",
        json.dumps({"pid": 99999999, "market": "KR", "session": "prep", "trade_date": "2026-08-13", "created_at": "2026-08-13T00:00:00+00:00"}),
        json.dumps({"pid": os.getpid(), "market": "KR", "session": "prep", "trade_date": "2026-08-12", "created_at": "2026-08-13T00:00:00+00:00"}),
    ]
    for index, content in enumerate(cases):
        lock, log = tmp_path / f"case-{index}.lock", tmp_path / f"case-{index}.log"
        lock.write_text(content)
        assert run_lock(lock, log).wait(timeout=5) == 0
        output = log.read_text()
        assert "[LOCK][STALE][DETECTED]" in output
        assert "[LOCK][STALE][REMOVED]" in output
        assert not lock.exists()


def test_only_one_concurrent_same_session_owner(tmp_path: Path):
    lock, log = tmp_path / "kr-prep.lock", tmp_path / "lock.log"
    first = run_lock(lock, log, hold=0.7)
    time.sleep(0.15)
    second = run_lock(lock, log)
    assert second.wait(timeout=5) == 75
    assert first.wait(timeout=5) == 0
    output = log.read_text()
    assert output.count("[LOCK][ACQUIRED]") == 1
    assert "[LOCK][ACTIVE][SAME_SESSION][SKIP]" in output
    assert "[SESSION][DUPLICATE][SKIP]" in output


def test_trap_cleanup_hook_and_health_warning_policy_are_present():
    init = (ROOT / "scripts/wsl/init-session-log.sh").read_text()
    health = (ROOT / "scripts/wsl/check-nullim-day-health.sh").read_text()
    assert "nullim_session_lock_release" in init
    assert "if forbidden or advisory_unavailable:" in health
    assert "stale_lock_detected_count" in health
