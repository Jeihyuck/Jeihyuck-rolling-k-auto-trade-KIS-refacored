from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time
import signal


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


def test_held_unverified_lock_is_not_unlinked_or_bypassed(tmp_path: Path):
    lock, log = tmp_path / "kr-prep.lock", tmp_path / "lock.log"
    holder = subprocess.Popen(
        ["bash", "-c", f'exec 8<>"{lock}"; flock -x 8; printf "{{broken" >&8; sleep 2']
    )
    try:
        time.sleep(0.15)
        inode = lock.stat().st_ino
        contender = run_lock(lock, log)
        assert contender.wait(timeout=5) == 76
        assert lock.stat().st_ino == inode
        assert lock.read_text() == "{broken"
        assert "[LOCK][HELD][OWNER_UNVERIFIED][WARN]" in log.read_text()
        assert "[LOCK][ACQUIRED]" not in log.read_text()
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_signal_exit_codes_release_owned_lock(tmp_path: Path):
    source = (ROOT / "scripts/wsl/run-us-trader.sh").read_text()
    start = source.index('SESSION_NAME="trader"')
    end = source.index('session="${1:-auto}"', start)
    wrapper_lock_setup = source[start:end]
    for sig, expected in ((signal.SIGINT, 130), (signal.SIGTERM, 143)):
        lock, log = tmp_path / f"signal-{sig}.lock", tmp_path / f"signal-{sig}.log"
        command = wrapper_lock_setup.replace(
            'LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"', f'LOCK_FILE="{lock}"'
        ).replace(
            'LOG_FILE="runtime/wsl-us-${SESSION_NAME}.log"', f'LOG_FILE="{log}"'
        ) + "while :; do read -r -t 0.1 _ || true; done\n"
        proc = subprocess.Popen(
            ["bash", "-c", command, "run-us-trader.sh"], cwd=ROOT,
            env=os.environ | {"NULLIM_TRADE_DATE": "2026-08-13"},
            start_new_session=True,
        )
        deadline = time.time() + 5
        while time.time() < deadline and (not log.exists() or "[LOCK][ACQUIRED]" not in log.read_text()):
            time.sleep(0.02)
        assert "[LOCK][ACQUIRED]" in log.read_text()
        os.killpg(proc.pid, sig)
        assert proc.wait(timeout=5) == expected
        assert not lock.exists()


def test_us_trader_error_exit_releases_lock(tmp_path: Path):
    source = (ROOT / "scripts/wsl/run-us-trader.sh").read_text()
    fragment = source[source.index('SESSION_NAME="trader"'):source.index('session="${1:-auto}"')]
    lock, log = tmp_path / "trader.lock", tmp_path / "trader.log"
    fragment = fragment.replace(
        'LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"', f'LOCK_FILE="{lock}"'
    ).replace('LOG_FILE="runtime/wsl-us-${SESSION_NAME}.log"', f'LOG_FILE="{log}"')
    completed = subprocess.run(
        ["bash", "-c", fragment + "exit 2\n", "run-us-trader.sh"], cwd=ROOT,
        env=os.environ | {"NULLIM_TRADE_DATE": "2026-08-13"},
    )
    assert completed.returncode == 2
    assert not lock.exists()


def test_us_lock_only_exits_before_python_trading_runner(tmp_path: Path):
    source = (ROOT / "scripts/wsl/run-us-am.sh").read_text()
    start = source.index('SESSION_NAME="am"')
    end = source.index('export STRATEGY_ENV=', start)
    fragment = source[start:end]
    fake_python = tmp_path / "python"
    calls = tmp_path / "python.calls"
    fake_python.write_text(f'#!/bin/sh\necho "$*" >> "{calls}"\necho "{{}}"\n')
    fake_python.chmod(0o755)
    log = tmp_path / "am.log"
    env = os.environ | {
        "PYTHON_BIN": str(fake_python), "US_LOCK_ONLY": "1",
        "NULLIM_TRADE_DATE": "2026-08-13", "NULLIM_SESSION_LOG": str(log),
    }
    completed = subprocess.run(["bash", "-c", fragment], cwd=ROOT, env=env, text=True)
    assert completed.returncode == 0
    assert "LOCK_ONLY_DONE" in log.read_text()
    assert "trade_session_runner" not in calls.read_text()


def test_deploy_preflight_only_defers_sync_rc_75():
    for rc, expected in ((75, 0), (1, 1), (2, 2), (128, 128)):
        day = f"2099-12-{rc % 28 + 1:02d}"
        pin = ROOT / "runtime/code-pins" / f"US-{day}.sha"
        pin.unlink(missing_ok=True)
        command = f'''
source scripts/wsl/deploy-preflight.sh
_deploy_sync_market_code() {{ return {rc}; }}
export NULLIM_RESOLVED_REPO_ROOT="{ROOT}"
export NULLIM_WRAPPER=scripts/wsl/run-us-am.sh WSL_RUN_MARKET=US WSL_RUN_SESSION=am
export NULLIM_TRADE_DATE={day} ALLOW_STALE_CODE=1
deploy_preflight
'''
        completed = subprocess.run(["bash", "-c", command], cwd=ROOT, text=True, capture_output=True)
        try:
            assert completed.returncode == expected, completed.stdout + completed.stderr
            if rc == 75:
                assert "code_sync_deferred" in completed.stdout
                assert pin.exists()
            else:
                assert f"[DEPLOY][SYNC][FAIL] rc={rc}" in completed.stdout
                assert not pin.exists()
        finally:
            pin.unlink(missing_ok=True)


def test_signal_failure_contract_is_not_health_ok():
    init = (ROOT / "scripts/wsl/init-session-log.sh").read_text()
    assert "if [[ \"$rc\" == 0 ]]; then status=OK" in init
    for wrapper in ("run-us-am.sh", "run-us-trader.sh"):
        text = (ROOT / "scripts/wsl" / wrapper).read_text()
        assert "trap 'exit 130' INT" in text
        assert "trap 'exit 143' TERM" in text


def test_trap_cleanup_hook_and_health_warning_policy_are_present():
    init = (ROOT / "scripts/wsl/init-session-log.sh").read_text()
    health = (ROOT / "scripts/wsl/check-nullim-day-health.sh").read_text()
    assert "nullim_session_lock_release" in init
    assert "if forbidden or advisory_unavailable:" in health
    assert "stale_lock_detected_count" in health
    assert "kr_inf_stale_pending_count" in health
    assert "policy_missing_exit_starvation_count" in health
    assert "policy_authority_conflict_count" in health
    assert "execution_truth_mismatch_count" in health
