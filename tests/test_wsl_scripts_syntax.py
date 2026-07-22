from pathlib import Path
import subprocess


def test_wsl_scripts_parse_and_common_venv_wrapper_exists():
    scripts = [Path("run_pb1_kr.sh"), *Path("scripts/wsl").glob("*.sh")]
    assert Path("scripts/wsl/with-venv.sh").is_file()
    for script in scripts:
        assert subprocess.run(["bash", "-n", str(script)], check=False).returncode == 0, script


def test_cron_installer_is_cleanup_only():
    text = Path("scripts/wsl/install-nullim-cron.sh").read_text()
    assert "[CRON_INSTALL][BLOCK] reason=WINDOWS_TASK_SCHEDULER_ONLY" in text
    assert "cat <<EOF" not in text
    assert "CRON_TZ=" not in text

def test_kr_close_uses_the_same_advisory_lock_policy_as_intraday_sessions():
    text = Path("scripts/wsl/run-kr-close.sh").read_text()
    for name, value in (
        ("DB_LOCK_CONN_LOCK_TIMEOUT_MS", "5000"),
        ("DB_LOCK_CONN_STATEMENT_TIMEOUT_MS", "15000"),
        ("DB_LOCK_CONN_IDLE_IN_TX_SESSION_TIMEOUT_MS", "30000"),
        ("DB_XACT_LOCK_IDLE_IN_TX_SESSION_TIMEOUT_MS", "0"),
        ("KR_LOCK_STALE_XACT_SEC", "300"),
        ("KR_LOCK_TERMINATE_STALE_HOLDER", "0"),
        ("PB1_LOCK_LOG_OWNER_ON_FAIL", "1"),
        ("LOCK_ACQUIRE_RETRIES", "3"),
        ("LOCK_ACQUIRE_SLEEP_SEC", "0.5"),
    ):
        assert f'export {name}="${{{name}:-{value}}}"' in text


def test_kr_workflows_keep_xact_lock_owner_idle_timeout_disabled():
    for workflow in (Path(".github/workflows/trade-am.yml"), Path(".github/workflows/trade-afternoon.yml")):
        assert 'DB_XACT_LOCK_IDLE_IN_TX_SESSION_TIMEOUT_MS: "0"' in workflow.read_text()
