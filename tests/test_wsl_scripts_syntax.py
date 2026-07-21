from pathlib import Path
import subprocess


def test_wsl_scripts_parse_and_common_venv_wrapper_exists():
    scripts = [Path("run_pb1_kr.sh"), *Path("scripts/wsl").glob("*.sh")]
    assert Path("scripts/wsl/with-venv.sh").is_file()
    for script in scripts:
        assert subprocess.run(["bash", "-n", str(script)], check=False).returncode == 0, script


def test_cron_installer_creates_kr_log_directory_and_keeps_us_in_new_york():
    text = Path("scripts/wsl/install-nullim-cron.sh").read_text()
    assert 'mkdir -p "$APP/runtime/cron_logs"' in text
    kr_start = text.index("CRON_TZ=Asia/Seoul")
    us_start = text.index("CRON_TZ=America/New_York")
    assert kr_start < us_start
    us_block = text[us_start:]
    for session in ("run-us-prep.sh", "run-us-am.sh", "run-us-afternoon.sh", "run-us-close.sh"):
        assert session in us_block


def test_kr_close_uses_the_same_advisory_lock_policy_as_intraday_sessions():
    text = Path("scripts/wsl/run-kr-close.sh").read_text()
    for name, value in (
        ("DB_LOCK_CONN_LOCK_TIMEOUT_MS", "5000"),
        ("DB_LOCK_CONN_STATEMENT_TIMEOUT_MS", "15000"),
        ("DB_LOCK_CONN_IDLE_IN_TX_SESSION_TIMEOUT_MS", "30000"),
        ("KR_LOCK_STALE_XACT_SEC", "300"),
        ("KR_LOCK_TERMINATE_STALE_HOLDER", "0"),
        ("PB1_LOCK_LOG_OWNER_ON_FAIL", "1"),
        ("LOCK_ACQUIRE_RETRIES", "3"),
        ("LOCK_ACQUIRE_SLEEP_SEC", "0.5"),
    ):
        assert f'export {name}="${{{name}:-{value}}}"' in text
