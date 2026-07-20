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
