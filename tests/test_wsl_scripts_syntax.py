from pathlib import Path
import subprocess


def test_wsl_scripts_parse_and_common_venv_wrapper_exists():
    scripts = [Path("run_pb1_kr.sh"), *Path("scripts/wsl").glob("*.sh")]
    assert Path("scripts/wsl/with-venv.sh").is_file()
    for script in scripts:
        assert subprocess.run(["bash", "-n", str(script)], check=False).returncode == 0, script
