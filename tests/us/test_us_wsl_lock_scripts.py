from __future__ import annotations

from pathlib import Path
import subprocess
import time

import pytest


@pytest.mark.parametrize(
    "script,session",
    [
        ("run-us-prep.sh", "prep"),
        ("run-us-am.sh", "am"),
        ("run-us-afternoon.sh", "afternoon"),
        ("run-us-close.sh", "close"),
        ("run-us-trader.sh", "trader"),
    ],
)
def test_us_wsl_scripts_have_flock(script: str, session: str):
    text = Path("scripts/wsl", script).read_text(encoding="utf-8")
    assert "flock -n 9" in text
    assert "[US_WSL_LOCK][SKIP_DUPLICATE]" in text
    assert f'us-${{SESSION_NAME}}.lock' in text
    assert f'SESSION_NAME="{session}"' in text



def test_wsl_flock_allows_only_one_process(tmp_path):
    script = tmp_path / "lock_test.sh"
    log = tmp_path / "lock.log"
    lock = tmp_path / "test.lock"
    script.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"exec 9>\"{lock}\"\n"
        "if ! flock -n 9; then\n"
        f"echo \"SKIP_DUPLICATE\" >> \"{log}\"\n"
        "exit 0\n"
        "fi\n"
        f"echo \"ACQUIRED\" >> \"{log}\"\n"
        "sleep 1\n"
        f"echo \"DONE\" >> \"{log}\"\n",
        encoding="utf-8",
    )
    script.chmod(0o755)

    p1 = subprocess.Popen([str(script)])
    time.sleep(0.1)
    p2 = subprocess.Popen([str(script)])
    p1.wait(timeout=5)
    p2.wait(timeout=5)

    text = log.read_text(encoding="utf-8")
    assert text.count("ACQUIRED") == 1
    assert text.count("SKIP_DUPLICATE") == 1
    assert text.count("DONE") == 1
