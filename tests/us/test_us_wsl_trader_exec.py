from __future__ import annotations

from pathlib import Path


def test_run_us_trader_does_not_exec_child_scripts():
    text = Path("scripts/wsl/run-us-trader.sh").read_text(encoding="utf-8")
    assert 'exec "${repo_dir}/scripts/wsl/run-us-' not in text
    for script in ("run-us-prep.sh", "run-us-am.sh", "run-us-afternoon.sh", "run-us-close.sh"):
        assert f'"${{repo_dir}}/scripts/wsl/{script}"' in text
    assert "exit $?" in text
