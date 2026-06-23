from __future__ import annotations

from pathlib import Path

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
