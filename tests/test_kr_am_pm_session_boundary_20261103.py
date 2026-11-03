"""KR PB1 session handoff regression: AM through 12:55, PM from 13:00.

Time policy only. Does not relax any BUY/SELL signal or KIS broker safety gate.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def test_router_has_live_am_window_and_five_minute_gap() -> None:
    # Test a fresh interpreter: the router caches time boundaries at import.
    env = os.environ.copy()
    env.pop("PB1_MORNING_WINDOW_END", None)
    env.pop("PB1_AFTERNOON_WINDOW_START", None)
    code = """
from datetime import datetime
from zoneinfo import ZoneInfo
from trader.window_router import decide_window
def check(t, override, expected):
    now = datetime.fromisoformat('2026-11-03T' + t + ':00+09:00')
    decision = decide_window(now=now, override=override)
    got = decision.name if decision else None
    assert got == expected, (t, override, got, expected)
for t in ('09:00', '10:30', '12:54'):
    check(t, 'morning', 'morning')
    check(t, 'auto', 'morning')
check('12:55', 'morning', None)
check('12:55', 'auto', None)
check('12:59', 'auto', None)
check('12:59', 'day', None)
check('13:00', 'morning', None)
check('13:00', 'day', 'day')
check('13:00', 'auto', 'day')
check('15:09', 'day', 'day')
check('15:15', 'day', None)
check('15:15', 'close', 'close')
"""
    completed = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=env,
        capture_output=True, text=True, timeout=20, check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_live_wsl_am_isolation_and_pm_handoff_contract() -> None:
    am = (REPO / "scripts/wsl/run-kr-am.sh").read_text(encoding="utf-8")
    pm = (REPO / "scripts/wsl/run-kr-afternoon.sh").read_text(encoding="utf-8")
    runner = (REPO / "trader/pb1_runner.py").read_text(encoding="utf-8")
    assert 'PB1_AM_SESSION_END="12:55"' in am
    assert 'PB1_MORNING_WINDOW_END="$PB1_AM_SESSION_END"' in am
    assert 'MORNING_WINDOW_END="$PB1_AM_SESSION_END"' in am
    assert 'PB1_AFTERNOON_WINDOW_START="13:00"' in am
    assert 'AFTERNOON_WINDOW_START="13:00"' in am
    # Expected start/recovery cutoff is not the already running loop deadline.
    assert "export PB1_AM_EXPECTED_END=" not in am
    assert 'active_end = session_end_dt.time()' in runner
    assert 'PM_TARGET_START_TIME="${PM_TARGET_START_TIME:-13:00}"' in pm
    assert 'PM_SESSION_END="${PM_SESSION_END:-15:10}"' in pm
    assert 'PB1_PM_SESSION_END="${PB1_PM_SESSION_END:-$PM_SESSION_END}"' in pm
    assert 'PB1_AFTERNOON_WINDOW_START="13:00"' in pm



def test_legacy_1030_env_is_corrected_by_am_launcher_without_running_trading() -> None:
    """Exercise ONLY the WSL export statements in a bash subprocess, never the runner."""
    am = (REPO / "scripts/wsl/run-kr-am.sh").read_text(encoding="utf-8")
    export_lines = [
        line for line in am.splitlines()
        if line.startswith("export PB1_AM_SESSION_END=")
        or line.startswith("export PB1_MORNING_WINDOW_END=")
        or line.startswith("export MORNING_WINDOW_END=")
        or line.startswith("export PB1_AFTERNOON_WINDOW_START=")
        or line.startswith("export AFTERNOON_WINDOW_START=")
    ]
    assert len(export_lines) == 5
    assert am.index("source .env;") < am.index('export PB1_MORNING_WINDOW_END="$PB1_AM_SESSION_END"')
    env = os.environ.copy()
    env.update({
        "PB1_AM_SESSION_END": "10:30",
        "PB1_MORNING_WINDOW_END": "10:30",
        "MORNING_WINDOW_END": "10:30",
        "PB1_AFTERNOON_WINDOW_START": "10:30",
        "AFTERNOON_WINDOW_START": "10:30",
    })
    code = "\n".join(export_lines) + "\nprintf '%s|%s|%s|%s' " \
        '"$PB1_MORNING_WINDOW_END" "$MORNING_WINDOW_END" ' \
        '"$PB1_AFTERNOON_WINDOW_START" "$AFTERNOON_WINDOW_START"'
    completed = subprocess.run(
        ["bash", "-c", code], cwd=REPO, env=env,
        capture_output=True, text=True, timeout=10, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "12:55|12:55|13:00|13:00"


def test_workflow_boundary_contract() -> None:
    am = (REPO / ".github/workflows/trade-am.yml").read_text(encoding="utf-8")
    pm = (REPO / ".github/workflows/trade-pm.yml").read_text(encoding="utf-8")
    assert 'PB1_AM_SESSION_END: "12:55"' in am
    assert 'PB1_MORNING_WINDOW_END: "12:55"' in am
    assert 'MORNING_WINDOW_END: "12:55"' in am
    assert 'PB1_AFTERNOON_WINDOW_START: "13:00"' in am
    assert 'PB1_PM_SESSION_END: "15:10"' in pm
    assert 'PB1_AFTERNOON_WINDOW_START: "13:00"' in pm


def test_config_default_times_match_kr_handoff() -> None:
    c = (REPO / "trader/config.py").read_text(encoding="utf-8")
    for expected in (
        '"PB1_MORNING_WINDOW_END": "12:55"',
        '"MORNING_WINDOW_END": "12:55"',
        '"AFTERNOON_WINDOW_START": "13:00"',
        'PB1_MORNING_WINDOW_END = _cfg("PB1_MORNING_WINDOW_END") or "12:55"',
        'AFTERNOON_WINDOW_START = _cfg("AFTERNOON_WINDOW_START") or "13:00"',
    ):
        assert expected in c
