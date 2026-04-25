"""
tests/test_workflow_prewarm_cron_contract.py

Prewarm cron 계약 검증:
- AM: 08:45 KST = 23:45 UTC 전날  →  "45 23 * * *"
- PM: 12:45 KST = 03:45 UTC       →  "45 3 * * *"
- CLOSE: 15:00 KST = 06:00 UTC    →  "0 6 * * *"
- 각 workflow에 PB1_TARGET_START_TIME / Wait until target start step 존재
- timeout-minutes: AM=280, PM=175, CLOSE=50
"""
from __future__ import annotations

import re
from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"


def _read(name: str) -> str:
    return (WORKFLOWS_DIR / name).read_text()


# ── cron ────────────────────────────────────────────────────────────────────

def test_am_cron_is_prewarm():
    content = _read("trade-am.yml")
    assert re.search(r'cron:\s*"45 23 \* \* \*"', content), (
        "trade-am.yml cron should be '45 23 * * *' (08:45 KST prewarm)"
    )


def test_pm_cron_is_prewarm():
    content = _read("trade-pm.yml")
    assert re.search(r'cron:\s*"45 3 \* \* \*"', content), (
        "trade-pm.yml cron should be '45 3 * * *' (12:45 KST prewarm)"
    )


def test_close_cron_is_prewarm():
    content = _read("trade-close.yml")
    assert re.search(r'cron:\s*"0 6 \* \* \*"', content), (
        "trade-close.yml cron should be '0 6 * * *' (15:00 KST prewarm)"
    )


# ── PB1_TARGET_START_TIME ────────────────────────────────────────────────────

def test_am_target_start_time():
    content = _read("trade-am.yml")
    assert 'PB1_TARGET_START_TIME: "09:00"' in content, (
        "trade-am.yml must set PB1_TARGET_START_TIME: \"09:00\""
    )


def test_pm_target_start_time():
    content = _read("trade-pm.yml")
    assert 'PB1_TARGET_START_TIME: "13:00"' in content, (
        "trade-pm.yml must set PB1_TARGET_START_TIME: \"13:00\""
    )


def test_close_target_start_time():
    content = _read("trade-close.yml")
    assert 'PB1_TARGET_START_TIME: "15:15"' in content, (
        "trade-close.yml must set PB1_TARGET_START_TIME: \"15:15\""
    )


# ── PB1_START_ALLOW_UNTIL ────────────────────────────────────────────────────

def test_am_start_allow_until():
    content = _read("trade-am.yml")
    assert 'PB1_START_ALLOW_UNTIL: "09:10"' in content


def test_pm_start_allow_until():
    content = _read("trade-pm.yml")
    assert 'PB1_START_ALLOW_UNTIL: "13:10"' in content


def test_close_start_allow_until():
    content = _read("trade-close.yml")
    assert 'PB1_START_ALLOW_UNTIL: "15:20"' in content


# ── PB1_PREWARM_ENABLED ───────────────────────────────────────────────────────

def test_all_workflows_have_prewarm_enabled():
    for wf in ["trade-am.yml", "trade-pm.yml", "trade-close.yml"]:
        content = _read(wf)
        assert 'PB1_PREWARM_ENABLED: "1"' in content, (
            f"{wf} must set PB1_PREWARM_ENABLED: \"1\""
        )


# ── Wait until target start step ────────────────────────────────────────────

def test_am_has_wait_until_target_start_step():
    content = _read("trade-am.yml")
    assert "Wait until target start" in content, (
        "trade-am.yml must contain 'Wait until target start' step"
    )


def test_pm_has_wait_until_target_start_step():
    content = _read("trade-pm.yml")
    assert "Wait until target start" in content, (
        "trade-pm.yml must contain 'Wait until target start' step"
    )


def test_close_has_wait_until_target_start_step():
    content = _read("trade-close.yml")
    assert "Wait until target start" in content, (
        "trade-close.yml must contain 'Wait until target start' step"
    )


# ── timeout-minutes ───────────────────────────────────────────────────────────

def test_am_timeout_280():
    content = _read("trade-am.yml")
    assert re.search(r"timeout-minutes:\s*280", content), (
        "trade-am.yml timeout-minutes should be 280"
    )


def test_pm_timeout_175():
    content = _read("trade-pm.yml")
    assert re.search(r"timeout-minutes:\s*175", content), (
        "trade-pm.yml timeout-minutes should be 175"
    )


def test_close_timeout_50():
    content = _read("trade-close.yml")
    assert re.search(r"timeout-minutes:\s*50", content), (
        "trade-close.yml timeout-minutes should be 50"
    )


# ── session loop config ───────────────────────────────────────────────────────

def test_am_run_loop_minutes():
    content = _read("trade-am.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "235"' in content


def test_pm_run_loop_minutes():
    content = _read("trade-pm.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "130"' in content


def test_close_run_loop_minutes():
    content = _read("trade-close.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "15"' in content
