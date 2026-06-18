# -*- coding: utf-8 -*-
"""Tests: manual workflow yaml에 force_now/max_ticks input 및 guard 존재 검증."""
from __future__ import annotations

import os
import re

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
WF_DIR = os.path.join(REPO_ROOT, ".github", "workflows")


def _read_wf(name: str) -> str:
    path = os.path.join(WF_DIR, name)
    with open(path, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Test 1: us-trade-am.yml inputs
# ---------------------------------------------------------------------------

def test_us_trade_am_has_force_now_input():
    content = _read_wf("us-trade-am.yml")
    assert "force_now:" in content, "us-trade-am.yml에 force_now input이 없음"


def test_us_trade_am_has_max_ticks_input():
    content = _read_wf("us-trade-am.yml")
    assert "max_ticks:" in content, "us-trade-am.yml에 max_ticks input이 없음"


def test_us_trade_am_has_manual_max_minutes_input():
    content = _read_wf("us-trade-am.yml")
    assert "manual_max_minutes:" in content, "us-trade-am.yml에 manual_max_minutes input이 없음"


def test_us_trade_am_has_manual_interval_sec_input():
    content = _read_wf("us-trade-am.yml")
    assert "manual_interval_sec:" in content, "us-trade-am.yml에 manual_interval_sec input이 없음"


# ---------------------------------------------------------------------------
# Test 2: us-trade-afternoon.yml inputs
# ---------------------------------------------------------------------------

def test_us_trade_afternoon_has_force_now_input():
    content = _read_wf("us-trade-afternoon.yml")
    assert "force_now:" in content


def test_us_trade_afternoon_has_max_ticks_input():
    content = _read_wf("us-trade-afternoon.yml")
    assert "max_ticks:" in content


def test_us_trade_afternoon_has_manual_max_minutes_input():
    content = _read_wf("us-trade-afternoon.yml")
    assert "manual_max_minutes:" in content


def test_us_trade_afternoon_has_manual_interval_sec_input():
    content = _read_wf("us-trade-afternoon.yml")
    assert "manual_interval_sec:" in content


# ---------------------------------------------------------------------------
# Test 3: us-trade-prep.yml inputs
# ---------------------------------------------------------------------------

def test_us_trade_prep_has_force_now_input():
    content = _read_wf("us-trade-prep.yml")
    assert "force_now:" in content


def test_us_trade_prep_has_manual_skip_phase_guard_input():
    content = _read_wf("us-trade-prep.yml")
    assert "manual_skip_phase_guard:" in content


# ---------------------------------------------------------------------------
# Test 4: us-trade-close.yml inputs
# ---------------------------------------------------------------------------

def test_us_trade_close_has_force_now_input():
    content = _read_wf("us-trade-close.yml")
    assert "force_now:" in content


def test_us_trade_close_has_manual_skip_phase_guard_input():
    content = _read_wf("us-trade-close.yml")
    assert "manual_skip_phase_guard:" in content


# ---------------------------------------------------------------------------
# Test 5: 모든 workflow에 force_now safety guard 존재
# ---------------------------------------------------------------------------

def test_us_trade_am_has_force_now_guard():
    content = _read_wf("us-trade-am.yml")
    assert "US_FORCE_NOW_GUARD" in content
    assert "force_now manual smoke requires dry_run=true or offline=true" in content


def test_us_trade_afternoon_has_force_now_guard():
    content = _read_wf("us-trade-afternoon.yml")
    assert "US_FORCE_NOW_GUARD" in content
    assert "force_now manual smoke requires dry_run=true or offline=true" in content


def test_us_trade_prep_has_force_now_guard():
    content = _read_wf("us-trade-prep.yml")
    assert "US_FORCE_NOW_GUARD" in content
    assert "force_now manual smoke requires dry_run=true or offline=true" in content


def test_us_trade_close_has_force_now_guard():
    content = _read_wf("us-trade-close.yml")
    assert "US_FORCE_NOW_GUARD" in content
    assert "force_now manual smoke requires dry_run=true or offline=true" in content


def test_us_agent_has_force_now_guard():
    content = _read_wf("us-agent.yml")
    assert "US_FORCE_NOW_GUARD" in content
    assert "force_now manual smoke requires dry_run=true or offline=true" in content


# ---------------------------------------------------------------------------
# Test 6: schedule path에 force_now 사용하지 않음
# ---------------------------------------------------------------------------

def _schedule_section(content: str) -> str:
    """schedule job의 실행 step 중 force_now가 하드코딩되지 않았는지 확인.
    
    workflow_dispatch 섹션 바깥에서 --force-now가 나타나지 않아야 한다.
    단, inputs.force_now를 조건부로 참조하는 것은 허용.
    """
    return content


def test_schedule_does_not_hardcode_force_now_am():
    """WSL trigger 전환 후 AM workflow에는 GitHub schedule이 없다."""
    content = _read_wf("us-trade-am.yml")
    assert "schedule:" not in content
    assert "cron:" not in content
    assert "force_now manual smoke" in content or "US_FORCE_NOW_GUARD" in content


def test_schedule_does_not_hardcode_force_now_prep():
    content = _read_wf("us-trade-prep.yml")
    assert "schedule:" not in content
    assert "cron:" not in content
    assert "US_FORCE_NOW_GUARD" in content


def test_schedule_does_not_hardcode_force_now_close():
    content = _read_wf("us-trade-close.yml")
    assert "schedule:" not in content
    assert "cron:" not in content
    assert "US_FORCE_NOW_GUARD" in content
