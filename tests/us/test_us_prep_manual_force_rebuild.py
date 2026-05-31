# -*- coding: utf-8 -*-
"""tests/us/test_us_prep_manual_force_rebuild.py

us-trade-prep manual force_rebuild_prep 기능 테스트.
"""
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "us-trade-prep.yml"


# ── 1. workflow YAML에 force_rebuild_prep input이 있어야 한다 ──────────────

def test_us_prep_has_force_rebuild_input():
    """us-trade-prep.yml에 force_rebuild_prep input과 US_FORCE_REBUILD_PREP env가 있어야 한다."""
    content = WORKFLOW.read_text(encoding="utf-8")
    assert "force_rebuild_prep" in content, "Missing force_rebuild_prep input"
    assert "US_FORCE_REBUILD_PREP" in content, "Missing US_FORCE_REBUILD_PREP env"
    assert (
        "ignore existing US prep" in content or "rebuild dynamic universe" in content
    ), "Missing description about rebuild behavior"


def test_us_prep_workflow_has_force_rebuild_step():
    """Resolve US prep force rebuild flag 스텝이 있어야 한다."""
    content = WORKFLOW.read_text(encoding="utf-8")
    assert "Resolve US prep force rebuild flag" in content
    assert "GITHUB_ENV" in content


def test_us_prep_workflow_schedule_no_force_rebuild():
    """schedule 트리거에서는 US_FORCE_REBUILD_PREP가 1로 설정되지 않아야 한다.

    GITHUB_ENV에 US_FORCE_REBUILD_PREP=1을 쓰는 조건에 workflow_dispatch가 반드시 있어야 한다.
    """
    content = WORKFLOW.read_text(encoding="utf-8")
    # workflow_dispatch 조건 없이 US_FORCE_REBUILD_PREP=1을 쓰는 라인이 있으면 안 된다
    for line in content.splitlines():
        stripped = line.strip()
        if "US_FORCE_REBUILD_PREP=1" in stripped and "GITHUB_ENV" in stripped:
            # 이 줄이 workflow_dispatch 조건 블록 안에 있어야 함
            # 단순 grep으로는 전체 조건 블록 확인이 어려우므로
            # 해당 라인 근처에 workflow_dispatch 문자열이 있는지 확인한다
            idx = content.find(stripped)
            surrounding = content[max(0, idx - 500): idx + 200]
            assert "workflow_dispatch" in surrounding, (
                f"US_FORCE_REBUILD_PREP=1 is set without workflow_dispatch guard:\n{surrounding}"
            )


def test_us_prep_workflow_confirm_required():
    """force_rebuild_prep=true라도 confirm=RUN_US_PREP_MANUAL 없이는 US_FORCE_REBUILD_PREP=1이 되지 않아야 한다."""
    content = WORKFLOW.read_text(encoding="utf-8")
    # Resolve step에 RUN_US_PREP_MANUAL 확인 조건이 있어야 한다
    assert "RUN_US_PREP_MANUAL" in content


def test_us_prep_workflow_has_bypass_log():
    """CHECK_ALREADY_PREPARED][BYPASS 로그가 workflow에 있어야 한다."""
    content = WORKFLOW.read_text(encoding="utf-8")
    assert "CHECK_ALREADY_PREPARED][BYPASS" in content


# ── 2. schedule에서는 force rebuild가 절대 켜지지 않아야 한다 ──────────────

def test_force_rebuild_bypasses_already_prepared():
    """workflow_dispatch + confirm_ok + force_rebuild_prep 일 때 bypass."""
    from trader.us.runner.prep_runner import should_bypass_already_prepared_guard

    assert should_bypass_already_prepared_guard(
        force_rebuild_prep=True,
        event_name="workflow_dispatch",
        confirm_ok=True,
    ) is True


def test_schedule_never_force_rebuilds():
    """schedule 이벤트에서는 force_rebuild_prep=True라도 bypass하지 않는다."""
    from trader.us.runner.prep_runner import should_bypass_already_prepared_guard

    assert should_bypass_already_prepared_guard(
        force_rebuild_prep=True,
        event_name="schedule",
        confirm_ok=True,
    ) is False


def test_confirm_required_for_force_rebuild():
    """confirm_ok=False이면 force_rebuild_prep=True라도 bypass하지 않는다."""
    from trader.us.runner.prep_runner import should_bypass_already_prepared_guard

    assert should_bypass_already_prepared_guard(
        force_rebuild_prep=True,
        event_name="workflow_dispatch",
        confirm_ok=False,
    ) is False


def test_force_rebuild_false_no_bypass():
    """force_rebuild_prep=False이면 어떤 경우에도 bypass하지 않는다."""
    from trader.us.runner.prep_runner import should_bypass_already_prepared_guard

    assert should_bypass_already_prepared_guard(
        force_rebuild_prep=False,
        event_name="workflow_dispatch",
        confirm_ok=True,
    ) is False


# ── 3. _env_true helper 테스트 ───────────────────────────────────────────────

def test_env_true_helper_truthy(monkeypatch):
    """_env_true가 1/true/yes/on을 True로 파싱한다."""
    from trader.us.runner.prep_runner import _env_true

    for val in ("1", "true", "TRUE", "yes", "YES", "on", "ON"):
        monkeypatch.setenv("US_FORCE_REBUILD_PREP", val)
        assert _env_true("US_FORCE_REBUILD_PREP") is True, f"Expected True for value={val!r}"


def test_env_true_helper_falsy(monkeypatch):
    """_env_true가 0/false/no/off를 False로 파싱한다."""
    from trader.us.runner.prep_runner import _env_true

    for val in ("0", "false", "FALSE", "no", "NO", "off", "OFF", ""):
        monkeypatch.setenv("US_FORCE_REBUILD_PREP", val)
        assert _env_true("US_FORCE_REBUILD_PREP") is False, f"Expected False for value={val!r}"
