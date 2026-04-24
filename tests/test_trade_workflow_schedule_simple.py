"""
테스트 1: workflow schedule 개수 검증
- trade-am.yml schedule은 cron "0 0 * * 1-5" 1개만 허용
- trade-pm.yml schedule은 cron "0 4 * * 1-5" 1개만 허용
- trade-close.yml schedule은 cron "15 6 * * 1-5" 1개만 허용
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"


def _load_workflow(name: str) -> str:
    path = WORKFLOWS_DIR / name
    assert path.exists(), f"workflow file not found: {path}"
    return path.read_text(encoding="utf-8")


def _extract_cron_entries(text: str) -> list[str]:
    """YAML 내 - cron: "..." 항목을 모두 추출한다."""
    return re.findall(r'- cron:\s*"([^"]+)"', text)


class TestAmWorkflowSchedule:
    def test_only_one_schedule(self):
        text = _load_workflow("trade-am.yml")
        crons = _extract_cron_entries(text)
        assert len(crons) == 1, f"trade-am.yml must have exactly 1 schedule, got {crons}"

    def test_schedule_is_0_0_weekdays(self):
        text = _load_workflow("trade-am.yml")
        crons = _extract_cron_entries(text)
        assert crons[0] == "0 0 * * 1-5", f"trade-am.yml schedule must be '0 0 * * 1-5', got '{crons[0]}'"

    def test_no_recovery_schedules(self):
        text = _load_workflow("trade-am.yml")
        crons = _extract_cron_entries(text)
        forbidden = {"7 0 * * 1-5", "12 0 * * 1-5", "17 0 * * 1-5", "22 0 * * 1-5", "27 0 * * 1-5"}
        found = set(crons) & forbidden
        assert not found, f"trade-am.yml contains forbidden recovery schedules: {found}"


class TestPmWorkflowSchedule:
    def test_only_one_schedule(self):
        text = _load_workflow("trade-pm.yml")
        crons = _extract_cron_entries(text)
        assert len(crons) == 1, f"trade-pm.yml must have exactly 1 schedule, got {crons}"

    def test_schedule_is_0_4_weekdays(self):
        text = _load_workflow("trade-pm.yml")
        crons = _extract_cron_entries(text)
        assert crons[0] == "0 4 * * 1-5", f"trade-pm.yml schedule must be '0 4 * * 1-5', got '{crons[0]}'"

    def test_no_recovery_schedules(self):
        text = _load_workflow("trade-pm.yml")
        crons = _extract_cron_entries(text)
        forbidden = {"5 4 * * 1-5", "10 4 * * 1-5", "15 4 * * 1-5"}
        found = set(crons) & forbidden
        assert not found, f"trade-pm.yml contains forbidden recovery schedules: {found}"


class TestCloseWorkflowSchedule:
    def test_only_one_schedule(self):
        text = _load_workflow("trade-close.yml")
        crons = _extract_cron_entries(text)
        assert len(crons) == 1, f"trade-close.yml must have exactly 1 schedule, got {crons}"

    def test_schedule_is_15_6_weekdays(self):
        text = _load_workflow("trade-close.yml")
        crons = _extract_cron_entries(text)
        assert crons[0] == "15 6 * * 1-5", f"trade-close.yml schedule must be '15 6 * * 1-5', got '{crons[0]}'"

    def test_no_recovery_schedules(self):
        text = _load_workflow("trade-close.yml")
        crons = _extract_cron_entries(text)
        forbidden = {"18 6 * * 1-5", "21 6 * * 1-5", "24 6 * * 1-5"}
        found = set(crons) & forbidden
        assert not found, f"trade-close.yml contains forbidden recovery schedules: {found}"
