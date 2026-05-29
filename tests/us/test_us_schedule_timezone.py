# -*- coding: utf-8 -*-
"""tests/us/test_us_schedule_timezone.py

US workflow schedule 검증:
- dual cron 제거 확인
- timezone=America/New_York 적용 확인
- dispatcher schedule 제거 확인
- cancel-in-progress: true 확인
"""
from __future__ import annotations

import re
from pathlib import Path

WORKFLOW_DIR = Path(__file__).parent.parent.parent / ".github" / "workflows"

_US_SESSION_WORKFLOWS = [
    "us-trade-prep.yml",
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
    "us-trade-close.yml",
]

_DISPATCHER_WORKFLOW = "us-market-dispatcher.yml"


def _read(name: str) -> str:
    return (WORKFLOW_DIR / name).read_text(encoding="utf-8")


class TestScheduleTimezone:
    def test_prep_single_cron(self):
        """prep: cron은 1개만 있어야 한다."""
        content = _read("us-trade-prep.yml")
        cron_matches = re.findall(r"^\s+- cron:", content, re.MULTILINE)
        assert len(cron_matches) == 1, f"prep has {len(cron_matches)} crons, expected 1"

    def test_am_single_cron(self):
        content = _read("us-trade-am.yml")
        cron_matches = re.findall(r"^\s+- cron:", content, re.MULTILINE)
        assert len(cron_matches) == 1, f"am has {len(cron_matches)} crons, expected 1"

    def test_afternoon_single_cron(self):
        content = _read("us-trade-afternoon.yml")
        cron_matches = re.findall(r"^\s+- cron:", content, re.MULTILINE)
        assert len(cron_matches) == 1, f"afternoon has {len(cron_matches)} crons, expected 1"

    def test_close_single_cron(self):
        content = _read("us-trade-close.yml")
        cron_matches = re.findall(r"^\s+- cron:", content, re.MULTILINE)
        assert len(cron_matches) == 1, f"close has {len(cron_matches)} crons, expected 1"

    def test_timezone_present_in_all_session_workflows(self):
        """모든 세션 workflow에 timezone: America/New_York이 있어야 한다."""
        for name in _US_SESSION_WORKFLOWS:
            content = _read(name)
            assert 'timezone: "America/New_York"' in content, (
                f"{name} missing timezone: America/New_York"
            )

    def test_no_dual_cron_strings(self):
        """dualcron, crons=2, edt_est_dualcron 문자열이 없어야 한다."""
        forbidden = ["dualcron", "crons=2", "edt_est_dualcron"]
        for name in _US_SESSION_WORKFLOWS:
            content = _read(name)
            for f in forbidden:
                assert f not in content, f"{name} contains forbidden string: {f}"

    def test_dispatcher_has_no_schedule(self):
        """dispatcher에 schedule 트리거가 없어야 한다."""
        content = _read(_DISPATCHER_WORKFLOW)
        # schedule: 라인이 있으면 안 된다 (workflow_dispatch는 허용)
        assert "schedule:" not in content, "dispatcher must not have schedule trigger"

    def test_cancel_in_progress_true(self):
        """모든 세션 workflow의 concurrency.cancel-in-progress가 true여야 한다."""
        for name in _US_SESSION_WORKFLOWS:
            content = _read(name)
            assert "cancel-in-progress: true" in content, (
                f"{name} cancel-in-progress must be true"
            )


class TestScheduleConfigLog:
    def test_schedule_config_comment_present(self):
        """schedule_mode=timezone_single_cron 주석이 있어야 한다."""
        for name in _US_SESSION_WORKFLOWS:
            content = _read(name)
            assert "timezone_single_cron" in content, (
                f"{name} missing timezone_single_cron comment/log"
            )
