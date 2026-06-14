# -*- coding: utf-8 -*-
"""tests/us/test_us_schedule_timezone.py

US workflow schedule 검증:
- dual cron 제거 확인
- timezone=America/New_York 적용 확인
- dispatcher schedule 제거 확인
- trade session/prep cancel-in-progress 설정 확인
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
    def test_order_capable_us_workflows_have_no_schedule(self):
        """WSL 이전 후 US 주문 가능 workflow에는 GitHub schedule이 없어야 한다."""
        for name in _US_SESSION_WORKFLOWS:
            content = _read(name)
            assert "schedule:" not in content, f"{name} must not have schedule trigger"
            assert "workflow_dispatch:" in content, f"{name} must keep workflow_dispatch"

    def test_us_workflows_default_to_safe_manual_mode(self):
        """수동 실행은 기본 DRY_RUN/INTENT_ONLY 안전모드여야 한다."""
        expected = (
            'DRY_RUN: "1"',
            'DISABLE_LIVE_TRADING: "1"',
            'LIVE_TRADING_ENABLED: "0"',
            'STRATEGY_MODE: "INTENT_ONLY"',
            'FORCE_STRATEGY_MODE: "INTENT_ONLY"',
        )
        for name in _US_SESSION_WORKFLOWS:
            content = _read(name)
            for marker in expected:
                assert marker in content, f"{name} missing {marker}"

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
        assert "schedule:" not in content, "dispatcher must not have schedule trigger"

    def test_cancel_in_progress_contract(self):
        """AM/Afternoon/prep은 기존 실행을 죽이지 않도록 false, close만 true여야 한다."""
        cancel_false_workflows = [
            "us-trade-prep.yml",
            "us-trade-am.yml",
            "us-trade-afternoon.yml",
        ]
        for name in cancel_false_workflows:
            content = _read(name)
            assert "cancel-in-progress: false" in content, (
                f"{name} cancel-in-progress must be false (do not kill running session/prep)"
            )

        close_content = _read("us-trade-close.yml")
        assert "cancel-in-progress: true" in close_content, (
            "us-trade-close.yml cancel-in-progress must be true"
        )


class TestScheduleConfigLog:
    def test_schedule_removed_runbook_exists(self):
        """스케줄 제거 후 WSL runbook이 운영 기준을 문서화해야 한다."""
        runbook = Path("docs/WSL_KR_US_SCHEDULE_RUNBOOK.md").read_text(encoding="utf-8")
        assert "GitHub Actions schedule" in runbook
        assert "run-us-trader.sh" in runbook
        assert "run-kr-trader.sh" in runbook
