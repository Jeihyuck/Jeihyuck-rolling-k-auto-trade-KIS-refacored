"""
테스트 6: session_end 정책
- am session_end는 12:55
- pm session_end는 15:10
- close session_end는 15:30
- PB1_MIN_TICK_BUDGET_SEC 가드가 pb1_runner.py에 존재하는지 확인
- portfolio_full 가드가 pb1_engine.py에 존재하는지 확인
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).parent.parent
RUNNER_PY = REPO_ROOT / "trader" / "pb1_runner.py"
ENGINE_PY = REPO_ROOT / "trader" / "pb1_engine.py"
AM_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "trade-am.yml"
PM_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "trade-pm.yml"
CLOSE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "trade-close.yml"


class TestSessionEndEnvDefaults:
    def test_am_session_end_default_in_workflow(self):
        text = AM_WORKFLOW.read_text(encoding="utf-8")
        assert 'PB1_AM_SESSION_END: "12:55"' in text or "PB1_AM_SESSION_END: '12:55'" in text, (
            "trade-am.yml must define PB1_AM_SESSION_END: '12:55'"
        )

    def test_pm_session_end_default_in_workflow(self):
        text = PM_WORKFLOW.read_text(encoding="utf-8")
        assert 'PB1_PM_SESSION_END: "15:10"' in text or "PB1_PM_SESSION_END: '15:10'" in text, (
            "trade-pm.yml must define PB1_PM_SESSION_END: '15:10'"
        )

    def test_close_session_end_default_in_workflow(self):
        text = CLOSE_WORKFLOW.read_text(encoding="utf-8")
        assert 'PB1_CLOSE_SESSION_END: "15:30"' in text or "PB1_CLOSE_SESSION_END: '15:30'" in text, (
            "trade-close.yml must define PB1_CLOSE_SESSION_END: '15:30'"
        )


class TestResolveSessionEndDt:
    def test_am_session_end_reads_env(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        with patch.dict(os.environ, {"PB1_SESSION_KIND": "am", "PB1_AM_SESSION_END": "12:55"}):
            from trader.pb1_runner import _resolve_session_end_dt
            now = datetime(2026, 4, 24, 9, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
            end_dt = _resolve_session_end_dt(now)
            assert end_dt.hour == 12
            assert end_dt.minute == 55

    def test_pm_session_end_reads_env(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        with patch.dict(os.environ, {"PB1_SESSION_KIND": "pm", "PB1_PM_SESSION_END": "15:10"}):
            from trader.pb1_runner import _resolve_session_end_dt
            now = datetime(2026, 4, 24, 13, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
            end_dt = _resolve_session_end_dt(now)
            assert end_dt.hour == 15
            assert end_dt.minute == 10

    def test_close_session_end_reads_env(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        with patch.dict(os.environ, {"PB1_SESSION_KIND": "close", "PB1_CLOSE_SESSION_END": "15:30"}):
            from trader.pb1_runner import _resolve_session_end_dt
            now = datetime(2026, 4, 24, 15, 15, 0, tzinfo=ZoneInfo("Asia/Seoul"))
            end_dt = _resolve_session_end_dt(now)
            assert end_dt.hour == 15
            assert end_dt.minute == 30


class TestMinTickBudgetGuard:
    def test_min_tick_budget_guard_exists_in_runner(self):
        """pb1_runner.py에 PB1_MIN_TICK_BUDGET_SEC 가드 코드가 있어야 한다."""
        text = RUNNER_PY.read_text(encoding="utf-8")
        assert "PB1_MIN_TICK_BUDGET_SEC" in text, (
            "pb1_runner.py must contain PB1_MIN_TICK_BUDGET_SEC guard"
        )
        assert "remaining_budget_too_small" in text, (
            "pb1_runner.py must log 'remaining_budget_too_small' when budget is too small"
        )

    def test_min_tick_budget_guard_breaks_loop(self):
        """remaining_budget_too_small 조건 시 loop를 break하는 코드가 있어야 한다."""
        text = RUNNER_PY.read_text(encoding="utf-8")
        # remaining_budget_too_small 로그 직후 exit_reason = "session_end" + break 패턴 확인
        pattern = r"remaining_budget_too_small.*?exit_reason\s*=\s*[\"']session_end[\"']"
        assert re.search(pattern, text, re.DOTALL), (
            "pb1_runner.py must set exit_reason='session_end' after remaining_budget_too_small"
        )


class TestPortfolioFullGuard:
    def test_portfolio_full_guard_exists_in_engine(self):
        """pb1_engine.py에 SKIP_PORTFOLIO_FULL 가드 코드가 있어야 한다."""
        text = ENGINE_PY.read_text(encoding="utf-8")
        assert "SKIP_PORTFOLIO_FULL" in text, (
            "pb1_engine.py must contain [ENTRY][SKIP_PORTFOLIO_FULL] guard"
        )
        assert "PORTFOLIO_FULL" in text, (
            "pb1_engine.py must contain PORTFOLIO_FULL status/reason"
        )

    def test_portfolio_full_guard_checks_correct_condition(self):
        """portfolio_full 가드가 existing_positions_count >= max_positions 조건을 확인해야 한다."""
        text = ENGINE_PY.read_text(encoding="utf-8")
        assert "existing_positions_count >= max_positions" in text or \
               "existing_positions_count>=max_positions" in text, (
            "pb1_engine.py must check existing_positions_count >= max_positions"
        )
        assert "target_new_positions <= 0" in text or \
               "target_new_positions<=0" in text, (
            "pb1_engine.py must check target_new_positions <= 0"
        )

    def test_portfolio_full_returns_ok_no_trade(self):
        """portfolio_full 시 OK_NO_TRADE status로 반환해야 한다."""
        text = ENGINE_PY.read_text(encoding="utf-8")
        assert '"OK_NO_TRADE"' in text or "'OK_NO_TRADE'" in text, (
            "pb1_engine.py must return 'OK_NO_TRADE' status for portfolio_full"
        )
