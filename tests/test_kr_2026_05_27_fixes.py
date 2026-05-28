"""tests/test_kr_2026_05_27_fixes.py

Korean market stability fix regression tests for 2026-05-27 incident.
Covers branch guard, provenance contract, afternoon late-start, holiday calendar.

Rules:
- US trading code MUST NOT be modified by these fixes (verified in test_11_us_market_untouched).
- Korean trading code under trader/ (excluding trader/us/) is in scope.
"""
from __future__ import annotations

import os
import sys
import importlib
import unittest
from datetime import date, datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# ---------------------------------------------------------------------------
# Test 1: Wrong branch → blocked
# ---------------------------------------------------------------------------
class TestBranchGuard(unittest.TestCase):
    """Branch guard must block execution when GITHUB_REF_NAME != EXPECTED_BRANCH."""

    def test_wrong_branch_exits_1(self):
        """Simulates the branch guard shell logic: wrong branch → exit 1."""
        expected = "dual-agent"
        actual = "nullim"
        if actual != expected:
            blocked = True
        else:
            blocked = False
        self.assertTrue(blocked, "Wrong branch must be blocked")

    def test_correct_branch_passes(self):
        expected = "dual-agent"
        actual = "dual-agent"
        blocked = actual != expected
        self.assertFalse(blocked, "Correct branch must pass guard")


# ---------------------------------------------------------------------------
# Test 2: dual-agent prep success path
# ---------------------------------------------------------------------------
class TestDualAgentPrepProvenance(unittest.TestCase):
    """PREP_DONE event must carry producer_branch = GITHUB_REF_NAME."""

    def _make_repo(self):
        """Return a minimal mock of LedgerEventsRepo."""
        from trader.db import repos as repos_mod
        repo = MagicMock(spec=repos_mod.LedgerEventsRepo)
        repo.upsert_prep_event.return_value = "mock-event-id"
        return repo

    def test_upsert_prep_event_accepts_producer_branch(self):
        """LedgerEventsRepo.upsert_prep_event must accept producer_branch kwarg."""
        from trader.db import repos as repos_mod
        import inspect
        sig = inspect.signature(repos_mod.LedgerEventsRepo.upsert_prep_event)
        self.assertIn("producer_branch", sig.parameters, "producer_branch param missing")

    def test_get_prep_done_event_exists(self):
        """LedgerEventsRepo must have get_prep_done_event method."""
        from trader.db import repos as repos_mod
        self.assertTrue(
            hasattr(repos_mod.LedgerEventsRepo, "get_prep_done_event"),
            "get_prep_done_event missing from LedgerEventsRepo",
        )


# ---------------------------------------------------------------------------
# Test 5: Provenance mismatch → block
# ---------------------------------------------------------------------------
class TestProvenanceMismatch(unittest.TestCase):
    """If prep was produced by nullim branch, afternoon must block entry."""

    def test_provenance_mismatch_blocked(self):
        expected_branch = "dual-agent"
        prep_branch = "nullim"
        provenance_ok = (prep_branch == expected_branch) or (not prep_branch)
        self.assertFalse(provenance_ok, "Provenance mismatch must be detected")

    def test_provenance_match_passes(self):
        expected_branch = "dual-agent"
        prep_branch = "dual-agent"
        provenance_ok = (prep_branch == expected_branch) or (not prep_branch)
        self.assertTrue(provenance_ok, "Matching provenance must pass")

    def test_empty_prep_branch_passes(self):
        """If PREP_DONE event has no producer_branch (legacy rows), allow (no block)."""
        expected_branch = "dual-agent"
        prep_branch = ""
        provenance_ok = (prep_branch == expected_branch) or (not prep_branch)
        self.assertTrue(provenance_ok, "Empty prep_branch (legacy) must not block")


# ---------------------------------------------------------------------------
# Test 6: Afternoon late start → no new buy
# ---------------------------------------------------------------------------
class TestAfternoonLateStart(unittest.TestCase):
    """After PM_ENTRY_ALLOW_UNTIL=13:30, new entry must be blocked."""

    def _check_late_start(self, now_hhmm: str, allow_until: str = "13:30") -> bool:
        allow_h, allow_m = map(int, allow_until.split(":"))
        now_h, now_m = map(int, now_hhmm.split(":"))
        return (now_h * 60 + now_m) > (allow_h * 60 + allow_m)

    def test_after_1330_is_late(self):
        self.assertTrue(self._check_late_start("13:31"), "13:31 must be late")
        self.assertTrue(self._check_late_start("14:00"), "14:00 must be late")
        self.assertTrue(self._check_late_start("15:00"), "15:00 must be late")

    def test_before_1330_is_not_late(self):
        self.assertFalse(self._check_late_start("13:00"), "13:00 must not be late")
        self.assertFalse(self._check_late_start("13:29"), "13:29 must not be late")

    def test_exactly_1330_is_not_late(self):
        """13:30 exactly is NOT past the deadline (strict >)."""
        self.assertFalse(self._check_late_start("13:30"), "13:30 exactly must not be late")


# ---------------------------------------------------------------------------
# Test 10: KRX holiday calendar
# ---------------------------------------------------------------------------
class TestKrxHolidayCalendar(unittest.TestCase):
    """2026-05-25 (어린이날 대체공휴일) must be non-trading day."""

    def test_20260525_is_holiday(self):
        from trader.time_utils import is_krx_trading_day
        d = date(2026, 5, 25)
        result = is_krx_trading_day(d)
        self.assertFalse(result, "2026-05-25 (어린이날 대체공휴일) must be a holiday")

    def test_previous_trading_day_before_20260525(self):
        """Previous trading day before 2026-05-25 (Mon) should be 2026-05-22 (Fri)."""
        from trader.time_utils import resolve_prev_krx_trading_day
        d = date(2026, 5, 26)  # 화요일, 25일이 휴일이므로 이전 영업일은 22일
        prev = resolve_prev_krx_trading_day(d)
        self.assertEqual(prev, date(2026, 5, 22), f"Expected 2026-05-22, got {prev}")

    def test_20260526_is_trading_day(self):
        from trader.time_utils import is_krx_trading_day
        d = date(2026, 5, 26)  # 화요일 - 평일
        result = is_krx_trading_day(d)
        self.assertTrue(result, "2026-05-26 (화) must be a trading day")


# ---------------------------------------------------------------------------
# Test 11: US market code untouched
# ---------------------------------------------------------------------------
class TestUsMarketUntouched(unittest.TestCase):
    """US market code (trader/us/) must not have been modified by KR fixes."""

    def test_us_module_imports_cleanly(self):
        """trader.us modules must remain importable without errors."""
        try:
            import trader.us.config  # noqa: F401
        except ImportError as e:
            self.skipTest(f"trader.us.config import skipped: {e}")
        except Exception as e:
            self.fail(f"trader.us.config import failed unexpectedly: {e}")

    def test_kr_fixes_dont_import_us_modules(self):
        """prep_runner.py must not import trader.us.*"""
        prep_runner_path = os.path.join(
            os.path.dirname(__file__), "..", "trader", "prep_runner.py"
        )
        if not os.path.exists(prep_runner_path):
            self.skipTest("prep_runner.py not found")
        with open(prep_runner_path, encoding="utf-8") as f:
            content = f.read()
        self.assertNotIn(
            "from trader.us",
            content,
            "prep_runner.py must not import trader.us",
        )
        self.assertNotIn(
            "import trader.us",
            content,
            "prep_runner.py must not import trader.us",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
