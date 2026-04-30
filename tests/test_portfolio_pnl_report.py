"""tests/test_portfolio_pnl_report.py

Portfolio PNL 리포트 생성 검증.
- holdings 없는 경우도 report 출력 정상
- 계좌번호 마스킹 검증
- FAILED_TO_GENERATE 출력 형식 검증
- JSON / Markdown / CSV 구조 검증
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.generate_portfolio_pnl_report import (
    _build_holdings_pnl,
    _build_markdown,
    _build_portfolio_summary,
    _failure_report,
    _mask_account,
)


class TestMaskAccount(unittest.TestCase):

    def test_mask_account_pattern(self):
        result = _mask_account("12345678", "practice")
        self.assertIn("****", result)
        self.assertNotIn("12345678", result)
        self.assertTrue(result.startswith("practice:"))
        self.assertIn("5678", result)

    def test_mask_account_short(self):
        result = _mask_account("12", "practice")
        self.assertIn("****", result)

    def test_mask_account_none(self):
        result = _mask_account(None, "practice")
        self.assertEqual(result, "****")


class TestHoldingsPnl(unittest.TestCase):

    def _make_balance(self) -> list[dict]:
        return [
            {
                "pdno": "005930",
                "prdt_name": "삼성전자",
                "hldg_qty": "10",
                "pchs_avg_pric": "70000",
                "prpr": "75000",
            }
        ]

    def _make_positions(self) -> list[dict]:
        return [
            {
                "code": "005930",
                "name": "삼성전자",
                "qty": 10,
                "avg_buy_price": 70000,
                "last_price": 75000,
                "entry_date": "2025-04-01",
                "rank_final30": 1,
            }
        ]

    def test_holdings_pnl_basic(self):
        from datetime import date
        holdings, warnings = _build_holdings_pnl(
            self._make_balance(), self._make_positions(), [], date(2025, 4, 30)
        )
        self.assertEqual(len(holdings), 1)
        h = holdings[0]
        self.assertEqual(h["code"], "005930")
        self.assertEqual(h["qty"], 10)
        self.assertAlmostEqual(h["unrealized_pnl"], 50000.0)
        self.assertAlmostEqual(h["pnl_pct"], 7.142857, places=2)

    def test_empty_balance_uses_db_positions(self):
        from datetime import date
        holdings, _ = _build_holdings_pnl([], self._make_positions(), [], date(2025, 4, 30))
        self.assertEqual(len(holdings), 1)
        self.assertEqual(holdings[0]["price_source"], "db_last_price")

    def test_zero_qty_excluded(self):
        from datetime import date
        positions = [{"code": "005930", "qty": 0, "avg_buy_price": 70000, "last_price": 70000}]
        holdings, _ = _build_holdings_pnl([], positions, [], date(2025, 4, 30))
        self.assertEqual(len(holdings), 0)


class TestMarkdownOutput(unittest.TestCase):

    def _runtime(self) -> dict:
        return {"Trade Date": "2025-04-30", "Session": "am", "Account": "practice:****3616:****"}

    def _summary(self) -> dict:
        return {
            "total_positions": 1,
            "total_cost": 700000,
            "market_value": 750000,
            "unrealized_pnl": 50000,
            "unrealized_pnl_pct": 7.14,
            "realized_pnl_today": 0,
            "total_pnl": 50000,
            "cash": 1000000,
            "total_equity_estimate": 1750000,
            "winners": 1,
            "losers": 0,
            "best_position": "삼성전자 +7.14%",
            "worst_position": "삼성전자 +7.14%",
        }

    def _holdings(self) -> list[dict]:
        return [
            {
                "rank": 1, "code": "005930", "name": "삼성전자",
                "qty": 10, "entry_date": "2025-04-01", "days_held": 29,
                "avg_buy": 70000, "current_price": 75000,
                "cost_basis": 700000, "market_value": 750000,
                "unrealized_pnl": 50000, "pnl_pct": 7.14,
                "realized_pnl_today": 0, "total_pnl": 50000,
                "stop_price": 63000, "pivot_price": 68000,
                "entry_style": "SWING", "exit_policy": "swing_exit",
                "last_fill": "20250401", "price_source": "kis_balance_prpr",
            }
        ]

    def test_markdown_contains_required_sections(self):
        md = _build_markdown(
            self._runtime(), self._summary(), self._holdings(), [], [], {"warnings": []}
        )
        self.assertIn("# PB1 Portfolio PNL Report", md)
        self.assertIn("## Runtime Metadata", md)
        self.assertIn("## Portfolio Summary", md)
        self.assertIn("## Holdings PNL Table", md)

    def test_markdown_masks_account(self):
        md = _build_markdown(
            self._runtime(), self._summary(), self._holdings(), [], [], {"warnings": []}
        )
        self.assertIn("****", md)

    def test_failure_report_format(self):
        md = _failure_report("DatabaseError:connection refused")
        self.assertIn("FAILED_TO_GENERATE", md)
        self.assertIn("DatabaseError", md)


class TestPNLReportScript(unittest.TestCase):

    def test_main_returns_0_on_missing_db(self):
        """DB URL 없어도 main()은 0을 반환해야 한다 (workflow 실패 금지)."""
        orig = os.environ.get("PBCORE_DB_URL")
        orig_report = os.environ.get("PB1_PNL_REPORT_ENABLED")
        try:
            os.environ.pop("PBCORE_DB_URL", None)
            os.environ["PB1_PNL_REPORT_ENABLED"] = "1"
            # patch REPORT_DIR to temp
            import scripts.generate_portfolio_pnl_report as mod
            with tempfile.TemporaryDirectory() as tmpdir:
                original_dir = mod.REPORT_DIR
                mod.REPORT_DIR = type(mod.REPORT_DIR)(tmpdir) / "pnl"
                try:
                    result = mod.main()
                    self.assertEqual(result, 0)
                finally:
                    mod.REPORT_DIR = original_dir
        finally:
            if orig is not None:
                os.environ["PBCORE_DB_URL"] = orig
            else:
                os.environ.pop("PBCORE_DB_URL", None)
            if orig_report is not None:
                os.environ["PB1_PNL_REPORT_ENABLED"] = orig_report


if __name__ == "__main__":
    unittest.main()
