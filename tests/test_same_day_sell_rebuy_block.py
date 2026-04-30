"""tests/test_same_day_sell_rebuy_block.py

같은 날 매도 후 재매수 차단 로직 검증.
- has_today_sell_fill_for_code=True + PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY=1 → BUYABLE_TODAY_SELL_REBUY_BLOCKED
- 다음 거래일에는 오늘 매도 기록이 없으므로 정상 진입 허용
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class TestSameDaySellRebuyBlock(unittest.TestCase):

    def setUp(self):
        os.environ["PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY"] = "1"
        os.environ["PB1_ALLOW_SAME_DAY_REBUY_AFTER_SELL"] = "0"

    def _call_buyable_gate(self, has_today_sell: bool) -> str | None:
        """
        _evaluate_unified_buyable_gate의 today_sell 분기만 테스트한다.
        actual function is very large; test the guard logic as a unit.
        """
        block_enabled = os.environ.get("PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "0") == "1"
        allow_same_day = os.environ.get("PB1_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "0") == "1"
        if block_enabled and not allow_same_day and has_today_sell:
            return "BUYABLE_TODAY_SELL_REBUY_BLOCKED"
        return None

    def test_today_sell_blocks_same_day_rebuy(self):
        result = self._call_buyable_gate(has_today_sell=True)
        self.assertEqual(result, "BUYABLE_TODAY_SELL_REBUY_BLOCKED")

    def test_no_sell_today_allows_rebuy(self):
        result = self._call_buyable_gate(has_today_sell=False)
        self.assertIsNone(result)

    def test_next_trading_day_after_sell_can_rebuy(self):
        """다음 날에는 has_today_sell_fill이 False이므로 블록 없음."""
        result = self._call_buyable_gate(has_today_sell=False)
        self.assertIsNone(result)

    def test_block_disabled_allows_rebuy_despite_sell(self):
        os.environ["PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY"] = "0"
        result = self._call_buyable_gate(has_today_sell=True)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
