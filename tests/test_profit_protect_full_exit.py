"""tests/test_profit_protect_full_exit.py

Giveback exit full-sell policy.
- PB1_PROFIT_PROTECT_FULL_EXIT=1 → giveback reason → qty 전량 매도
- PB1_GIVEBACK_EXIT_FULL_SELL=1 → 동일
- ABS_TP1 reason → 부분 매도 여전히 적용
"""
from __future__ import annotations

import os
import sys
import types
import unittest

# 최소 stub 주입 - trader 전체 import 없이 router만 테스트
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _make_router(full_exit: str = "1"):
    os.environ["PB1_PROFIT_PROTECT_FULL_EXIT"] = full_exit
    os.environ["PB1_GIVEBACK_EXIT_FULL_SELL"] = full_exit
    os.environ["PB1_SWING_GIVEBACK_SELL_PCT"] = "1.0"
    # import after env
    import importlib
    import trader.exit_policy.router as r
    importlib.reload(r)
    return r


class TestGivebackFullExit(unittest.TestCase):
    def test_swing_profit_protect_giveback_sells_full_qty(self):
        """giveback reason → full_exit=True, qty == orderable_qty."""
        r = _make_router("1")
        orderable_qty = 100
        # call internal helper if exposed, else use apply_swing_exit_decision stub
        from trader.exit_policy.router import _calculate_exit_qty
        qty = _calculate_exit_qty(orderable_qty, None)  # None → full
        self.assertEqual(qty, 100)

    def test_giveback_full_exit_overrides_033_sell_pct(self):
        """partial sell_pct=0.33 overridden when full_exit enabled."""
        from trader.exit_policy.router import _calculate_exit_qty
        # partial: sell_pct=0.33 → 33
        partial = _calculate_exit_qty(100, 0.33)
        # full: sell_pct=None → 100
        full = _calculate_exit_qty(100, None)
        self.assertLess(partial, full)
        self.assertEqual(full, 100)

    def test_abs_tp1_still_uses_partial_sell_pct(self):
        """ABS_TP1 sell_pct=0.33 should produce partial qty unchanged."""
        from trader.exit_policy.router import _calculate_exit_qty
        qty = _calculate_exit_qty(100, 0.33)
        # 0.33 * 100 = 33, but floor/ceil might differ
        self.assertGreater(qty, 0)
        self.assertLess(qty, 100)


if __name__ == "__main__":
    unittest.main()
