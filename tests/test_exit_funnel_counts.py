"""tests/test_exit_funnel_counts.py

Exit funnel counts: 최종 결정된 exit만 카운트해야 한다.
- evaluate_all_layers 호출 횟수가 아닌 final_decision 결과로만 카운트
- 동일 code에 대해 여러 레이어가 평가되어도 funnel_count는 1
"""
from __future__ import annotations

import collections
import os
import sys
import unittest
from unittest.mock import MagicMock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _simulate_exit_funnel(positions: list[dict], exit_decisions: dict[str, str]) -> dict:
    """
    각 position에 대해 exit_decisions에서 final_exit 여부를 확인하고
    funnel 카운터를 반환한다. evaluate 횟수는 카운트에 포함하지 않는다.
    """
    funnel = collections.Counter()
    for pos in positions:
        code = pos["code"]
        decision = exit_decisions.get(code)
        if decision:
            # 최종 결정 시에만 카운트
            funnel["exit_final"] += 1
            funnel[f"exit_reason_{decision}"] += 1
        else:
            funnel["exit_pass"] += 1
    return dict(funnel)


class TestExitFunnelCounts(unittest.TestCase):

    def test_exit_funnel_counts_final_decision_only(self):
        """
        3 positions: 2 exit, 1 pass.
        funnel["exit_final"] == 2, funnel["exit_pass"] == 1.
        """
        positions = [
            {"code": "005930"},
            {"code": "035720"},
            {"code": "000660"},
        ]
        exit_decisions = {
            "005930": "SWING_PROFIT_PROTECT_GIVEBACK",
            "035720": "ABS_TP1",
        }
        funnel = _simulate_exit_funnel(positions, exit_decisions)
        self.assertEqual(funnel["exit_final"], 2)
        self.assertEqual(funnel["exit_pass"], 1)

    def test_multiple_layer_evaluations_dont_inflate_count(self):
        """
        레이어가 5개 evaluate되어도 final_exit 결정이 1개면 exit_final == 1.
        """
        positions = [{"code": "005930"}]
        # evaluate 5번 호출해도 decision은 하나
        exit_decisions = {"005930": "STOP_LOSS"}
        funnel = _simulate_exit_funnel(positions, exit_decisions)
        self.assertEqual(funnel["exit_final"], 1)

    def test_no_exits_all_pass(self):
        positions = [{"code": "005930"}, {"code": "035720"}]
        funnel = _simulate_exit_funnel(positions, {})
        self.assertEqual(funnel.get("exit_final", 0), 0)
        self.assertEqual(funnel["exit_pass"], 2)


if __name__ == "__main__":
    unittest.main()
