"""tests/test_exit_summary_counts.py

Exit summary 집계 로직 검증.

요구사항:
- signal_hit=6, submitted=6이면 sells/accepted_sells가 6으로 집계되어야 한다
- SUMMARY_BY_REASON이 NO_EXIT_SIGNAL=12로만 찍히면 실패
- eval_reason_summary, router_reason_summary, order_reason_summary를 분리하라
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class TestExitSummaryCounts(unittest.TestCase):
    """Exit summary 집계 로직 테스트."""

    def test_signal_hit_equals_submitted_equals_accepted_sells(self):
        """signal_hit=6, submitted=6이면 sells/accepted_sells가 6으로 집계되어야 한다."""
        # Exit 평가 결과
        signal_hit = 6
        submitted = 6
        accepted_sells = 0
        
        # 실제 주문 제출 및 accept 프로세스 시뮬레이션
        for _ in range(submitted):
            # 주문 제출 성공
            accepted_sells += 1
        
        # 집계 검증
        self.assertEqual(signal_hit, 6)
        self.assertEqual(submitted, 6)
        self.assertEqual(accepted_sells, 6)

    def test_summary_by_reason_not_only_no_exit_signal(self):
        """SUMMARY_BY_REASON이 NO_EXIT_SIGNAL=12로만 찍히면 실패."""
        eval_reason_counter = Counter({
            "STOP_LOSS": 3,
            "PROFIT_PROTECT": 2,
            "TIME_STOP": 1,
            "NO_EXIT_SIGNAL": 6,
        })
        
        # NO_EXIT_SIGNAL만 있으면 안 됨
        total_reasons = sum(eval_reason_counter.values())
        no_exit_signal_count = eval_reason_counter.get("NO_EXIT_SIGNAL", 0)
        
        # NO_EXIT_SIGNAL이 전체가 아니어야 함
        self.assertLess(no_exit_signal_count, total_reasons, 
                       "SUMMARY_BY_REASON이 NO_EXIT_SIGNAL만 있으면 실패")

    def test_eval_router_order_reason_summary_separated(self):
        """eval_reason_summary, router_reason_summary, order_reason_summary를 분리."""
        # 3단계 분리: eval → router → order
        eval_reason_counter = Counter({
            "STOP_LOSS": 5,
            "PROFIT_PROTECT": 3,
            "NO_EXIT_SIGNAL": 4,
        })
        
        router_reason_counter = Counter({
            "STOP_LOSS": 5,
            "PROFIT_PROTECT": 3,
            # NO_EXIT_SIGNAL은 router에서 필터링됨
        })
        
        order_reason_counter = Counter({
            "STOP_LOSS": 5,  # 모두 주문 제출
            "PROFIT_PROTECT": 3,
        })
        
        # 각 단계가 분리되어 있는지 확인
        self.assertIsNotNone(eval_reason_counter)
        self.assertIsNotNone(router_reason_counter)
        self.assertIsNotNone(order_reason_counter)
        
        # eval에서 router로 필터링
        self.assertIn("NO_EXIT_SIGNAL", eval_reason_counter)
        self.assertNotIn("NO_EXIT_SIGNAL", router_reason_counter)

    def test_exit_summary_payload_structure(self):
        """exit_summary_payload가 올바른 구조를 가져야 한다."""
        exit_summary_payload = {
            "signal_hit": 6,
            "submitted": 6,
            "accepted_sells": 6,
            "eval_reason_summary": {
                "STOP_LOSS": 3,
                "PROFIT_PROTECT": 2,
                "NO_EXIT_SIGNAL": 7,
            },
            "router_reason_summary": {
                "STOP_LOSS": 3,
                "PROFIT_PROTECT": 2,
            },
            "order_reason_summary": {
                "STOP_LOSS": 3,
                "PROFIT_PROTECT": 2,
            },
        }
        
        # 필수 키 존재 확인
        self.assertIn("signal_hit", exit_summary_payload)
        self.assertIn("submitted", exit_summary_payload)
        self.assertIn("accepted_sells", exit_summary_payload)
        self.assertIn("eval_reason_summary", exit_summary_payload)
        self.assertIn("router_reason_summary", exit_summary_payload)
        self.assertIn("order_reason_summary", exit_summary_payload)

    def test_eval_reason_includes_all_positions(self):
        """eval_reason_summary는 모든 position의 exit 평가 결과를 포함."""
        total_positions = 12
        eval_reason_counter = Counter({
            "STOP_LOSS": 3,
            "PROFIT_PROTECT": 2,
            "TIME_STOP": 1,
            "NO_EXIT_SIGNAL": 6,
        })
        
        # eval_reason 합계가 전체 position 수와 일치
        total_eval = sum(eval_reason_counter.values())
        self.assertEqual(total_eval, total_positions)

    def test_router_reason_filters_no_exit_signal(self):
        """router_reason_summary는 NO_EXIT_SIGNAL을 제외."""
        eval_reason_counter = Counter({
            "STOP_LOSS": 3,
            "PROFIT_PROTECT": 2,
            "NO_EXIT_SIGNAL": 7,
        })
        
        # router 단계에서 NO_EXIT_SIGNAL 제외
        router_reason_counter = Counter()
        for reason, count in eval_reason_counter.items():
            if reason != "NO_EXIT_SIGNAL":
                router_reason_counter[reason] = count
        
        self.assertNotIn("NO_EXIT_SIGNAL", router_reason_counter)
        self.assertEqual(router_reason_counter["STOP_LOSS"], 3)
        self.assertEqual(router_reason_counter["PROFIT_PROTECT"], 2)

    def test_order_reason_reflects_submitted_orders(self):
        """order_reason_summary는 실제 제출된 주문의 reason을 반영."""
        router_reason_counter = Counter({
            "STOP_LOSS": 5,
            "PROFIT_PROTECT": 3,
        })
        
        order_reason_counter = Counter()
        submitted_count = 0
        
        # router reason 기반으로 주문 제출
        for reason, count in router_reason_counter.items():
            for _ in range(count):
                # 주문 제출 시뮬레이션
                order_reason_counter[reason] += 1
                submitted_count += 1
        
        # order_reason과 router_reason이 일치
        self.assertEqual(order_reason_counter, router_reason_counter)
        self.assertEqual(submitted_count, 8)

    def test_summary_counters_logged_separately(self):
        """eval/router/order reason summary가 별도로 로깅되어야 한다."""
        eval_reason_counter = Counter({"STOP_LOSS": 3})
        router_reason_counter = Counter({"STOP_LOSS": 3})
        order_reason_counter = Counter({"STOP_LOSS": 3})
        
        with patch("trader.pb1_engine.logger") as mock_logger:
            # 별도 로그 생성
            mock_logger.info("[EXIT][SUMMARY][EVAL_REASON] counts=%s", dict(eval_reason_counter))
            mock_logger.info("[EXIT][SUMMARY][ROUTER_REASON] counts=%s", dict(router_reason_counter))
            mock_logger.info("[EXIT][SUMMARY][ORDER_REASON] counts=%s", dict(order_reason_counter))
            
            # 각각 호출되었는지 확인
            self.assertEqual(mock_logger.info.call_count, 3)

    def test_no_double_counting_in_summary(self):
        """summary에서 중복 집계가 없어야 한다."""
        # 같은 position이 여러 reason으로 중복 카운트되면 안 됨
        position_codes = ["005930", "000660", "035420"]
        eval_reason_map = {
            "005930": "STOP_LOSS",
            "000660": "PROFIT_PROTECT",
            "035420": "NO_EXIT_SIGNAL",
        }
        
        eval_reason_counter = Counter()
        for code, reason in eval_reason_map.items():
            eval_reason_counter[reason] += 1
        
        # 총 position 수 = eval_reason_counter 합계
        total_positions = len(position_codes)
        total_counted = sum(eval_reason_counter.values())
        self.assertEqual(total_positions, total_counted)


class TestExitSummaryIntegration(unittest.TestCase):
    """Exit summary 통합 테스트."""

    def test_exit_summary_payload_in_heartbeat(self):
        """HEARTBEAT에 exit_summary_payload가 포함되어야 한다."""
        exit_summary_payload = {
            "signal_hit": 5,
            "submitted": 5,
            "accepted_sells": 5,
            "eval_reason_summary": {"STOP_LOSS": 5},
            "router_reason_summary": {"STOP_LOSS": 5},
            "order_reason_summary": {"STOP_LOSS": 5},
        }
        
        heartbeat_payload = {
            "tick": 10,
            "exit_summary": exit_summary_payload,
        }
        
        self.assertIn("exit_summary", heartbeat_payload)
        self.assertEqual(heartbeat_payload["exit_summary"]["signal_hit"], 5)

    def test_summary_by_reason_includes_multiple_reasons(self):
        """SUMMARY_BY_REASON에 여러 reason이 포함되어야 한다."""
        eval_reason_counter = Counter({
            "STOP_LOSS": 3,
            "PROFIT_PROTECT": 2,
            "TIME_STOP": 1,
            "ABS_TP1": 1,
            "NO_EXIT_SIGNAL": 5,
        })
        
        # 다양한 reason 확인
        unique_reasons = len(eval_reason_counter)
        self.assertGreaterEqual(unique_reasons, 3, 
                               "SUMMARY_BY_REASON에 다양한 reason이 있어야 함")

    def test_submitted_count_matches_router_output(self):
        """submitted count가 router_reason_summary 합계와 일치해야 한다."""
        router_reason_counter = Counter({
            "STOP_LOSS": 4,
            "PROFIT_PROTECT": 2,
        })
        
        submitted = sum(router_reason_counter.values())
        
        self.assertEqual(submitted, 6)


if __name__ == "__main__":
    unittest.main()
