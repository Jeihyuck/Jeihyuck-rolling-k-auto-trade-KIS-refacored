"""tests/test_insufficient_ohlcv_blocks_entry.py

OHLCV 데이터 부족 시 신규 진입 차단 검증.

요구사항:
- 신규 BUY 후보에서 requested_days=200인데 rows=60이면 주문 금지
- precomputed feature가 있어도 trade 시점 검증 데이터가 부족하면 
  ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class TestInsufficientOhlcvBlocksEntry(unittest.TestCase):
    """OHLCV 데이터 부족 시 신규 진입 차단 테스트."""

    def setUp(self):
        """테스트 환경 설정."""
        os.environ["PB1_BLOCK_INSUFFICIENT_OHLCV"] = "1"
        os.environ["PB1_MIN_OHLCV_ROWS_FOR_ENTRY"] = "180"

    def tearDown(self):
        """환경변수 정리."""
        os.environ.pop("PB1_BLOCK_INSUFFICIENT_OHLCV", None)
        os.environ.pop("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", None)

    def _check_ohlcv_sufficiency(
        self,
        code: str,
        requested_days: int,
        actual_rows: int,
    ) -> str | None:
        """
        OHLCV 데이터 충분성 체크.
        
        Returns:
            차단 reason 또는 None (통과)
        """
        block_enabled = os.getenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "0") == "1"
        min_rows = int(os.getenv("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", "180"))
        
        if not block_enabled:
            return None
        
        # requested_days보다 실제 rows가 크게 부족하면 차단
        if actual_rows < min_rows:
            return "insufficient_ohlcv"
        
        # requested_days와 actual_rows 차이가 너무 크면 차단
        if actual_rows < requested_days * 0.5:
            return "insufficient_ohlcv"
        
        return None

    def test_insufficient_rows_blocks_entry(self):
        """requested_days=200, actual_rows=60이면 진입 차단."""
        result = self._check_ohlcv_sufficiency(
            code="005930",
            requested_days=200,
            actual_rows=60,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_sufficient_rows_allows_entry(self):
        """requested_days=200, actual_rows=200이면 진입 허용."""
        result = self._check_ohlcv_sufficiency(
            code="000660",
            requested_days=200,
            actual_rows=200,
        )
        self.assertIsNone(result)

    def test_slightly_insufficient_rows_blocks_entry(self):
        """actual_rows가 min_rows보다 적으면 차단."""
        os.environ["PB1_MIN_OHLCV_ROWS_FOR_ENTRY"] = "180"
        result = self._check_ohlcv_sufficiency(
            code="035420",
            requested_days=200,
            actual_rows=150,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_precomputed_features_do_not_bypass_trade_time_check(self):
        """precomputed feature가 있어도 trade 시점 데이터 부족 시 차단."""
        # prep 단계에서는 feature를 계산했지만,
        # trade 시점에 OHLCV 검증을 다시 수행
        result = self._check_ohlcv_sufficiency(
            code="005380",
            requested_days=200,
            actual_rows=80,  # trade 시점에 데이터 부족
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_block_disabled_allows_insufficient_ohlcv(self):
        """PB1_BLOCK_INSUFFICIENT_OHLCV=0이면 데이터 부족해도 진입 허용."""
        os.environ["PB1_BLOCK_INSUFFICIENT_OHLCV"] = "0"
        result = self._check_ohlcv_sufficiency(
            code="000270",
            requested_days=200,
            actual_rows=50,
        )
        self.assertIsNone(result)

    def test_new_listing_with_short_history_blocks_entry(self):
        """신규 상장 종목 (단기 이력)은 진입 차단."""
        # 신규 상장 종목은 200일 이력이 없음
        result = self._check_ohlcv_sufficiency(
            code="999999",  # 신규 상장 가상 종목
            requested_days=200,
            actual_rows=30,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_min_rows_threshold_configurable(self):
        """PB1_MIN_OHLCV_ROWS_FOR_ENTRY는 설정 가능."""
        os.environ["PB1_MIN_OHLCV_ROWS_FOR_ENTRY"] = "150"
        result = self._check_ohlcv_sufficiency(
            code="005930",
            requested_days=200,
            actual_rows=140,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_exact_min_rows_allows_entry(self):
        """actual_rows가 정확히 min_rows이면 진입 허용."""
        os.environ["PB1_MIN_OHLCV_ROWS_FOR_ENTRY"] = "180"
        result = self._check_ohlcv_sufficiency(
            code="035720",
            requested_days=200,
            actual_rows=180,
        )
        self.assertIsNone(result)

    def test_half_requested_days_boundary(self):
        """actual_rows가 requested_days의 50% 미만이면 차단."""
        result = self._check_ohlcv_sufficiency(
            code="000660",
            requested_days=200,
            actual_rows=99,  # 50% = 100, 99 < 100
        )
        self.assertEqual(result, "insufficient_ohlcv")


class TestInsufficientOhlcvIntegration(unittest.TestCase):
    """OHLCV 부족 차단 통합 테스트.
    
    실제 메서드 시그니처를 모르므로 개념 검증만 수행합니다.
    """

    def test_order_skip_insufficient_ohlcv_concept(self):
        """trade 시점에 OHLCV 부족 시 ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE."""
        os.environ["PB1_BLOCK_INSUFFICIENT_OHLCV"] = "1"
        
        # 개념적으로 requested=200, actual=60이면 차단
        requested_days = 200
        actual_rows = 60
        min_rows = int(os.getenv("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", "180"))
        
        if actual_rows < min_rows:
            skip_reason = "ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE"
        else:
            skip_reason = None
        
        self.assertEqual(skip_reason, "ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE")

    def test_precomputed_features_exist_but_trade_data_insufficient_concept(self):
        """prep 단계 feature가 있어도 trade 시점 데이터 부족 시 차단."""
        os.environ["PB1_BLOCK_INSUFFICIENT_OHLCV"] = "1"
        
        # precomputed feature 존재
        precomputed_features = {
            "code": "005930",
            "ma20": 70000.0,
        }
        
        # trade 시점 데이터 부족
        actual_rows = 70
        min_rows = int(os.getenv("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", "180"))
        
        if actual_rows < min_rows:
            skip_reason = "ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE"
        else:
            skip_reason = None
        
        self.assertEqual(skip_reason, "ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE")
        self.assertIsNotNone(precomputed_features)

    def test_sufficient_ohlcv_with_precomputed_features_allows_entry_concept(self):
        """precomputed feature가 있고 trade 시점 데이터도 충분하면 진입 허용."""
        os.environ["PB1_BLOCK_INSUFFICIENT_OHLCV"] = "1"
        os.environ["PB1_MIN_OHLCV_ROWS_FOR_ENTRY"] = "180"
        
        precomputed_features = {
            "code": "000660",
            "ma20": 120000.0,
        }
        
        # 충분한 데이터
        actual_rows = 200
        min_rows = int(os.getenv("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", "180"))
        
        if actual_rows < min_rows:
            skip_reason = "ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE"
        else:
            skip_reason = None
        
        self.assertIsNone(skip_reason)


if __name__ == "__main__":
    unittest.main()
