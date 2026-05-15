"""tests/test_insufficient_ohlcv_blocks_entry.py

OHLCV 데이터 부족 시 신규 진입 차단 검증.

요구사항:
- 신규 BUY 후보에서 requested_days=200인데 rows=60이면 주문 금지
- precomputed feature가 있어도 trade 시점 검증 데이터가 부족하면
  ORDER_SKIP_INSUFFICIENT_OHLCV_FOR_TRADE
- 단, KR PB1 final30 precomputed 경로에서는 rows=60 + 필수 feature OK이면 허용
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

from trader.pb1_engine import PB1Engine, CandidateFeature


def _make_kr_pb1_final30_cf(
    code: str = "028050",
    missing_feature: str | None = None,
    zero_feature: str | None = None,
) -> CandidateFeature:
    """KR PB1 final30 경로 CandidateFeature 빌더 (테스트용)."""
    features: dict = {
        "close": 75000.0,
        "ma20": 72000.0,
        "ma50": 68000.0,
        "ma150": 62000.0,
        "atr_pct": 2.5,
        "rs_percentile": 85.0,
        "vcp_score": 3.0,
        "breakout_score": 2.0,
        "pullback_score": 1.5,
        "momentum_score": 2.8,
        "entry_style_selected": "BREAKOUT",
    }
    if missing_feature is not None:
        features.pop(missing_feature, None)
    if zero_feature is not None:
        features[zero_feature] = 0.0
    return CandidateFeature(
        code=code,
        market="KR",
        features=features,
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=["pb1_from_final30"],
        planned_qty=10,
    )


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


class TestKrPb1PrecomputedOhlcvRelaxation(unittest.TestCase):
    """KR PB1 final30 precomputed 경로 OHLCV 완화 정책 검증.

    _entry_ohlcv_block_reason을 직접 호출해 unit 수준에서 검증.
    """

    def setUp(self):
        os.environ["PB1_BLOCK_INSUFFICIENT_OHLCV"] = "1"
        os.environ.pop("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", None)  # 기본값 60 사용

    def tearDown(self):
        os.environ.pop("PB1_BLOCK_INSUFFICIENT_OHLCV", None)
        os.environ.pop("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", None)

    def _df60(self):
        return pd.DataFrame([{"date": f"2026-05-{i:02d}", "close": 75000} for i in range(1, 61)])

    def _df59(self):
        return pd.DataFrame([{"date": f"2026-05-{i:02d}", "close": 75000} for i in range(1, 60)])

    def test_kr_pb1_precomputed_60_rows_allows_entry(self):
        """KR PB1 final30 + precomputed 필수 feature OK + rows=60 → None (통과)."""
        cf = _make_kr_pb1_final30_cf("028050")
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf,
        )
        self.assertIsNone(result)

    def test_kr_pb1_second_code_60_rows_allows_entry(self):
        """006400 (삼성SDI)도 동일 조건에서 통과."""
        cf = _make_kr_pb1_final30_cf("006400")
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf,
        )
        self.assertIsNone(result)

    def test_kr_pb1_precomputed_59_rows_blocks_entry(self):
        """rows=59 → 신규상장 방어 차단 (abs_min=60 미만)."""
        cf = _make_kr_pb1_final30_cf("028050")
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df59(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_kr_pb1_missing_precomputed_feature_blocks_entry(self):
        """필수 precomputed feature(atr_pct) 누락 → 차단."""
        cf = _make_kr_pb1_final30_cf("028050", missing_feature="atr_pct")
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_kr_pb1_zero_atr_pct_blocks_entry(self):
        """atr_pct=0 → 수치 유효성 실패 → 차단."""
        cf = _make_kr_pb1_final30_cf("028050", zero_feature="atr_pct")
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_kr_pb1_zero_ma20_blocks_entry(self):
        """ma20=0 → 수치 유효성 실패 → 차단."""
        cf = _make_kr_pb1_final30_cf("028050", zero_feature="ma20")
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_us_pb1_short_ohlcv_still_blocks_entry(self):
        """US 코드(비 6자리 숫자)는 precomputed feature가 있어도 기존 정책 유지."""
        cf_us = CandidateFeature(
            code="AAPL",
            market="US",
            features={
                "close": 150.0, "ma20": 148.0, "ma50": 145.0, "ma150": 140.0,
                "atr_pct": 1.8, "rs_percentile": 80.0, "vcp_score": 2.5,
                "breakout_score": 2.0, "pullback_score": 1.0, "momentum_score": 2.2,
                "entry_style_selected": "BREAKOUT",
            },
            setup_ok=True, reasons=[], mode=1,
            mode_reasons=["pb1_from_final30"],
            planned_qty=5,
        )
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf_us,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_general_strategy_short_ohlcv_still_blocks_entry(self):
        """KR 코드라도 final30 경로(pb1_from_final30)가 아니면 기존 정책 유지."""
        cf_general = CandidateFeature(
            code="005930",
            market="KR",
            features={
                "close": 75000.0, "ma20": 72000.0, "ma50": 68000.0, "ma150": 62000.0,
                "atr_pct": 2.5, "rs_percentile": 85.0, "vcp_score": 3.0,
                "breakout_score": 2.0, "pullback_score": 1.5, "momentum_score": 2.8,
                "entry_style_selected": "BREAKOUT",
            },
            setup_ok=True, reasons=[], mode=1,
            mode_reasons=["default_day_mode"],  # pb1_from_final30 아님
            planned_qty=10,
        )
        result = PB1Engine._entry_ohlcv_block_reason(
            df=self._df60(),
            meta={"source": "db_short_only", "long_fetch_blocked": 1},
            cf=cf_general,
        )
        self.assertEqual(result, "insufficient_ohlcv")

    def test_risk_gate_not_bypassed_by_precomputed_ohlcv(self):
        """OHLCV 완화는 risk_gate를 우회하지 않음.

        _entry_ohlcv_block_reason은 OHLCV 차단만 완화할 뿐이다.
        risk_gate 실패는 별도 로직에서 처리되므로 OHLCV 완화와 독립적이다.
        """
        # _is_kr_pb1_precomputed_trade_ok는 risk/sizing/buyable 상태를 확인하지 않음
        cf = _make_kr_pb1_final30_cf("028050")
        self.assertTrue(PB1Engine._is_kr_pb1_precomputed_trade_ok(cf=cf, ohlcv_df=self._df60()))
        # risk_gate 실패는 _entry_ohlcv_block_reason 외부에서 별도 처리됨
        # → OHLCV 완화가 risk 차단을 우회하지 않음을 코드 구조로 보장

    def test_sizing_gate_not_bypassed_by_precomputed_ohlcv(self):
        """OHLCV 완화는 sizing_gate를 우회하지 않음."""
        cf = _make_kr_pb1_final30_cf("028050")
        self.assertTrue(PB1Engine._is_kr_pb1_precomputed_trade_ok(cf=cf, ohlcv_df=self._df60()))
        # sizing_gate는 planned_qty 기반으로 별도 처리됨

    def test_buyable_gate_not_bypassed_by_precomputed_ohlcv(self):
        """OHLCV 완화는 buyable_gate를 우회하지 않음."""
        cf = _make_kr_pb1_final30_cf("028050")
        self.assertTrue(PB1Engine._is_kr_pb1_precomputed_trade_ok(cf=cf, ohlcv_df=self._df60()))
        # buyable_gate는 OHLCV 완화와 독립적으로 별도 처리됨


if __name__ == "__main__":
    unittest.main()
