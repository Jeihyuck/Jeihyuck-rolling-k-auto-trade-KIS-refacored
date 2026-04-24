"""
tests/test_entry_horizon_classification.py

_classify_trade_horizon, _horizon_to_exit_family, _resolve_position_horizon
단위 테스트
"""
from __future__ import annotations
import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

# ---------------------------------------------------------------------------
# 모듈에서 직접 import
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from trader.pb1_engine import (
    _classify_trade_horizon,
    _horizon_to_exit_family,
    _resolve_position_horizon,
    _calculate_exit_qty,
)


# ---------------------------------------------------------------------------
# _classify_trade_horizon
# ---------------------------------------------------------------------------
class TestClassifyTradeHorizon:
    def test_breakout_entry_returns_day_protect(self):
        features = {"entry_style_selected": "ENTRY_BREAKOUT"}
        assert _classify_trade_horizon(features) == "DAY_PROTECT"

    def test_momentum_entry_returns_day_protect(self):
        features = {"entry_style_selected": "ENTRY_MOMENTUM"}
        assert _classify_trade_horizon(features) == "DAY_PROTECT"

    def test_open_push_returns_day_protect(self):
        features = {"entry_style_selected": "ENTRY_OPEN_PUSH"}
        assert _classify_trade_horizon(features) == "DAY_PROTECT"

    def test_breakout_signal_high_atr_returns_day_protect(self):
        features = {"entry_style_selected": "OTHER", "breakout_signal": True, "atr_pct": 6.0}
        assert _classify_trade_horizon(features) == "DAY_PROTECT"

    def test_breakout_signal_low_atr_falls_through(self):
        # atr_pct < 5 이면 DAY_PROTECT 아님
        features = {"entry_style_selected": "OTHER", "breakout_signal": True, "atr_pct": 3.0,
                    "trend_template_ok": False, "score_final": 60}
        result = _classify_trade_horizon(features)
        assert result in {"SWING_CARRY", "CORE_CARRY"}

    def test_pullback_entry_returns_swing(self):
        features = {"entry_style_selected": "ENTRY_PULLBACK"}
        assert _classify_trade_horizon(features) == "SWING_CARRY"

    def test_vcp_entry_returns_swing(self):
        features = {"entry_style_selected": "ENTRY_VCP"}
        assert _classify_trade_horizon(features) == "SWING_CARRY"

    def test_minervini_entry_returns_swing(self):
        features = {"entry_style_selected": "ENTRY_MINERVINI"}
        assert _classify_trade_horizon(features) == "SWING_CARRY"

    def test_high_vcp_score_trend_ok_returns_swing(self):
        features = {"trend_template_ok": True, "vcp_score": 50, "score_final": 70}
        assert _classify_trade_horizon(features) == "SWING_CARRY"

    def test_high_score_trend_ok_low_atr_returns_core(self):
        features = {
            "score_final": 90,
            "trend_template_ok": True,
            "atr_pct": 3.0,
            "entry_style_selected": "",
        }
        assert _classify_trade_horizon(features) == "CORE_CARRY"

    def test_high_score_high_atr_not_core(self):
        # atr_pct > 4.0 이면 CORE_CARRY 제외
        features = {
            "score_final": 90,
            "trend_template_ok": True,
            "atr_pct": 5.0,
            "entry_style_selected": "",
        }
        result = _classify_trade_horizon(features)
        assert result != "CORE_CARRY"

    def test_default_returns_swing(self):
        # 아무 조건도 해당 안하면 SWING_CARRY
        features = {}
        assert _classify_trade_horizon(features) == "SWING_CARRY"

    def test_case_insensitive_entry_style(self):
        features = {"entry_style_selected": "entry_breakout"}
        assert _classify_trade_horizon(features) == "DAY_PROTECT"

    def test_entry_reason_fallback(self):
        # entry_style_selected 없을 때 entry_reason 사용
        features = {"entry_reason": "ENTRY_PULLBACK"}
        assert _classify_trade_horizon(features) == "SWING_CARRY"


# ---------------------------------------------------------------------------
# _horizon_to_exit_family
# ---------------------------------------------------------------------------
class TestHorizonToExitFamily:
    def test_day_protect_maps_to_intraday(self):
        assert _horizon_to_exit_family("DAY_PROTECT") == "INTRADAY_PROFIT_PROTECT"

    def test_swing_carry_maps_to_swing_staged(self):
        assert _horizon_to_exit_family("SWING_CARRY") == "SWING_STAGED_EXIT"

    def test_core_carry_maps_to_core_trend(self):
        assert _horizon_to_exit_family("CORE_CARRY") == "CORE_TREND_FOLLOW"

    def test_unknown_returns_default(self):
        assert _horizon_to_exit_family("UNKNOWN") == "SWING_STAGED_EXIT"


# ---------------------------------------------------------------------------
# _resolve_position_horizon
# ---------------------------------------------------------------------------
class TestResolvePositionHorizon:
    def test_reads_from_position_meta(self):
        pos = {"position_meta": {"trade_horizon": "CORE_CARRY"}}
        assert _resolve_position_horizon(pos) == "CORE_CARRY"

    def test_reads_from_entry_meta_json(self):
        pos = {"entry_meta_json": {"trade_horizon": "DAY_PROTECT"}}
        assert _resolve_position_horizon(pos) == "DAY_PROTECT"

    def test_position_meta_takes_priority(self):
        pos = {
            "position_meta": {"trade_horizon": "CORE_CARRY"},
            "entry_meta_json": {"trade_horizon": "DAY_PROTECT"},
        }
        assert _resolve_position_horizon(pos) == "CORE_CARRY"

    def test_today_entry_fallback_day_protect(self):
        today = datetime.now(ZoneInfo("Asia/Seoul")).date()
        pos = {"entry_date": today.isoformat()}
        result = _resolve_position_horizon(pos, now_kst_date=today)
        assert result == "DAY_PROTECT"

    def test_old_entry_fallback_swing(self):
        pos = {"entry_date": "2020-01-01"}
        import datetime as _dt
        result = _resolve_position_horizon(pos, now_kst_date=_dt.date(2024, 6, 15))
        assert result == "SWING_CARRY"

    def test_no_meta_no_date_returns_swing(self):
        pos = {}
        assert _resolve_position_horizon(pos) == "SWING_CARRY"

    def test_invalid_meta_returns_swing(self):
        pos = {"position_meta": "bad_string"}
        result = _resolve_position_horizon(pos)
        assert result == "SWING_CARRY"

    def test_valid_json_string_in_position_meta(self):
        import json
        meta = json.dumps({"trade_horizon": "SWING_CARRY"})
        pos = {"position_meta": meta}
        assert _resolve_position_horizon(pos) == "SWING_CARRY"


# ---------------------------------------------------------------------------
# _calculate_exit_qty
# ---------------------------------------------------------------------------
class TestCalculateExitQty:
    def test_full_exit_when_sell_pct_none(self):
        assert _calculate_exit_qty(10, 8, None) == 8

    def test_partial_exit_50_pct(self):
        assert _calculate_exit_qty(10, 10, 0.5) == 5

    def test_partial_exit_33_pct_floors(self):
        qty = _calculate_exit_qty(10, 9, 0.33)
        assert qty == 2  # floor(9 * 0.33) = 2

    def test_minimum_one_when_small_qty(self):
        # orderable=1, 50% -> floor(0.5) = 0 -> 최소 1
        assert _calculate_exit_qty(1, 1, 0.5) == 1

    def test_zero_orderable_returns_zero(self):
        assert _calculate_exit_qty(5, 0, 0.5) == 0

    def test_cannot_exceed_orderable(self):
        # holding=100, orderable=5, 100% -> capped at 5
        qty = _calculate_exit_qty(100, 5, 1.0)
        assert qty == 5
