"""
tests/test_core_carry_exit_policy.py

_resolve_core_trend_follow_exit 단위 테스트
"""
from __future__ import annotations
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from trader.pb1_engine import _resolve_core_trend_follow_exit


def _pos(
    avg: float = 10000.0,
    qty: int = 10,
    orderable_qty: int = 10,
    initial_stop: float = 9200.0,
    meta: dict | None = None,
    holding_days: int = 0,
) -> dict:
    return {
        "avg_buy_price": avg,
        "qty": qty,
        "orderable_qty": orderable_qty,
        "holding_days": holding_days,
        "position_meta": meta or {"initial_stop_price": initial_stop},
    }


class TestCoreHardStop:
    def test_hard_stop_triggers_at_threshold(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_HARD_STOP_PCT", "8.0")
        pos = _pos(orderable_qty=10)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9100.0, ma20=None, ma50=None,
            ret_pct=-9.0, days_held=5, regime=""
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_CORE_HARD_STOP"
        assert result["qty"] == 10

    def test_no_hard_stop_within_threshold(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_HARD_STOP_PCT", "8.0")
        pos = _pos(orderable_qty=10)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9300.0, ma20=None, ma50=None,
            ret_pct=-7.0, days_held=5, regime=""
        )
        assert result.get("reason") != "EXIT_CORE_HARD_STOP"


class TestCoreTakeProfit:
    def test_tp1_partial_at_3r(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_TP1_R", "3.0")
        monkeypatch.setenv("PB1_CORE_TP1_SELL_PCT", "0.25")
        # avg=10000, stop=9200 → risk=800/share, 3R → mark=12400+
        pos = _pos(avg=10000.0, orderable_qty=8, initial_stop=9200.0,
                   meta={"initial_stop_price": 9200.0, "core_tp1_done": False})
        result = _resolve_core_trend_follow_exit(
            pos, mark=12500.0, ma20=None, ma50=None,
            ret_pct=25.0, days_held=10, regime=""
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_CORE_TP1"
        assert result["qty"] == 2  # floor(8*0.25)=2
        assert result["update_meta"]["core_tp1_done"] is True

    def test_tp1_not_repeated(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_TP1_R", "3.0")
        pos = _pos(avg=10000.0, orderable_qty=6, initial_stop=9200.0,
                   meta={"initial_stop_price": 9200.0, "core_tp1_done": True})
        result = _resolve_core_trend_follow_exit(
            pos, mark=12600.0, ma20=None, ma50=None,
            ret_pct=26.0, days_held=15, regime=""
        )
        assert result.get("reason") != "EXIT_CORE_TP1"

    def test_tp1_also_triggered_via_tp1_done_flag(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_TP1_R", "3.0")
        # tp1_done=True를 meta에 세팅하면 core_tp1_done 대체 가능
        pos = _pos(avg=10000.0, orderable_qty=6, initial_stop=9200.0,
                   meta={"initial_stop_price": 9200.0, "tp1_done": True, "core_tp1_done": False})
        result = _resolve_core_trend_follow_exit(
            pos, mark=12600.0, ma20=None, ma50=None,
            ret_pct=26.0, days_held=15, regime=""
        )
        # core_tp1_done = core_tp1_done OR tp1_done → True → skip
        assert result.get("reason") != "EXIT_CORE_TP1"


class TestCoreMA50Break:
    def test_ma50_break_exits(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9800.0, ma20=None, ma50=10000.0,
            ret_pct=-2.0, days_held=12, regime=""
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_CORE_MA50_BREAK"

    def test_no_ma50_exit_when_above(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        result = _resolve_core_trend_follow_exit(
            pos, mark=10200.0, ma20=None, ma50=10000.0,
            ret_pct=2.0, days_held=12, regime=""
        )
        assert result.get("reason") != "EXIT_CORE_MA50_BREAK"

    def test_no_ma50_exit_when_none(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9500.0, ma20=None, ma50=None,
            ret_pct=-5.0, days_held=12, regime=""
        )
        assert result.get("reason") != "EXIT_CORE_MA50_BREAK"


class TestCoreRiskOff:
    def test_bear_regime_ma20_and_ma50_both_broken(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9600.0, ma20=9800.0, ma50=9700.0,
            ret_pct=-4.0, days_held=15, regime="BEAR"
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_CORE_RISK_OFF"

    def test_bear_but_only_ma20_broken(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        # mark < ma20 but mark > ma50
        result = _resolve_core_trend_follow_exit(
            pos, mark=9900.0, ma20=10000.0, ma50=9800.0,
            ret_pct=-1.0, days_held=15, regime="BEAR"
        )
        # bear + ma20 break → but ma50 not broken (9900 > 9800) → no RISK_OFF
        assert result.get("reason") != "EXIT_CORE_RISK_OFF"

    def test_no_risk_off_in_bull_regime(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9600.0, ma20=9800.0, ma50=9700.0,
            ret_pct=-4.0, days_held=15, regime="BULL"
        )
        # BULL 레짐이므로 risk-off 아님; MA50 이탈로 처리
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_CORE_MA50_BREAK"

    def test_downtrend_regime_treated_as_bear(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        pos = _pos(orderable_qty=8)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9600.0, ma20=9800.0, ma50=9700.0,
            ret_pct=-4.0, days_held=15, regime="DOWNTREND"
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_CORE_RISK_OFF"


class TestCoreHold:
    def test_hold_in_uptrend(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "1")
        monkeypatch.setenv("PB1_CORE_HARD_STOP_PCT", "8.0")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9200.0,
                   meta={"initial_stop_price": 9200.0})
        result = _resolve_core_trend_follow_exit(
            pos, mark=10500.0, ma20=10200.0, ma50=9900.0,
            ret_pct=5.0, days_held=10, regime="BULL"
        )
        assert result["exit_ok"] is False
        assert result["reason"] == "CORE_HOLD_TREND_OK"

    def test_disabled_returns_skip(self, monkeypatch):
        monkeypatch.setenv("PB1_CORE_EXIT_ENABLED", "0")
        pos = _pos(orderable_qty=10)
        result = _resolve_core_trend_follow_exit(
            pos, mark=9000.0, ma20=None, ma50=None,
            ret_pct=-50.0, days_held=30, regime="BEAR"
        )
        assert result["exit_ok"] is False
        assert result["reason"] == "CORE_EXIT_DISABLED"
