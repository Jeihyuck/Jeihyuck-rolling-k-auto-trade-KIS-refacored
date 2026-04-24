"""
tests/test_day_protect_exit_policy.py

_resolve_day_protect_exit 단위 테스트
"""
from __future__ import annotations
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from trader.pb1_engine import _resolve_day_protect_exit


def _pos(avg: float = 10000.0, qty: int = 10, orderable_qty: int = 10, meta: dict | None = None) -> dict:
    return {
        "avg_buy_price": avg,
        "qty": qty,
        "orderable_qty": orderable_qty,
        "position_meta": meta or {},
    }


class TestDayProtectHardStop:
    def test_hard_stop_triggers_full_exit(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_STOP_LOSS_PCT", "2.0")
        pos = _pos(avg=10000.0, orderable_qty=10)
        # -2.1% 하락
        result = _resolve_day_protect_exit(
            pos, mark=9790.0, now_hhmm=1000,
            ret_pct=-2.1, max_pnl_pct=0.0, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_STOP_LOSS"
        assert result["qty"] == 10

    def test_stop_hit_flag_triggers_regardless_of_pct(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_STOP_LOSS_PCT", "5.0")
        pos = _pos(orderable_qty=5)
        result = _resolve_day_protect_exit(
            pos, mark=9900.0, now_hhmm=1000,
            ret_pct=-0.5, max_pnl_pct=0.0, stop_hit=True
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_STOP_LOSS"

    def test_no_stop_below_threshold(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_STOP_LOSS_PCT", "2.0")
        pos = _pos(orderable_qty=10)
        result = _resolve_day_protect_exit(
            pos, mark=9850.0, now_hhmm=1000,
            ret_pct=-1.5, max_pnl_pct=0.0, stop_hit=False
        )
        assert result["exit_ok"] is False


class TestDayProtectTakeProfit:
    def test_partial_take_profit_at_target(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_TAKE_PROFIT_PCT", "4.0")
        monkeypatch.setenv("PB1_DAY_TAKE_PROFIT_SELL_PCT", "0.50")
        pos = _pos(orderable_qty=10, meta={"tp1_done": False})
        result = _resolve_day_protect_exit(
            pos, mark=10420.0, now_hhmm=1100,
            ret_pct=4.2, max_pnl_pct=4.2, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_TAKE_PROFIT_50"
        assert result["qty"] == 5
        assert result.get("update_meta", {}).get("tp1_done") is True

    def test_no_partial_when_tp1_already_done(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_TAKE_PROFIT_PCT", "4.0")
        pos = _pos(orderable_qty=5, meta={"tp1_done": True})
        result = _resolve_day_protect_exit(
            pos, mark=10500.0, now_hhmm=1100,
            ret_pct=5.0, max_pnl_pct=5.0, stop_hit=False
        )
        # tp1 완료 후 다시 트리거 안됨
        assert result.get("reason") != "EXIT_DAY_TAKE_PROFIT_50"


class TestDayProtectBreakevenProtect:
    def test_breakeven_triggered_after_profit_reversal(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_PROFIT_ARM_PCT", "1.5")
        monkeypatch.setenv("PB1_DAY_BREAKEVEN_PROTECT_PCT", "0.2")
        pos = _pos(orderable_qty=10)
        # max_pnl_pct=2.0 이상이었지만 현재 0.1%로 back
        result = _resolve_day_protect_exit(
            pos, mark=10010.0, now_hhmm=1100,
            ret_pct=0.1, max_pnl_pct=2.0, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_BREAKEVEN_PROTECT"

    def test_breakeven_not_triggered_if_arm_not_reached(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_PROFIT_ARM_PCT", "1.5")
        pos = _pos(orderable_qty=10)
        result = _resolve_day_protect_exit(
            pos, mark=10010.0, now_hhmm=1100,
            ret_pct=0.1, max_pnl_pct=1.0, stop_hit=False  # max_pnl < arm
        )
        assert result["exit_ok"] is False


class TestDayProtectTrailProtect:
    def test_trail_protect_triggered(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_TRAIL_ARM_PCT", "2.0")
        monkeypatch.setenv("PB1_DAY_TRAIL_DROP_PCT", "1.0")
        pos = _pos(orderable_qty=10)
        # max_pnl=3.0 reached, then drop to 1.5 (drop=1.5 >= drop threshold 1.0)
        result = _resolve_day_protect_exit(
            pos, mark=10150.0, now_hhmm=1200,
            ret_pct=1.5, max_pnl_pct=3.0, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_TRAIL_PROTECT"


class TestDayProtectClose:
    def test_force_close_at_1520(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_FORCE_EXIT_TIME", "15:20")
        pos = _pos(orderable_qty=7)
        result = _resolve_day_protect_exit(
            pos, mark=10000.0, now_hhmm=1520,
            ret_pct=0.0, max_pnl_pct=0.0, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_FORCE_CLOSE"

    def test_close_protect_profit_at_1505(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_CLOSE_PROTECT_TIME", "15:05")
        pos = _pos(orderable_qty=8)
        result = _resolve_day_protect_exit(
            pos, mark=10100.0, now_hhmm=1510,
            ret_pct=1.0, max_pnl_pct=1.0, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_CLOSE_PROFIT_PROTECT"

    def test_close_loss_cut_at_1505(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_CLOSE_PROTECT_TIME", "15:05")
        pos = _pos(orderable_qty=8)
        result = _resolve_day_protect_exit(
            pos, mark=9880.0, now_hhmm=1510,
            ret_pct=-1.2, max_pnl_pct=0.0, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_DAY_CLOSE_LOSS_CUT"

    def test_hold_before_close_time(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_CLOSE_PROTECT_TIME", "15:05")
        monkeypatch.setenv("PB1_DAY_FORCE_EXIT_TIME", "15:20")
        monkeypatch.setenv("PB1_DAY_STOP_LOSS_PCT", "2.0")
        pos = _pos(orderable_qty=10)
        result = _resolve_day_protect_exit(
            pos, mark=10000.0, now_hhmm=1400,
            ret_pct=0.0, max_pnl_pct=0.0, stop_hit=False
        )
        assert result["exit_ok"] is False
        assert result["reason"] == "DAY_HOLD_PROFIT_OK"


class TestDayProtectDisabled:
    def test_disabled_returns_no_exit(self, monkeypatch):
        monkeypatch.setenv("PB1_DAY_PROTECT_ENABLED", "0")
        pos = _pos(orderable_qty=10)
        result = _resolve_day_protect_exit(
            pos, mark=9000.0, now_hhmm=1520,
            ret_pct=-10.0, max_pnl_pct=0.0, stop_hit=True
        )
        # 비활성화 시 stop_hit/force_close보다 먼저 disabled 체크
        assert result["reason"] == "DAY_PROTECT_DISABLED"
