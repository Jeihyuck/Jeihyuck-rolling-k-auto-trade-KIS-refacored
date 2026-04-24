"""
tests/test_swing_staged_exit_policy.py

_resolve_swing_staged_exit 단위 테스트
"""
from __future__ import annotations
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from trader.pb1_engine import _resolve_swing_staged_exit


def _pos(
    avg: float = 10000.0,
    qty: int = 10,
    orderable_qty: int = 10,
    initial_stop: float = 9500.0,
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


class TestSwingInitialStop:
    def test_initial_stop_hit_by_price(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0)
        result = _resolve_swing_staged_exit(
            pos, mark=9480.0, ma20=None,
            ret_pct=-5.2, days_held=3, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_INITIAL_STOP"
        assert result["qty"] == 10

    def test_initial_stop_by_flag(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
        pos = _pos(avg=10000.0, orderable_qty=8, initial_stop=9500.0)
        result = _resolve_swing_staged_exit(
            pos, mark=9510.0, ma20=None,
            ret_pct=-4.9, days_held=2, stop_hit=True
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_INITIAL_STOP"

    def test_above_stop_no_exit(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0)
        result = _resolve_swing_staged_exit(
            pos, mark=9600.0, ma20=None,
            ret_pct=-4.0, days_held=2, stop_hit=False
        )
        assert result["exit_ok"] is False


class TestSwingTP1:
    def test_tp1_triggered_at_2r(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
        monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
        # avg=10000, stop=9500 -> risk=500 / share
        # 2R -> mark = 10000 + 500*2 = 11000
        pos = _pos(avg=10000.0, orderable_qty=9, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": False})
        result = _resolve_swing_staged_exit(
            pos, mark=11050.0, ma20=None,
            ret_pct=10.5, days_held=5, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_TP1"
        assert result["qty"] == 2  # floor(9*0.33)=2
        assert result["update_meta"]["tp1_done"] is True

    def test_tp1_not_repeated(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
        pos = _pos(avg=10000.0, orderable_qty=6, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": True})
        result = _resolve_swing_staged_exit(
            pos, mark=11100.0, ma20=None,
            ret_pct=11.0, days_held=7, stop_hit=False
        )
        assert result.get("reason") != "EXIT_SWING_TP1"


class TestSwingTP2:
    def test_tp2_triggered_after_tp1(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_TP2_R", "3.0")
        monkeypatch.setenv("PB1_SWING_TP2_SELL_PCT", "0.33")
        # 3R -> mark = 10000 + 500*3 = 11500
        pos = _pos(avg=10000.0, orderable_qty=6, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": True, "tp2_done": False})
        result = _resolve_swing_staged_exit(
            pos, mark=11550.0, ma20=None,
            ret_pct=15.5, days_held=8, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_TP2"
        assert result["update_meta"]["tp2_done"] is True

    def test_tp2_skipped_if_tp1_not_done(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_TP2_R", "3.0")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": False, "tp2_done": False})
        result = _resolve_swing_staged_exit(
            pos, mark=11600.0, ma20=None,
            ret_pct=16.0, days_held=8, stop_hit=False
        )
        # tp1 없으면 tp2 도달해도 tp1 먼저
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_TP1"


class TestSwingRunnerMA20:
    def test_runner_exits_on_ma20_break_after_tp1(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
        pos = _pos(avg=10000.0, orderable_qty=4, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": True, "tp2_done": False})
        result = _resolve_swing_staged_exit(
            pos, mark=10100.0, ma20=10200.0,
            ret_pct=1.0, days_held=10, stop_hit=False
        )
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_RUNNER_MA20_BREAK"

    def test_no_ma20_exit_before_tp1(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": False})
        # mark < ma20 but tp1 not done
        result = _resolve_swing_staged_exit(
            pos, mark=9900.0, ma20=10000.0,
            ret_pct=-1.0, days_held=3, stop_hit=False
        )
        # ma20 break 전의 단계: stop 여부만 체크
        assert result.get("reason") not in {"EXIT_SWING_RUNNER_MA20_BREAK"}


class TestSwingTimeStop:
    def test_time_stop_triggered(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": False},
                   holding_days=12)
        # current_r < 1.0 (not making enough progress)
        result = _resolve_swing_staged_exit(
            pos, mark=10200.0, ma20=None,
            ret_pct=2.0, days_held=12, stop_hit=False
        )
        # 2% / 500 = 0.4R < 1.0 -> time stop
        assert result["exit_ok"] is True
        assert result["reason"] == "EXIT_SWING_TIME_STOP"

    def test_no_time_stop_if_positive_r(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0, "tp1_done": False})
        # 1.5R → no time stop
        result = _resolve_swing_staged_exit(
            pos, mark=10750.0, ma20=None,
            ret_pct=7.5, days_held=12, stop_hit=False
        )
        assert result.get("reason") != "EXIT_SWING_TIME_STOP"


class TestSwingHold:
    def test_hold_when_all_conditions_negative(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
        monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")
        pos = _pos(avg=10000.0, orderable_qty=10, initial_stop=9500.0,
                   meta={"initial_stop_price": 9500.0}, holding_days=3)
        result = _resolve_swing_staged_exit(
            pos, mark=10100.0, ma20=9900.0,
            ret_pct=1.0, days_held=3, stop_hit=False
        )
        assert result["exit_ok"] is False
        assert result["reason"] == "SWING_HOLD_TREND_OK"

    def test_disabled_returns_skip(self, monkeypatch):
        monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "0")
        pos = _pos(orderable_qty=10)
        result = _resolve_swing_staged_exit(
            pos, mark=9000.0, ma20=None,
            ret_pct=-10.0, days_held=20, stop_hit=True
        )
        assert result["exit_ok"] is False
        assert result["reason"] == "SWING_STAGED_EXIT_DISABLED"
