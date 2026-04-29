"""
tests/test_swing_staged_exit_live_contract.py

_resolve_swing_staged_exit 라이브/통합 계약 검증:
- R-based TP 로그([EXIT][SWING][R_CTX], [EXIT][SWING][TP_CHECK]) 소스 확인
- TP1(2R) / TP2(3R) 계산이 entry_meta의 r_value 기반인지 확인
- 로그가 포함된 채 올바른 exit 결정을 내리는지 확인
- position_meta 업데이트 필드 확인
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from trader.pb1_engine import _resolve_swing_staged_exit


ENGINE_SRC = Path(__file__).parent.parent / "trader" / "pb1_engine.py"


# ── 소스 수준 계약 검증 ───────────────────────────────────────────────────────

def test_r_ctx_log_in_source():
    """_resolve_swing_staged_exit 소스에 [EXIT][SWING][R_CTX] 로그가 있어야 함."""
    src = ENGINE_SRC.read_text()
    assert re.search(r"\[EXIT\]\[SWING\]\[R_CTX\]", src), (
        "[EXIT][SWING][R_CTX] log must be present in _resolve_swing_staged_exit"
    )


def test_tp_check_log_in_source():
    """_resolve_swing_staged_exit에 [EXIT][SWING][TP_CHECK] 로그가 있어야 함."""
    src = ENGINE_SRC.read_text()
    assert re.search(r"\[EXIT\]\[SWING\]\[TP_CHECK\]", src), (
        "[EXIT][SWING][TP_CHECK] log must be present in _resolve_swing_staged_exit"
    )


def test_tp1_check_before_tp2_check_in_source():
    src = ENGINE_SRC.read_text()
    tp1_pos = src.find("TP1_HIT")
    tp2_pos = src.find("TP2_HIT")
    assert tp1_pos != -1 and tp2_pos != -1
    assert tp1_pos < tp2_pos, "TP1 check must precede TP2 check"


# ── R-based TP 동작 검증 ──────────────────────────────────────────────────────

def _pos(
    avg: float = 10000.0,
    initial_stop: float = 9500.0,
    orderable_qty: int = 10,
    tp1_done: bool = False,
    tp2_done: bool = False,
) -> dict:
    r = avg - initial_stop
    return {
        "avg_buy_price": avg,
        "orderable_qty": orderable_qty,
        "qty": orderable_qty,
        "position_meta": {
            "initial_stop_price": initial_stop,
            "r_value": r,
            "tp1_done": tp1_done,
            "tp2_done": tp2_done,
            "current_stop_price": initial_stop,
        },
    }


def test_tp1_triggers_at_2r(monkeypatch):
    """avg=10000, stop=9500 → R=500, TP1 at 11000 (2R)."""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    # R-based TP1을 격리 테스트하기 위해 profit_protect/abs_tp1 비활성화
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _pos(avg=10000.0, initial_stop=9500.0)
    # mark = 11000 = entry + 2*R → current_r = (11000 - 10000) / 500 = 2.0
    result = _resolve_swing_staged_exit(
        pos, mark=11000.0, ma20=None,
        ret_pct=10.0, days_held=3, stop_hit=False,
    )
    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_TP1"
    assert "update_meta" in result
    assert result["update_meta"]["tp1_done"] is True
    assert result["update_meta"]["current_stop_price"] >= 10000.0  # breakeven 이상


def test_tp1_not_triggered_below_2r(monkeypatch):
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _pos(avg=10000.0, initial_stop=9500.0)
    # mark = 10900 → r = 1.8 < 2.0
    result = _resolve_swing_staged_exit(
        pos, mark=10900.0, ma20=None,
        ret_pct=9.0, days_held=3, stop_hit=False,
    )
    assert result["exit_ok"] is False


def test_tp2_triggers_at_3r(monkeypatch):
    """TP1 이미 처리됨, TP2 at 3R."""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TP2_R", "3.0")
    monkeypatch.setenv("PB1_SWING_TP2_SELL_PCT", "0.33")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _pos(avg=10000.0, initial_stop=9500.0, tp1_done=True)
    # mark = 11500 → current_r = (11500 - 10000) / 500 = 3.0
    result = _resolve_swing_staged_exit(
        pos, mark=11500.0, ma20=None,
        ret_pct=15.0, days_held=5, stop_hit=False,
    )
    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_TP2"
    assert result["update_meta"]["tp2_done"] is True


def test_tp1_not_retriggered_when_done(monkeypatch):
    """tp1_done=True이면 2R 이상이어도 TP1을 다시 트리거하지 않음."""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_SWING_TP2_R", "3.0")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _pos(avg=10000.0, initial_stop=9500.0, tp1_done=True, tp2_done=True)
    # mark = 11000 (2R) but both done
    result = _resolve_swing_staged_exit(
        pos, mark=11000.0, ma20=None,
        ret_pct=10.0, days_held=3, stop_hit=False,
    )
    assert result.get("reason") != "EXIT_SWING_TP1"
    assert result.get("reason") != "EXIT_SWING_TP2"


def test_ma20_runner_exit_after_tp1(monkeypatch):
    """TP1 완료 후 MA20 이탈 시 청산."""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _pos(avg=10000.0, initial_stop=9500.0, tp1_done=True)
    result = _resolve_swing_staged_exit(
        pos, mark=10200.0, ma20=10300.0,
        ret_pct=2.0, days_held=7, stop_hit=False,
    )
    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_RUNNER_MA20_BREAK"


def test_time_stop_triggers(monkeypatch):
    """보유일 >= time_stop_days 이고 R < 1.0이면 청산."""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _pos(avg=10000.0, initial_stop=9500.0)
    result = _resolve_swing_staged_exit(
        pos, mark=10300.0, ma20=None,
        ret_pct=3.0, days_held=10, stop_hit=False,
    )
    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_TIME_STOP"


def test_disabled_returns_exit_false(monkeypatch):
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "0")
    pos = _pos(avg=10000.0, initial_stop=9500.0)
    result = _resolve_swing_staged_exit(
        pos, mark=11000.0, ma20=None,
        ret_pct=10.0, days_held=3, stop_hit=False,
    )
    assert result["exit_ok"] is False
    assert result["reason"] == "SWING_STAGED_EXIT_DISABLED"
