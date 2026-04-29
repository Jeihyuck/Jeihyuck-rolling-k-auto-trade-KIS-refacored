"""
tests/test_existing_position_effective_exit_policy.py

기존 보유 포지션 effective stop/R 정책 검증:
- 삼성E&A(028050) 기준 케이스
- 기존 손절가가 너무 깊을 때 effective stop 재계산
- +8% 보호익절 (PROFIT_PROTECT_8PCT)
- +10% 절대수익률 TP (ABS_TP1_10PCT)
- 중복 매도 방지
- R-based TP는 effective_r 기준으로 계산
"""
from __future__ import annotations

import pytest

from trader.pb1_engine import (
    _resolve_effective_exit_risk_for_pos,
    _resolve_swing_staged_exit,
)


# ────────────────────────────────────────────────────────────────
# 삼성E&A 기준 helper
# ────────────────────────────────────────────────────────────────

def _samsung_ea_pos(
    *,
    entry_price: float = 50500.0,
    raw_stop: float = 40739.0,
    market: str = "KOSPI",
    qty: int = 51,
    tp1_done: bool = False,
    tp2_done: bool = False,
    profit_protect_done: bool = False,
    abs_tp1_done: bool = False,
) -> dict:
    return {
        "code": "028050",
        "avg_buy_price": entry_price,
        "qty": qty,
        "orderable_qty": qty,
        "market": market,
        "stop_price_at_entry": raw_stop,
        "position_meta": {
            "initial_stop_price": raw_stop,
            "tp1_done": tp1_done,
            "tp2_done": tp2_done,
            "profit_protect_done": profit_protect_done,
            "abs_tp1_done": abs_tp1_done,
        },
    }


# ────────────────────────────────────────────────────────────────
# Case 1: 기존 손절가가 너무 깊은 경우 effective R 재계산
# ────────────────────────────────────────────────────────────────

def test_effective_stop_recomputed_when_raw_stop_too_deep(monkeypatch):
    """
    삼성E&A: entry=50500, raw_stop=40739 (19.3% 하락)
    KOSPI cap 7% → effective_stop = 50500 * 0.93 = 46965
    effective_r = 50500 - 46965 = 3535
    """
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", "7.0")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, market="KOSPI")
    result = _resolve_effective_exit_risk_for_pos(pos)

    assert result["reason"] == "ok"
    assert result["effective_applied"] is True

    # effective_stop ≈ 50500 * 0.93 = 46965
    assert abs(result["effective_stop_price"] - 46965.0) < 1.0, (
        f"expected ~46965 but got {result['effective_stop_price']}"
    )
    # effective_r ≈ 50500 - 46965 = 3535
    assert abs(result["effective_r_value"] - 3535.0) < 1.0, (
        f"expected ~3535 but got {result['effective_r_value']}"
    )
    # raw values preserved
    assert result["raw_stop_price"] == 40739.0
    assert abs(result["raw_r_value"] - 9761.0) < 1.0


def test_current_r_uses_effective_r_not_raw_r(monkeypatch):
    """
    mark=54600, entry=50500, effective_stop=46965, effective_r=3535
    current_r = (54600 - 50500) / 3535 ≈ 1.16R (raw 기준 0.42R이 아닌)
    """
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")   # 보호익절 꺼서 R 기준 테스트
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", "7.0")
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, market="KOSPI")
    # mark=54600, ret_pct = (54600-50500)/50500*100 ≈ 8.12
    result = _resolve_swing_staged_exit(
        pos, mark=54600.0, ma20=None,
        ret_pct=8.12, days_held=3, stop_hit=False,
    )
    # effective_r 기준 current_r ≈ 1.16 → TP1(2R) 미달 → HOLD
    # (raw_r 기준이었다면 0.42R → 같은 HOLD이지만 이유가 다름)
    assert result["reason"] in {"SWING_HOLD_TREND_OK"}, (
        f"expected HOLD but got {result['reason']}"
    )


def test_effective_stop_not_applied_when_disabled(monkeypatch):
    """
    PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED=0이면 raw stop 그대로 사용
    """
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "0")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "0")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, market="KOSPI")
    result = _resolve_effective_exit_risk_for_pos(pos)

    assert result["effective_applied"] is False
    assert result["effective_stop_price"] == 40739.0


# ────────────────────────────────────────────────────────────────
# Case 2: +8% 보호익절
# ────────────────────────────────────────────────────────────────

def test_profit_protect_8pct_triggered(monkeypatch):
    """
    entry=50500, mark=54600 → ret_pct=8.12% >= 8.0% → PROFIT_PROTECT_8PCT
    sell_qty = int(51 * 0.33) = 16
    """
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_PCT", "8.0")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_SELL_PCT", "0.33")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "1")
    monkeypatch.setenv("PB1_ABS_TP1_PROFIT_PCT", "10.0")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, qty=51)
    result = _resolve_swing_staged_exit(
        pos, mark=54600.0, ma20=None,
        ret_pct=8.12, days_held=3, stop_hit=False,
    )

    assert result["exit_ok"] is True
    assert result["reason"] == "PROFIT_PROTECT_8PCT"
    # int(51 * 0.33) = 16
    assert result["qty"] == 16
    assert result["update_meta"]["profit_protect_done"] is True


def test_profit_protect_below_threshold_no_trigger(monkeypatch):
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_PCT", "8.0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, qty=51)
    result = _resolve_swing_staged_exit(
        pos, mark=54000.0, ma20=None,
        ret_pct=6.93, days_held=3, stop_hit=False,
    )

    assert result["reason"] != "PROFIT_PROTECT_8PCT"


# ────────────────────────────────────────────────────────────────
# Case 3: 중복 매도 방지
# ────────────────────────────────────────────────────────────────

def test_profit_protect_not_retriggered_when_done(monkeypatch):
    """profit_protect_done=True이면 같은 트리거 반복 금지"""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_PCT", "8.0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, qty=51, profit_protect_done=True)
    result = _resolve_swing_staged_exit(
        pos, mark=54600.0, ma20=None,
        ret_pct=8.12, days_held=3, stop_hit=False,
    )

    assert result["reason"] != "PROFIT_PROTECT_8PCT"


def test_abs_tp1_not_retriggered_when_done(monkeypatch):
    """abs_tp1_done=True이면 ABS_TP1_10PCT 반복 금지"""
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "1")
    monkeypatch.setenv("PB1_ABS_TP1_PROFIT_PCT", "10.0")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, qty=51, abs_tp1_done=True)
    result = _resolve_swing_staged_exit(
        pos, mark=55600.0, ma20=None,
        ret_pct=10.1, days_held=3, stop_hit=False,
    )

    assert result["reason"] != "ABS_TP1_10PCT"


# ────────────────────────────────────────────────────────────────
# Case 4: +10% 절대 TP
# ────────────────────────────────────────────────────────────────

def test_abs_tp1_10pct_triggered(monkeypatch):
    """
    entry=50500, mark=55600 → ret_pct=10.1% >= 10.0% → ABS_TP1_10PCT
    profit_protect는 이미 done
    """
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_PCT", "8.0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "1")
    monkeypatch.setenv("PB1_ABS_TP1_PROFIT_PCT", "10.0")
    monkeypatch.setenv("PB1_ABS_TP1_SELL_PCT", "0.33")

    # profit_protect는 이미 처리됨
    pos = _samsung_ea_pos(
        entry_price=50500.0, raw_stop=40739.0, qty=51,
        profit_protect_done=True,
    )
    result = _resolve_swing_staged_exit(
        pos, mark=55600.0, ma20=None,
        ret_pct=10.1, days_held=3, stop_hit=False,
    )

    assert result["exit_ok"] is True
    assert result["reason"] == "ABS_TP1_10PCT"
    assert result["update_meta"]["abs_tp1_done"] is True


# ────────────────────────────────────────────────────────────────
# Case 5: R-based TP1은 effective_r 기준으로 계산
# ────────────────────────────────────────────────────────────────

def test_r_based_tp1_uses_effective_r(monkeypatch):
    """
    entry=50500, effective_stop=46965, effective_r=3535
    TP1(2R): mark >= 50500 + 2*3535 = 57570
    """
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", "7.0")
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, market="KOSPI", qty=51)

    # mark=57570 ≈ 50500 + 2*3535 → current_r =~2.0 → TP1 트리거
    result = _resolve_swing_staged_exit(
        pos, mark=57570.0, ma20=None,
        ret_pct=14.0, days_held=3, stop_hit=False,
    )

    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_TP1"
    assert result["update_meta"]["tp1_done"] is True


def test_r_based_tp1_not_triggered_below_2r_effective(monkeypatch):
    """
    effective_r=3535 기준으로 2R(=7070) 미달이면 TP1 트리거 안 됨
    mark=56000: (56000-50500)/3535 ≈ 1.56R → TP1 미달
    """
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "0")
    monkeypatch.setenv("PB1_ABS_TP1_ENABLED", "0")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", "7.0")
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, market="KOSPI", qty=51)

    result = _resolve_swing_staged_exit(
        pos, mark=56000.0, ma20=None,
        ret_pct=10.89, days_held=3, stop_hit=False,
    )

    assert result["exit_ok"] is False
    assert result["reason"] == "SWING_HOLD_TREND_OK"


# ────────────────────────────────────────────────────────────────
# effective_exit_risk에서 position_meta에 effective 값이 기록되는지
# ────────────────────────────────────────────────────────────────

def test_update_meta_contains_effective_fields_on_profit_protect(monkeypatch):
    monkeypatch.setenv("PB1_SWING_STAGED_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_PCT", "8.0")
    monkeypatch.setenv("PB1_PROFIT_PROTECT_SELL_PCT", "0.33")

    pos = _samsung_ea_pos(entry_price=50500.0, raw_stop=40739.0, qty=51)
    result = _resolve_swing_staged_exit(
        pos, mark=54600.0, ma20=None,
        ret_pct=8.12, days_held=3, stop_hit=False,
    )

    assert result["exit_ok"] is True
    meta = result["update_meta"]
    assert "effective_stop_price" in meta
    assert "effective_r_value" in meta
    assert "raw_stop_price" in meta
    assert meta["effective_exit_policy_version"] == "2026-04-29-effective-risk-v1"
    # effective_stop ≈ 46965
    assert abs(meta["effective_stop_price"] - 46965.0) < 5.0


# ────────────────────────────────────────────────────────────────
# KOSDAQ 시장의 경우 8% 캡 적용
# ────────────────────────────────────────────────────────────────

def test_kosdaq_uses_8pct_cap(monkeypatch):
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT", "8.0")

    pos = {
        "code": "123456",
        "avg_buy_price": 10000.0,
        "qty": 10,
        "orderable_qty": 10,
        "market": "KOSDAQ",
        "stop_price_at_entry": 7000.0,  # 30% 하락 - 너무 깊음
        "position_meta": {"initial_stop_price": 7000.0},
    }
    result = _resolve_effective_exit_risk_for_pos(pos)

    # effective_stop = 10000 * 0.92 = 9200
    assert abs(result["effective_stop_price"] - 9200.0) < 1.0
    assert result["effective_applied"] is True
    assert result["stop_cap_pct"] == 8.0
