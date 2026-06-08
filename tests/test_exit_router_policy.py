"""tests/test_exit_router_policy.py

Multi-Layer Exit Router 정책 검증 테스트.

acceptance criteria:
1. R 기반 로직이 완전히 삭제되지 않았다.
2. R 단독으로 익절이 결정되지 않는다 (trend_strong=True이면 억제).
3. 삼성E&A 028050 +7.13%는 하드코딩 매도 없음 (tp1=12%, activate=8%).
4. profit protect (giveback) 작동.
5. momentum은 더 빠르게 보호.
6. core carry 조기 익절 금지.
7. 기존 보유 포지션도 exit router 적용.
"""
from __future__ import annotations

import pytest

from trader.exit_policy.router import (
    resolve_exit_policy_for_position,
    apply_swing_exit_decision,
)


# ────────────────────────────────────────────────────────────────
# 공통 helper
# ────────────────────────────────────────────────────────────────

def _swing_pos(
    *,
    avg: float = 50500.0,
    qty: int = 50,
    initial_stop: float = 46965.0,
    market: str = "KOSPI",
    tp1_done: bool = False,
    tp2_done: bool = False,
    giveback_protect_done: bool = False,
    entry_style: str = "ENTRY_PULLBACK",
    exit_policy_family: str = "SWING_STAGED_EXIT",
) -> dict:
    return {
        "code": "028050",
        "avg_buy_price": avg,
        "orderable_qty": qty,
        "qty": qty,
        "market": market,
        "entry_style_selected": entry_style,
        "exit_policy_family": exit_policy_family,
        "position_meta": {
            "initial_stop_price": initial_stop,
            "tp1_done": tp1_done,
            "tp2_done": tp2_done,
            "giveback_protect_done": giveback_protect_done,
        },
    }


def _policy_swing_default(monkeypatch, *, trend_strong: bool = False, trend_ok: bool = True):
    """기본 SWING policy 생성 helper."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_SWING_TP1_R", "2.0")
    monkeypatch.setenv("PB1_SWING_TP1_PROFIT_PCT", "12.0")
    monkeypatch.setenv("PB1_SWING_TP2_PROFIT_PCT", "18.0")
    monkeypatch.setenv("PB1_SWING_PROFIT_ACTIVATE_PCT", "8.0")
    monkeypatch.setenv("PB1_SWING_PROFIT_GIVEBACK_PCT", "3.0")
    monkeypatch.setenv("PB1_SWING_PROFIT_FLOOR_PCT", "5.0")
    monkeypatch.setenv("PB1_SWING_TP1_SELL_PCT", "0.33")
    monkeypatch.setenv("PB1_SWING_TP2_SELL_PCT", "0.33")
    monkeypatch.setenv("PB1_SWING_TIME_STOP_DAYS", "10")
    return {"trend_strong": trend_strong, "trend_ok": trend_ok}


# ────────────────────────────────────────────────────────────────
# TC1: R 기반 로직은 삭제되지 않는다
# ────────────────────────────────────────────────────────────────

def test_r_based_hard_stop_still_works(monkeypatch):
    """R 기반 hard stop은 router에서도 작동한다."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "0")

    pos = _swing_pos(avg=50500.0, initial_stop=47165.0)
    # mark < effective stop → stop hit
    policy = resolve_exit_policy_for_position(
        pos=pos,
        features={},
        holding_ctx={"days_held": 3, "current_return_pct": -7.0, "current_r": -1.0, "mark": 46900.0},
        market_ctx={},
    )
    assert policy["hard_stop_enabled"] is True
    assert policy["r_take_profit_enabled"] is True

    # apply_swing_exit_decision: effective_stop 기반 stop hit
    result = apply_swing_exit_decision(
        pos, 46900.0, policy,
        ret_pct=-7.2,
        current_r=-1.0,
        highest_ret_pct=-2.0,
        days_held=3,
        stop_hit=False,
        effective_stop=47165.0,
        effective_r=3335.0,
    )
    assert result["exit_ok"] is True
    assert result["reason"] == "STOP_HIT_EFFECTIVE"


def test_current_r_calculation_preserved(monkeypatch):
    """current_r 계산이 router policy에 포함된다."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")

    pos = _swing_pos(avg=10000.0, initial_stop=9500.0)
    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 3,
            "current_return_pct": 10.0,
            "current_r": 2.0,      # R은 계속 계산
            "mark": 11000.0,
        },
        market_ctx={},
    )
    # R-based TP1은 policy에 rule로 존재 (삭제 안 됨)
    assert policy["r_take_profit_enabled"] is True
    r_rules = [r for r in policy.get("partial_sell_rules", []) if r.get("trigger") == "r_hybrid"]
    assert len(r_rules) >= 1, "R-based rule이 policy에 존재해야 한다"


# ────────────────────────────────────────────────────────────────
# TC2: R 단독으로 TP가 결정되지 않는다 (trend_strong=True이면 억제)
# ────────────────────────────────────────────────────────────────

def test_r_tp1_suppressed_when_trend_strong(monkeypatch):
    """current_r >= 2이고 trend_strong=True이면 R 기반 TP1 매도 금지."""
    _policy_swing_default(monkeypatch)
    # below tp1_profit_pct(12%) and below giveback activated(8% peak)
    pos = _swing_pos(avg=10000.0, initial_stop=9500.0)
    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 3,
            "current_return_pct": 10.0,
            "current_r": 2.0,
            "highest_return_pct": 10.0,
            "mark": 11000.0,
        },
        market_ctx={"ma20": 9500.0, "ma50": 9000.0},  # mark 위에 있음 → trend_strong=True
    )
    assert policy["trend_strong"] is True

    result = apply_swing_exit_decision(
        pos, 11000.0, policy,
        ret_pct=10.0,
        current_r=2.0,
        highest_ret_pct=10.0,
        days_held=3,
        stop_hit=False,
        effective_stop=9300.0,  # mark 훨씬 아래
        effective_r=700.0,
    )
    # trend_strong + ret < 12%(tp1_profit_pct) → R-based TP1 억제
    assert result["exit_ok"] is False or result.get("reason") not in {
        "EXIT_SWING_TP1",
        "STOP_HIT_EFFECTIVE",
    }, f"trend_strong 상태에서 R 단독 TP1 금지: got {result}"
    # 실제 reason이 HOLD여야 함 (giveback 미발생, pct tp1 미달)
    assert result["exit_ok"] is False


def test_r_tp1_fires_when_trend_weak(monkeypatch):
    """current_r >= 2이고 trend_strong=False이면 R 기반 TP1 허용."""
    _policy_swing_default(monkeypatch)

    pos = _swing_pos(avg=10000.0, initial_stop=9500.0)
    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 3,
            "current_return_pct": 10.0,
            "current_r": 2.0,
            "highest_return_pct": 10.0,
            "mark": 11000.0,
        },
        market_ctx={"ma20": 11500.0, "ma50": 11200.0},  # mark 아래 → trend_strong=False
    )
    assert policy["trend_strong"] is False

    result = apply_swing_exit_decision(
        pos, 11000.0, policy,
        ret_pct=10.0,
        current_r=2.0,
        highest_ret_pct=10.0,
        days_held=3,
        stop_hit=False,
        effective_stop=9300.0,
        effective_r=700.0,
    )
    # trend 약함: R 기준 TP1 발동 가능 (ret < 12%이므로 pct tp1은 미발동)
    assert result["exit_ok"] is True
    assert result["reason"] == "EXIT_SWING_TP1"


def test_trail_stop_hit_exits_with_non_zero_qty(monkeypatch):
    _policy_swing_default(monkeypatch)
    monkeypatch.setenv("PB1_SWING_MIN_TRAIL_BARS", "2")

    pos = _swing_pos(avg=10000.0, qty=9, initial_stop=9500.0)
    pos["holding_bars"] = 5
    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 5,
            "current_return_pct": 9.0,
            "current_r": 1.8,
            "highest_return_pct": 14.0,
            "mark": 10850.0,
        },
        market_ctx={"ma20": 10300.0, "ma50": 10100.0},
    )

    result = apply_swing_exit_decision(
        pos,
        10850.0,
        policy,
        ret_pct=8.5,
        current_r=1.8,
        highest_ret_pct=14.0,
        days_held=5,
        stop_hit=False,
        trail_hit=True,
        trail_stop_price=10900.0,
        effective_stop=9500.0,
        effective_r=500.0,
    )

    assert result["exit_ok"] is True
    assert result["reason"] == "TRAIL_STOP_HIT"
    assert int(result["qty"]) > 0



# ────────────────────────────────────────────────────────────────
# TC3: 삼성E&A +7.13% 하드코딩 매도 금지
# ────────────────────────────────────────────────────────────────

def test_samsung_ea_7pct_no_hardcoded_sell(monkeypatch):
    """삼성E&A 028050 +7.13%: policy tp1=12%, activate=8% → HOLD.
    삼성E&A 전용 임계값 하드코딩 없음.
    """
    _policy_swing_default(monkeypatch)

    pos = _swing_pos(
        avg=50500.0,
        initial_stop=46965.0,
        market="KOSPI",
        entry_style="ENTRY_PULLBACK",
    )
    mark = 54100.0  # +7.13%
    ret_pct = (mark - 50500.0) / 50500.0 * 100.0  # ≈ 7.13%

    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 6,
            "current_return_pct": ret_pct,
            "current_r": 1.02,
            "highest_return_pct": ret_pct,
            "mark": mark,
        },
        market_ctx={"ma20": 50000.0},  # mark > ma20 → trend_ok
    )

    result = apply_swing_exit_decision(
        pos, mark, policy,
        ret_pct=ret_pct,
        current_r=1.02,
        highest_ret_pct=ret_pct,
        days_held=6,
        stop_hit=False,
        effective_stop=47165.0,
        effective_r=3335.0,
    )

    assert result["exit_ok"] is False, (
        f"삼성E&A +{ret_pct:.2f}%는 tp1=12%, activate=8%이면 HOLD여야 함. "
        f"got reason={result.get('reason')}"
    )
    assert result["reason"] == "SWING_HOLD_PROFIT_NOT_YET_PROTECTED"


# ────────────────────────────────────────────────────────────────
# TC4: profit protect (giveback) 작동
# ────────────────────────────────────────────────────────────────

def test_profit_protect_giveback_triggers(monkeypatch):
    """highest_ret=11%, current_ret=7%, giveback=4%p → activate=8%, floor=5% → 부분매도."""
    _policy_swing_default(monkeypatch)

    pos = _swing_pos(avg=10000.0, initial_stop=9500.0)
    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 5,
            "current_return_pct": 7.0,
            "current_r": 1.4,
            "highest_return_pct": 11.0,   # 11%까지 올라갔다가
            "mark": 10700.0,
        },
        market_ctx={},
    )

    result = apply_swing_exit_decision(
        pos, 10700.0, policy,
        ret_pct=7.0,
        current_r=1.4,
        highest_ret_pct=11.0,
        days_held=5,
        stop_hit=False,
        effective_stop=9300.0,
        effective_r=700.0,
    )

    # highest=11% >= activate=8%, drawdown=4% >= giveback=3%, current=7% >= floor=5%
    assert result["exit_ok"] is True
    assert result["reason"] == "SWING_PROFIT_PROTECT_GIVEBACK"
    assert result.get("sell_pct", 1.0) < 1.0, "부분 매도여야 함"
    assert result["update_meta"]["giveback_protect_done"] is True


def test_profit_protect_not_triggered_below_activate(monkeypatch):
    """highest_ret=6% (< activate=8%) → giveback protect 미발동."""
    _policy_swing_default(monkeypatch)

    pos = _swing_pos(avg=10000.0, initial_stop=9500.0)
    policy = resolve_exit_policy_for_position(
        pos, features={}, holding_ctx={
            "days_held": 4, "current_return_pct": 4.0, "current_r": 0.8,
            "highest_return_pct": 6.0, "mark": 10400.0,
        }, market_ctx={},
    )
    result = apply_swing_exit_decision(
        pos, 10400.0, policy,
        ret_pct=4.0, current_r=0.8, highest_ret_pct=6.0,
        days_held=4, stop_hit=False, effective_stop=9300.0,
    )
    assert result["exit_ok"] is False


# ────────────────────────────────────────────────────────────────
# TC5: momentum은 더 빠르게 보호
# ────────────────────────────────────────────────────────────────

def test_momentum_exit_family_is_intraday(monkeypatch):
    """ENTRY_MOMENTUM → exit_family=INTRADAY_PROFIT_PROTECT."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_MOMENTUM_PROFIT_ACTIVATE_PCT", "5.0")
    monkeypatch.setenv("PB1_MOMENTUM_PROFIT_GIVEBACK_PCT", "2.0")

    pos = _swing_pos(
        avg=10000.0,
        entry_style="ENTRY_MOMENTUM",
        exit_policy_family="INTRADAY_PROFIT_PROTECT",
    )
    policy = resolve_exit_policy_for_position(
        pos, features={},
        holding_ctx={"days_held": 0, "current_return_pct": 5.0, "current_r": 1.0, "mark": 10500.0},
        market_ctx={},
    )
    assert policy["exit_family"] == "INTRADAY_PROFIT_PROTECT"
    assert policy["profit_protect_enabled"] is True
    # MOMENTUM: activate_pct=5, giveback=2 (SWING: 8, 3보다 빠름)
    gb_rule = next((r for r in policy.get("partial_sell_rules", []) if r.get("trigger") == "giveback"), None)
    assert gb_rule is not None
    assert gb_rule["activate_pct"] <= 6.0, "MOMENTUM은 SWING(8%)보다 빠른 profit protection이어야 함"


# ────────────────────────────────────────────────────────────────
# TC6: core carry 조기 익절 금지
# ────────────────────────────────────────────────────────────────

def test_core_carry_no_early_exit(monkeypatch):
    """CORE_CARRY: ret=10%, ma20 유지 → HOLD (tp1=20% 미달)."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_CORE_TP1_PROFIT_PCT", "20.0")
    monkeypatch.setenv("PB1_CORE_TP2_PROFIT_PCT", "30.0")

    pos = _swing_pos(
        avg=10000.0,
        exit_policy_family="CORE_TREND_FOLLOW",
        entry_style="ENTRY_PULLBACK",
    )
    policy = resolve_exit_policy_for_position(
        pos, features={"score_final": 88.0},
        holding_ctx={
            "days_held": 10, "current_return_pct": 10.0, "current_r": 2.0,
            "highest_return_pct": 10.0, "mark": 11000.0,
        },
        market_ctx={"ma20": 9500.0},  # mark > ma20 → trend_ok
    )
    assert policy["exit_family"] == "CORE_TREND_FOLLOW"
    assert policy["r_take_profit_enabled"] is False, "CORE_TREND_FOLLOW은 R 단독 TP 금지"
    assert policy["profit_protect_enabled"] is False, "CORE_TREND_FOLLOW은 단기 반납에 반응 안 함"


def test_core_carry_tp1_at_20pct(monkeypatch):
    """CORE_CARRY: ret=21% → tp1=20% 도달, 부분 익절."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")
    monkeypatch.setenv("PB1_CORE_TP1_PROFIT_PCT", "20.0")
    monkeypatch.setenv("PB1_CORE_TP1_SELL_PCT", "0.25")

    from trader.exit_policy.router import _policy_core, apply_swing_exit_decision

    pos = _swing_pos(
        avg=10000.0,
        initial_stop=9000.0,
        exit_policy_family="CORE_TREND_FOLLOW",
    )
    policy = _policy_core(score_final=90.0, trend_strong=True, atr_pct=2.0, days_held=15)
    policy.update({"exit_family": "CORE_TREND_FOLLOW", "router_enabled": True})

    result = apply_swing_exit_decision(
        pos, 12100.0, policy,
        ret_pct=21.0,
        current_r=3.1,
        highest_ret_pct=21.0,
        days_held=15,
        stop_hit=False,
        effective_stop=9100.0,
        effective_r=900.0,
    )
    assert result["exit_ok"] is True
    assert result["reason"] == "SWING_PCT_TP1"
    assert result["update_meta"]["tp1_done"] is True


# ────────────────────────────────────────────────────────────────
# TC7: 기존 보유 포지션도 exit router 적용
# ────────────────────────────────────────────────────────────────

def test_existing_position_exit_router_applied(monkeypatch):
    """entry_meta 일부 누락 기존 보유분 → fallback policy로 평가."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")

    # entry_style 없음, trade_horizon 없음 → SWING_CARRY fallback
    pos = {
        "code": "005930",
        "avg_buy_price": 75000.0,
        "orderable_qty": 10,
        "qty": 10,
        "market": "KOSPI",
        # entry_style_selected 없음 (기존 보유)
        "position_meta": {
            "initial_stop_price": 69750.0,  # 7% 아래
        },
    }

    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 30,
            "current_return_pct": 3.0,
            "current_r": 0.5,
            "highest_return_pct": 5.0,
            "mark": 77250.0,
        },
        market_ctx={},
    )
    # entry_style 없으면 SWING_STAGED_EXIT (default)
    assert policy["exit_family"] == "SWING_STAGED_EXIT"
    assert policy["router_enabled"] is True
    # hard_stop 항상 작동
    assert policy["hard_stop_enabled"] is True


def test_existing_position_fallback_entry_unknown(monkeypatch):
    """ENTRY_UNKNOWN이어도 exit router가 SWING_STAGED_EXIT을 fallback으로 적용한다."""
    monkeypatch.setenv("PB1_EXIT_ROUTER_ENABLED", "1")

    pos = {
        "code": "999999",
        "avg_buy_price": 100000.0,
        "orderable_qty": 5,
        "qty": 5,
        "market": "KOSDAQ",
        "entry_style_selected": "ENTRY_UNKNOWN",
        "position_meta": {"initial_stop_price": 93000.0},
    }

    policy = resolve_exit_policy_for_position(
        pos,
        features={},
        holding_ctx={
            "days_held": 5,
            "current_return_pct": 2.0,
            "current_r": 0.3,
            "highest_return_pct": 3.0,
            "mark": 102000.0,
        },
        market_ctx={},
    )
    assert policy["exit_family"] == "SWING_STAGED_EXIT"
    assert policy["hard_stop_enabled"] is True
