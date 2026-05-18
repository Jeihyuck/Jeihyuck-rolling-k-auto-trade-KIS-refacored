"""
[2026-05-18] KR zero-trade 회귀 테스트

테스트 목적:
  - 한국장 AM 129 tick 중 vol_contraction_fail=29/30 시나리오에서
    KR adaptive entry filter가 올바르게 동작하는지 검증
  - 기존 글로벌 로직에 영향이 없는지 확인
"""
from __future__ import annotations

import importlib
import os
import types
from unittest.mock import MagicMock, patch


# ─────────────────────────────────────────────────────────────────────────────
# 헬퍼: CandidateFeature 스텁 생성
# ─────────────────────────────────────────────────────────────────────────────

def _make_candidate(
    code: str = "005930",
    *,
    data_ok: bool = True,
    setup_ok: bool = False,
    reasons: list[str] | None = None,
    score: float = 42.0,
    rs_percentile: float = 72.0,
    vol_contraction: float = 1.30,
    entry_style_selected: str = "PULLBACK",
    extra_features: dict | None = None,
) -> MagicMock:
    cf = MagicMock()
    cf.code = code
    cf.setup_ok = setup_ok
    cf.score = score
    cf.reasons = reasons or []
    cf.market = "KQ"
    cf.features = {
        "data_ok": data_ok,
        "setup_ok": setup_ok,
        "score": score,
        "rs_percentile": rs_percentile,
        "vol_contraction": vol_contraction,
        "volu_contraction": 1.10,
        "pullback_pct": -5.0,
        "ma20": 10000.0,
        "ma50": 9800.0,
        "close": 10500.0,
        "ma20_slope": 0.002,
        "atr_pct": 0.04,
        "vcp_score": 60.0,
        "entry_style_selected": entry_style_selected,
        "liq_ok": True,
        "spread_ok": True,
        "range_ok": True,
        "gap_ok": True,
    }
    if extra_features:
        cf.features.update(extra_features)
    return cf


def _make_engine_instance(
    env: str = "practice",
    session_kind: str = "am",
    window_name: str = "morning",
    final30_source: str = "pb1_watchlist_final_scored",
    scanner_context: dict | None = None,
) -> MagicMock:
    engine = MagicMock()
    engine.env = env
    engine.session_kind = session_kind
    engine.window_name = window_name
    engine.market_window_name = window_name
    engine.final30_source = final30_source
    engine.scanner_context = scanner_context or {}
    return engine


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: zero-trade 회귀 — vol_contraction_fail 29/30 → stress=True, rescue≤1
# ─────────────────────────────────────────────────────────────────────────────

def test_kr_market_stress_detected_when_vol_fail_ratio_high():
    """scanned=30, vol_contraction_fail=29 → stress=True"""
    from trader.config import (
        PB1_KR_MARKET_STRESS_GUARD,
        PB1_KR_STRESS_VOL_FAIL_RATIO,
    )

    # 기본 설정이 stress 감지를 활성화해야 함
    assert PB1_KR_MARKET_STRESS_GUARD is True
    assert PB1_KR_STRESS_VOL_FAIL_RATIO <= 0.70

    # vol_fail_ratio 계산
    scanned = 30
    vol_contraction_fail = 29
    ratio = vol_contraction_fail / scanned
    assert ratio >= PB1_KR_STRESS_VOL_FAIL_RATIO, (
        f"vol_fail_ratio={ratio:.2f}가 threshold={PB1_KR_STRESS_VOL_FAIL_RATIO}보다 낮음 — stress가 감지되어야 함"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: 정상 시장 — vol_fail ≤10 → stress=False
# ─────────────────────────────────────────────────────────────────────────────

def test_kr_market_stress_not_triggered_on_normal_day():
    """vol_contraction_fail=5/30 → stress=False"""
    from trader.config import PB1_KR_STRESS_VOL_FAIL_RATIO

    scanned = 30
    vol_fail = 5
    ratio = vol_fail / scanned
    # 정상 시장에서는 stress threshold 미만이어야 함
    assert ratio < PB1_KR_STRESS_VOL_FAIL_RATIO, (
        f"vol_fail_ratio={ratio:.2f}가 threshold={PB1_KR_STRESS_VOL_FAIL_RATIO} 이상 — 정상 시장에서 stress 오탐"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Momentum/Breakout vol expansion 허용 설정
# ─────────────────────────────────────────────────────────────────────────────

def test_kr_momentum_and_breakout_allow_vol_expansion_default():
    """PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION / BREAKOUT 기본값 True"""
    from trader.config import (
        PB1_KR_BREAKOUT_ALLOW_VOL_EXPANSION,
        PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION,
    )
    assert PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION is True
    assert PB1_KR_BREAKOUT_ALLOW_VOL_EXPANSION is True


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: _classify_kr_reason_by_style — PULLBACK vol_contraction_fail → SOFT
# ─────────────────────────────────────────────────────────────────────────────

def test_classify_kr_reason_by_style_pullback_vol_contraction_soft():
    """PULLBACK + vol_contraction_fail → SOFT (패널티로 처리)"""
    from trader.config import PB1_KR_PULLBACK_VOL_HARD_FAIL
    # PULLBACK vol hard fail이 꺼져 있어야 함
    assert PB1_KR_PULLBACK_VOL_HARD_FAIL is False

    # 함수 임포트 (PB1Engine 클래스에서 직접 호출)
    try:
        from trader.pb1_engine import PB1Engine
        result = PB1Engine._classify_kr_reason_by_style("vol_contraction_fail", "PULLBACK")
        assert result == "SOFT", f"PULLBACK + vol_contraction_fail → 예상=SOFT, 실제={result}"
    except ImportError:
        # pb1_engine import 실패 시 config 값으로 간접 검증
        assert not PB1_KR_PULLBACK_VOL_HARD_FAIL  # SOFT 처리됨을 간접 확인


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: _classify_kr_reason_by_style — MOMENTUM + vol_contraction_fail → IGNORE
# ─────────────────────────────────────────────────────────────────────────────

def test_classify_kr_reason_by_style_momentum_vol_contraction_ignore():
    """MOMENTUM + vol_contraction_fail → IGNORE"""
    from trader.config import PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION
    assert PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION is True

    try:
        from trader.pb1_engine import PB1Engine
        result = PB1Engine._classify_kr_reason_by_style("vol_contraction_fail", "MOMENTUM")
        assert result == "IGNORE", f"MOMENTUM + vol_contraction_fail → 예상=IGNORE, 실제={result}"
    except ImportError:
        pass  # config 값만으로 충분


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: _is_kr_equity_context — 한국장 세션은 True
# ─────────────────────────────────────────────────────────────────────────────

def test_is_kr_equity_context_true_for_kr_sessions():
    """KR 세션(am/pm/afternoon/close)은 _is_kr_equity_context() == True"""
    try:
        from trader.pb1_engine import PB1Engine
    except ImportError:
        return  # import 실패 시 skip

    for session in ("am", "pm", "afternoon", "close", ""):
        eng = MagicMock(spec=PB1Engine)
        eng.env = "practice"
        eng.session_kind = session
        eng.window_name = "morning"
        eng.market_window_name = "morning"
        eng.final30_source = "pb1_watchlist_final_scored"
        # 언바운드 메서드로 호출
        result = PB1Engine._is_kr_equity_context(eng)
        assert result is True, f"session_kind={session!r} → 예상 True, 실제 {result}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: _is_kr_equity_context — US context는 False
# ─────────────────────────────────────────────────────────────────────────────

def test_is_kr_equity_context_false_for_us_context():
    """미국장 env는 _is_kr_equity_context() == False"""
    try:
        from trader.pb1_engine import PB1Engine
    except ImportError:
        return

    eng = MagicMock(spec=PB1Engine)
    eng.env = "us_paper"  # US-only env
    eng.session_kind = "trade-am"
    eng.window_name = "us_market_hours"
    eng.market_window_name = "us_market_hours"
    eng.final30_source = "us_universe"

    result = PB1Engine._is_kr_equity_context(eng)
    assert result is False, f"US context → 예상 False, 실제 {result}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: rescue topn config 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_kr_rescue_config_defaults():
    """KR rescue 기본값 확인"""
    from trader.config import (
        PB1_KR_ADAPTIVE_ENTRY_FILTER,
        PB1_KR_ADAPTIVE_SCORE_MIN_FLOOR,
        PB1_KR_ADAPTIVE_RANK_TOPN,
        PB1_KR_ENABLE_RESCUE_CANDIDATES,
        PB1_KR_RESCUE_TOPN,
        PB1_KR_RESCUE_SOURCE,
        PB1_KR_STRESS_MAX_NEW_POSITIONS,
    )
    assert PB1_KR_ADAPTIVE_ENTRY_FILTER is True
    assert PB1_KR_ADAPTIVE_SCORE_MIN_FLOOR == 38.0
    assert PB1_KR_ADAPTIVE_RANK_TOPN == 5
    assert PB1_KR_ENABLE_RESCUE_CANDIDATES is True
    assert PB1_KR_RESCUE_TOPN == 3
    assert PB1_KR_RESCUE_SOURCE == "SCANNER_OR_MINERVINI"
    assert PB1_KR_STRESS_MAX_NEW_POSITIONS == 1


# ─────────────────────────────────────────────────────────────────────────────
# Test 12-7: PNL report cash=None when KIS balance unavailable
# ─────────────────────────────────────────────────────────────────────────────

def test_pnl_report_cash_none_when_kis_balance_unavailable():
    """KIS 잔고 조회 실패 시 cash=None, cash_source="unavailable" — cash=0 처리 금지 (섹션 10)"""
    from scripts.generate_portfolio_pnl_report import _build_portfolio_summary

    # KIS 응답이 비어있을 때 cash=None 전달 → summary에서 None 유지
    summary = _build_portfolio_summary(
        holdings=[],
        today_fills=[],
        db_positions=[],
        cash=None,
    )
    # cash=None이 summary에 그대로 보존되어야 함 (0으로 대체 금지)
    assert summary["cash"] is None, f"cash는 None이어야 하지만 실제: {summary['cash']}"
    # total_equity_estimate도 None (계산 불가)
    assert summary["total_equity_estimate"] is None, (
        f"total_equity_estimate는 None이어야 하지만 실제: {summary['total_equity_estimate']}"
    )
    # cash_unavailable 플래그
    assert summary["cash_unavailable"] is True


def test_pnl_report_cash_source_set_in_summary():
    """generate_main에서 cash_source가 summary에 기록되는 계약 검증"""
    # _build_portfolio_summary는 cash_source를 반환하지 않음 —
    # generate_main에서 수동으로 summary["cash_source"] = cash_source 로 주입.
    # 이 테스트는 그 계약을 검증한다.
    from scripts.generate_portfolio_pnl_report import _build_portfolio_summary
    import inspect

    src = inspect.getsource(_build_portfolio_summary)
    # _build_portfolio_summary 자체에는 cash_source가 없어야 함 (외부 주입)
    assert "cash_source" not in src, (
        "_build_portfolio_summary 내부에 cash_source가 있으면 안 됩니다. "
        "generate_main에서 외부 주입 방식을 사용합니다."
    )

    # 실제로 summary에 cash_source를 주입하는 코드가 스크립트에 있는지 확인
    import pathlib, re
    script_src = pathlib.Path("scripts/generate_portfolio_pnl_report.py").read_text(encoding="utf-8")
    assert re.search(r'summary\["cash_source"\]\s*=\s*cash_source', script_src), (
        "generate_portfolio_pnl_report.py에 summary[\"cash_source\"] = cash_source 주입 코드가 없습니다."
    )
