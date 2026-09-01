# -*- coding: utf-8 -*-
"""US Trading Explanation System.

한국장 PB1 수준의 매수/매도 사유 설명 로직을 미국장에 완전 이식.

### 핵심 설명 필드 ###

Entry Explanations:
- entry_style_selected: "ENTRY_BREAKOUT", "ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_VCP", "ENTRY_GENERIC"
- entry_component: 더 세밀한 하위 분류 (예: "breakout_pivot", "pullback_reversal")
- score_breakdown: {
    "breakout_score": 0-100,
    "pullback_score": 0-100,
    "momentum_score": 0-100,
    "vcp_score": 0-100,
    "tech_score": 0-100,
    "score_final": 0-100,
  }
- reasons: ["high_rs_percentile", "ma_alignment", "volume_surge", ...]
- reject_reasons: ["insufficient_volume", "weak_momentum", ...]
- filters_passed: ["liquidity_filter", "price_range_filter", ...]
- filters_failed: ["atr_exceed", "pullback_range_fail", ...]

Exit Explanations:
- exit_style: "hard_stop", "trailing_stop", "profit_protect", "giveback", "time_stop", "risk_off"
- exit_trigger: "stop_loss_hit", "trail_threshold_breach", "time_limit_exceed"
- why_not_sell: "no_exit_signal", "price_above_trail", "within_risk_tolerance"
- score_breakdown: 진출 점수 (보유 필요성)
- reasons: 청산 판단 근거
- reject_reasons: 보유 지속 근거

### 설명 품질 레벨 ###

- FULL: 모든 설명 필드 완비 (entry_style, score_breakdown, reasons, filters_passed/failed)
- PARTIAL: 일부 필드만 제공 (score_breakdown 없음 등)
- MINIMAL: score/rank만 제공, 설명 없음
- MISSING: 설명 전무

### 로깅 패턴 ###

Entry decisions:
- [US_ENTRY][WHY_BUY] symbol=AAPL entry_style=ENTRY_BREAKOUT breakout_score=85 reasons=["pivot_break", "volume_surge"]
- [US_ENTRY][WHY_SKIP] symbol=MSFT reject_reason=insufficient_volume filters_failed=["volume_filter"]

Exit decisions:
- [US_EXIT][WHY_SELL] symbol=TSLA exit_style=trailing_stop pnl_pct=0.12 reason=trail_threshold_breach
- [US_EXIT][WHY_HOLD] symbol=NVDA why_not_sell=no_exit_signal pnl_pct=0.05

### 기대 효과 ###

1. 매수/매도 이유 완전 추적 가능
2. 설명 누락 시 경고 (모두 MINIMAL이면 문제)
3. 리포트에 buy_decisions/buy_skips/sell_decisions/no_exit_signals 통계 추가
4. 로그로 의사결정 과정 완전 재현 가능
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Entry Style Families ─────────────────────────────────────────────────────
ENTRY_STYLE_FAMILIES = {"ENTRY_BREAKOUT", "ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_VCP", "ENTRY_GENERIC", "SKIP"}

# ── Exit Style Families ──────────────────────────────────────────────────────
EXIT_STYLE_FAMILIES = {"hard_stop", "trailing_stop", "profit_protect", "giveback", "time_stop", "risk_off", "manual", "no_exit"}

# ── Explanation Quality Levels ───────────────────────────────────────────────
QUALITY_FULL = "FULL"        # 모든 설명 필드 완비
QUALITY_PARTIAL = "PARTIAL"  # 일부 필드만 제공
QUALITY_MINIMAL = "MINIMAL"  # score/rank만 제공
QUALITY_MISSING = "MISSING"  # 설명 전무


def _safe_float(val: Any, default: float = 0.0) -> float:
    """안전한 float 변환."""
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def normalize_us_entry_style(value: Any) -> str:
    """US entry style 정규화.

    Args:
        value: 원시 entry_style 값 (str/None)

    Returns:
        정규화된 entry_style: ENTRY_BREAKOUT, ENTRY_PULLBACK, ENTRY_MOMENTUM, ENTRY_VCP, ENTRY_GENERIC, SKIP
    """
    raw = str(value or "").strip().upper()
    if raw.startswith("PB1_"):
        raw = raw[4:]
    
    # Breakout 계열
    if raw in {"ENTRY_BREAKOUT", "BREAKOUT", "ENTRY_BREAKOUT_CONFIRMED", "BREAKOUT_PIVOT"}:
        return "ENTRY_BREAKOUT"
    
    # Pullback 계열
    if raw in {"ENTRY_PULLBACK", "PULLBACK", "ENTRY_PULLBACK_OVERRIDE", "PULLBACK_REVERSAL"}:
        return "ENTRY_PULLBACK"
    
    # Momentum 계열
    if raw in {"ENTRY_MOMENTUM", "MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION", "MOMENTUM_CONTINUATION"}:
        return "ENTRY_MOMENTUM"
    
    # VCP 계열 (Volatility Contraction Pattern)
    if raw in {"ENTRY_VCP", "VCP", "ENTRY_VCP_CONFIRMED"}:
        return "ENTRY_VCP"
    
    # Generic fallback
    if raw in {"ENTRY_GENERIC", "GENERIC"}:
        return "ENTRY_GENERIC"
    
    return "SKIP"


def extract_us_score_breakdown(entry_data: dict[str, Any]) -> dict[str, float]:
    """US watchlist entry에서 score breakdown 추출.

    Args:
        entry_data: locked watchlist entry row (dict)

    Returns:
        {
            "breakout_score": 0-100,
            "pullback_score": 0-100,
            "momentum_score": 0-100,
            "vcp_score": 0-100,
            "tech_score": 0-100,
            "score_final": 0-100,
        }
    """
    return {
        "breakout_score": _safe_float(entry_data.get("breakout_score", 0.0)),
        "pullback_score": _safe_float(entry_data.get("pullback_score", 0.0)),
        "momentum_score": _safe_float(entry_data.get("momentum_score", 0.0)),
        "vcp_score": _safe_float(entry_data.get("vcp_score", 0.0)),
        "tech_score": _safe_float(entry_data.get("tech_score", 0.0)),
        "score_final": _safe_float(entry_data.get("score_final", 0.0)),
    }


def build_us_entry_reasons(entry_data: dict[str, Any], entry_style: str) -> list[str]:
    """US entry 사유 리스트 생성.

    Args:
        entry_data: locked watchlist entry row
        entry_style: 정규화된 entry_style (ENTRY_BREAKOUT, ENTRY_PULLBACK, ENTRY_MOMENTUM, ...)

    Returns:
        ["high_rs_percentile", "ma_alignment", "volume_surge", ...]
    """
    reasons: list[str] = []
    
    # RS percentile
    rs = _safe_float(entry_data.get("rs_percentile", 0.0))
    if rs >= 90:
        reasons.append("high_rs_percentile_90+")
    elif rs >= 80:
        reasons.append("high_rs_percentile_80+")
    
    # MA alignment
    ma20 = _safe_float(entry_data.get("ma20", 0.0))
    ma50 = _safe_float(entry_data.get("ma50", 0.0))
    close = _safe_float(entry_data.get("close", 0.0))
    
    if ma20 > 0 and ma50 > 0 and close > 0:
        if close > ma20 > ma50:
            reasons.append("ma_alignment_ok")
        elif close > ma20:
            reasons.append("above_ma20")
    
    # Volume surge
    volume = _safe_float(entry_data.get("volume", 0.0))
    volume_avg20 = _safe_float(entry_data.get("volume_avg20", 0.0))
    if volume_avg20 > 0:
        vol_ratio = volume / volume_avg20
        if vol_ratio >= 2.0:
            reasons.append("volume_surge_2x")
        elif vol_ratio >= 1.5:
            reasons.append("volume_surge_1.5x")
    
    # Style-specific reasons
    if entry_style == "ENTRY_BREAKOUT":
        high_50 = _safe_float(entry_data.get("high_50", 0.0))
        if close > 0 and high_50 > 0 and close >= high_50:
            reasons.append("breakout_50d_high")
        
        pivot = _safe_float(entry_data.get("pivot_price", 0.0))
        if pivot > 0 and close >= pivot:
            reasons.append("pivot_break")
    
    elif entry_style == "ENTRY_PULLBACK":
        pullback_pct_raw = _safe_float(entry_data.get("pullback_pct", 0.0))
        pullback_pct = pullback_pct_raw / 100.0 if pullback_pct_raw > 1.0 else pullback_pct_raw
        if 0.05 <= pullback_pct <= 0.15:
            reasons.append("pullback_optimal_5-15%")
        elif 0.03 <= pullback_pct <= 0.18:
            reasons.append("pullback_within_range")
    
    elif entry_style == "ENTRY_MOMENTUM":
        if rs >= 80:
            reasons.append("momentum_rs_threshold")
    
    elif entry_style == "ENTRY_VCP":
        vcp_score = _safe_float(entry_data.get("vcp_score", 0.0))
        if vcp_score >= 60:
            reasons.append("vcp_contraction_pattern")
    
    # Fallback if no reasons found
    if not reasons:
        reasons.append("entry_condition_met")
    
    return reasons


def build_us_entry_reject_reasons(skip_reason: str, entry_data: dict[str, Any]) -> list[str]:
    """US entry 거부 사유 리스트 생성.

    Args:
        skip_reason: 메인 skip 사유 (sold_today, held_position, insufficient_volume, ...)
        entry_data: locked watchlist entry row (추가 분석용)

    Returns:
        ["insufficient_volume", "weak_momentum", ...]
    """
    reject_reasons: list[str] = [skip_reason]
    
    # 추가 세부 분석
    volume = _safe_float(entry_data.get("volume", 0.0))
    volume_avg20 = _safe_float(entry_data.get("volume_avg20", 0.0))
    rs = _safe_float(entry_data.get("rs_percentile", 0.0))
    atr_pct = _safe_float(entry_data.get("atr_pct", 0.0))
    
    if volume_avg20 > 0 and volume < volume_avg20 * 0.5:
        reject_reasons.append("volume_too_low")
    
    if rs < 70:
        reject_reasons.append("weak_rs_percentile")
    
    if atr_pct > 12.0:
        reject_reasons.append("atr_exceed_12%")
    
    return reject_reasons


def build_us_filters_passed(entry_data: dict[str, Any]) -> list[str]:
    """US entry에서 통과한 필터 목록.

    Args:
        entry_data: locked watchlist entry row

    Returns:
        ["liquidity_filter", "price_range_filter", "atr_filter", ...]
    """
    filters: list[str] = []
    
    volume = _safe_float(entry_data.get("volume", 0.0))
    volume_avg20 = _safe_float(entry_data.get("volume_avg20", 0.0))
    
    # Liquidity filter
    if volume_avg20 >= 500_000:  # 일평균 거래량 50만주 이상
        filters.append("liquidity_filter")
    
    # Price range filter
    close = _safe_float(entry_data.get("close", 0.0))
    if 5.0 <= close <= 500.0:
        filters.append("price_range_filter")
    
    # ATR filter
    atr_pct = _safe_float(entry_data.get("atr_pct", 0.0))
    if atr_pct <= 12.0:
        filters.append("atr_filter")
    
    # RS filter
    rs = _safe_float(entry_data.get("rs_percentile", 0.0))
    if rs >= 70:
        filters.append("rs_filter")
    
    return filters


def build_us_filters_failed(entry_data: dict[str, Any]) -> list[str]:
    """US entry에서 실패한 필터 목록.

    Args:
        entry_data: locked watchlist entry row

    Returns:
        ["liquidity_filter", "atr_exceed", ...]
    """
    filters: list[str] = []
    
    volume = _safe_float(entry_data.get("volume", 0.0))
    volume_avg20 = _safe_float(entry_data.get("volume_avg20", 0.0))
    
    # Liquidity filter
    if volume_avg20 < 500_000:
        filters.append("liquidity_filter")
    
    # Price range filter
    close = _safe_float(entry_data.get("close", 0.0))
    if close < 5.0:
        filters.append("price_too_low")
    elif close > 500.0:
        filters.append("price_too_high")
    
    # ATR filter
    atr_pct = _safe_float(entry_data.get("atr_pct", 0.0))
    if atr_pct > 12.0:
        filters.append("atr_exceed")
    
    # RS filter
    rs = _safe_float(entry_data.get("rs_percentile", 0.0))
    if rs < 70:
        filters.append("rs_filter")
    
    # Pullback range filter
    pullback_pct_raw = _safe_float(entry_data.get("pullback_pct", 0.0))
    pullback_pct = pullback_pct_raw / 100.0 if pullback_pct_raw > 1.0 else pullback_pct_raw
    if pullback_pct < 0.03 or pullback_pct > 0.18:
        filters.append("pullback_range_fail")
    
    return filters


def build_us_entry_explanation(
    symbol: str,
    entry_data: dict[str, Any],
    decision: str,  # "BUY" 또는 "SKIP"
    skip_reason: str | None = None,
) -> dict[str, Any]:
    """US entry decision에 대한 완전한 설명 생성.

    Args:
        symbol: 종목 심볼 (AAPL, MSFT, ...)
        entry_data: locked watchlist entry row (dict)
        decision: "BUY" 또는 "SKIP"
        skip_reason: SKIP인 경우 메인 사유

    Returns:
        {
            "symbol": "AAPL",
            "decision": "BUY" | "SKIP",
            "entry_style_selected": "ENTRY_BREAKOUT",
            "entry_component": "breakout_pivot",
            "score_breakdown": {...},
            "reasons": [...],
            "reject_reasons": [...],
            "filters_passed": [...],
            "filters_failed": [...],
            "explanation_quality": "FULL",
        }
    """
    # Entry style 정규화
    raw_style = entry_data.get("entry_style_selected") or entry_data.get("entry_signal") or ""
    entry_style = normalize_us_entry_style(raw_style)
    
    # Score breakdown 추출
    score_breakdown = extract_us_score_breakdown(entry_data)
    
    # Reasons 생성
    if decision == "BUY":
        reasons = build_us_entry_reasons(entry_data, entry_style)
        reject_reasons = []
    else:
        reasons = []
        reject_reasons = build_us_entry_reject_reasons(skip_reason or "unknown", entry_data)
    
    # Filters 분석
    filters_passed = build_us_filters_passed(entry_data)
    filters_failed = build_us_filters_failed(entry_data)
    
    # Entry component (세밀한 하위 분류)
    entry_component = "generic"
    if entry_style == "ENTRY_BREAKOUT":
        pivot = _safe_float(entry_data.get("pivot_price", 0.0))
        entry_component = "breakout_pivot" if pivot > 0 else "breakout_50d_high"
    elif entry_style == "ENTRY_PULLBACK":
        entry_component = "pullback_reversal"
    elif entry_style == "ENTRY_MOMENTUM":
        entry_component = "momentum_continuation"
    elif entry_style == "ENTRY_VCP":
        entry_component = "vcp_contraction"
    
    # Explanation quality 평가
    quality = evaluate_explanation_quality({
        "entry_style_selected": entry_style,
        "score_breakdown": score_breakdown,
        "reasons": reasons,
        "reject_reasons": reject_reasons,
        "filters_passed": filters_passed,
        "filters_failed": filters_failed,
    })
    
    return {
        "symbol": symbol,
        "decision": decision.upper(),
        "entry_style_selected": entry_style,
        "entry_component": entry_component,
        "score_breakdown": score_breakdown,
        "reasons": reasons,
        "reject_reasons": reject_reasons,
        "filters_passed": filters_passed,
        "filters_failed": filters_failed,
        "explanation_quality": quality,
    }


def build_us_exit_explanation(
    symbol: str,
    position: dict[str, Any],
    exit_intent: dict[str, Any] | None,
    current_price: float,
) -> dict[str, Any]:
    """US exit decision에 대한 완전한 설명 생성.

    Args:
        symbol: 종목 심볼
        position: 포지션 정보 {qty, entry_price, entry_date, max_price, ...}
        exit_intent: 청산 intent (None이면 보유)
        current_price: 현재가

    Returns:
        {
            "symbol": "AAPL",
            "decision": "SELL" | "HOLD",
            "exit_style": "trailing_stop" | "no_exit",
            "exit_trigger": "trail_threshold_breach" | None,
            "why_not_sell": "no_exit_signal",
            "pnl_pct": 0.12,
            "unrealized_pnl_usd": 1200.0,
            "reasons": ["trail_threshold_breach"],
            "explanation_quality": "FULL",
        }
    """
    entry_price = _safe_float(position.get("entry_price", 0.0))
    max_price = _safe_float(position.get("max_price", current_price))
    qty = int(position.get("qty", 0))

    # entry_price missing 처리 — 0.0000으로 위장 금지
    if entry_price <= 0:
        pnl_pct: float | None = None
        unrealized_pnl_usd: float | None = None
    else:
        pnl_pct = (current_price - entry_price) / entry_price
        unrealized_pnl_usd = (current_price - entry_price) * qty

    if exit_intent is None:
        # Hold decision
        if entry_price <= 0:
            why_not_sell = "pnl_missing_entry_price"
        else:
            why_not_sell = "no_exit_signal"

            # 보유 근거 상세 분석
            if pnl_pct is not None and pnl_pct < 0:
                if abs(pnl_pct) < 0.05:
                    why_not_sell = "within_risk_tolerance_under_5%"
                else:
                    why_not_sell = "stop_not_hit_yet"
            else:
                if max_price > 0 and current_price < max_price:
                    trail_pct = (max_price - current_price) / max_price
                    if trail_pct < 0.05:
                        why_not_sell = "price_near_high_no_trail_signal"
                    else:
                        why_not_sell = "trail_not_breached_yet"
                else:
                    why_not_sell = "no_exit_conditions_met"

        return {
            "symbol": symbol,
            "decision": "HOLD",
            "exit_style": "no_exit",
            "exit_trigger": None,
            "why_not_sell": why_not_sell,
            "pnl_pct": round(pnl_pct, 4) if pnl_pct is not None else None,
            "unrealized_pnl_usd": round(unrealized_pnl_usd, 2) if unrealized_pnl_usd is not None else None,
            "reasons": [why_not_sell],
            "explanation_quality": "FULL",
        }
    
    # Sell decision
    exit_type = exit_intent.get("exit_type", "unknown")
    exit_reason = exit_intent.get("reason", "")
    
    # Exit trigger 정규화
    exit_trigger_map = {
        "hard_stop": "stop_loss_hit",
        "trailing_stop": "trail_threshold_breach",
        "profit_protect": "profit_protect_triggered",
        "giveback": "giveback_limit_exceed",
        "time_stop": "time_limit_exceed",
        "risk_off": "risk_off_signal",
        "pnl_missing_fail_closed": "pnl_missing_fail_closed",
    }
    exit_trigger = exit_trigger_map.get(exit_type, f"{exit_type}_triggered")

    # Reasons 생성
    reasons = [exit_reason, exit_trigger]

    # pnl_pct / unrealized_pnl_usd — pnl_missing_fail_closed 시 None/FAIL_CLOSED
    if exit_type == "pnl_missing_fail_closed" or pnl_pct is None:
        _pnl_out = None
        _upnl_out = None
    else:
        _pnl_out = round(pnl_pct, 4)
        _upnl_out = round(unrealized_pnl_usd, 2) if unrealized_pnl_usd is not None else None

    return {
        "symbol": symbol,
        "decision": "SELL",
        "exit_style": exit_type,
        "exit_trigger": exit_trigger,
        "why_not_sell": None,
        "pnl_pct": _pnl_out,
        "unrealized_pnl_usd": _upnl_out,
        "reasons": reasons,
        "explanation_quality": "FULL",
    }


def evaluate_explanation_quality(explanation: dict[str, Any]) -> str:
    """설명 품질 레벨 평가.

    Args:
        explanation: entry/exit explanation dict

    Returns:
        "FULL", "PARTIAL", "MINIMAL", "MISSING"
    """
    # FULL: 모든 핵심 필드 완비
    required_full = {
        "entry_style_selected",
        "score_breakdown",
        "reasons",
    }
    
    has_entry_style = bool(explanation.get("entry_style_selected"))
    has_score_breakdown = bool(explanation.get("score_breakdown"))
    has_reasons = bool(explanation.get("reasons"))
    has_filters = bool(explanation.get("filters_passed") or explanation.get("filters_failed"))
    
    if has_entry_style and has_score_breakdown and has_reasons and has_filters:
        return QUALITY_FULL
    
    # PARTIAL: 일부 핵심 필드만 제공
    if has_entry_style and (has_score_breakdown or has_reasons):
        return QUALITY_PARTIAL
    
    # MINIMAL: score/rank만 제공
    if has_score_breakdown or has_reasons:
        return QUALITY_MINIMAL
    
    # MISSING: 설명 전무
    return QUALITY_MISSING


def validate_explanations_batch(explanations: list[dict[str, Any]]) -> dict[str, Any]:
    """설명 품질 일괄 검증.

    Args:
        explanations: entry/exit explanation 리스트

    Returns:
        {
            "total_count": 10,
            "full_count": 7,
            "partial_count": 2,
            "minimal_count": 1,
            "missing_count": 0,
            "quality_warning": bool,
            "quality_summary": "70% FULL, 20% PARTIAL, 10% MINIMAL",
        }
    """
    if not explanations:
        return {
            "total_count": 0,
            "full_count": 0,
            "partial_count": 0,
            "minimal_count": 0,
            "missing_count": 0,
            "quality_warning": True,
            "quality_summary": "NO_EXPLANATIONS",
        }
    
    total = len(explanations)
    quality_counts = {
        QUALITY_FULL: 0,
        QUALITY_PARTIAL: 0,
        QUALITY_MINIMAL: 0,
        QUALITY_MISSING: 0,
    }
    
    for exp in explanations:
        quality = exp.get("explanation_quality", QUALITY_MISSING)
        quality_counts[quality] = quality_counts.get(quality, 0) + 1
    
    full_pct = (quality_counts[QUALITY_FULL] / total) * 100
    partial_pct = (quality_counts[QUALITY_PARTIAL] / total) * 100
    minimal_pct = (quality_counts[QUALITY_MINIMAL] / total) * 100
    missing_pct = (quality_counts[QUALITY_MISSING] / total) * 100
    
    # Quality warning: 모두 MINIMAL이거나 MISSING이 있으면 경고
    quality_warning = (
        quality_counts[QUALITY_FULL] == 0 
        or quality_counts[QUALITY_MISSING] > 0
        or quality_counts[QUALITY_MINIMAL] == total
    )
    
    summary_parts = []
    if quality_counts[QUALITY_FULL] > 0:
        summary_parts.append(f"{full_pct:.0f}% FULL")
    if quality_counts[QUALITY_PARTIAL] > 0:
        summary_parts.append(f"{partial_pct:.0f}% PARTIAL")
    if quality_counts[QUALITY_MINIMAL] > 0:
        summary_parts.append(f"{minimal_pct:.0f}% MINIMAL")
    if quality_counts[QUALITY_MISSING] > 0:
        summary_parts.append(f"{missing_pct:.0f}% MISSING")
    
    quality_summary = ", ".join(summary_parts) if summary_parts else "NO_QUALITY_DATA"
    
    return {
        "total_count": total,
        "full_count": quality_counts[QUALITY_FULL],
        "partial_count": quality_counts[QUALITY_PARTIAL],
        "minimal_count": quality_counts[QUALITY_MINIMAL],
        "missing_count": quality_counts[QUALITY_MISSING],
        "quality_warning": quality_warning,
        "quality_summary": quality_summary,
    }


def log_us_entry_decision(
    symbol: str,
    decision: str,
    explanation: dict[str, Any],
) -> None:
    """US entry decision 로그 출력.

    Args:
        symbol: 종목 심볼
        decision: "BUY" 또는 "SKIP"
        explanation: build_us_entry_explanation 결과
    """
    entry_style = explanation.get("entry_style_selected", "UNKNOWN")
    score_breakdown = explanation.get("score_breakdown", {})
    reasons = explanation.get("reasons", [])
    reject_reasons = explanation.get("reject_reasons", [])
    
    if decision == "BUY":
        logger.info(
            "[US_ENTRY][WHY_BUY] symbol=%s entry_style=%s breakout_score=%.1f "
            "pullback_score=%.1f momentum_score=%.1f reasons=%s",
            symbol,
            entry_style,
            score_breakdown.get("breakout_score", 0.0),
            score_breakdown.get("pullback_score", 0.0),
            score_breakdown.get("momentum_score", 0.0),
            reasons,
        )
    else:
        logger.info(
            "[US_ENTRY][WHY_SKIP] symbol=%s reject_reasons=%s filters_failed=%s",
            symbol,
            reject_reasons,
            explanation.get("filters_failed", []),
        )


def log_us_exit_decision(
    symbol: str,
    decision: str,
    explanation: dict[str, Any],
) -> None:
    """US exit decision 로그 출력.

    Args:
        symbol: 종목 심볼
        decision: "SELL" 또는 "HOLD"
        explanation: build_us_exit_explanation 결과
    """
    exit_style = explanation.get("exit_style", "UNKNOWN")
    exit_trigger = explanation.get("exit_trigger")
    why_not_sell = explanation.get("why_not_sell")
    pnl_pct = explanation.get("pnl_pct")
    reasons = explanation.get("reasons", [])

    # pnl_pct None → MISSING 표시
    pnl_display = "MISSING" if pnl_pct is None else f"{pnl_pct:.4f}"

    if decision == "SELL":
        _exit_type = explanation.get("exit_style", "unknown")
        if _exit_type == "pnl_missing_fail_closed":
            logger.error(
                "[US_EXIT][WHY_SELL] symbol=%s exit_style=%s pnl_pct=%s "
                "exit_trigger=%s reasons=%s note=FAIL_CLOSED",
                symbol,
                exit_style,
                pnl_display,
                exit_trigger,
                reasons,
            )
        else:
            logger.info(
                "[US_EXIT][WHY_SELL] symbol=%s exit_style=%s pnl_pct=%s "
                "exit_trigger=%s reasons=%s",
                symbol,
                exit_style,
                pnl_display,
                exit_trigger,
                reasons,
            )
    else:
        logger.info(
            "[US_EXIT][WHY_HOLD] symbol=%s why_not_sell=%s pnl_pct=%s",
            symbol,
            why_not_sell,
            pnl_display,
        )
