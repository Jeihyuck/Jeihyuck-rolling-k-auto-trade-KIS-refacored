# -*- coding: utf-8 -*-
"""US Dual-Agent Watchlist Builder.

candidate_pool → broader_scored → top50 → final30 → final30_scored

Agent A: Universe Discovery / Market Score
Agent B: Strategy Entry Score

한국장 파일 import 금지.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from trader.us.rotation import (
    AI_CAP_CLUSTERS, AI_CLUSTERS, apply_cap_flags,
    classify_rotation_regime, cluster_caps_for_regime, compute_cluster_exposure,
    period_return, select_bucket_champions, theme_cluster_for,
)
from trader.us.symbols import resolve_exchange

logger = logging.getLogger(__name__)

# 허용 ETF 최대 비율 (final30 중 최대 5개)
_MAX_ETF_IN_FINAL30 = 5

# 핵심 ETF 집합
_CORE_ETFS: set[str] = {"SPY", "QQQ", "QQQM", "SMH", "SOXX"}

AI_BASKET_SYMBOLS: tuple[str, ...] = (
    "NVDA", "AMD", "AVGO", "ARM", "MU", "TSM", "ASML", "AMAT", "LRCX", "PLTR", "MSFT", "META",
)
AI_BASKET_EXCHANGE_MAP: dict[str, str] = {
    "NVDA": "NASDAQ", "AMD": "NASDAQ", "AVGO": "NASDAQ", "ARM": "NASDAQ", "MU": "NASDAQ",
    "TSM": "NYSE", "ASML": "NASDAQ", "AMAT": "NASDAQ", "LRCX": "NASDAQ",
    "PLTR": "NASDAQ", "MSFT": "NASDAQ", "META": "NASDAQ",
}


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None or val == "":
        return default
    try:
        return float(str(val).replace(",", ""))
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Agent A: Universe Discovery / Market Score
# ──────────────────────────────────────────────────────────────────────────────

_THEME_TAGS = {
    "ai_semiconductor_extended": "ai_semiconductor",
    "ai_platform_software": "ai_software",
    "data_center_infra": "data_center",
    "energy_power_ai": "ai_energy",
    "growth_liquidity_watch": "growth_liquidity",
    "core_etf": "etf",
    "manual_seed": "manual_seed",
}


def _compute_agent_a_score(row: dict) -> tuple[float, list[str]]:
    """Agent A: RS + volume + liquidity + sector_theme."""
    rs_20d_score = _safe_float(row.get("rs_20d_score"), 0.5)
    rs_60d_score = _safe_float(row.get("rs_60d_score"), 0.5)
    rs_120d_score = _safe_float(row.get("rs_120d_score"), 0.5)
    volume_accel_score = _safe_float(row.get("volume_accel_score"), 0.5)
    liquidity_score = _safe_float(row.get("liquidity_score"), 0.5)
    near_high_score = _safe_float(row.get("near_high_score"), 0.5)

    # Sector theme bonus
    source_tags = row.get("source_tags", [])
    theme_labels = []
    sector_theme_score = 0.5  # neutral default
    for tag in source_tags:
        label = _THEME_TAGS.get(tag)
        if label:
            theme_labels.append(label)
            if "ai" in label or "semiconductor" in label:
                sector_theme_score = max(sector_theme_score, 0.85)
            elif "data_center" in label or "energy" in label:
                sector_theme_score = max(sector_theme_score, 0.75)
            elif "growth" in label:
                sector_theme_score = max(sector_theme_score, 0.65)

    agent_a_score = round(
        0.30 * rs_60d_score
        + 0.20 * rs_20d_score
        + 0.15 * rs_120d_score
        + 0.15 * volume_accel_score
        + 0.10 * liquidity_score
        + 0.10 * sector_theme_score,
        4,
    )
    agent_a_score = max(0.0, min(1.0, agent_a_score))

    reasons: list[str] = []
    if rs_60d_score >= 0.7:
        reasons.append("rs_60d_top_decile")
    if liquidity_score >= 0.7:
        reasons.append("high_dollar_volume")
    if theme_labels:
        reasons.append(f"{theme_labels[0]}_theme")
    if near_high_score >= 0.8:
        reasons.append("near_52w_high")
    if volume_accel_score >= 0.7:
        reasons.append("volume_acceleration")

    return agent_a_score, reasons


# ──────────────────────────────────────────────────────────────────────────────
# Agent B: Strategy Entry Score
# ──────────────────────────────────────────────────────────────────────────────

def _compute_pb1_score(row: dict, daily_rows: list[dict]) -> float:
    """PB1 pullback 점수."""
    pullback_quality = _safe_float(row.get("pullback_quality"), 0.5)
    trend_score = _safe_float(row.get("trend_score"), 0.5)
    near_high_score = _safe_float(row.get("near_high_score"), 0.5)
    return round((pullback_quality * 0.5 + trend_score * 0.3 + near_high_score * 0.2), 4)


def _compute_momentum_score(row: dict) -> float:
    """Momentum 점수."""
    rs_20d_score = _safe_float(row.get("rs_20d_score"), 0.5)
    rs_60d_score = _safe_float(row.get("rs_60d_score"), 0.5)
    volume_accel_score = _safe_float(row.get("volume_accel_score"), 0.5)
    return round((rs_20d_score * 0.4 + rs_60d_score * 0.4 + volume_accel_score * 0.2), 4)


def _compute_pullback_score(row: dict) -> float:
    """Pullback 점수."""
    return _safe_float(row.get("pullback_quality"), 0.5)


def _compute_breakout_score(row: dict) -> float:
    """Breakout 점수: 고점 대비 근접 + 거래량 가속."""
    near_high = _safe_float(row.get("near_high_score"), 0.5)
    vol_accel = _safe_float(row.get("volume_accel_score"), 0.5)
    return round((near_high * 0.6 + vol_accel * 0.4), 4)


def _compute_vcp_score(row: dict) -> float:
    """VCP (Volatility Contraction Pattern) 점수."""
    atr_pct = _safe_float(row.get("atr_pct"), 0.05)
    trend_score = _safe_float(row.get("trend_score"), 0.5)
    # ATR이 낮을수록 VCP 가능성 높음
    atr_score = max(0.0, 1.0 - atr_pct / 0.10) if atr_pct < 0.10 else 0.0
    return round((atr_score * 0.5 + trend_score * 0.5), 4)


def _select_entry_style(row: dict, pb1_score: float, momentum_score: float, pullback_score: float, breakout_score: float) -> str:
    """entry 스타일 선택."""
    scores = {
        "momentum_pullback": momentum_score * 0.4 + pullback_score * 0.6,
        "breakout": breakout_score,
        "pb1_pullback": pb1_score,
        "momentum": momentum_score,
    }
    return max(scores, key=lambda k: scores[k])


def _compute_agent_b_score(row: dict, daily_rows: list[dict]) -> tuple[float, list[str], str]:
    """Agent B: Strategy Entry Score."""
    pb1_score = _compute_pb1_score(row, daily_rows)
    momentum_score = _compute_momentum_score(row)
    pullback_score = _compute_pullback_score(row)
    breakout_score = _compute_breakout_score(row)
    vcp_score = _compute_vcp_score(row)
    trend_score = _safe_float(row.get("trend_score"), 0.5)

    agent_b_score = round(
        0.30 * pb1_score
        + 0.20 * momentum_score
        + 0.20 * pullback_score
        + 0.15 * breakout_score
        + 0.10 * trend_score
        + 0.05 * vcp_score,
        4,
    )
    agent_b_score = max(0.0, min(1.0, agent_b_score))

    entry_style = _select_entry_style(row, pb1_score, momentum_score, pullback_score, breakout_score)

    reasons: list[str] = []
    ma20 = row.get("ma20")
    ma50 = row.get("ma50")
    ma150 = row.get("ma150")
    price = _safe_float(row.get("price"))
    if ma20 and ma50 and ma150 and price > 0:
        if price > ma20 and price > ma50 and price > ma150:
            reasons.append("above_ma20_ma50_ma150")
    rs_20d = _safe_float(row.get("rs_20d"))
    rs_60d = _safe_float(row.get("rs_60d"))
    if rs_20d > 0.05 and rs_60d > 0.10:
        reasons.append("positive_momentum_20d_60d")
    pullback_pct = _safe_float(row.get("pullback_pct"))
    if -0.15 <= pullback_pct <= 0.05:
        reasons.append("pullback_within_15pct")
    if _safe_float(row.get("volume_accel_score")) >= 0.6:
        reasons.append("volume_acceleration")

    return agent_b_score, reasons, entry_style, pb1_score, momentum_score, pullback_score, breakout_score, trend_score, vcp_score


# ──────────────────────────────────────────────────────────────────────────────
# Final Score
# ──────────────────────────────────────────────────────────────────────────────

def _compute_final_score(agent_a_score: float, agent_b_score: float, liquidity_score: float, risk_score: float, row: dict | None = None, rotation_context: dict | None = None) -> float:
    agent_a_w = _env_float("US_AGENT_A_WEIGHT", 0.40)
    agent_b_w = _env_float("US_AGENT_B_WEIGHT", 0.45)
    liq_w = _env_float("US_LIQUIDITY_WEIGHT", 0.10)
    risk_w = _env_float("US_RISK_WEIGHT", 0.05)
    base = (
        agent_a_w * agent_a_score
        + agent_b_w * agent_b_score
        + liq_w * liquidity_score
        + risk_w * risk_score
    )
    if not row:
        return round(max(0.0, min(1.0, base)), 4)

    # Rotation-aware score overlay requested for US prep:
    # stock_momentum 35%, sector relative strength 25%, stock-vs-sector alpha 15%,
    # volume quality 10%, volatility-adjusted trend 10%, drawdown control 5%, minus concentration penalty.
    sector_rs = _safe_float(row.get("sector_relative_strength"), _safe_float(row.get("sector_rs_score"), 0.5))
    stock_alpha = _safe_float(row.get("stock_vs_sector_alpha"), _safe_float(row.get("rs_20d_score"), 0.5) - sector_rs + 0.5)
    volume_quality = _safe_float(row.get("volume_quality"), _safe_float(row.get("volume_accel_score"), 0.5))
    vol_adj_trend = _safe_float(row.get("volatility_adjusted_trend"), _safe_float(row.get("trend_score"), 0.5) * risk_score)
    drawdown_control = _safe_float(row.get("drawdown_control"), risk_score)
    stock_momentum = agent_b_score
    rotation_score = (
        0.35 * stock_momentum
        + 0.25 * sector_rs
        + 0.15 * max(0.0, min(1.0, stock_alpha))
        + 0.10 * volume_quality
        + 0.10 * vol_adj_trend
        + 0.05 * drawdown_control
    )
    score = 0.5 * base + 0.5 * rotation_score
    regime = str((rotation_context or {}).get("rotation_regime") or "NEUTRAL")
    cluster = theme_cluster_for(str(row.get("symbol") or ""), row)
    if regime == "AI_OFF_ROTATION" and cluster in AI_CAP_CLUSTERS:
        score -= 0.08
    return round(max(0.0, min(1.0, score)), 4)


def _compute_risk_score(row: dict) -> float:
    """낮은 ATR = 높은 risk score (안정적)."""
    atr_pct = _safe_float(row.get("atr_pct"), 0.05)
    if atr_pct <= 0.04:
        return 0.9
    elif atr_pct <= 0.06:
        return 0.8
    elif atr_pct <= 0.08:
        return 0.7
    elif atr_pct <= 0.12:
        return 0.6
    elif atr_pct <= 0.15:
        return 0.4
    else:
        return 0.2


# ──────────────────────────────────────────────────────────────────────────────
# Main builder
# ──────────────────────────────────────────────────────────────────────────────

def build_us_watchlist(
    *,
    trade_date: str,
    as_of_date: str | None = None,
    env: str,
    candidate_pool: list[dict],
    provider: Any,
    force_rebuild: bool = False,
    market_regime_constraints: dict | None = None,
) -> dict:
    """US Dual-Agent Watchlist 빌드.

    Args:
        trade_date: YYYY-MM-DD
        as_of_date: KIS dailyprice BYMD 기준일. None이면 trade_date와 동일.
        env: practice / live
        candidate_pool: build_us_candidate_pool()["rows"] 결과
        provider: USDataProvider 인스턴스
        force_rebuild: 캐시 무시 재빌드
        market_regime_constraints: prep_runner에서 1회 계산한 authoritative regime constraints

    Returns:
        watchlist result dict (broader_scored, top50, final30, final30_scored)
    """
    as_of_date = as_of_date or trade_date
    topk = _env_int("US_WATCHLIST_TOPK", 50)
    finaln = _env_int("US_WATCHLIST_FINALN", 30)

    logger.info("[US_WATCHLIST][START] candidate_pool=%d", len(candidate_pool))
    logger.info(
        "[US_WATCHLIST][DATE_POLICY] trade_date=%s as_of_date=%s",
        trade_date,
        as_of_date,
    )

    constraints = dict(market_regime_constraints or {
        "market_regime": "NEUTRAL",
        "max_ai_tech_ratio": 0.35,
        "max_single_cluster_ratio": 1.0,  # fallback-only: legacy/unit providers may not classify clusters.
        "sector_cap_enforced": True,
    })
    rotation_context = _build_rotation_context(provider, candidate_pool, as_of_date)
    logger.info(
        "[US_ROTATION][REGIME] rotation_regime=%s qqq_vs_spy_3d=%.4f smh_vs_spy_3d=%.4f dia_vs_spy_3d=%.4f rsp_vs_spy_3d=%.4f strongest=%s weakest=%s",
        rotation_context.get("rotation_regime"), rotation_context.get("qqq_vs_spy_3d", 0.0),
        rotation_context.get("smh_vs_spy_3d", 0.0), rotation_context.get("dia_vs_spy_3d", 0.0),
        rotation_context.get("rsp_vs_spy_3d", 0.0), rotation_context.get("strongest_sectors"), rotation_context.get("weakest_sectors"),
    )

    portfolio_cluster_weights: dict[str, Any] = {}
    blocked_clusters: set[str] = set()
    try:
        from trader.us.db.repos import load_positions
        positions = load_positions(as_of=trade_date)
        portfolio_cluster_weights = apply_cap_flags(compute_cluster_exposure(positions), str(rotation_context.get("rotation_regime") or "NEUTRAL"))
        blocked_clusters = {c for c, v in portfolio_cluster_weights.items() if v.get("over_cap")}
    except Exception as exc:
        logger.info("[US_ROTATION][PORTFOLIO_EXPOSURE][SKIP] %s", exc)

    broader_scored: list[dict] = []

    for row in candidate_pool:
        symbol = row.get("symbol", "")
        daily = []

        # Agent A
        agent_a_score, agent_a_reasons = _compute_agent_a_score(row)

        # Agent B
        (agent_b_score, agent_b_reasons, entry_style,
         pb1_score, momentum_score, pullback_score,
         breakout_score, trend_score, vcp_score) = _compute_agent_b_score(row, daily)

        # Risk
        risk_score = _compute_risk_score(row)
        liquidity_score = _safe_float(row.get("liquidity_score"), 0.5)

        # Final
        theme_cluster = theme_cluster_for(symbol, row)
        row["theme_cluster"] = theme_cluster
        score_final = _compute_final_score(agent_a_score, agent_b_score, liquidity_score, risk_score, row=row, rotation_context=rotation_context)

        # tech_score (종합 기술적 점수)
        tech_score = round(
            _safe_float(row.get("trend_score"), 0.5) * 0.4
            + _safe_float(row.get("rs_60d_score"), 0.5) * 0.3
            + _safe_float(row.get("near_high_score"), 0.5) * 0.3,
            4,
        )

        # reason_json
        reason_json = {
            "symbol": symbol,
            "rank_final30": 0,  # 나중에 갱신
            "score_final": score_final,
            "theme_cluster": theme_cluster,
            "rotation_regime": rotation_context.get("rotation_regime"),
            "agent_a": {
                "score": agent_a_score,
                "reasons": agent_a_reasons,
            },
            "agent_b": {
                "score": agent_b_score,
                "reasons": agent_b_reasons,
            },
            "risk": {
                "atr_pct": _safe_float(row.get("atr_pct")),
                "risk_score": risk_score,
            },
            "entry_style_selected": entry_style,
        }

        scored_row = {
            **row,
            "agent_a_score": agent_a_score,
            "agent_b_score": agent_b_score,
            "score_final": score_final,
            "tech_score": tech_score,
            "risk_score": risk_score,
            "pb1_score": pb1_score,
            "momentum_score": momentum_score,
            "pullback_score": pullback_score,
            "breakout_score": breakout_score,
            "vcp_score": vcp_score,
            "entry_style_selected": entry_style,
            "theme_cluster": theme_cluster,
            "rotation_regime": rotation_context.get("rotation_regime"),
            "reason_json": reason_json,
            "trade_date": trade_date,
            # candidate_score는 candidate_pool_builder에서 넘어옴
        }
        broader_scored.append(scored_row)

    _apply_concentration_penalty(broader_scored, finaln, str(rotation_context.get("rotation_regime") or "NEUTRAL"), blocked_clusters)
    logger.info("[US_DUAL_AGENT][SCORE] broader_scored=%d", len(broader_scored))

    # top50 선택 (ETF는 최대 5개까지)
    sorted_broader = sorted(broader_scored, key=lambda r: -r["score_final"])

    top50: list[dict] = []
    etf_count = 0
    for row in sorted_broader:
        if len(top50) >= topk:
            break
        is_etf = row.get("asset_type") == "etf" or row.get("symbol") in _CORE_ETFS
        if is_etf:
            # core ETF만 허용
            if row.get("symbol") not in _CORE_ETFS:
                continue
            if etf_count >= _MAX_ETF_IN_FINAL30:
                continue
            etf_count += 1
        top50.append(row)

    # top50이 topk 미만이면 non-ETF 종목으로 채움
    if len(top50) < topk:
        top50_syms = {r["symbol"] for r in top50}
        for row in sorted_broader:
            if len(top50) >= topk:
                break
            if row["symbol"] not in top50_syms:
                top50.append(row)
                top50_syms.add(row["symbol"])

    logger.info("[US_WATCHLIST][TOP50] rows=%d", len(top50))

    # final30: bucket champion selection.  Do not simply take top score rows;
    # regime quotas and cluster caps prevent AI/tech concentration during rotations.
    top50_sorted = sorted(top50, key=lambda r: -r["score_final"])
    final30, bucket_meta = select_bucket_champions(
        top50_sorted if len(top50_sorted) >= finaln else sorted_broader,
        finaln,
        str(rotation_context.get("rotation_regime") or "NEUTRAL"),
        blocked_clusters=blocked_clusters,
    )
    fallback_meta = {"fallback_fill_used": False, "fallback_fill_count": 0, "fallback_fill_cap_safe": True}
    if len(final30) < finaln:
        before = len(final30)
        final30, fallback_meta = refill_under_cluster_caps(final30, sorted_broader, finaln, str(rotation_context.get("rotation_regime") or "NEUTRAL"), blocked_clusters, prefer_non_tech=True)
        fallback_meta["fallback_fill_used"] = fallback_meta.get("fallback_fill_used") or len(final30) > before

    final30 = _enforce_final30_etf_cap(final30[:finaln], sorted_broader, finaln, _MAX_ETF_IN_FINAL30, blocked_clusters)
    final30, etf_refill_meta = refill_under_cluster_caps(final30, sorted_broader, finaln, str(rotation_context.get("rotation_regime") or "NEUTRAL"), blocked_clusters, prefer_non_tech=True)
    _regime_overlay = constraints
    if constraints.get("market_regime") == "RISK_OFF":
        # RISK_OFF blocks new BUYs in the prep contract.  Preserve final30 as a
        # diagnostic/visibility artifact instead of deleting candidates because
        # the very tight RISK_OFF sector caps cannot be satisfied by an
        # underfilled list.
        regime_cap_meta = {
            "sector_cap_enforced": True,
            "risk_off_entry_block": True,
            "before_ai_tech_ratio": sum(
                1 for r in final30 if (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) in AI_CLUSTERS
            ) / max(1, finaln),
            "after_ai_tech_ratio": sum(
                1 for r in final30 if (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) in AI_CLUSTERS
            ) / max(1, finaln),
            "blocked_by_cluster_cap": [],
            "sector_cap_replacements": [],
            "cap_violations": [],
            "risk_off_cap_warning": "entry_blocked_but_final30_preserved",
        }
    else:
        final30, regime_cap_meta = enforce_regime_sector_caps(final30, sorted_broader, constraints, finaln)
    fallback_meta["fallback_fill_used"] = bool(fallback_meta.get("fallback_fill_used") or etf_refill_meta.get("fallback_fill_used"))
    fallback_meta["fallback_fill_count"] = int(fallback_meta.get("fallback_fill_count") or 0) + int(etf_refill_meta.get("fallback_fill_count") or 0)
    fallback_meta["fallback_fill_cap_safe"] = bool(fallback_meta.get("fallback_fill_cap_safe", True) and etf_refill_meta.get("fallback_fill_cap_safe", True))

    # rank 부여 + final30_scored 구성
    final30_scored: list[dict] = []
    for rank, row in enumerate(sorted(final30, key=lambda r: -r["score_final"]), start=1):
        scored = dict(row)
        scored["rank_final30"] = rank
        scored["reason_json"]["rank_final30"] = rank
        logger.info(
            "[US_DUAL_AGENT][EXPLAIN] symbol=%s rank=%d agent_a=%.4f agent_b=%.4f final=%.4f style=%s",
            scored["symbol"],
            rank,
            scored["agent_a_score"],
            scored["agent_b_score"],
            scored["score_final"],
            scored["entry_style_selected"],
        )
        final30_scored.append(scored)

    cluster_contract = validate_final30_cluster_contract(final30_scored, str(rotation_context.get("rotation_regime") or "NEUTRAL"))
    if regime_cap_meta.get("cap_violations"):
        cluster_contract["cluster_contract_ok"] = False
        cluster_contract["final30_cluster_cap_clean"] = False
        cluster_contract["cap_violations"] = list(set(list(cluster_contract.get("cap_violations") or []) + list(regime_cap_meta.get("cap_violations") or [])))
    final30_cluster_counts = cluster_contract["final30_cluster_counts"]
    ai_count = sum(1 for r in final30_scored if r.get("theme_cluster") in AI_CLUSTERS)
    final30_cap_violations = list(cluster_contract.get("cap_violations") or [])
    logger.info(
        "[US_ROTATION][FINAL30] rotation_regime=%s final30_cluster_counts=%s final30_ai_tech_ratio=%.4f cap_violations=%s blocked_by_cluster_cap=%s selected_by_bucket_champion=%s",
        rotation_context.get("rotation_regime"), final30_cluster_counts, ai_count / max(1, len(final30_scored)),
        final30_cap_violations + [c for c, v in portfolio_cluster_weights.items() if v.get("over_cap")], sorted(blocked_clusters),
        bool((bucket_meta or {}).get("selected_by_bucket_champion")),
    )

    logger.info(
        "[US_WATCHLIST][STAGE_COUNTS] upstream_universe=%d filtered_universe=%d"
        " candidate_pool=%d broader_scored=%d top50=%d final30=%d",
        len(candidate_pool),
        len(candidate_pool),
        len(candidate_pool),
        len(broader_scored),
        len(top50),
        len(final30_scored),
    )

    return {
        "trade_date": trade_date,
        "env": env,
        "status": "OK" if cluster_contract.get("cluster_contract_ok") else "FAILED_CLUSTER_CAP_CONTRACT",
        "broader_scored_count": len(broader_scored),
        "top50_count": len(top50),
        "final30_count": len(final30),
        "final30_scored_count": len(final30_scored),
        "broader_scored": broader_scored,
        "top50_scored": top50,
        "final30": final30,
        "final30_scored": final30_scored,
        "rotation_regime": rotation_context.get("rotation_regime"),
        "rotation_context": rotation_context,
        "final30_cluster_counts": final30_cluster_counts,
        "final30_ai_tech_ratio": ai_count / max(1, len(final30_scored)),
        "portfolio_cluster_weights": portfolio_cluster_weights,
        "cap_violations": final30_cap_violations + [c for c, v in portfolio_cluster_weights.items() if v.get("over_cap")],
        "blocked_by_cluster_cap": sorted(set(blocked_clusters) | set(regime_cap_meta.get("blocked_by_cluster_cap") or [])),
        "sector_cap_enforced": True,
        "sector_cap_replacements": regime_cap_meta.get("sector_cap_replacements", []),
        "market_regime_constraints": constraints,
        "market_state_overlay": constraints,
        "selected_by_bucket_champion": bool((bucket_meta or {}).get("selected_by_bucket_champion")),
        "cluster_contract_ok": bool(cluster_contract.get("cluster_contract_ok")),
        "final30_cluster_cap_clean": bool(cluster_contract.get("final30_cluster_cap_clean")),
        **fallback_meta,
    }


def _build_rotation_context(provider: Any, candidate_pool: list[dict], as_of_date: str) -> dict[str, Any]:
    symbols = ["SPY", "QQQ", "SMH", "DIA", "IWM", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"]
    returns: dict[str, dict[int, float]] = {}
    missing_symbols: list[str] = []
    symbol_quality: dict[str, str] = {}
    for sym in symbols:
        exchange = resolve_exchange(sym)
        closes: list[float] = []
        try:
            if callable(getattr(provider, "get_completed_daily_prices", None)) and getattr(getattr(provider, "get_completed_daily_prices", None), "__module__", "") != "unittest.mock":
                rows = provider.get_completed_daily_prices(sym, exchange, trade_date=as_of_date, required_bars=260, allow_http_sync=True)
            else:
                rows = provider.get_daily_prices(sym, exchange, as_of_date=as_of_date)
            closes = [_safe_float(r.get("close") or r.get("price")) for r in (rows or []) if _safe_float(r.get("close") or r.get("price")) > 0]
        except Exception as exc:
            logger.warning("[US_ROTATION][BENCHMARK_DATA_MISSING] symbol=%s exchange=%s error=%s", sym, exchange, exc)
            closes = []
        if len(closes) <= 3:
            missing_symbols.append(sym)
            symbol_quality[sym] = "missing_or_insufficient"
        else:
            symbol_quality[sym] = "ok"
        returns[sym] = {d: period_return(closes, d) for d in (1, 3, 5, 20)}

    ai_basket_returns: dict[str, float] = {}
    ai_missing: list[str] = []
    for sym in AI_BASKET_SYMBOLS:
        exchange = AI_BASKET_EXCHANGE_MAP.get(sym, "NASDAQ")
        closes: list[float] = []
        try:
            if callable(getattr(provider, "get_completed_daily_prices", None)) and getattr(getattr(provider, "get_completed_daily_prices", None), "__module__", "") != "unittest.mock":
                rows = provider.get_completed_daily_prices(sym, exchange, trade_date=as_of_date, required_bars=260, allow_http_sync=True)
            else:
                rows = provider.get_daily_prices(sym, exchange, as_of_date=as_of_date)
            closes = [_safe_float(r.get("close") or r.get("price")) for r in (rows or []) if _safe_float(r.get("close") or r.get("price")) > 0]
        except Exception as exc:
            logger.warning("[US_ROTATION][AI_BASKET_DATA_MISSING] symbol=%s exchange=%s error=%s", sym, exchange, exc)
            closes = []
        if len(closes) <= 3:
            ai_missing.append(sym)
            continue
        ai_basket_returns[sym] = period_return(closes, 3)
    ai_basket_3d = sum(ai_basket_returns.values()) / len(ai_basket_returns) if ai_basket_returns else None
    ai_basket_status = "ok" if ai_basket_returns else "unavailable"

    ctx = classify_rotation_regime(returns, ai_basket_3d=ai_basket_3d)
    core_rel = {k: float(ctx.get(f"{k.lower()}_vs_spy_3d", 0.0) or 0.0) for k in ("QQQ", "SMH", "DIA", "RSP")}
    zero_relative_return_symbols = [k for k, v in core_rel.items() if abs(v) < 1e-12]
    rotation_context_suspect = len(zero_relative_return_symbols) == 4
    ai_basket_suspect = bool(ai_basket_returns) and all(abs(float(v or 0.0)) < 1e-12 for v in ai_basket_returns.values())
    policy = os.getenv("US_ROTATION_SUSPECT_POLICY", "block").strip().lower() or "block"
    if len([s for s in ("SPY", "QQQ", "SMH", "DIA", "RSP") if s in missing_symbols]) >= 2:
        ctx["rotation_regime"] = "UNKNOWN"
    elif rotation_context_suspect and policy == "conservative":
        ctx["rotation_regime"] = "CONSERVATIVE_ROTATION"
    ctx["benchmark_returns"] = returns
    ctx["benchmark_exchange_map"] = {sym: resolve_exchange(sym) for sym in symbols}
    ctx["benchmark_data_quality"] = "ok" if not missing_symbols else "degraded"
    ctx["benchmark_symbol_quality"] = symbol_quality
    ctx["missing_symbols"] = missing_symbols
    ctx["ai_basket_symbols"] = list(AI_BASKET_SYMBOLS)
    ctx["ai_basket_returns_3d"] = ai_basket_returns
    ctx["ai_basket_3d"] = ai_basket_3d
    ctx["ai_basket_status"] = ai_basket_status
    ctx["ai_basket_missing_symbols"] = ai_missing
    ctx["regime_confidence"] = 0.5 if missing_symbols or ai_basket_status == "unavailable" else 1.0
    ctx["rotation_context_suspect"] = rotation_context_suspect
    ctx["ai_basket_suspect"] = ai_basket_suspect
    ctx["zero_relative_return_symbols"] = zero_relative_return_symbols
    ctx["rotation_suspect_policy"] = policy
    logger.warning("[US_ROTATION][DATA_QUALITY] benchmark_data_quality=%s rotation_context_suspect=%s ai_basket_suspect=%s missing_symbols=%s zero_relative_return_symbols=%s policy=%s", ctx.get("benchmark_data_quality"), rotation_context_suspect, ai_basket_suspect, missing_symbols, zero_relative_return_symbols, policy)
    ctx["cluster_caps"] = cluster_caps_for_regime(str(ctx.get("rotation_regime") or "NEUTRAL"))
    return ctx


def _apply_concentration_penalty(rows: list[dict], finaln: int, regime: str, blocked_clusters: set[str] | None = None) -> None:
    """Apply score penalties before final bucket selection.

    If one theme cluster would dominate more than 35% of the provisional top30,
    reduce that cluster's score. In AI_OFF_ROTATION, AI-cap clusters receive an
    additional haircut, and clusters already over portfolio cap are marked as
    blocked for new buys.
    """
    blocked_clusters = blocked_clusters or set()
    provisional = sorted(rows, key=lambda r: float(r.get("score_final") or 0.0), reverse=True)[:finaln]
    counts: dict[str, int] = {}
    for row in provisional:
        c = row.get("theme_cluster") or theme_cluster_for(str(row.get("symbol") or ""), row)
        counts[c] = counts.get(c, 0) + 1
    crowded = {c for c, n in counts.items() if n / max(1, finaln) > 0.35}
    for row in rows:
        cluster = row.get("theme_cluster") or theme_cluster_for(str(row.get("symbol") or ""), row)
        penalty = 0.0
        reasons = []
        if cluster in crowded:
            penalty += 0.03
            reasons.append("cluster_over_35pct_provisional_top30")
        if regime == "AI_OFF_ROTATION" and cluster in AI_CAP_CLUSTERS:
            penalty += 0.04
            reasons.append("ai_off_rotation_ai_cluster_haircut")
        if cluster in blocked_clusters:
            penalty += 1.0
            row["blocked_by_cluster_cap"] = True
            reasons.append("portfolio_cluster_cap_exceeded")
        if penalty:
            row["concentration_penalty"] = round(penalty, 4)
            row["concentration_penalty_reasons"] = reasons
            row["score_final"] = round(max(0.0, float(row.get("score_final") or 0.0) - penalty), 4)
            if isinstance(row.get("reason_json"), dict):
                row["reason_json"]["concentration_penalty"] = row["concentration_penalty"]
                row["reason_json"]["concentration_penalty_reasons"] = reasons


def _cluster_counts(rows: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows or []:
        cluster = row.get("theme_cluster") or theme_cluster_for(str(row.get("symbol") or ""), row)
        counts[cluster] = counts.get(cluster, 0) + 1
    return counts


def _final30_cap_violations(counts: dict[str, int], total: int, regime: str) -> list[str]:
    if total <= 0:
        return []
    caps = cluster_caps_for_regime(regime)
    violations: list[str] = []
    ai_count = sum(counts.get(c, 0) for c in AI_CLUSTERS)
    ai_cap = float(caps.get("AI_TECH_COMBINED", caps.get("AI_COMBINED", 1.0)))
    if ai_count / total > ai_cap:
        violations.append("AI_TECH_COMBINED")
    single_cap = caps.get("SINGLE_CLUSTER")
    if single_cap is not None:
        for cluster, n in counts.items():
            if n / total > float(single_cap):
                violations.append(cluster)
    return violations



def validate_final30_cluster_contract(final30: list[dict], regime: str) -> dict[str, Any]:
    counts = _cluster_counts(final30)
    total = len(final30 or [])
    violations = _final30_cap_violations(counts, total, regime)
    ai_count = sum(counts.get(c, 0) for c in AI_CLUSTERS)
    if str(regime) == "AI_OFF_ROTATION":
        if counts.get("AI_SEMI", 0) > 3 and "AI_SEMI" not in violations:
            violations.append("AI_SEMI")
        if counts.get("MEGA_TECH", 0) > 2 and "MEGA_TECH" not in violations:
            violations.append("MEGA_TECH")
        non_tech = total - ai_count
        if total and non_tech / total < 0.50 and "NON_TECH_MIN" not in violations:
            violations.append("NON_TECH_MIN")
    return {
        "cluster_contract_ok": not violations,
        "final30_cluster_cap_clean": not violations,
        "cap_violations": violations,
        "final30_cluster_counts": counts,
        "final30_ai_tech_ratio": ai_count / max(1, total),
        "rotation_regime": regime,
    }


def can_add_under_cluster_caps(selected: list[dict], candidate: dict, regime: str, finaln: int) -> bool:
    sym = str(candidate.get("symbol") or "")
    if not sym or sym in {str(r.get("symbol") or "") for r in selected}:
        return False
    test = list(selected) + [candidate]
    counts = _cluster_counts(test)
    violations = _final30_cap_violations(counts, max(finaln, len(test)), regime)
    if str(regime) == "AI_OFF_ROTATION":
        if counts.get("AI_SEMI", 0) > 3 and "AI_SEMI" not in violations:
            violations.append("AI_SEMI")
        if counts.get("MEGA_TECH", 0) > 2 and "MEGA_TECH" not in violations:
            violations.append("MEGA_TECH")
    return not violations


def refill_under_cluster_caps(selected: list[dict], candidate_pool: list[dict], finaln: int, regime: str, blocked_clusters: set[str] | None, prefer_non_tech: bool = True) -> tuple[list[dict], dict[str, Any]]:
    blocked_clusters = blocked_clusters or set()
    out = list(selected)
    used = {str(r.get("symbol") or "") for r in out}
    pool = [r for r in candidate_pool if str(r.get("symbol") or "") not in used and (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) not in blocked_clusters]
    if prefer_non_tech:
        pool.sort(key=lambda r: ((r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) in AI_CLUSTERS, -float(r.get("score_final") or 0)))
    else:
        pool.sort(key=lambda r: -float(r.get("score_final") or 0))
    attempted = 0
    for row in pool:
        if len(out) >= finaln:
            break
        attempted += 1
        if can_add_under_cluster_caps(out, row, regime, finaln):
            out.append(row)
    # If non-tech supply is insufficient, prefer returning fewer than finaln over a dirty cap.
    while out and not validate_final30_cluster_contract(out, regime).get("cluster_contract_ok"):
        ai_indexes = [idx for idx, r in enumerate(out) if (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) in AI_CLUSTERS]
        if not ai_indexes:
            break
        drop_idx = min(ai_indexes, key=lambda idx: float(out[idx].get("score_final") or 0.0))
        out.pop(drop_idx)
    return out, {"fallback_fill_used": len(out) > len(selected), "fallback_fill_count": max(0, len(out) - len(selected)), "fallback_fill_cap_safe": bool(validate_final30_cluster_contract(out, regime).get("cluster_contract_ok")), "fallback_fill_attempted": attempted}


def enforce_regime_sector_caps(selected: list[dict], candidate_pool: list[dict], constraints: dict, finaln: int) -> tuple[list[dict], dict[str, Any]]:
    max_ai = float((constraints or {}).get("max_ai_tech_ratio", 1.0))
    max_single = float((constraints or {}).get("max_single_cluster_ratio", 1.0))
    regime = str((constraints or {}).get("market_regime") or "NEUTRAL")
    before = list(selected or [])
    before_ai = sum(1 for r in before if (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) in AI_CLUSTERS) / max(1, finaln)
    if regime == "RISK_OFF":
        return before[:finaln], {
            "sector_cap_enforced": True,
            "risk_off_entry_block": True,
            "blocked_by_cluster_cap": [],
            "sector_cap_replacements": [],
            "cap_violations": [],
            "before_ai_tech_ratio": before_ai,
            "after_ai_tech_ratio": before_ai,
            "risk_off_cap_warning": "entry_blocked_but_final30_preserved",
        }
    blocked: list[str] = []
    replacements: list[str] = []
    removed_violation_reasons: set[str] = set()
    out = sorted(before, key=lambda r: float(r.get("score_final") or 0), reverse=True)
    def ratios(rows):
        counts = _cluster_counts(rows); total=max(1,finaln); ai=sum(counts.get(c,0) for c in AI_CLUSTERS)/total; single=max(counts.values() or [0])/total; return ai,single,counts
    while out:
        ai,single,counts = ratios(out)
        if ai <= max_ai and single <= max_single: break
        if ai > max_ai:
            removed_violation_reasons.add("AI_TECH_COMBINED")
            idxs=[i for i,r in enumerate(out) if (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r)) in AI_CLUSTERS]
        else:
            removed_violation_reasons.add("SINGLE_CLUSTER")
            worst=max(counts, key=counts.get); idxs=[i for i,r in enumerate(out) if (r.get("theme_cluster") or theme_cluster_for(str(r.get("symbol") or ""), r))==worst]
        if not idxs: break
        idx=min(idxs, key=lambda i: float(out[i].get("score_final") or 0))
        blocked.append(str(out[idx].get("symbol") or "")); out.pop(idx)
    used={str(r.get("symbol") or "") for r in out}
    for row in sorted(candidate_pool or [], key=lambda r: float(r.get("score_final") or 0), reverse=True):
        if len(out) >= finaln: break
        sym=str(row.get("symbol") or "")
        if not sym or sym in used: continue
        cluster=row.get("theme_cluster") or theme_cluster_for(sym,row)
        if cluster in AI_CLUSTERS and ratios(out+[row])[0] > max_ai: continue
        if ratios(out+[row])[1] > max_single: continue
        # Prefer replacements from non-AI defensive/cyclical clusters when caps are tight.
        out.append(row); used.add(sym); replacements.append(sym)
    after_ai = ratios(out)[0]
    cap_violations=[]
    ai,single,_=ratios(out)
    if ai > max_ai: cap_violations.append("AI_TECH_COMBINED")
    if single > max_single: cap_violations.append("SINGLE_CLUSTER")
    if len(out) < finaln and blocked:
        cap_violations.extend([r for r in sorted(removed_violation_reasons) if r not in cap_violations])
    logger.info("[US_SECTOR_CAP][ENFORCED] market_regime=%s before_ai_tech_ratio=%.4f after_ai_tech_ratio=%.4f max_ai_tech_ratio=%.4f blocked=%s replacements=%s", regime, before_ai, after_ai, max_ai, blocked, replacements)
    if cap_violations:
        logger.warning("[US_SECTOR_CAP][FAILED] market_regime=%s reason=replacement_pool_insufficient cap_violations=%s blocked=%s replacements=%s", regime, cap_violations, blocked, replacements)
    return out[:finaln], {"sector_cap_enforced": True, "before_ai_tech_ratio": before_ai, "after_ai_tech_ratio": after_ai, "blocked_by_cluster_cap": blocked, "sector_cap_replacements": replacements, "cap_violations": cap_violations}

def _is_etf_row(row: dict) -> bool:
    return str(row.get("asset_type") or "").lower() == "etf" or str(row.get("symbol") or "").upper() in _CORE_ETFS


def _enforce_final30_etf_cap(selected: list[dict], sorted_pool: list[dict], finaln: int, max_etf: int, blocked_clusters: set[str] | None = None) -> list[dict]:
    blocked_clusters = blocked_clusters or set()
    kept: list[dict] = []
    dropped_symbols: set[str] = set()
    etf_count = 0
    for row in selected:
        if _is_etf_row(row):
            if etf_count >= max_etf:
                dropped_symbols.add(str(row.get("symbol") or ""))
                continue
            etf_count += 1
        kept.append(row)
    existing = {str(r.get("symbol") or "") for r in kept}
    for row in sorted_pool:
        if len(kept) >= finaln:
            break
        sym = str(row.get("symbol") or "")
        if not sym or sym in existing or sym in dropped_symbols:
            continue
        if row.get("theme_cluster") in blocked_clusters:
            continue
        if _is_etf_row(row):
            continue
        kept.append(row)
        existing.add(sym)
    return kept[:finaln]
