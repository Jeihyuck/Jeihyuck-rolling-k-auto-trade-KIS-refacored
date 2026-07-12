# -*- coding: utf-8 -*-
"""US Candidate Pool Builder.

dynamic_universe → 1차 technical/liquidity/RS scoring → candidate_pool 50~200개

한국장 파일 import 금지.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


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


def _compute_rs(daily_rows: list[dict], days: int) -> float:
    """N일 RS (현재가 / N일 전 종가 - 1)."""
    if len(daily_rows) < days + 1:
        return 0.0
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        current_close = _safe_float(rows[-1].get("clos"))
        past_close = _safe_float(rows[-(days + 1)].get("clos"))
        if past_close <= 0:
            return 0.0
        return round((current_close / past_close) - 1.0, 4)
    except Exception:
        return 0.0


def _compute_ma(daily_rows: list[dict], period: int) -> float | None:
    """N일 이동평균."""
    if len(daily_rows) < period:
        return None
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        closes = [_safe_float(r.get("clos")) for r in rows[-period:]]
        closes = [c for c in closes if c > 0]
        if not closes:
            return None
        return round(sum(closes) / len(closes), 4)
    except Exception:
        return None


def _compute_volume_accel(daily_rows: list[dict]) -> float:
    """최근 5일 거래량 / 20일 평균 거래량 비율."""
    if len(daily_rows) < 20:
        return 1.0
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        recent5 = [_safe_float(r.get("tvol")) for r in rows[-5:]]
        all20 = [_safe_float(r.get("tvol")) for r in rows[-20:]]
        avg5 = sum(v for v in recent5 if v > 0) / max(1, sum(1 for v in recent5 if v > 0))
        avg20 = sum(v for v in all20 if v > 0) / max(1, sum(1 for v in all20 if v > 0))
        if avg20 <= 0:
            return 1.0
        return round(avg5 / avg20, 4)
    except Exception:
        return 1.0


def _compute_near_high(daily_rows: list[dict], price: float, days: int = 52) -> float:
    """현재가가 N일 고점 대비 얼마나 가까운지 (1.0 = 고점, 0.0 = 바닥)."""
    if len(daily_rows) < 5:
        return 0.5
    try:
        rows = sorted(daily_rows, key=lambda r: str(r.get("xymd", "")))
        period = rows[-days:] if len(rows) >= days else rows
        highs = [_safe_float(r.get("high", r.get("clos"))) for r in period]
        highs = [h for h in highs if h > 0]
        if not highs or price <= 0:
            return 0.5
        period_high = max(highs)
        period_low = min(h for h in [_safe_float(r.get("low", r.get("clos"))) for r in period] if h > 0) or price
        spread = period_high - period_low
        if spread <= 0:
            return 1.0
        return round(min(1.0, max(0.0, (price - period_low) / spread)), 4)
    except Exception:
        return 0.5


def _score_to_01(value: float, low: float = -0.5, high: float = 0.5) -> float:
    """임의 range 값을 0~1로 정규화."""
    if high <= low:
        return 0.5
    return round(min(1.0, max(0.0, (value - low) / (high - low))), 4)


def _score_symbol_candidate(
    sym_data: dict,
    daily_rows: list[dict],
    all_rs20: list[float],
    all_rs60: list[float],
    all_rs120: list[float],
) -> dict:
    """단일 종목 candidate 점수 계산."""
    symbol = sym_data.get("symbol", "")
    price = _safe_float(sym_data.get("price"))
    atr_pct = _safe_float(sym_data.get("atr_pct"), 0.0)
    avg_vol = _safe_float(sym_data.get("avg_volume_20d"), 0.0)
    avg_dv = _safe_float(sym_data.get("avg_dollar_volume_20d"), 0.0)

    rs_20d = _compute_rs(daily_rows, 20)
    rs_60d = _compute_rs(daily_rows, 60)
    rs_120d = _compute_rs(daily_rows, 120)

    ma20 = _compute_ma(daily_rows, 20)
    ma50 = _compute_ma(daily_rows, 50)
    ma150 = _compute_ma(daily_rows, 150)
    ma200 = _compute_ma(daily_rows, 200)
    ma200_prev = _compute_ma(daily_rows[:-20], 200) if len(daily_rows) >= 220 else None
    ma200_slope = round((ma200 - ma200_prev) / ma200_prev, 6) if ma200 and ma200_prev and ma200_prev > 0 else None
    daily_bar_count = len(daily_rows or [])
    if daily_bar_count >= 200:
        daily_history_quality = "OK"
    elif daily_bar_count >= 150:
        daily_history_quality = "DEGRADED_NO_MA200"
    elif daily_bar_count >= 60:
        daily_history_quality = "DEGRADED_SHORT_HISTORY"
    else:
        daily_history_quality = "ERROR_INSUFFICIENT_HISTORY"
    daily_metrics_as_of = None
    if daily_rows:
        last_row = sorted(daily_rows, key=lambda r: str(r.get("xymd", r.get("date", ""))))[-1]
        daily_metrics_as_of = str(last_row.get("date") or last_row.get("xymd") or "")

    vol_accel = _compute_volume_accel(daily_rows)
    near_high = _compute_near_high(daily_rows, price, 52)

    # Percentile (전체 universe 대비) — 나중에 정규화
    # RS score (0~1)
    rs_20d_score = _score_to_01(rs_20d, -0.3, 0.5)
    rs_60d_score = _score_to_01(rs_60d, -0.3, 0.8)
    rs_120d_score = _score_to_01(rs_120d, -0.3, 1.0)

    # Trend score: MA 관계
    trend_score = 0.0
    if price > 0 and ma20 and ma50 and ma150:
        above_ma20 = 1.0 if price > ma20 else 0.0
        above_ma50 = 1.0 if price > ma50 else 0.0
        above_ma150 = 1.0 if price > ma150 else 0.0
        ma_order = 1.0 if (ma20 and ma50 and ma150 and ma20 > ma50 > ma150) else 0.0
        trend_score = round((above_ma20 * 0.3 + above_ma50 * 0.3 + above_ma150 * 0.2 + ma_order * 0.2), 4)
    elif price > 0 and ma20 and ma50:
        above_ma20 = 1.0 if price > ma20 else 0.0
        above_ma50 = 1.0 if price > ma50 else 0.0
        trend_score = round((above_ma20 * 0.5 + above_ma50 * 0.5), 4)

    # Volume acceleration score (0~1)
    volume_accel_score = round(min(1.0, max(0.0, (vol_accel - 0.5) / 2.0)), 4)

    # Liquidity score (0~1)
    liquidity_score = _score_to_01(avg_dv, 0, 5_000_000_000.0)

    # Pullback quality: price vs MA20 (5~15% 아래 = 좋은 pullback)
    pullback_pct = 0.0
    pullback_quality = 0.5
    if ma20 and ma20 > 0 and price > 0:
        pullback_pct = round((price - ma20) / ma20, 4)
        # 0~5% 위가 이상적, -15% 이내도 허용
        if 0.0 <= pullback_pct <= 0.05:
            pullback_quality = 0.9
        elif -0.05 <= pullback_pct < 0.0:
            pullback_quality = 0.85
        elif -0.15 <= pullback_pct < -0.05:
            pullback_quality = 0.7
        elif 0.05 < pullback_pct <= 0.15:
            pullback_quality = 0.6
        else:
            pullback_quality = 0.3

    # Near high score
    near_high_score = near_high

    # Volatility penalty (ATR > 12% → penalty)
    volatility_penalty = round(max(0.0, (atr_pct - 0.12) / 0.06), 4) if atr_pct > 0.12 else 0.0

    # Theme bonus (etf = 0.1)
    asset_type = sym_data.get("asset_type", "stock")
    theme_bonus = 0.1 if asset_type == "etf" else 0.0

    # candidate_score 공식
    candidate_score = round(
        0.25 * rs_60d_score
        + 0.20 * rs_20d_score
        + 0.15 * trend_score
        + 0.15 * volume_accel_score
        + 0.10 * pullback_quality
        + 0.10 * liquidity_score
        + 0.05 * theme_bonus
        - 0.10 * volatility_penalty,
        4,
    )
    candidate_score = max(0.0, min(1.0, candidate_score))

    return {
        "symbol": symbol,
        "exchange": sym_data.get("exchange", "NASDAQ"),
        "asset_type": asset_type,
        "source_tags": sym_data.get("source_tags", []),
        "price": price,
        "avg_volume_20d": avg_vol,
        "avg_dollar_volume_20d": avg_dv,
        "history_days": sym_data.get("history_days", 0),
        "atr_pct": atr_pct,
        # RS
        "rs_20d": rs_20d,
        "rs_60d": rs_60d,
        "rs_120d": rs_120d,
        "rs_percentile": 0.5,  # 전체 정규화 후 갱신
        # MA
        "ma20": ma20,
        "ma50": ma50,
        "ma150": ma150,
        "ma200": ma200,
        "ma200_slope": ma200_slope,
        "daily_bar_count": daily_bar_count,
        "daily_metrics_as_of": daily_metrics_as_of,
        "daily_metrics_source": "price_daily",
        "daily_history_quality": daily_history_quality,
        "pullback_pct": pullback_pct,
        # Scores
        "rs_20d_score": rs_20d_score,
        "rs_60d_score": rs_60d_score,
        "rs_120d_score": rs_120d_score,
        "trend_score": trend_score,
        "volume_accel_score": volume_accel_score,
        "liquidity_score": liquidity_score,
        "pullback_quality": pullback_quality,
        "near_high_score": near_high_score,
        "volatility_penalty": volatility_penalty,
        "theme_bonus": theme_bonus,
        "candidate_score": candidate_score,
    }


def _percentile_rank(value: float, all_values: list[float]) -> float:
    """value의 백분위 순위 (0~1)."""
    if not all_values:
        return 0.5
    below = sum(1 for v in all_values if v < value)
    return round(below / len(all_values), 4)


def build_us_candidate_pool(
    *,
    trade_date: str,
    as_of_date: str | None = None,
    env: str,
    dynamic_universe: list[dict],
    provider: Any,
    force_rebuild: bool = False,
) -> dict:
    """US Candidate Pool 빌드.

    Args:
        trade_date: YYYY-MM-DD
        as_of_date: KIS dailyprice BYMD 기준일. None이면 trade_date와 동일.
        env: practice / live
        dynamic_universe: build_us_dynamic_universe()["symbols"] 결과
        provider: USDataProvider 인스턴스
        force_rebuild: 캐시 무시 재빌드

    Returns:
        candidate pool result dict
    """
    as_of_date = as_of_date or trade_date
    pool_min = _env_int("US_CANDIDATE_POOL_MIN", 50)
    pool_target = _env_int("US_CANDIDATE_POOL_TARGET", 120)
    pool_max = _env_int("US_CANDIDATE_POOL_MAX", 200)

    logger.info("[US_CANDIDATE_POOL][START] universe=%d", len(dynamic_universe))
    logger.info(
        "[US_CANDIDATE_POOL][DATE_POLICY] trade_date=%s as_of_date=%s",
        trade_date,
        as_of_date,
    )

    scored_rows: list[dict] = []
    failed_count = 0

    # 전체 RS 배열 (percentile 계산용)
    all_rs20: list[float] = []
    all_rs60: list[float] = []
    all_rs120: list[float] = []

    # 1차 scoring
    for sym_data in dynamic_universe:
        symbol = sym_data.get("symbol", "")
        exchange = sym_data.get("exchange", "NASDAQ")
        try:
            if hasattr(provider, "get_completed_daily_prices"):
                daily = provider.get_completed_daily_prices(symbol, exchange, trade_date=trade_date, required_bars=int(os.getenv("US_DAILY_REQUIRED_BARS", "260")), allow_http_sync=True)
            else:
                daily = provider.get_daily_prices(symbol, exchange, count=int(os.getenv("US_DAILY_REQUIRED_BARS", "260")), as_of_date=as_of_date)
            row = _score_symbol_candidate(sym_data, daily, all_rs20, all_rs60, all_rs120)
            scored_rows.append(row)
            all_rs20.append(row["rs_20d"])
            all_rs60.append(row["rs_60d"])
            all_rs120.append(row["rs_120d"])
        except Exception as exc:
            logger.debug("[US_CANDIDATE_POOL][SKIP] symbol=%s error=%s", symbol, exc)
            failed_count += 1

    logger.info(
        "[US_CANDIDATE_POOL][SCORE] scored=%d failed=%d",
        len(scored_rows),
        failed_count,
    )

    # percentile 갱신
    for row in scored_rows:
        row["rs_percentile"] = _percentile_rank(row["rs_60d"], all_rs60)

    # strict filter: candidate_score >= 0.35 + trend_score >= 0.3
    strict_rows = [
        r for r in scored_rows
        if r["candidate_score"] >= 0.35 and r["trend_score"] >= 0.3
    ]
    logger.info("[US_CANDIDATE_POOL][STRICT] kept=%d", len(strict_rows))

    # relaxed filter: candidate_score >= 0.20
    relaxed_rows: list[dict] = []
    if len(strict_rows) < pool_min:
        strict_symbols = {r["symbol"] for r in strict_rows}
        relaxed_candidates = [
            r for r in scored_rows
            if r["symbol"] not in strict_symbols and r["candidate_score"] >= 0.20
        ]
        need = pool_min - len(strict_rows)
        relaxed_rows = sorted(relaxed_candidates, key=lambda r: -r["candidate_score"])[:need]
        logger.info("[US_CANDIDATE_POOL][RELAXED] kept=%d", len(relaxed_rows))

    combined = strict_rows + relaxed_rows
    combined_symbols = {r["symbol"] for r in combined}

    # fallback top liquidity
    fallback_rows: list[dict] = []
    if len(combined) < pool_min:
        fallback_candidates = [
            r for r in scored_rows
            if r["symbol"] not in combined_symbols
        ]
        fallback_sorted = sorted(
            fallback_candidates,
            key=lambda r: (-r["liquidity_score"], -r["rs_60d"]),
        )
        need = pool_min - len(combined)
        fallback_rows = fallback_sorted[:need]
        logger.info("[US_CANDIDATE_POOL][FALLBACK_TOPUP] added=%d", len(fallback_rows))

    all_selected = combined + fallback_rows

    # 점수 기준 정렬 후 pool_max 제한
    all_selected = sorted(all_selected, key=lambda r: -r["candidate_score"])[:pool_max]
    selected_count = len(all_selected)

    # 상태 판정
    if selected_count < pool_min:
        status = "ERROR"
        logger.error(
            "[US_CANDIDATE_POOL][ERROR] selected=%d < pool_min=%d → hard fail",
            selected_count,
            pool_min,
        )
    elif selected_count < pool_target:
        status = "OK_WITH_WARNINGS"
    else:
        status = "OK"

    logger.info(
        "[US_CANDIDATE_POOL][DONE] selected=%d status=%s",
        selected_count,
        status,
    )

    return {
        "trade_date": trade_date,
        "env": env,
        "status": status,
        "input_count": len(dynamic_universe),
        "selected_count": selected_count,
        "min_required": pool_min,
        "target": pool_target,
        "selection_mode_counts": {
            "strict": len(strict_rows),
            "relaxed": len(relaxed_rows),
            "fallback": len(fallback_rows),
        },
        "rows": all_selected,
    }
