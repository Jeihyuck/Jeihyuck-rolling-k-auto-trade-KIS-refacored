from __future__ import annotations

import os
from typing import Any
import logging

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


def normalize_rs_percentile(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(numeric):
        return 0.0
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    return round(max(0.0, min(100.0, numeric)), 2)


def normalize_vcp_score(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(numeric):
        return 0.0
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    return round(max(0.0, min(100.0, numeric)), 2)


def normalize_trend_score(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(numeric):
        return 0.0
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    return round(max(0.0, min(100.0, numeric)), 2)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _fetch_daily_df(kis: Any, symbol: str, days: int) -> pd.DataFrame:
    if hasattr(kis, "_fetch_daily"):
        df, _meta = kis._fetch_daily(symbol, days=days)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if hasattr(kis, "get_ohlcv"):
        result = kis.get_ohlcv(symbol, days)
        if isinstance(result, pd.DataFrame):
            return result
        if isinstance(result, tuple) and len(result) >= 1 and isinstance(result[0], pd.DataFrame):
            return result[0]
        if hasattr(result, "df") and isinstance(result.df, pd.DataFrame):
            return result.df
    return pd.DataFrame()


def _atr_pct(df: pd.DataFrame, window: int = 14) -> float:
    if len(df) < window + 1:
        return float("nan")
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    tr = np.maximum(high, close.shift(1)) - np.minimum(low, close.shift(1))
    atr = tr.rolling(window).mean().iloc[-1]
    last_close = float(close.iloc[-1])
    if last_close <= 0 or not np.isfinite(atr):
        return float("nan")
    return float(atr / last_close)


def _trend_pass(df: pd.DataFrame) -> tuple[bool, dict[str, float], list[str]]:
    reasons: list[str] = []
    close = df["close"].astype(float)
    ma50 = close.rolling(50).mean().iloc[-1] if len(df) >= 50 else float("nan")
    ma150 = close.rolling(150).mean().iloc[-1] if len(df) >= 150 else float("nan")
    ma200 = close.rolling(200).mean().iloc[-1] if len(df) >= 200 else float("nan")
    ma200_prev = close.rolling(200).mean().shift(20).iloc[-1] if len(df) >= 220 else float("nan")
    last_close = float(close.iloc[-1])

    ok = True
    if not (np.isfinite(ma50) and np.isfinite(ma150) and np.isfinite(ma200)):
        ok = False
        reasons.append("missing_ma")
    else:
        if not (last_close > ma50 > ma150 > ma200):
            ok = False
            reasons.append("trend_template_fail")
        if not (np.isfinite(ma200_prev) and ma200 > ma200_prev):
            ok = False
            reasons.append("ma200_not_rising")

    return (
        ok,
        {
            "close": last_close,
            "ma50": float(ma50) if np.isfinite(ma50) else float("nan"),
            "ma150": float(ma150) if np.isfinite(ma150) else float("nan"),
            "ma200": float(ma200) if np.isfinite(ma200) else float("nan"),
        },
        reasons,
    )


def compute_minervini_signals(kis, as_of: str, symbols: list[str], benchmark: str = "229200") -> dict:
    precomputed_final30 = getattr(kis, "_precomputed_final30_map", {}) if kis is not None else {}
    precomputed_derived = getattr(kis, "_precomputed_derived_map", {}) if kis is not None else {}

    if symbols and (precomputed_final30 or precomputed_derived):
        items: list[dict] = []
        for symbol in [str(s or "").zfill(6) for s in symbols if s]:
            row = dict(precomputed_derived.get(symbol) or {})
            row.update(dict(precomputed_final30.get(symbol) or {}))
            if not row:
                items.append({"code": symbol, "data_ok": False, "reasons": ["precomputed_missing"]})
                continue
            rs_percentile = _float_env("MINERVINI_RS_MIN_PCTILE", 80.0)
            rs_raw = float(row.get("rs_percentile") or row.get("rs_pctile") or 0.0)
            vcp_raw = float(row.get("vcp_score") or 0.0)
            trend_raw = float(row.get("trend_score") or 0.0)
            rs_pctile = normalize_rs_percentile(rs_raw)
            vcp_score = normalize_vcp_score(vcp_raw)
            trend_score = normalize_trend_score(trend_raw)
            breakout_score = float(row.get("breakout_score") or 0.0)
            pullback_score = float(row.get("pullback_score") or 0.0)
            momentum_score = float(row.get("momentum_score") or 0.0)
            atr_pct = row.get("atr_pct")
            atr_val = float(atr_pct) if atr_pct is not None else 0.0
            trend_ok = trend_score > 0.0
            atr_ok = atr_pct is None or atr_val <= _float_env("ATR_MAX_PCT", 0.10)
            logger.info(
                "[MINERVINI][PRECOMPUTED] code=%s rs=%s vcp=%s trend=%s",
                symbol,
                rs_pctile,
                vcp_score,
                trend_score,
            )
            items.append(
                {
                    "code": symbol,
                    "data_ok": True,
                    "trend_pass": bool(trend_ok),
                    "atr_pass": bool(atr_ok),
                    "rs_raw": float(row.get("rs_score") or rs_raw),
                    "rs_pctile": rs_pctile,
                    "rs_pctile_normalized": rs_pctile,
                    "vcp_score_raw": vcp_raw,
                    "vcp_score": vcp_score,
                    "trend_score_raw": trend_raw,
                    "trend_score": trend_score,
                    "pivot": float(row.get("pivot") or 0.0),
                    "atr_pct": atr_val,
                    "breakout_score": breakout_score,
                    "pullback_score": pullback_score,
                    "momentum_score": momentum_score,
                    "reasons": [] if trend_ok else ["trend_template_fail"],
                }
            )
        return {
            "as_of": as_of,
            "benchmark": benchmark,
            "regime_mode": (os.getenv("MINERVINI_REGIME_MODE") or "STRICT").upper(),
            "regime_pass": True,
            "items": items,
        }

    rs_lookbacks_raw = (os.getenv("MINERVINI_RS_LOOKBACKS") or "63,126").split(",")
    rs_lookbacks = [max(1, int(x.strip())) for x in rs_lookbacks_raw if x.strip()]
    vcp_lb = _int_env("MINERVINI_VCP_LOOKBACK", 120)
    pivot_days = _int_env("MINERVINI_PIVOT_DAYS", 55)
    atr_max = _float_env("ATR_MAX_PCT", 0.10)

    max_lb = max(max(rs_lookbacks, default=126), vcp_lb, 200 + 20, pivot_days) + 5
    bench_df = _fetch_daily_df(kis, benchmark, max_lb)
    regime_pass = False
    if len(bench_df) >= 200:
        close = bench_df["close"].astype(float)
        bench_ma50 = close.rolling(50).mean().iloc[-1]
        bench_ma200 = close.rolling(200).mean().iloc[-1]
        regime_pass = bool(np.isfinite(bench_ma50) and np.isfinite(bench_ma200) and bench_ma50 > bench_ma200)

    items: list[dict] = []
    rs_scores: list[tuple[str, float]] = []
    by_symbol_df: dict[str, pd.DataFrame] = {}
    bench_close = bench_df["close"].astype(float) if not bench_df.empty else pd.Series(dtype=float)

    for symbol in [str(s or "").zfill(6) for s in symbols if s]:
        df = _fetch_daily_df(kis, symbol, max_lb)
        if df is None or df.empty:
            items.append({"code": symbol, "data_ok": False, "reasons": ["ohlcv_missing"]})
            continue
        by_symbol_df[symbol] = df
        close = df["close"].astype(float)

        rs_parts: list[float] = []
        for lb in rs_lookbacks:
            if len(close) <= lb or len(bench_close) <= lb:
                continue
            stock_ret = float(close.iloc[-1] / close.iloc[-1 - lb] - 1.0)
            bench_ret = float(bench_close.iloc[-1] / bench_close.iloc[-1 - lb] - 1.0)
            rs_parts.append(stock_ret - bench_ret)
        rs_raw = float(np.mean(rs_parts)) if rs_parts else float("nan")
        rs_scores.append((symbol, rs_raw))

    finite_rs = [v for _k, v in rs_scores if np.isfinite(v)]
    sorted_rs = sorted(finite_rs)

    def _pctile(v: float) -> float:
        if not sorted_rs or not np.isfinite(v):
            return 0.0
        pos = np.searchsorted(sorted_rs, v, side="right")
        return float((pos / len(sorted_rs)) * 100.0)

    rs_map = {code: raw for code, raw in rs_scores}
    rs_pct_map = {code: _pctile(raw) for code, raw in rs_scores}

    for symbol in [str(s or "").zfill(6) for s in symbols if s]:
        df = by_symbol_df.get(symbol)
        if df is None or df.empty:
            continue

        trend_ok, trend_vals, reasons = _trend_pass(df)
        atr_pct = _atr_pct(df, window=14)
        atr_pass = bool(np.isfinite(atr_pct) and atr_pct <= atr_max)
        if not atr_pass:
            reasons.append("atr_too_high")

        close = df["close"].astype(float)
        high = df["high"].astype(float)
        vol = df["volume"].astype(float)
        std20 = float(close.pct_change().tail(20).std() or 0.0)
        std60 = float(close.pct_change().tail(60).std() or 0.0)
        vol20 = float(vol.tail(20).mean() or 0.0)
        vol60 = float(vol.tail(60).mean() or 0.0)
        high20 = float(high.tail(20).max() or 0.0)
        ma200 = trend_vals.get("ma200")
        ma200_prev = close.rolling(200).mean().shift(20).iloc[-1] if len(df) >= 220 else float("nan")

        vcp_score = 0.0
        if trend_ok:
            vcp_score += 50.0
        if np.isfinite(ma200) and np.isfinite(ma200_prev) and ma200 > ma200_prev:
            vcp_score += 10.0
        if std20 < std60:
            vcp_score += 20.0
        if vol20 < vol60:
            vcp_score += 10.0
        if high20 > 0 and float(close.iloc[-1]) >= 0.92 * high20:
            vcp_score += 10.0

        pivot = float(high.tail(max(1, pivot_days)).max() or 0.0)
        rs_pctile = float(rs_pct_map.get(symbol, 0.0))

        items.append(
            {
                "code": symbol,
                "data_ok": True,
                "trend_pass": trend_ok,
                "atr_pass": atr_pass,
                "rs_raw": float(rs_map.get(symbol) or 0.0),
                "rs_pctile": rs_pctile,
                "vcp_score": float(vcp_score),
                "pivot": pivot,
                "atr_pct": float(atr_pct) if np.isfinite(atr_pct) else None,
                "reasons": reasons,
            }
        )

    return {
        "as_of": as_of,
        "benchmark": benchmark,
        "regime_mode": (os.getenv("MINERVINI_REGIME_MODE") or "STRICT").upper(),
        "regime_pass": regime_pass,
        "items": items,
    }


def select_buyable_with_relax(
    signals: dict,
    min_buyable: int,
    relax_passes: int,
    rs_step: int,
    vcp_step: int,
    keep_trend: bool,
) -> tuple[list[str], dict]:
    items = list(signals.get("items") or [])
    regime_pass = bool(signals.get("regime_pass"))
    bootstrap_enabled = os.getenv("PB1_BOOTSTRAP_ENABLE", "0") == "1"
    base_rs = _int_env("MINERVINI_RS_MIN_PCTILE", 80)
    base_vcp = _int_env("MINERVINI_VCP_MIN_SCORE", 70)
    if bootstrap_enabled:
        base_rs = _int_env("BOOTSTRAP_MINERVINI_RS_MIN_PCTILE", 60)
        base_vcp = _int_env("BOOTSTRAP_MINERVINI_VCP_MIN_SCORE", 45)
    allow_rs_only = os.getenv("ALLOW_RS_ONLY_WHEN_STRONG_TREND", "1") == "1"

    if not regime_pass:
        return [], {
            "relax_level_used": None,
            "rs_cut_used": base_rs,
            "vcp_cut_used": base_vcp,
            "final_buyable_count": 0,
            "pass_counts": {},
            "pass_codes": {},
            "final_buyable_codes": [],
            "base_rs": base_rs,
            "base_vcp": base_vcp,
            "regime_pass": False,
        }

    pass_counts: dict[str, int] = {}
    pass_codes: dict[str, list[str]] = {}
    chosen_codes: list[str] = []
    used_level = 0
    used_rs = base_rs
    used_vcp = base_vcp
    final_gating_mode = "rank_only_fallback"
    selected_states: dict[str, str] = {}

    for p in range(max(0, int(relax_passes)) + 1):
        rs_cut = max(0, base_rs - (rs_step * p))
        vcp_cut = max(0, base_vcp - (vcp_step * p))
        passed: list[str] = []
        for item in items:
            if not item.get("data_ok"):
                continue
            if keep_trend and not bool(item.get("trend_pass")):
                continue
            if not bool(item.get("atr_pass")):
                continue
            rs_pctile = normalize_rs_percentile(item.get("rs_pctile") or item.get("rs_percentile") or 0.0)
            vcp_score = normalize_vcp_score(item.get("vcp_score") or 0.0)
            if rs_pctile < float(rs_cut):
                continue
            if vcp_score < float(vcp_cut):
                continue
            code = str(item.get("code") or "").zfill(6)
            passed.append(code)
            selected_states[code] = "hard_pass" if p == 0 else "soft_pass_relaxed"

        pass_counts[f"pass{p}"] = len(passed)
        pass_codes[f"pass{p}"] = passed[:]
        logger.info(
            "[MINERVINI][RELAX][PASS_CODES] pass=%s count=%s codes=%s rs_cut=%s vcp_cut=%s",
            p,
            len(passed),
            passed,
            rs_cut,
            vcp_cut,
        )
        chosen_codes = passed
        used_level = p
        used_rs = rs_cut
        used_vcp = vcp_cut
        final_gating_mode = "hard_pass" if p == 0 else "soft_pass_relaxed"
        if len(passed) >= int(min_buyable):
            break

    if not chosen_codes and items:
        ranked = sorted(
            [item for item in items if item.get("data_ok") and (not keep_trend or bool(item.get("trend_pass"))) and bool(item.get("atr_pass"))],
            key=lambda item: (
                normalize_rs_percentile(item.get("rs_pctile") or item.get("rs_percentile") or 0.0),
                normalize_vcp_score(item.get("vcp_score") or 0.0),
                normalize_trend_score(item.get("trend_score") or 0.0),
            ),
            reverse=True,
        )
        chosen_codes = [str(item.get("code") or "").zfill(6) for item in ranked[: int(min_buyable)]]
        for code in chosen_codes:
            selected_states[code] = "rank_only_fallback"
        final_gating_mode = "rank_only_fallback"
        logger.warning("[MINERVINI][DEGRADED][RANK_ONLY] count=%s", len(chosen_codes))

    logger.info(
        "[MINERVINI][RELAX][FINAL_CODES] used=%s count=%s codes=%s",
        used_level,
        len(chosen_codes),
        chosen_codes,
    )

    report = {
        "relax_level_used": used_level,
        "rs_cut_used": used_rs,
        "vcp_cut_used": used_vcp,
        "final_buyable_count": len(chosen_codes),
        "pass_counts": pass_counts,
        "pass_codes": pass_codes,
        "final_buyable_codes": chosen_codes[:],
        "base_rs": base_rs,
        "base_vcp": base_vcp,
        "regime_pass": True,
        "final_gating_mode": final_gating_mode,
        "selected_states": selected_states,
        "normalized_items": [
            {
                "code": str(item.get("code") or "").zfill(6),
                "raw_rs": item.get("rs_raw", item.get("rs_pctile")),
                "normalized_rs": normalize_rs_percentile(item.get("rs_pctile") or item.get("rs_percentile") or 0.0),
                "raw_vcp": item.get("vcp_score_raw", item.get("vcp_score")),
                "normalized_vcp": normalize_vcp_score(item.get("vcp_score") or 0.0),
                "raw_trend": item.get("trend_score_raw", item.get("trend_score")),
                "normalized_trend": normalize_trend_score(item.get("trend_score") or 0.0),
                "applied_rs_cut": used_rs,
                "applied_vcp_cut": used_vcp,
            }
            for item in items
        ],
    }
    return chosen_codes, report


def minervini_filter(df: pd.DataFrame) -> bool:
    """
    Minervini Trend Template 필터
    - 가격 > MA50 > MA150 > MA200
    - 가격 > 52주 저점 * 1.3
    """
    if len(df) < 200:
        return False
    
    ma50 = df['close'].rolling(50).mean().iloc[-1]
    ma150 = df['close'].rolling(150).mean().iloc[-1]
    ma200 = df['close'].rolling(200).mean().iloc[-1]
    price = df['close'].iloc[-1]
    
    # 조건 1: price > ma50 > ma150 > ma200
    cond1 = price > ma50
    cond2 = ma50 > ma150
    cond3 = ma150 > ma200
    
    # 조건 2: price > 52주 저점 * 1.3
    week52_low = df['low'].rolling(52 * 5).min().iloc[-1]  # 52주 = 약 260일
    cond4 = price > week52_low * 1.3
    
    return cond1 and cond2 and cond3 and cond4
