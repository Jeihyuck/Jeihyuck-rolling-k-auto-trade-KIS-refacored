from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta
from typing import Iterable

import pandas as pd

from trader.config import RS_BENCHMARK, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, RS_COMPOSITE_W1, RS_COMPOSITE_W2, RS_MIN_PCTILE
from trader.db.repos import DerivedMinerviniRepo, load_price_daily
from trader.factors.rs_rank import rank_rs
from trader.strategies.pb1_minervini_v2 import MinerviniConfig, compute_features, compute_pivot, detect_vcp, evaluate_filters, score_setup
from trader.time_utils import now_kst
from trader.utils.json_sanitize import to_jsonable

logger = logging.getLogger(__name__)


def _score_or_zero(value: object) -> float:
    """Convert score-like input to numeric 0..100 with safe fallback to 0."""
    if value is None:
        return 0.0
    try:
        if isinstance(value, bool):
            return 100.0 if value else 0.0
        out = float(value)
        if pd.isna(out) or out == float("inf") or out == float("-inf"):
            return 0.0
        return max(0.0, min(100.0, out))
    except Exception:
        return 0.0


def _compute_trend_score(feats: dict) -> float:
    close = _score_or_zero(feats.get("close"))
    ma20 = _score_or_zero(feats.get("ma20"))
    ma50 = _score_or_zero(feats.get("ma50"))
    ma150 = _score_or_zero(feats.get("ma150"))
    ma200 = _score_or_zero(feats.get("ma200"))
    ma200_slope = _score_or_zero(feats.get("ma200_slope"))

    score = 0.0
    if close > 0 and ma20 > 0 and close >= ma20:
        score += 25.0
    if ma20 > 0 and ma50 > 0 and ma20 >= ma50:
        score += 25.0
    if ma50 > 0 and ma150 > 0 and ma50 >= ma150:
        score += 25.0
    if ma150 > 0 and ma200 > 0 and ma150 >= ma200:
        score += 15.0
    if ma200 > 0 and ma200_slope > 0:
        score += 10.0
    return max(0.0, min(100.0, score))


def _compute_entry_scores(feats: dict, latest: dict, rs_pct: float) -> tuple[float, float, float, dict]:
    close = _score_or_zero(feats.get("close") or latest.get("close"))
    high = _score_or_zero(latest.get("high"))
    low = _score_or_zero(latest.get("low"))
    volume = _score_or_zero(latest.get("volume") or feats.get("last_volume"))
    volume_avg20 = _score_or_zero(feats.get("vol20"))
    atr = _score_or_zero(feats.get("atr14"))
    ma20 = _score_or_zero(feats.get("ma20"))
    ma50 = _score_or_zero(feats.get("ma50"))
    hi_52w = _score_or_zero(feats.get("hi_52w"))
    pivot = _score_or_zero(feats.get("pivot"))
    ma50_slope = _score_or_zero(feats.get("ma50_slope"))
    ma20_slope = _score_or_zero(feats.get("ma20_slope"))
    ret_63 = _score_or_zero((feats.get("ret_63") or 0.0) * 100.0)
    ret_126 = _score_or_zero((feats.get("ret_126") or 0.0) * 100.0)
    flat_placeholder = bool(close > 0 and ((pivot > 0 and abs(close - pivot) < 1e-9) or (hi_52w > 0 and abs(close - hi_52w) < 1e-9)))
    if flat_placeholder:
        logger.warning(
            "[DERIVED][ENTRY][ANOMALY][FLAT_VALUES] close=%.4f pivot=%.4f hi_52w=%.4f volume=%.4f",
            close,
            pivot,
            hi_52w,
            volume,
        )

    # Breakout score (0~100)
    breakout = 0.0
    breakout_distance = 0.0
    if close > 0 and pivot > 0:
        breakout_distance = ((close - pivot) / pivot) * 100.0
        if breakout_distance >= 0.0:
            breakout += 45.0
        elif breakout_distance >= -2.0:
            breakout += 25.0
    elif close > 0 and hi_52w > 0:
        breakout_distance = ((close - hi_52w) / hi_52w) * 100.0
        if breakout_distance >= -1.0:
            breakout += 30.0
        elif breakout_distance >= -4.0:
            breakout += 15.0

    if volume > 0 and volume_avg20 > 0:
        vol_ratio = volume / volume_avg20
        if vol_ratio >= 1.50:
            breakout += 30.0
        elif vol_ratio >= 1.20:
            breakout += 18.0

    if breakout_distance > 7.0:
        breakout -= min(20.0, (breakout_distance - 7.0) * 2.0)

    if atr > 0 and close > 0:
        atr_pct = (atr / close) * 100.0
        if 1.0 <= atr_pct <= 5.0:
            breakout += 10.0

    # Pullback score (0~100)
    pullback = 0.0
    pullback_depth = 0.0
    if hi_52w > 0 and close > 0:
        pullback_depth = max(0.0, ((hi_52w - close) / hi_52w) * 100.0)

    if close > 0 and ma20 > 0 and close >= ma20:
        pullback += 22.0
    if close > 0 and ma50 > 0 and close >= ma50:
        pullback += 20.0

    if 3.0 <= pullback_depth <= 18.0:
        pullback += 40.0
    elif 1.0 <= pullback_depth <= 25.0:
        pullback += 25.0

    if volume > 0 and volume_avg20 > 0:
        vol_ratio = volume / volume_avg20
        if vol_ratio < 0.80:
            pullback += 28.0
        elif vol_ratio < 1.00:
            pullback += 18.0

    if pullback_depth > 0 and pullback_depth <= 8.0:
        pullback += 12.0

    # Momentum score (0~100)
    momentum = 0.0
    if ret_63 > 0:
        momentum += 28.0
    if ret_126 > 0:
        momentum += 32.0
    if rs_pct >= 80.0:
        momentum += 20.0
    elif rs_pct >= 65.0:
        momentum += 10.0
    if ma20_slope > 0:
        momentum += 8.0
    if ma50_slope > 0:
        momentum += 12.0

    breakout_score = _score_or_zero(breakout)
    pullback_score = _score_or_zero(pullback)
    momentum_score = _score_or_zero(momentum)

    context = {
        "close": close,
        "high": high,
        "low": low,
        "volume": volume,
        "atr": atr,
        "pivot": pivot,
        "breakout_distance": breakout_distance,
        "pullback_depth": pullback_depth,
        "rs_percentile": rs_pct,
    }
    return breakout_score, pullback_score, momentum_score, context


def _as_of_date(value: date | str | None) -> date:
    if value is None:
        return now_kst().date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _load_df_from_db(*, engine, symbol: str, as_of: date, days: int) -> pd.DataFrame:
    start_date = as_of - timedelta(days=days * 2)
    candles = load_price_daily(engine, symbol, start_date, as_of)
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def compute_minervini_features_for_asof(
    *,
    engine,
    symbols: Iterable[str],
    env: str | None = None,
    as_of: date | str | None = None,
    lookback_days: int | None = None,
) -> list[dict]:
    """Compute Minervini features from DB OHLCV and return rows for upsert."""
    as_of_date = _as_of_date(as_of)
    symbols_list = [str(s).zfill(6) for s in symbols if s]
    if not symbols_list:
        return []
    env_n = (env or os.getenv("STRATEGY_ENV", "practice")).strip().lower()

    need_days = int(lookback_days or 520)

    bench_df = _load_df_from_db(engine=engine, symbol=RS_BENCHMARK, as_of=as_of_date, days=need_days)
    bench_close = bench_df["close"] if not bench_df.empty and "close" in bench_df.columns else pd.Series(dtype=float)

    price_series: dict[str, pd.Series] = {}
    features_map: dict[str, dict] = {}
    rows: list[dict] = []

    cfg = MinerviniConfig(rs_min_percentile=RS_MIN_PCTILE / 100.0)

    entry_diag_samples: list[dict] = []

    for symbol in symbols_list:
        df = _load_df_from_db(engine=engine, symbol=symbol, as_of=as_of_date, days=need_days)
        # 데이터 부족 시 스킵 (NaN 생성 방지)
        if df.empty or "close" not in df.columns or len(df) < 127:
            logger.debug("[DERIVED][MINERVINI][SKIP] symbol=%s len=%d (required>=127)", symbol, len(df))
            continue
        price_series[symbol] = df["close"]
        try:
            feats = compute_features(df)
            vcp_info = detect_vcp(df, cfg)
            pivot_val, _ = compute_pivot(df, cfg)
            feats["vcp_ok"] = vcp_info.get("vcp_ok")
            raw_vcp_score = vcp_info.get("score")
            feats["vcp_score"] = float(raw_vcp_score) if raw_vcp_score is not None else None
            feats["vcp_reason"] = str(vcp_info.get("reason") or "unknown")
            feats["vcp_contractions"] = list(vcp_info.get("contractions") or [])
            feats["vcp_contraction_ok"] = bool(vcp_info.get("contraction_ok"))
            feats["vcp_vol_dryup"] = bool(vcp_info.get("vol_dryup"))
            feats["vcp_tight_close"] = bool(vcp_info.get("tight_close"))
            feats["pivot"] = float(pivot_val) if pd.notna(pivot_val) else None
            if not df.empty:
                latest = df.iloc[-1]
                feats["latest_high"] = float(latest.get("high", 0.0) or 0.0)
                feats["latest_low"] = float(latest.get("low", 0.0) or 0.0)
                feats["latest_volume"] = float(latest.get("volume", 0.0) or 0.0)
            features_map[symbol] = feats
        except Exception as exc:
            logger.debug("[DERIVED][MINERVINI][FEATURE_FAIL] symbol=%s err=%s", symbol, exc)
            continue

    rs_df = rank_rs(
        price_series,
        bench_close,
        lookback_days=RS_LOOKBACK_DAYS,
        lookback2_days=RS_LOOKBACK2_DAYS,
        w1=RS_COMPOSITE_W1,
        w2=RS_COMPOSITE_W2,
    )
    rs_map = {row["ticker"]: float(row.get("pctile") or 0.0) for _, row in rs_df.iterrows()}

    for symbol, feats in features_map.items():
        rs_pct = float(rs_map.get(symbol) or 0.0)
        feats["rs_percentile"] = rs_pct
        ok, reasons = evaluate_filters(feats, cfg)
        vcp_info = {"score": feats.get("vcp_score"), "vcp_ok": feats.get("vcp_ok")}
        score = float(score_setup(feats, rs_percentile=rs_pct, vcp_info=vcp_info, cfg=cfg))
        trend_score = _compute_trend_score(feats)
        breakout_score, pullback_score, momentum_score, entry_ctx = _compute_entry_scores(
            feats,
            {
                "high": feats.get("latest_high"),
                "low": feats.get("latest_low"),
                "volume": feats.get("latest_volume"),
                "close": feats.get("close"),
            },
            rs_pct,
        )
        rows.append(
            {
                "env": env_n,
                "symbol": symbol,
                "as_of": as_of_date,
                "close": feats.get("close"),
                "ma50": feats.get("ma50"),
                "ma150": feats.get("ma150"),
                "ma200": feats.get("ma200"),
                "ma200_slope": feats.get("ma200_slope"),
                "dollar_vol_50": feats.get("dollar_vol_50"),
                "atr": feats.get("atr14"),
                "atr_pct": feats.get("atr_pct"),
                "rs_percentile": rs_pct,
                "rs_score": rs_pct,
                "vcp_score": feats.get("vcp_score"),
                "trend_score": trend_score,
                "breakout_score": breakout_score,
                "pullback_score": pullback_score,
                "momentum_score": momentum_score,
                "vcp_ok": feats.get("vcp_ok"),
                "pivot": feats.get("pivot"),
                "minervini_score": score,
                "minervini_pass": bool(ok),
                "features_json": {
                    **{k: v for k, v in feats.items() if k not in {"close", "ma50", "ma150", "ma200", "ma200_slope", "dollar_vol_50", "atr14", "atr_pct", "rs_percentile", "pivot"}},
                    "vcp": {
                        "score": feats.get("vcp_score"),
                        "ok": feats.get("vcp_ok"),
                        "reason": feats.get("vcp_reason"),
                        "contractions": feats.get("vcp_contractions", []),
                        "contraction_ok": feats.get("vcp_contraction_ok"),
                        "vol_dryup": feats.get("vcp_vol_dryup"),
                        "tight_close": feats.get("vcp_tight_close"),
                        "pivot": float(feats.get("pivot")) if feats.get("pivot") is not None else None,
                    },
                    "entry_scores": {
                        "breakout_score": breakout_score,
                        "pullback_score": pullback_score,
                        "momentum_score": momentum_score,
                        "trend_score": trend_score,
                    },
                    "reasons": reasons,
                },
            }
        )
        if len(entry_diag_samples) < 5:
            entry_diag_samples.append(
                {
                    "symbol": symbol,
                    "close": entry_ctx["close"],
                    "pivot_price": entry_ctx["pivot"],
                    "pullback_depth": entry_ctx["pullback_depth"],
                    "rs_percentile": entry_ctx["rs_percentile"],
                    "breakout_score": breakout_score,
                    "pullback_score": pullback_score,
                    "momentum_score": momentum_score,
                }
            )

    has_close = sum(1 for r in rows if _score_or_zero(r.get("close")) > 0)
    has_high = sum(1 for r in rows if _score_or_zero(((r.get("features_json") or {}).get("latest_high"))) > 0)
    has_low = sum(1 for r in rows if _score_or_zero(((r.get("features_json") or {}).get("latest_low"))) > 0)
    has_volume = sum(1 for r in rows if _score_or_zero(((r.get("features_json") or {}).get("last_volume"))) > 0)
    has_atr = sum(1 for r in rows if _score_or_zero(r.get("atr")) > 0)
    has_rs = sum(1 for r in rows if _score_or_zero(r.get("rs_percentile")) > 0)
    breakout_nonzero = sum(1 for r in rows if _score_or_zero(r.get("breakout_score")) > 0)
    pullback_nonzero = sum(1 for r in rows if _score_or_zero(r.get("pullback_score")) > 0)
    momentum_nonzero = sum(1 for r in rows if _score_or_zero(r.get("momentum_score")) > 0)

    logger.info(
        "[DERIVED][MINERVINI][ENTRY_INPUTS] rows=%d has_close=%d has_high=%d has_low=%d has_volume=%d has_atr=%d has_rs=%d",
        len(rows),
        has_close,
        has_high,
        has_low,
        has_volume,
        has_atr,
        has_rs,
    )
    logger.info(
        "[DERIVED][MINERVINI][ENTRY_SCORES] rows=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
        len(rows),
        breakout_nonzero,
        pullback_nonzero,
        momentum_nonzero,
    )
    logger.info("[DERIVED][MINERVINI][ENTRY_SCORES][SAMPLE] %s", entry_diag_samples)

    return rows


def compute_and_store_derived_minervini(
    *,
    engine,
    symbols: Iterable[str],
    env: str | None = None,
    as_of: date | str | None = None,
    lookback_days: int | None = None,
) -> int:
    as_of_date = _as_of_date(as_of)
    start = time.monotonic()
    symbols_list = [str(s).zfill(6) for s in symbols if s]
    rows = compute_minervini_features_for_asof(
        engine=engine,
        symbols=symbols_list,
        env=env,
        as_of=as_of_date,
        lookback_days=lookback_days,
    )
    
    # NaN/Inf 완전 차단 (DB upsert 직전 sanitize)
    for r in rows:
        for score_key in ("rs_score", "trend_score", "breakout_score", "pullback_score", "momentum_score"):
            r[score_key] = _score_or_zero(r.get(score_key))
        fj = r.get("features_json")
        if fj is not None:
            r["features_json"] = to_jsonable(fj)

    includes_breakout = int(any("breakout_score" in r for r in rows))
    includes_pullback = int(any("pullback_score" in r for r in rows))
    includes_momentum = int(any("momentum_score" in r for r in rows))
    breakout_nonzero = sum(1 for r in rows if _score_or_zero(r.get("breakout_score")) > 0)
    pullback_nonzero = sum(1 for r in rows if _score_or_zero(r.get("pullback_score")) > 0)
    momentum_nonzero = sum(1 for r in rows if _score_or_zero(r.get("momentum_score")) > 0)

    logger.info(
        "[DERIVED][MINERVINI][UPSERT_SCHEMA] includes_breakout=%d includes_pullback=%d includes_momentum=%d",
        includes_breakout,
        includes_pullback,
        includes_momentum,
    )
    logger.info(
        "[DERIVED][MINERVINI][UPSERT_NONZERO] breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
        breakout_nonzero,
        pullback_nonzero,
        momentum_nonzero,
    )
    
    repo = DerivedMinerviniRepo(engine)
    env_n = (env or os.getenv("STRATEGY_ENV", "practice")).strip().lower()
    upserted = repo.upsert_rows(env=env_n, rows=rows)
    dt = time.monotonic() - start
    logger.info(
        "[DERIVED][MINERVINI] as_of=%s symbols=%s upserted=%s dt=%.2f",
        as_of_date,
        len(symbols_list),
        upserted,
        dt,
    )
    return upserted
