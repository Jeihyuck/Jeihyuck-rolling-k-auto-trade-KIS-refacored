from __future__ import annotations

import logging
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
    as_of: date | str | None = None,
    lookback_days: int | None = None,
) -> list[dict]:
    """Compute Minervini features from DB OHLCV and return rows for upsert."""
    as_of_date = _as_of_date(as_of)
    symbols_list = [str(s).zfill(6) for s in symbols if s]
    if not symbols_list:
        return []

    need_days = int(lookback_days or 520)

    bench_df = _load_df_from_db(engine=engine, symbol=RS_BENCHMARK, as_of=as_of_date, days=need_days)
    bench_close = bench_df["close"] if not bench_df.empty and "close" in bench_df.columns else pd.Series(dtype=float)

    price_series: dict[str, pd.Series] = {}
    features_map: dict[str, dict] = {}
    rows: list[dict] = []

    cfg = MinerviniConfig(rs_min_percentile=RS_MIN_PCTILE / 100.0)

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
            feats["vcp_ok"] = bool(vcp_info.get("vcp_ok"))
            feats["vcp_score"] = float(vcp_info.get("score") or 0.0)
            feats["pivot"] = float(pivot_val) if pd.notna(pivot_val) else None
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
        rows.append(
            {
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
                "vcp_score": feats.get("vcp_score"),
                "vcp_ok": feats.get("vcp_ok"),
                "pivot": feats.get("pivot"),
                "minervini_score": score,
                "minervini_pass": bool(ok),
                "features_json": {
                    **{k: v for k, v in feats.items() if k not in {"close", "ma50", "ma150", "ma200", "ma200_slope", "dollar_vol_50", "atr14", "atr_pct", "rs_percentile", "vcp_score", "vcp_ok", "pivot"}},
                    "reasons": reasons,
                },
            }
        )

    return rows


def compute_and_store_derived_minervini(
    *,
    engine,
    symbols: Iterable[str],
    as_of: date | str | None = None,
    lookback_days: int | None = None,
) -> int:
    as_of_date = _as_of_date(as_of)
    start = time.monotonic()
    symbols_list = [str(s).zfill(6) for s in symbols if s]
    rows = compute_minervini_features_for_asof(
        engine=engine,
        symbols=symbols_list,
        as_of=as_of_date,
        lookback_days=lookback_days,
    )
    
    # NaN/Inf 완전 차단 (DB upsert 직전 sanitize)
    for r in rows:
        fj = r.get("features_json")
        if fj is not None:
            r["features_json"] = to_jsonable(fj)
    
    repo = DerivedMinerviniRepo(engine)
    upserted = repo.upsert_rows(rows)
    dt = time.monotonic() - start
    logger.info(
        "[DERIVED][MINERVINI] as_of=%s symbols=%s upserted=%s dt=%.2f",
        as_of_date,
        len(symbols_list),
        upserted,
        dt,
    )
    return upserted
