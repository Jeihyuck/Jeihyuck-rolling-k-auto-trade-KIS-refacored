from __future__ import annotations

import logging
import os
from uuid import uuid4

import pandas as pd

from trader.db.engine import get_engine
from trader.db.repos_minervini_v2 import upsert_minervini_v2
from trader.db.repos_ohlcv import load_ohlcv_df
from trader.db.repos_watchlist import load_watchlist_codes

logger = logging.getLogger(__name__)

BENCHMARK = "229200"
LOOKBACK = 260
VCP_PIVOT_WINDOW = 55
ATR_MAX_PCT_DEFAULT = 0.10
RS_CUT_DEFAULT = 80.0


def _atr14(df: pd.DataFrame) -> float | None:
    if len(df) < 15:
        return None
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    val = tr.rolling(14).mean().iloc[-1]
    return float(val) if pd.notna(val) else None


def _safe_sma(series: pd.Series, window: int) -> float | None:
    if len(series) < window:
        return None
    value = series.rolling(window).mean().iloc[-1]
    return float(value) if pd.notna(value) else None


def _trend_template(df: pd.DataFrame) -> tuple[dict, list[str], bool]:
    close_s = df["close"].astype(float)
    close = float(close_s.iloc[-1])
    ma50 = _safe_sma(close_s, 50)
    ma150 = _safe_sma(close_s, 150)
    ma200 = _safe_sma(close_s, 200)
    ma200_20ago = float(close_s.rolling(200).mean().iloc[-21]) if len(close_s) >= 220 else None
    high_52w = float(df["high"].astype(float).tail(252).max()) if len(df) >= 252 else float(df["high"].astype(float).max())

    reasons: list[str] = []
    if ma50 is None or close <= ma50:
        reasons.append("trend_close_below_ma50")

    ma_stack_ok = (
        ma50 is not None
        and ma150 is not None
        and ma200 is not None
        and (ma50 > ma150 > ma200)
    )
    if not ma_stack_ok:
        reasons.append("trend_ma_stack_fail")

    ma200_up = ma200 is not None and ma200_20ago is not None and ma200 > ma200_20ago
    if not ma200_up:
        reasons.append("trend_ma200_not_rising")

    near_52w_high = high_52w > 0 and close >= 0.75 * high_52w
    if not near_52w_high:
        reasons.append("trend_too_far_from_52w_high")

    trend_pass = len(reasons) == 0
    return (
        {
            "close": close,
            "ma50": ma50,
            "ma150": ma150,
            "ma200": ma200,
            "ma200_up": bool(ma200_up),
            "high_52w": high_52w,
            "near_52w_high": bool(near_52w_high),
        },
        reasons,
        trend_pass,
    )


def _range_pct(df: pd.DataFrame, window: int) -> float | None:
    if len(df) < window:
        return None
    tail = df.tail(window)
    high_max = float(tail["high"].astype(float).max())
    low_min = float(tail["low"].astype(float).min())
    if high_max <= 0:
        return None
    return (high_max - low_min) / high_max


def _vcp_eval(df: pd.DataFrame, close: float, atr_pct: float | None) -> tuple[dict, list[str], bool, dict[str, bool]]:
    highs = df["high"].astype(float)
    lows = df["low"].astype(float)
    vols = df["volume"].astype(float)
    closes = df["close"].astype(float)

    pivot = float(highs.tail(VCP_PIVOT_WINDOW).max()) if len(df) >= VCP_PIVOT_WINDOW else None

    c1 = _range_pct(df, 60)
    c2 = _range_pct(df, 30)
    c3 = _range_pct(df, 15)

    contraction_count = 0
    if c1 is not None and c2 is not None and c2 < (c1 * 0.85):
        contraction_count += 1
    if c2 is not None and c3 is not None and c3 < (c2 * 0.85):
        contraction_count += 1
    contraction_ok = contraction_count >= 1

    tight_pct = None
    if len(closes) >= 10:
        last10 = closes.tail(10)
        den = float(last10.max())
        if den > 0:
            tight_pct = (float(last10.max()) - float(last10.min())) / den

    tight_ok = ((atr_pct is not None and atr_pct <= 0.06) or (tight_pct is not None and tight_pct <= 0.08))

    sma10_vol = _safe_sma(vols, 10)
    sma50_vol = _safe_sma(vols, 50)
    vol_shrink_ratio = (sma10_vol / sma50_vol) if (sma10_vol is not None and sma50_vol not in (None, 0.0)) else None
    vol_ok = vol_shrink_ratio is not None and vol_shrink_ratio <= 0.8

    near_ok = pivot is not None and close >= 0.92 * pivot
    guard_ok = c3 is not None and c3 <= 0.18

    checks = {
        "contraction_ok": contraction_ok,
        "tight_ok": bool(tight_ok),
        "vol_ok": bool(vol_ok),
        "near_ok": bool(near_ok),
        "guard_ok": bool(guard_ok),
    }

    passed_count = sum(1 for v in checks.values() if v)
    vcp_pass = passed_count >= 4

    vcp_reasons: list[str] = []
    if not contraction_ok:
        vcp_reasons.append("vcp_no_contraction")
    if not tight_ok:
        vcp_reasons.append("vcp_not_tight")
    if not vol_ok:
        vcp_reasons.append("vcp_volume_not_shrinking")
    if not near_ok:
        vcp_reasons.append("vcp_not_near_pivot")
    if not guard_ok:
        vcp_reasons.append("vcp_recent_range_too_wide")

    return (
        {
            "pivot_price": pivot,
            "contraction_count": contraction_count,
            "c1_pct": c1,
            "c2_pct": c2,
            "c3_pct": c3,
            "tight_pct": tight_pct,
            "vol_shrink_ratio": vol_shrink_ratio,
        },
        ([] if vcp_pass else vcp_reasons),
        vcp_pass,
        checks,
    )


def _percentile_map(values: dict[str, float]) -> dict[str, float]:
    if not values:
        return {}
    series = pd.Series(values)
    pct = series.rank(method="average", pct=True) * 100.0
    return {str(symbol): float(pct_val) for symbol, pct_val in pct.to_dict().items()}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    env = (os.getenv("STRATEGY_ENV") or "practice").strip().lower()
    as_of = (os.getenv("AS_OF_OVERRIDE") or "").strip()
    if not as_of:
        raise RuntimeError("AS_OF_OVERRIDE is required for minervini_v2_runner")

    rs_cut = float(os.getenv("RS_MIN_PCTILE", str(RS_CUT_DEFAULT)))
    atr_max_pct = float(os.getenv("ATR_MAX_PCT", str(ATR_MAX_PCT_DEFAULT)))
    run_id = str(uuid4())

    engine = get_engine()

    codes = load_watchlist_codes(engine, env, "pb1_watchlist_final", as_of)
    logger.info("[MINERVINI_V2][FINAL30] strategy=pb1_watchlist_final loaded=%s", len(codes))
    logger.info("[MINERVINI_V2][START] env=%s as_of=%s n_final30=%s", env, as_of, len(codes))

    bench_df = load_ohlcv_df(engine, env, BENCHMARK, as_of, LOOKBACK)
    if len(bench_df) < LOOKBACK:
        raise RuntimeError(f"benchmark data insufficient: symbol={BENCHMARK} rows={len(bench_df)} need={LOOKBACK}")

    bench_close = bench_df["close"].astype(float)
    bench_ma50 = _safe_sma(bench_close, 50)
    bench_ma200 = _safe_sma(bench_close, 200)
    regime_pass = bool(bench_ma50 is not None and bench_ma200 is not None and bench_ma50 > bench_ma200)
    logger.info("[MINERVINI_V2][REGIME] pass=%s", 1 if regime_pass else 0)

    per_symbol: dict[str, dict] = {}
    rs_scores: dict[str, float] = {}

    for symbol in codes:
        df = load_ohlcv_df(engine, env, symbol, as_of, LOOKBACK)
        row = {
            "symbol": symbol,
            "benchmark": BENCHMARK,
            "regime_pass": regime_pass,
            "rs63": None,
            "rs126": None,
            "rs_score": None,
            "rs_pctile": None,
            "rs_pass": False,
            "close": None,
            "ma50": None,
            "ma150": None,
            "ma200": None,
            "ma200_up": False,
            "high_52w": None,
            "near_52w_high": False,
            "trend_template_pass": False,
            "pivot_price": None,
            "contraction_count": 0,
            "c1_pct": None,
            "c2_pct": None,
            "c3_pct": None,
            "tight_pct": None,
            "atr14": None,
            "atr_pct": None,
            "vol_shrink_ratio": None,
            "vcp_pass": False,
            "vcp_reasons": [],
            "pass": False,
            "reject_reasons": [],
        }

        if len(df) < LOOKBACK:
            row["reject_reasons"] = ["data_insufficient"]
            per_symbol[symbol] = row
            continue

        close = float(df["close"].astype(float).iloc[-1])
        atr14 = _atr14(df)
        atr_pct = (atr14 / close) if (atr14 is not None and close > 0) else None

        trend_values, trend_reasons, trend_pass = _trend_template(df)

        bench63 = float(bench_close.iloc[-63])
        bench126 = float(bench_close.iloc[-126])
        close63 = float(df["close"].astype(float).iloc[-63])
        close126 = float(df["close"].astype(float).iloc[-126])
        rs63 = (close / close63) / (float(bench_close.iloc[-1]) / bench63) if close63 > 0 and bench63 > 0 else None
        rs126 = (close / close126) / (float(bench_close.iloc[-1]) / bench126) if close126 > 0 and bench126 > 0 else None
        rs_score = (0.5 * rs63 + 0.5 * rs126) if (rs63 is not None and rs126 is not None) else None
        if rs_score is not None:
            rs_scores[symbol] = rs_score

        vcp_values, vcp_reasons, vcp_pass, _checks = _vcp_eval(df, close, atr_pct)

        reject_reasons: list[str] = []
        reject_reasons.extend(trend_reasons)
        if not regime_pass:
            reject_reasons.append("regime_fail")
        if atr_pct is None or atr_pct > atr_max_pct:
            reject_reasons.append("atr_too_high")
        reject_reasons.extend(vcp_reasons)

        row.update(
            {
                **trend_values,
                **vcp_values,
                "atr14": atr14,
                "atr_pct": atr_pct,
                "trend_template_pass": trend_pass,
                "vcp_pass": vcp_pass,
                "vcp_reasons": vcp_reasons,
                "rs63": rs63,
                "rs126": rs126,
                "rs_score": rs_score,
                "reject_reasons": reject_reasons,
            }
        )
        per_symbol[symbol] = row

    rs_pctile_map = _percentile_map(rs_scores)
    rs_pass_count = 0
    for symbol, row in per_symbol.items():
        pctile = rs_pctile_map.get(symbol)
        row["rs_pctile"] = pctile
        rs_pass = pctile is not None and pctile >= rs_cut
        row["rs_pass"] = bool(rs_pass)
        if rs_pass:
            rs_pass_count += 1
        else:
            row["reject_reasons"].append("rs_below_cut")

        atr_ok = row["atr_pct"] is not None and row["atr_pct"] <= atr_max_pct
        row["pass"] = bool(
            row["regime_pass"]
            and row["rs_pass"]
            and row["trend_template_pass"]
            and row["vcp_pass"]
            and atr_ok
        )

        dedup: list[str] = []
        seen = set()
        for reason in row["reject_reasons"]:
            if reason not in seen:
                seen.add(reason)
                dedup.append(reason)
        row["reject_reasons"] = dedup

    vcp_fail_reason_counts: dict[str, int] = {}
    vcp_pass_count = 0
    for row in per_symbol.values():
        if row["vcp_pass"]:
            vcp_pass_count += 1
        for reason in row["vcp_reasons"]:
            vcp_fail_reason_counts[reason] = vcp_fail_reason_counts.get(reason, 0) + 1

    logger.info("[MINERVINI_V2][RS] cut=%s pass=%s", int(rs_cut), rs_pass_count)
    logger.info(
        "[MINERVINI_V2][VCP] pass=%s fail=%s reasons=%s",
        vcp_pass_count,
        len(per_symbol) - vcp_pass_count,
        vcp_fail_reason_counts,
    )

    rows = [per_symbol[code] for code in codes]
    upserted = upsert_minervini_v2(engine, env, as_of, run_id, rows)
    logger.info("[MINERVINI_V2][UPSERT] table=signals_minervini_daily_v2 upserted=%s", upserted)

    pass_codes = [row["symbol"] for row in rows if row["pass"]][:10]
    logger.info("[MINERVINI_V2][PASS_TOP10] codes=%s", pass_codes)


if __name__ == "__main__":
    main()
