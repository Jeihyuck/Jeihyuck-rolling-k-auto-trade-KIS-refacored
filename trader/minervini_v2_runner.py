from __future__ import annotations

import logging
import os
from uuid import uuid4

import pandas as pd
import sqlalchemy as sa

from trader.db.engine import get_engine
from trader.db.repos_minervini_vcp_relaxed import upsert_minervini_vcp_relaxed
from trader.db.repos_ohlcv import load_ohlcv_df
from trader.db.repos_watchlist import load_watchlist_entries
from trader.time_coerce import to_date

logger = logging.getLogger(__name__)

BENCHMARK = "229200"
LOOKBACK = 260
VCP_PIVOT_WINDOW = 55
ATR_MAX_PCT_DEFAULT = 0.10
RS_CUT_DEFAULT = 80.0

MINERVINI_RESULT_VERSION_DEFAULT = "v2_relaxed_vcp"
MINERVINI_INPUT_SOURCE_DEFAULT = "final30"
MINERVINI_RESULT_TABLE = "minervini_results_vcp_relaxed"
FINAL30_EXPECTED_COUNT = 30

MINERVINI_VCP_MAX_RECENT_RANGE_PCT_DEFAULT = 0.41
MINERVINI_VCP_PIVOT_NEAR_PCT_DEFAULT = 0.16
MINERVINI_VCP_VOL_SHRINK_MODE_DEFAULT = "median"
MINERVINI_VCP_VOL_SHRINK_TOL_DEFAULT = 1.65
MINERVINI_VCP_TIGHTNESS_TOL_MULT_DEFAULT = 1.30

BASE_ATR_TIGHT_MAX = 0.08
BASE_TIGHT_PCT_MAX = 0.10
VCP_RECENT_VOL_WINDOW = 10
VCP_PREV_VOL_WINDOW = 50


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning("[MINERVINI][CFG] invalid %s=%r fallback=%s", name, raw, default)
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    val = str(raw).strip()
    return val if val else default


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


def _volume_metric(series: pd.Series, mode: str) -> float | None:
    if series.empty:
        return None
    if mode == "avg":
        value = float(series.mean())
    else:
        value = float(series.median())
    return value if pd.notna(value) else None


def _default_vcp_cfg() -> dict[str, float | str]:
    return {
        "max_recent_range_pct": MINERVINI_VCP_MAX_RECENT_RANGE_PCT_DEFAULT,
        "pivot_near_pct": MINERVINI_VCP_PIVOT_NEAR_PCT_DEFAULT,
        "vol_shrink_mode": MINERVINI_VCP_VOL_SHRINK_MODE_DEFAULT,
        "vol_shrink_tol": MINERVINI_VCP_VOL_SHRINK_TOL_DEFAULT,
        "tightness_tol_mult": MINERVINI_VCP_TIGHTNESS_TOL_MULT_DEFAULT,
    }


def _vcp_eval(
    df: pd.DataFrame,
    close: float,
    atr_pct: float | None,
    symbol: str = "",
    vcp_cfg: dict[str, float | str] | None = None,
) -> tuple[dict, list[str], bool, dict[str, bool]]:
    cfg = dict(_default_vcp_cfg())
    if vcp_cfg:
        cfg.update(vcp_cfg)

    max_recent_range_pct = float(cfg["max_recent_range_pct"])
    pivot_near_pct = float(cfg["pivot_near_pct"])
    vol_shrink_mode = str(cfg["vol_shrink_mode"]).strip().lower()
    vol_shrink_tol = float(cfg["vol_shrink_tol"])
    tightness_tol_mult = float(cfg["tightness_tol_mult"])

    if vol_shrink_mode not in {"avg", "median"}:
        logger.warning("[MINERVINI][CFG] invalid vol_shrink_mode=%s fallback=median", vol_shrink_mode)
        vol_shrink_mode = "median"

    highs = df["high"].astype(float)
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

    atr_tight_max = BASE_ATR_TIGHT_MAX * tightness_tol_mult
    tight_pct_max = BASE_TIGHT_PCT_MAX * tightness_tol_mult
    tight_ok = (
        (atr_pct is not None and atr_pct <= atr_tight_max)
        or (tight_pct is not None and tight_pct <= tight_pct_max)
    )

    recent_vols = vols.tail(VCP_RECENT_VOL_WINDOW)
    prev_end = len(vols) - len(recent_vols)
    prev_start = max(0, prev_end - VCP_PREV_VOL_WINDOW)
    prev_vols = vols.iloc[prev_start:prev_end]

    prev_metric = _volume_metric(prev_vols, vol_shrink_mode)
    recent_metric = _volume_metric(recent_vols, vol_shrink_mode)
    vol_shrink_ratio = (
        recent_metric / prev_metric
        if (recent_metric is not None and prev_metric not in (None, 0.0))
        else None
    )
    vol_ok = (
        prev_metric is not None
        and recent_metric is not None
        and recent_metric <= (prev_metric * vol_shrink_tol)
    )

    near_threshold_price = (pivot * (1.0 - pivot_near_pct)) if pivot is not None else None
    near_ok = near_threshold_price is not None and close >= near_threshold_price

    guard_ok = c3 is not None and c3 <= max_recent_range_pct

    distance_from_pivot = None
    if pivot is not None and pivot > 0:
        distance_from_pivot = (pivot - close) / pivot

    symbol_label = symbol or "-"
    logger.info(
        "[VCP][RANGE] %s recent_range=%.4f threshold=%.4f pass=%s",
        symbol_label,
        c3 if c3 is not None else -1.0,
        max_recent_range_pct,
        bool(guard_ok),
    )
    logger.info(
        "[VCP][NEAR] %s close=%.4f pivot=%.4f distance=%.4f threshold=%.4f pass=%s",
        symbol_label,
        close,
        pivot if pivot is not None else -1.0,
        distance_from_pivot if distance_from_pivot is not None else -1.0,
        pivot_near_pct,
        bool(near_ok),
    )
    logger.info(
        "[VCP][VOL] %s mode=%s prev_metric=%.4f recent_metric=%.4f tol=%.4f pass=%s",
        symbol_label,
        vol_shrink_mode,
        prev_metric if prev_metric is not None else -1.0,
        recent_metric if recent_metric is not None else -1.0,
        vol_shrink_tol,
        bool(vol_ok),
    )

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
            "vcp_volume_metric_prev": prev_metric,
            "vcp_volume_metric_recent": recent_metric,
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


def _extract_reason_counts(rows: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for reason in row.get("reject_reasons") or []:
            counts[reason] = counts.get(reason, 0) + 1
    return counts


def _extract_selected_reason_counts(rows: list[dict], reason_keys: list[str]) -> dict[str, int]:
    counts = {key: 0 for key in reason_keys}
    for row in rows:
        for reason in row.get("reject_reasons") or []:
            if reason in counts:
                counts[reason] += 1
    return counts


def _load_legacy_reason_counts(engine: sa.Engine, env: str, as_of: str, reason_keys: list[str]) -> dict[str, int]:
    counts = {key: 0 for key in reason_keys}
    try:
        table = sa.Table("signals_minervini_daily_v2", sa.MetaData(), autoload_with=engine)
    except Exception:
        return counts

    try:
        as_of_date = to_date(as_of)
        with engine.connect() as conn:
            rows = conn.execute(
                sa.select(table.c.reject_reasons).where(
                    sa.and_(
                        table.c.env == env,
                        table.c.as_of == as_of_date,
                    )
                )
            ).all()
    except Exception:
        return counts

    for (reasons,) in rows:
        if not reasons:
            continue
        if isinstance(reasons, str):
            import json

            try:
                reasons = json.loads(reasons)
            except Exception:
                reasons = []
        for reason in reasons or []:
            if reason in counts:
                counts[reason] += 1
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    mode = _env_str("MODE", "minervini_test").strip().lower()
    if mode != "minervini_test":
        raise RuntimeError(f"minervini_v2_runner is minervini_test-only. mode={mode}")

    env = _env_str("STRATEGY_ENV", "practice").strip().lower()
    as_of = _env_str("AS_OF_OVERRIDE", "").strip()
    if not as_of:
        raise RuntimeError("AS_OF_OVERRIDE is required for minervini_v2_runner")

    input_source = _env_str("MINERVINI_INPUT_SOURCE", MINERVINI_INPUT_SOURCE_DEFAULT).strip().lower()
    if input_source != "final30":
        raise RuntimeError(f"MINERVINI_INPUT_SOURCE must be final30. got={input_source}")

    for disallow_flag in ("UNIVERSE_FORCE_REBUILD", "EMERGENCY_BUILD", "FORCE_CANDIDATE"):
        if _env_str(disallow_flag, "0") == "1":
            raise RuntimeError(f"{disallow_flag}=1 is forbidden for minervini_test final30-only run")

    rs_cut = _env_float("RS_MIN_PCTILE", RS_CUT_DEFAULT)
    atr_max_pct = _env_float("ATR_MAX_PCT", ATR_MAX_PCT_DEFAULT)

    vcp_cfg = {
        "max_recent_range_pct": _env_float(
            "MINERVINI_VCP_MAX_RECENT_RANGE_PCT", MINERVINI_VCP_MAX_RECENT_RANGE_PCT_DEFAULT
        ),
        "pivot_near_pct": _env_float("MINERVINI_VCP_PIVOT_NEAR_PCT", MINERVINI_VCP_PIVOT_NEAR_PCT_DEFAULT),
        "vol_shrink_mode": _env_str("MINERVINI_VCP_VOL_SHRINK_MODE", MINERVINI_VCP_VOL_SHRINK_MODE_DEFAULT).lower(),
        "vol_shrink_tol": _env_float("MINERVINI_VCP_VOL_SHRINK_TOL", MINERVINI_VCP_VOL_SHRINK_TOL_DEFAULT),
        "tightness_tol_mult": _env_float(
            "MINERVINI_VCP_TIGHTNESS_TOL_MULT", MINERVINI_VCP_TIGHTNESS_TOL_MULT_DEFAULT
        ),
    }

    version = _env_str("MINERVINI_RESULT_VERSION", MINERVINI_RESULT_VERSION_DEFAULT)
    param_snapshot = {
        "max_recent_range_pct": float(vcp_cfg["max_recent_range_pct"]),
        "pivot_near_pct": float(vcp_cfg["pivot_near_pct"]),
        "vol_shrink_mode": str(vcp_cfg["vol_shrink_mode"]),
        "vol_shrink_tol": float(vcp_cfg["vol_shrink_tol"]),
        "tightness_tol_mult": float(vcp_cfg["tightness_tol_mult"]),
        "rs_cut_changed": False,
    }

    run_id = str(uuid4())
    engine = get_engine()

    watchlist_entries = load_watchlist_entries(engine, env, "pb1_watchlist_final", as_of)
    codes = [str(item.get("code") or "").zfill(6) for item in watchlist_entries if item.get("code")]
    code_to_name = {str(item.get("code") or "").zfill(6): item.get("name") for item in watchlist_entries}

    logger.info("[MINERVINI][INPUT] source=%s count=%s", input_source, len(codes))
    if len(codes) != FINAL30_EXPECTED_COUNT:
        logger.warning("[WARN] final30 count mismatch: %s", len(codes))

    logger.info("[MINERVINI_V2][START] env=%s as_of=%s n_final30=%s run_id=%s", env, as_of, len(codes), run_id)

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
        name = code_to_name.get(symbol)
        df = load_ohlcv_df(engine, env, symbol, as_of, LOOKBACK)
        row = {
            "symbol": symbol,
            "name": name,
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
            "vcp_volume_metric_prev": None,
            "vcp_volume_metric_recent": None,
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

        vcp_values, vcp_reasons, vcp_pass, _checks = _vcp_eval(
            df,
            close,
            atr_pct,
            symbol=symbol,
            vcp_cfg=vcp_cfg,
        )

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

    rows = [per_symbol[code] for code in codes]

    result_rows = [
        {
            "symbol": row["symbol"],
            "name": row.get("name"),
            "rs_pass": bool(row["rs_pass"]),
            "trend_pass": bool(row["trend_template_pass"]),
            "vcp_pass": bool(row["vcp_pass"]),
            "final_pass": bool(row["pass"]),
            "fail_reasons": list(row.get("reject_reasons") or []),
            "version": version,
            "input_source": input_source,
            "param_snapshot": dict(param_snapshot),
            "rs_score": row.get("rs_score"),
            "pivot_price": row.get("pivot_price"),
            "close_price": row.get("close"),
            "recent_range_pct": row.get("c3_pct"),
            "vcp_volume_metric_prev": row.get("vcp_volume_metric_prev"),
            "vcp_volume_metric_recent": row.get("vcp_volume_metric_recent"),
        }
        for row in rows
    ]

    upserted = upsert_minervini_vcp_relaxed(engine, env, as_of, result_rows)
    logger.info("[MINERVINI_V2][UPSERT] table=%s upserted=%s", MINERVINI_RESULT_TABLE, upserted)

    pass_codes = [row["symbol"] for row in rows if row["pass"]][:10]
    logger.info("[MINERVINI_V2][PASS_TOP10] codes=%s", pass_codes)

    reason_counts_all = _extract_reason_counts(rows)
    selected_reason_keys = [
        "rs_below_cut",
        "vcp_recent_range_too_wide",
        "vcp_volume_not_shrinking",
        "vcp_not_near_pivot",
        "vcp_not_tight",
    ]
    after_counts = _extract_selected_reason_counts(rows, selected_reason_keys)
    before_counts = _load_legacy_reason_counts(engine, env, as_of, selected_reason_keys)

    rs_only_fail = 0
    rs_pass_but_vcp_trend_fail = 0
    rs_and_extra_fail = 0
    final_pass = 0
    for row in rows:
        reasons = list(row.get("reject_reasons") or [])
        has_rs_fail = "rs_below_cut" in reasons
        if row["pass"]:
            final_pass += 1
            continue
        if has_rs_fail and len(reasons) == 1:
            rs_only_fail += 1
        elif has_rs_fail:
            rs_and_extra_fail += 1
        else:
            rs_pass_but_vcp_trend_fail += 1

    logger.info("[MINERVINI V2 RELAXED VCP] Summary")
    logger.info("- version: %s", version)
    logger.info("- table: %s", MINERVINI_RESULT_TABLE)
    logger.info("- input_source: %s", input_source)
    logger.info("- input_count: %s", len(rows))
    logger.info("1) RS 단독 탈락: %s", rs_only_fail)
    logger.info("2) RS 통과 but VCP/Trend 탈락: %s", rs_pass_but_vcp_trend_fail)
    logger.info("3) RS도 탈락 + 추가사유: %s", rs_and_extra_fail)
    logger.info("4) 최종 통과: %s", final_pass)

    logger.info("Reason counts:")
    for reason in selected_reason_keys:
        logger.info("- %s: %s", reason, reason_counts_all.get(reason, 0))

    logger.info("[MINERVINI_V2][COMPARE] reason counts before(signals_minervini_daily_v2) vs after(%s)", MINERVINI_RESULT_TABLE)
    for reason in selected_reason_keys:
        before = int(before_counts.get(reason, 0))
        after = int(after_counts.get(reason, 0))
        logger.info("- %s before=%s after=%s diff=%+d", reason, before, after, after - before)


if __name__ == "__main__":
    main()
