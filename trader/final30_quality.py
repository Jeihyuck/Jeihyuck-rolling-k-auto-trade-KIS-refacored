from __future__ import annotations

from typing import Any

import pandas as pd

from trader.indicators import safe_nullable_float


ENTRY_CRITICAL_COLUMNS = (
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "entry_style_selected",
)


def is_placeholder_entry_value(value: Any) -> bool:
    numeric = safe_nullable_float(value)
    if numeric is None:
        return True
    return float(numeric) in (0.0, 100.0)


def normalize_entry_input_value(value: Any) -> float | None:
    numeric = safe_nullable_float(value)
    if numeric is None:
        return None
    if is_placeholder_entry_value(numeric):
        return None
    return float(numeric)


def is_invalid_entry_input(close: Any, pivot: Any, hi_52w: Any, volume: Any) -> bool:
    values = [
        normalize_entry_input_value(close),
        normalize_entry_input_value(pivot),
        normalize_entry_input_value(hi_52w),
        normalize_entry_input_value(volume),
    ]
    if any(value is None for value in values):
        return True
    close_f, pivot_f, hi_52w_f, volume_f = values
    if any(float(value) <= 0 for value in values if value is not None):
        return True
    if abs(float(close_f) - float(pivot_f)) < 1e-9 and abs(float(pivot_f) - float(hi_52w_f)) < 1e-9:
        return True
    return False


def score_distribution_is_monoculture(series: Any, threshold: float = 0.9) -> bool:
    values = pd.Series(series).dropna()
    if values.empty:
        return False
    vc = values.value_counts(normalize=True)
    return bool((not vc.empty) and float(vc.iloc[0]) >= threshold)


def dominant_value_ratio(series: Any) -> float:
    values = pd.Series(series).dropna()
    if values.empty:
        return 0.0
    vc = values.value_counts(normalize=True)
    return float(vc.iloc[0]) if not vc.empty else 0.0


def validate_trade_ready(final30_df: pd.DataFrame) -> None:
    errors: list[str] = []
    if final30_df is None or final30_df.empty:
        errors.append("final30_empty")
    if errors:
        raise RuntimeError(f"[TRADE][ABORT][FINAL30_INVALID] errors={errors}")

    if "ma20" not in final30_df.columns:
        errors.append("ma20_missing_column")
    elif (pd.to_numeric(final30_df["ma20"], errors="coerce").fillna(0) <= 0).any():
        errors.append("ma20_invalid_rows")

    if "atr_pct" not in final30_df.columns:
        errors.append("atr_pct_missing_column")
    elif (pd.to_numeric(final30_df["atr_pct"], errors="coerce").fillna(0) <= 0).any():
        errors.append("atr_pct_invalid_rows")

    for column in ENTRY_CRITICAL_COLUMNS:
        if column not in final30_df.columns:
            errors.append(f"{column}_missing_column")

    if errors:
        raise RuntimeError(f"[TRADE][ABORT][FINAL30_INVALID] errors={errors}")


def summarize_final30_quality(df: pd.DataFrame, *, required_rows: int = 30) -> dict[str, Any]:
    frame = df.copy(deep=True) if isinstance(df, pd.DataFrame) else pd.DataFrame()
    rows = int(len(frame))
    uniq_codes = int(frame["code"].astype(str).nunique()) if "code" in frame.columns else 0

    def _valid_ratio(column: str, *, positive: bool = False) -> float:
        if column not in frame.columns or rows == 0:
            return 0.0
        series = pd.to_numeric(frame[column], errors="coerce") if positive else frame[column]
        if positive:
            return float((series.fillna(0) > 0).mean())
        return float(series.notna().mean())

    valid_ma20_ratio = _valid_ratio("ma20", positive=True)
    valid_atr_ratio = _valid_ratio("atr_pct", positive=True)
    breakout_nonnull_ratio = _valid_ratio("breakout_score")
    pullback_nonnull_ratio = _valid_ratio("pullback_score")
    momentum_nonnull_ratio = _valid_ratio("momentum_score")
    valid_score_final_ratio = _valid_ratio("score_final", positive=True)

    entry_style_monoculture = score_distribution_is_monoculture(frame.get("entry_style_selected", pd.Series(dtype=object)))
    breakout_monoculture = score_distribution_is_monoculture(frame.get("breakout_score", pd.Series(dtype=float)))
    pullback_monoculture = score_distribution_is_monoculture(frame.get("pullback_score", pd.Series(dtype=float)))
    momentum_monoculture = score_distribution_is_monoculture(frame.get("momentum_score", pd.Series(dtype=float)))
    score_pattern_monoculture = False
    if rows > 0:
        pattern_df = pd.DataFrame(
            {
                "breakout": frame.get("breakout_score"),
                "pullback": frame.get("pullback_score"),
                "momentum": frame.get("momentum_score"),
                "entry_style": frame.get("entry_style_selected"),
            }
        )
        patterns = pattern_df.astype(str).agg("|".join, axis=1)
        score_pattern_monoculture = score_distribution_is_monoculture(patterns)

    score_monoculture = breakout_monoculture or pullback_monoculture or momentum_monoculture or score_pattern_monoculture
    hard_fail_reasons: list[str] = []
    if rows != required_rows:
        hard_fail_reasons.append(f"rows={rows}")
    if uniq_codes != required_rows:
        hard_fail_reasons.append(f"uniq_codes={uniq_codes}")
    if valid_score_final_ratio < 0.95:
        hard_fail_reasons.append(f"valid_score_final_ratio={valid_score_final_ratio:.3f}")
    if breakout_nonnull_ratio < 0.95:
        hard_fail_reasons.append(f"breakout_nonnull_ratio={breakout_nonnull_ratio:.3f}")
    if pullback_nonnull_ratio < 0.95:
        hard_fail_reasons.append(f"pullback_nonnull_ratio={pullback_nonnull_ratio:.3f}")
    if momentum_nonnull_ratio < 0.95:
        hard_fail_reasons.append(f"momentum_nonnull_ratio={momentum_nonnull_ratio:.3f}")

    soft_fail_reasons: list[str] = []
    if entry_style_monoculture:
        soft_fail_reasons.append("entry_style_monoculture")
    if breakout_monoculture:
        soft_fail_reasons.append("breakout_monoculture")
    if pullback_monoculture:
        soft_fail_reasons.append("pullback_monoculture")
    if momentum_monoculture:
        soft_fail_reasons.append("momentum_monoculture")
    if score_pattern_monoculture:
        soft_fail_reasons.append("score_pattern_monoculture")
    if score_monoculture:
        soft_fail_reasons.append("score_monoculture")

    ok = len(hard_fail_reasons) == 0
    soft_fail = len(soft_fail_reasons) > 0
    return {
        "rows": rows,
        "uniq_codes": uniq_codes,
        "valid_ma20_ratio": valid_ma20_ratio,
        "valid_atr_ratio": valid_atr_ratio,
        "breakout_nonnull_ratio": breakout_nonnull_ratio,
        "pullback_nonnull_ratio": pullback_nonnull_ratio,
        "momentum_nonnull_ratio": momentum_nonnull_ratio,
        "valid_score_final_ratio": valid_score_final_ratio,
        "entry_style_monoculture": entry_style_monoculture,
        "breakout_monoculture": breakout_monoculture,
        "pullback_monoculture": pullback_monoculture,
        "momentum_monoculture": momentum_monoculture,
        "score_pattern_monoculture": score_pattern_monoculture,
        "score_monoculture": score_monoculture,
        "hard_fail_reasons": hard_fail_reasons,
        "soft_fail_reasons": soft_fail_reasons,
        "soft_fail": soft_fail,
        "ok": ok,
    }