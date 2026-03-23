from __future__ import annotations

import math
import os
from typing import Any, Iterable

import pandas as pd

from trader.indicators import safe_nullable_float


FINAL30_PRESERVE_FIELDS = (
    "close",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
    "rs_percentile",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "tech_score",
    "score_final",
    "entry_style_selected",
)

FINAL30_REQUIRED_SCORE_FIELDS = (
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "score_final",
)

FINAL30_POSITIVE_REQUIRED_FIELDS = (
    "ma20",
    "atr_pct",
)

FINAL30_ZERO_INVALID_FIELDS = {
    "close",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
}

FINAL30_NUMERIC_FIELDS = {
    "close",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
    "rs_percentile",
    "rs_pctile",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "tech_score",
    "score_final",
    "final_score",
    "score",
    "flow_score",
    "score_flow",
    "score_tech",
    "rank",
    "rank_pool120",
    "rank_top50",
    "rank_final30",
}


def _env_bool(key: str, default: bool) -> bool:
    value = str(os.getenv(key, str(int(default)))).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return float(default)


def _is_blankish(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped.lower() in {"nan", "none", "null"}
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _normalize_string(value: Any) -> str | None:
    if _is_blankish(value):
        return None
    return str(value).strip()


def _normalize_numeric(value: Any) -> float | None:
    numeric = safe_nullable_float(value)
    if numeric is None:
        return None
    try:
        numeric_f = float(numeric)
    except Exception:
        return None
    if not math.isfinite(numeric_f):
        return None
    return numeric_f


def _pick_string(*values: Any) -> str | None:
    for value in values:
        normalized = _normalize_string(value)
        if normalized is not None:
            return normalized
    return None


def _pick_numeric(*values: Any, zero_invalid: bool = False) -> float | None:
    fallback: float | None = None
    for value in values:
        numeric = _normalize_numeric(value)
        if numeric is None:
            continue
        if not zero_invalid or numeric > 0:
            return numeric
        if fallback is None:
            fallback = numeric
    return fallback


def is_valid_positive_numeric(value: Any) -> bool:
    numeric = _normalize_numeric(value)
    return numeric is not None and numeric > 0


def _normalize_quality_flags(*sources: Any) -> list[str]:
    flags: list[str] = []
    for source in sources:
        if isinstance(source, list):
            flags.extend(str(item) for item in source if _normalize_string(item))
    return list(dict.fromkeys(flags))


def normalize_final30_contract_row(row: dict[str, Any] | None) -> dict[str, Any]:
    src = dict(row or {})
    meta = dict(src.get("meta") or {})
    out = dict(src)

    code = _pick_string(out.get("code"), meta.get("code"))
    if code is not None:
        out["code"] = code.zfill(6)
    out["name"] = _pick_string(out.get("name"), meta.get("name"), meta.get("stock_name"), meta.get("kor_name"))
    out["as_of"] = _pick_string(out.get("as_of"), meta.get("as_of"))
    out["entry_style_selected"] = _pick_string(
        out.get("entry_style_selected"),
        out.get("entry_style"),
        meta.get("entry_style_selected"),
        meta.get("entry_style"),
    )
    out["entry_component"] = _pick_string(out.get("entry_component"), meta.get("entry_component"))

    numeric_aliases: dict[str, tuple[Any, ...]] = {
        "close": (out.get("close"), meta.get("close"), out.get("last_close"), meta.get("last_close")),
        "ma20": (out.get("ma20"), meta.get("ma20")),
        "ma50": (out.get("ma50"), meta.get("ma50")),
        "ma150": (out.get("ma150"), meta.get("ma150")),
        "atr_pct": (out.get("atr_pct"), meta.get("atr_pct")),
        "rs_percentile": (out.get("rs_percentile"), out.get("rs_pctile"), meta.get("rs_percentile"), meta.get("rs_pctile")),
        "rs_pctile": (out.get("rs_pctile"), out.get("rs_percentile"), meta.get("rs_pctile"), meta.get("rs_percentile")),
        "breakout_score": (out.get("breakout_score"), out.get("score_breakout"), meta.get("breakout_score"), meta.get("score_breakout")),
        "pullback_score": (out.get("pullback_score"), out.get("score_pullback"), meta.get("pullback_score"), meta.get("score_pullback")),
        "momentum_score": (out.get("momentum_score"), out.get("score_momentum"), meta.get("momentum_score"), meta.get("score_momentum")),
        "tech_score": (out.get("tech_score"), out.get("score_tech"), meta.get("tech_score"), meta.get("score_tech")),
        "score_tech": (out.get("score_tech"), out.get("tech_score"), meta.get("score_tech"), meta.get("tech_score")),
        "flow_score": (out.get("flow_score"), out.get("score_flow"), meta.get("flow_score"), meta.get("score_flow")),
        "score_flow": (out.get("score_flow"), out.get("flow_score"), meta.get("score_flow"), meta.get("flow_score")),
        "score_final": (out.get("score_final"), out.get("final_score"), out.get("score"), meta.get("score_final"), meta.get("final_score"), meta.get("score")),
        "final_score": (out.get("final_score"), out.get("score_final"), out.get("score"), meta.get("final_score"), meta.get("score_final"), meta.get("score")),
        "score": (out.get("score"), out.get("score_final"), out.get("final_score"), meta.get("score"), meta.get("score_final"), meta.get("final_score")),
        "rank": (out.get("rank"), out.get("rank_final30"), meta.get("rank"), meta.get("rank_final30")),
        "rank_pool120": (out.get("rank_pool120"), meta.get("rank_pool120")),
        "rank_top50": (out.get("rank_top50"), meta.get("rank_top50")),
        "rank_final30": (out.get("rank_final30"), out.get("rank"), meta.get("rank_final30"), meta.get("rank")),
    }

    for field, values in numeric_aliases.items():
        out[field] = _pick_numeric(*values, zero_invalid=field in FINAL30_ZERO_INVALID_FIELDS)

    quality_flags = _normalize_quality_flags(out.get("quality_flags"), meta.get("quality_flags"))
    out["quality_flags"] = quality_flags

    for field in FINAL30_PRESERVE_FIELDS:
        if field in out and out.get(field) is not None:
            meta[field] = out.get(field)
        elif field in meta and meta.get(field) is not None:
            out[field] = meta.get(field)

    for field in ("score_final", "final_score", "score", "score_tech", "score_flow", "rank", "rank_pool120", "rank_top50", "rank_final30"):
        if field in out and out.get(field) is not None:
            meta[field] = out.get(field)

    if out.get("entry_style_selected") is not None:
        meta["entry_style_selected"] = out.get("entry_style_selected")
    if out.get("entry_component") is not None:
        meta["entry_component"] = out.get("entry_component")
    if out.get("code") is not None:
        meta["code"] = out.get("code")
    if out.get("name") is not None:
        meta["name"] = out.get("name")
    if out.get("as_of") is not None:
        meta["as_of"] = out.get("as_of")
    meta["quality_flags"] = quality_flags
    out["meta"] = meta
    return out


def _rounded_series(series: Iterable[Any], *, digits: int = 6) -> pd.Series:
    values = pd.Series(list(series), dtype=object)
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().any():
        return numeric.dropna().round(digits)
    return values.dropna().astype(str)


def _dominant_ratio(series: Iterable[Any], *, digits: int = 6) -> float:
    values = _rounded_series(series, digits=digits)
    if values.empty:
        return 0.0
    vc = values.value_counts(normalize=True)
    return float(vc.iloc[0]) if not vc.empty else 0.0


def _invalid_fields_for_row(row: dict[str, Any], *, include_score_fields: bool = True) -> list[str]:
    invalid: list[str] = []
    if not is_valid_positive_numeric(row.get("ma20")):
        invalid.append("ma20")
    if not is_valid_positive_numeric(row.get("atr_pct")):
        invalid.append("atr_pct")
    if include_score_fields:
        if _normalize_numeric(row.get("score_final")) is None:
            invalid.append("score_final")
        for field in ("breakout_score", "pullback_score", "momentum_score"):
            if _normalize_numeric(row.get(field)) is None:
                invalid.append(field)
    return invalid


def evaluate_final30_quality(rows: list[dict[str, Any]] | pd.DataFrame, *, required_rows: int = 30) -> dict[str, Any]:
    if isinstance(rows, pd.DataFrame):
        normalized_rows = [normalize_final30_contract_row(item) for item in rows.to_dict(orient="records")]
    else:
        normalized_rows = [normalize_final30_contract_row(item) for item in (rows or []) if isinstance(item, dict)]

    frame = pd.DataFrame(normalized_rows)
    rows_count = int(len(frame))
    uniq_codes = int(frame["code"].astype(str).nunique()) if "code" in frame.columns and rows_count > 0 else 0

    def _valid_ratio(column: str, *, positive: bool = False) -> float:
        if column not in frame.columns or rows_count == 0:
            return 0.0
        series = pd.to_numeric(frame[column], errors="coerce")
        if positive:
            return float((series.fillna(0) > 0).mean())
        return float(series.notna().mean())

    valid_ma20_ratio = _valid_ratio("ma20", positive=True)
    valid_atr_ratio = _valid_ratio("atr_pct", positive=True)
    breakout_nonnull_ratio = _valid_ratio("breakout_score")
    pullback_nonnull_ratio = _valid_ratio("pullback_score")
    momentum_nonnull_ratio = _valid_ratio("momentum_score")
    valid_score_final_ratio = _valid_ratio("score_final")

    entry_style_monoculture = _dominant_ratio(frame.get("entry_style_selected", pd.Series(dtype=object))) >= 0.9
    breakout_monoculture = _dominant_ratio(frame.get("breakout_score", pd.Series(dtype=float))) >= 0.9
    pullback_monoculture = _dominant_ratio(frame.get("pullback_score", pd.Series(dtype=float))) >= 0.9
    momentum_monoculture = _dominant_ratio(frame.get("momentum_score", pd.Series(dtype=float))) >= 0.9
    score_final_monoculture = _dominant_ratio(frame.get("score_final", pd.Series(dtype=float))) >= 0.9

    score_pattern_monoculture = False
    if rows_count > 0:
        pattern_df = pd.DataFrame(
            {
                "breakout": pd.to_numeric(frame.get("breakout_score"), errors="coerce").round(6),
                "pullback": pd.to_numeric(frame.get("pullback_score"), errors="coerce").round(6),
                "momentum": pd.to_numeric(frame.get("momentum_score"), errors="coerce").round(6),
            }
        )
        patterns = pattern_df.astype(str).agg("|".join, axis=1)
        score_pattern_monoculture = _dominant_ratio(patterns) >= 0.9

    score_monoculture = bool(score_final_monoculture or score_pattern_monoculture)

    hard_fail_reasons: list[str] = []
    if rows_count != required_rows:
        hard_fail_reasons.append(f"rows={rows_count}")
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

    warnings: list[str] = []
    if entry_style_monoculture:
        warnings.append("entry_style_monoculture")
    if breakout_monoculture:
        warnings.append("breakout_monoculture")
    if pullback_monoculture:
        warnings.append("pullback_monoculture")
    if score_final_monoculture:
        warnings.append("score_final_monoculture")
    if score_pattern_monoculture:
        warnings.append("score_pattern_monoculture")
    if momentum_monoculture:
        warnings.append("momentum_monoculture")
    if score_monoculture:
        warnings.append("score_monoculture")

    return {
        "rows": rows_count,
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
        "score_final_monoculture": score_final_monoculture,
        "score_pattern_monoculture": score_pattern_monoculture,
        "score_monoculture": score_monoculture,
        "hard_fail_reasons": hard_fail_reasons,
        "soft_fail_reasons": warnings,
        "soft_fail": bool(warnings),
        "ok": len(hard_fail_reasons) == 0,
        "rows_data": normalized_rows,
    }


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
    return _dominant_ratio(series) >= threshold


def dominant_value_ratio(series: Any) -> float:
    return _dominant_ratio(series)


def verify_final30_scored_rows(
    rows: list[dict[str, Any]] | pd.DataFrame,
    *,
    required_rows: int = 30,
    required_fields: Iterable[str] | None = None,
    source: str = "unknown",
) -> dict[str, Any]:
    quality = evaluate_final30_quality(rows, required_rows=required_rows)
    normalized_rows = list(quality.get("rows_data") or [])
    all_columns = sorted({str(key) for row in normalized_rows for key in row.keys()})
    required = [str(field) for field in (required_fields or [])]
    missing_fields = [field for field in required if field not in all_columns]

    errors: list[str] = []
    warnings: list[str] = list(quality.get("soft_fail_reasons") or [])
    if int(quality.get("rows") or 0) != int(required_rows):
        errors.append("rows_not_exact")
    if int(quality.get("uniq_codes") or 0) != int(required_rows):
        errors.append("uniq_codes_not_exact")
    if missing_fields:
        errors.append("missing_required_fields")

    min_valid_ma20_ratio = _env_float("PB1_FINAL30_MIN_VALID_MA20_RATIO", 1.0)
    min_valid_atr_ratio = _env_float("PB1_FINAL30_MIN_VALID_ATR_RATIO", 1.0)
    require_valid_ma20 = _env_bool("PB1_FINAL30_REQUIRE_VALID_MA20", True)
    require_valid_atr = _env_bool("PB1_FINAL30_REQUIRE_VALID_ATR", True)
    fail_on_momentum_monoculture = _env_bool("PB1_FINAL30_FAIL_ON_MOMENTUM_MONOCULTURE", True)
    fail_on_score_monoculture = _env_bool("PB1_FINAL30_FAIL_ON_SCORE_MONOCULTURE", True)

    if require_valid_ma20 and float(quality.get("valid_ma20_ratio", 0.0)) < min_valid_ma20_ratio:
        errors.append("ma20_invalid_rows")
    if require_valid_atr and float(quality.get("valid_atr_ratio", 0.0)) < min_valid_atr_ratio:
        errors.append("atr_pct_invalid_rows")
    if float(quality.get("valid_score_final_ratio", 0.0)) < 0.95:
        errors.append("score_final_invalid_rows")
    if float(quality.get("breakout_nonnull_ratio", 0.0)) < 0.95:
        errors.append("breakout_score_invalid_rows")
    if float(quality.get("pullback_nonnull_ratio", 0.0)) < 0.95:
        errors.append("pullback_score_invalid_rows")
    if float(quality.get("momentum_nonnull_ratio", 0.0)) < 0.95:
        errors.append("momentum_score_invalid_rows")
    if fail_on_momentum_monoculture and bool(quality.get("momentum_monoculture")):
        errors.append("momentum_monoculture")
    if fail_on_score_monoculture and bool(quality.get("score_monoculture")):
        errors.append("score_monoculture")

    invalid_details: dict[str, list[str]] = {}
    invalid_sample_codes: list[str] = []
    invalid_row_count = 0
    for row in normalized_rows:
        code = _pick_string(row.get("code")) or ""
        invalid_fields = _invalid_fields_for_row(row)
        if invalid_fields:
            invalid_row_count += 1
            if code and code not in invalid_details:
                invalid_details[code] = invalid_fields
                if len(invalid_sample_codes) < 10:
                    invalid_sample_codes.append(code)

    ok = len(errors) == 0
    return {
        **quality,
        "source": source,
        "columns": all_columns,
        "required_fields": required,
        "missing_fields": missing_fields,
        "required_cols_ok": not missing_fields,
        "rows_ok": int(quality.get("rows") or 0) == int(required_rows),
        "errors": list(dict.fromkeys(errors)),
        "warnings": list(dict.fromkeys(warnings)),
        "invalid_row_count": invalid_row_count,
        "invalid_details": invalid_details,
        "invalid_sample_codes": invalid_sample_codes,
        "ok": ok,
    }


def format_final30_abort_message(result: dict[str, Any]) -> str:
    details = dict(result.get("invalid_details") or {})
    return (
        "[TRADE][ABORT][FINAL30_INVALID] "
        f"source={result.get('source', 'unknown')} "
        f"rows={int(result.get('rows') or 0)} "
        f"invalid_rows={int(result.get('invalid_row_count') or 0)} "
        f"error_codes={list(result.get('invalid_sample_codes') or [])} "
        f"details={details} "
        f"errors={list(result.get('errors') or [])}"
    )


def validate_trade_ready(final30_df: pd.DataFrame) -> None:
    result = verify_final30_scored_rows(
        final30_df,
        required_rows=30,
        required_fields=("ma20", "atr_pct", *ENTRY_CRITICAL_COLUMNS, "score_final", "code"),
        source="dataframe",
    )
    if not bool(result.get("ok")):
        raise RuntimeError(format_final30_abort_message(result))


def summarize_final30_quality(df: pd.DataFrame, *, required_rows: int = 30) -> dict[str, Any]:
    return evaluate_final30_quality(df, required_rows=required_rows)