"""PB1 Watchlist Builder - 120 -> 50 -> 30 unified pipeline."""
from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import Engine

from trader.config import RS_BENCHMARK_KOSPI, RS_BENCHMARK_KOSDAQ, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, RS_MIN_PCTILE
from trader.constants import (
    CRITICAL_SCORED_COLS,
    FINAL30_SCORED_IDENTITY_COLS,
    FINAL30_SCORED_PERSIST_COLS,
    REQUIRED_FINAL30_SCORED_COLS,
)
from trader.db.repos import (
    DerivedFlowRepo,
    DerivedMinerviniRepo,
    WatchlistRepo,
)
from trader.flow_score import calculate_final_score, calculate_flow_score
from trader.utils.json_sanitize import to_jsonable
from trader.final30_quality import (
    FINAL30_PRESERVE_FIELDS,
    dominant_value_ratio,
    is_placeholder_entry_value,
    is_valid_positive_numeric,
    normalize_entry_input_value,
    normalize_final30_contract_row,
    score_distribution_is_monoculture,
    summarize_final30_quality,
    verify_final30_scored_rows,
)
from trader.factors.multifactor import (
    compute_ai_rs_scores,
    compute_liquidity_score,
    compute_rs_features,
    compute_volatility_score,
    optimize_meta_k,
)
from trader.indicators import compute_atr_pct_from_ohlcv, compute_ma20_from_ohlcv, safe_nullable_float
from trader.kr.regime import normalize_kr_market
from trader.score_columns import resolve_score_column
from trader.time_coerce import to_date
from trader.watchlist_entry_style import (
    infer_entry_style_from_scores as infer_entry_style_from_scores_impl,
    normalize_entry_style_value as normalize_entry_style_value_impl,
)
from trader.watchlist_column_utils import (
    MA20_NORMALIZE_PRIORITY,
    is_ma20_candidate_column as is_ma20_candidate_column_impl,
    ma20_candidate_priority as ma20_candidate_priority_impl,
    normalize_column_token as normalize_column_token_impl,
)
from trader.watchlist_ohlcv_utils import normalize_ohlcv_columns as normalize_ohlcv_columns_impl
from trader.watchlist_short_feature_utils import (
    short_feature_null_count as short_feature_null_count_impl,
    short_feature_sample_rows as short_feature_sample_rows_impl,
    should_backfill_short_horizon_features as should_backfill_short_horizon_features_impl,
)

logger = logging.getLogger(__name__)

FlowProvider = Callable[[str, date, int], Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Dict[str, Any]]]


def _is_transient_db_exception(exc: BaseException) -> bool:
    text = f"{type(exc).__module__}.{type(exc).__name__}: {exc}".lower()
    return any(
        needle in text
        for needle in (
            "edbhandlerexited",
            "connection to database closed",
            "rollback failure",
            "sqlalchemy.exc.internalerror",
            "psycopg.errors.internalerror",
            "internalerror_",
        )
    )


@dataclass
class WatchlistBundle:
    """
    Watchlist pipeline intermediate results bundle.
    
    All stages must have valid data (non-empty) for bundle to be considered complete.
    This ensures cache hit only when all stages are available.
    """
    as_of: str
    env: str
    strategy: str
    universe_scored: List[Dict[str, Any]]  # rows > 0 required
    pool120: List[Dict[str, Any]]          # rows == 120 required
    top50: List[Dict[str, Any]]            # rows == 50 required
    final30: List[Dict[str, Any]]          # rows == 30 required
    meta: Dict[str, Any]
    
    def is_complete(self, *, min_pool: int = 40, exact_top50: int = 40, exact_final30: int = 30) -> bool:
        """
        Check if bundle meets minimum requirements.
        
        Returns:
            True if all stages have sufficient data
        """
        if not self.universe_scored or len(self.universe_scored) == 0:
            return False
        if not self.pool120 or len(self.pool120) < min_pool:
            return False
        if not self.top50 or len(self.top50) < exact_top50:
            return False
        if not self.final30 or len(self.final30) < exact_final30:
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility."""
        return {
            "as_of": self.as_of,
            "env": self.env,
            "strategy": self.strategy,
            "universe_scored": self.universe_scored,
            "pool120": self.pool120,
            "top50": self.top50,
            "final30": self.final30,
            **self.meta,
        }


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    val = os.getenv(key, str(default)).lower()
    return val in ("1", "true", "yes", "on")


def _build_final30_saved_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    raw_keys = set()
    for row in rows or []:
        raw_keys.update((row or {}).keys())
    if rows and any(col not in raw_keys for col in CRITICAL_SCORED_COLS):
        missing = [col for col in CRITICAL_SCORED_COLS if col not in raw_keys]
        logger.warning("[WATCHLIST][FINAL30_SCORED][MISSING_CRITICAL] missing_cols=%s", missing)

    saved_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows or [], start=1):
        item = _sanitize_scored_item(dict(row or {}), source="final30_saved_rows")
        for col in FINAL30_SCORED_PERSIST_COLS:
            item.setdefault(col, None)
        score = item.get("score")
        if score is None:
            score = item.get("score_final")
        if score is None:
            score = item.get("final_score")
        try:
            score_val = float(score or 0.0)
        except Exception:
            score_val = 0.0
        normalized = {col: item.get(col) for col in FINAL30_SCORED_PERSIST_COLS}
        normalized["code"] = str(item.get("code") or "").zfill(6)
        normalized["rank"] = int(item.get("rank") or item.get("rank_final30") or idx)
        for field in FINAL30_CANONICAL_NUMERIC_FIELDS:
            normalized[field] = _canonicalize_numeric_field(field, item, score_fallback=score_val)
        normalized["score_final"] = normalized.get("score_final")
        normalized["final_score"] = normalized.get("score_final")
        normalized["score"] = normalized.get("score_final") if normalized.get("score_final") is not None else score_val
        for field in FLOW_PROVENANCE_FIELDS:
            normalized[field] = item.get(field)
        normalized["meta"] = item.get("meta", {})
        saved_rows.append({**normalized, "rank": normalized["rank"], "score": normalized["score"]})
    return saved_rows


def _log_final30_scored_rows(prefix: str, rows: List[Dict[str, Any]]) -> None:
    df = pd.DataFrame(rows or [])
    logger.info(
        "%s rows=%s has_tech_score=%s has_score_final=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s",
        prefix,
        int(len(df)),
        int(resolve_score_column(df, "tech") is not None),
        int(resolve_score_column(df, "final") is not None),
        int(resolve_score_column(df, "breakout") is not None),
        int(resolve_score_column(df, "pullback") is not None),
        int(resolve_score_column(df, "momentum") is not None),
    )


def _log_final30_scored_df_ready(final30_scored_df: pd.DataFrame) -> None:
    logger.info(
        "[WATCHLIST][FINAL30_SCORED][READY] rows=%s cols=%s has_tech=%s has_final=%s has_breakout=%s has_pullback=%s has_momentum=%s",
        len(final30_scored_df),
        list(final30_scored_df.columns),
        "tech_score" in final30_scored_df.columns,
        ("score_final" in final30_scored_df.columns) or ("final_score" in final30_scored_df.columns),
        "breakout_score" in final30_scored_df.columns,
        "pullback_score" in final30_scored_df.columns,
        "momentum_score" in final30_scored_df.columns,
    )


def _as_dataframe(value: Any) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, tuple):
        return pd.DataFrame(list(value))
    if isinstance(value, dict):
        return pd.DataFrame([value])
    try:
        return pd.DataFrame(value)
    except Exception:
        return pd.DataFrame()


def _normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    return normalize_ohlcv_columns_impl(df)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                return default
        return float(v)
    except Exception:
        return default


def _safe_nullable_float(v: Any) -> float | None:
    return safe_nullable_float(v)


def _sanitize_entry_input(v: Any) -> float | None:
    return normalize_entry_input_value(v)


def _prefer_valid_numeric(body_value: Any, meta_value: Any, *, zero_invalid: bool = False) -> float | None:
    body = _safe_nullable_float(body_value)
    meta = _safe_nullable_float(meta_value)
    if body is not None and (not zero_invalid or body > 0):
        return body
    if meta is not None and (not zero_invalid or meta > 0):
        return meta
    return body if body is not None else meta


INVALID_ZERO_NUMERIC_FIELDS = ("close", "ma20", "ma50", "ma150", "atr_pct")
ZERO_INVALID_ALWAYS_FIELDS = {"close", "ma20", "ma50", "ma150"}
NULL_ALLOWED_CONTRACT_WARNING_FIELDS = {"close", "ma20", "ma50", "ma150", "atr_pct", "rs_percentile"}
NONNULL_SCORE_FIELDS = {"breakout_score", "pullback_score", "momentum_score"}

# ── entry style 허용값 상수 ─────────────────────────────────────────────────────
ALLOWED_ENTRY_STYLES = {"BREAKOUT", "PULLBACK", "MOMENTUM"}
ENTRY_STYLE_SCORE_KEYS = {
    "BREAKOUT": "breakout_score",
    "PULLBACK": "pullback_score",
    "MOMENTUM": "momentum_score",
}
FINAL30_CANONICAL_NUMERIC_FIELDS = (
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
)
FINAL30_SAVE_REQUIRED_POSITIVE_FIELDS = {"close", "ma50", "ma150"}
FLOW_PROVENANCE_FIELDS = (
    "flow_data_available",
    "flow_provider_used",
    "flow_fail_reason",
    "flow_score_imputed",
    "foreign_flow_missing",
    "inst_flow_missing",
)
MA20_ALIAS_FIELDS = ("ma20", "ma_20", "sma20", "close_ma20", "moving_avg20", "avg20", "ma20_price")
MA20_NORMALIZE_PRIORITY = (
    "ma20",
    "ma_20",
    "sma20",
    "close_ma20",
    "moving_avg20",
    "avg20",
    "ma20_price",
)
FINAL30_CONTRACT_REQUIRED_FIELDS = tuple(
    dict.fromkeys(
        (
            "code",
            *FINAL30_PRESERVE_FIELDS,
            "breakout_score",
            "pullback_score",
            "momentum_score",
            "tech_score",
            "score_final",
            "entry_style_selected",
        )
    )
)


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _prefer_numeric_candidates(values: List[Any], *, zero_invalid: bool = False) -> float | None:
    fallback: float | None = None
    for value in values:
        numeric = _safe_nullable_float(value)
        if numeric is None:
            continue
        if not zero_invalid or numeric > 0:
            return numeric
        if fallback is None:
            fallback = numeric
    return fallback


def _normalize_column_token(value: Any) -> str:
    return normalize_column_token_impl(value)


def _ma20_candidate_priority(column: Any) -> int:
    return ma20_candidate_priority_impl(column)


def _is_ma20_candidate_column(column: Any) -> bool:
    return is_ma20_candidate_column_impl(column)


def _build_numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _ma20_candidate_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    stats: List[Dict[str, Any]] = []
    for column in df.columns:
        if not _is_ma20_candidate_column(column):
            continue
        numeric_series = _build_numeric_series(df, str(column))
        stats.append(
            {
                "column": str(column),
                "nonnull": int(numeric_series.notna().sum()),
                "priority": _ma20_candidate_priority(column),
            }
        )

    stats.sort(key=lambda item: (-int(item["nonnull"]), int(item["priority"]), str(item["column"])))
    return stats


def _ma20_sample_rows(df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    sample_columns = [column for column in ("code", "ma20", "ma50", "close") if column in df.columns]
    if not sample_columns:
        return []
    return df.loc[:, sample_columns].head(limit).to_dict(orient="records")


def log_final30_ma_diagnostics(df: pd.DataFrame, stage: str) -> None:
    if df is None:
        logger.info("[DEBUG][MA20_DIAG] stage=%s rows=0 cols=0 has_ma20=0 ma20_null=-1 ma50_null=-1 ma150_null=-1 close_null=-1 volume_avg20_null=-1", stage)
        logger.info("[DEBUG][MA20_SOURCE_CANDIDATES] stage=%s candidates=[] sample=[]", stage)
        return

    candidate_stats = _ma20_candidate_stats(df)
    ma20_null = int(_build_numeric_series(df, "ma20").isna().sum()) if "ma20" in df.columns else -1
    ma50_null = int(_build_numeric_series(df, "ma50").isna().sum()) if "ma50" in df.columns else -1
    ma150_null = int(_build_numeric_series(df, "ma150").isna().sum()) if "ma150" in df.columns else -1
    close_null = int(_build_numeric_series(df, "close").isna().sum()) if "close" in df.columns else -1
    volume_avg20_null = int(_build_numeric_series(df, "volume_avg20").isna().sum()) if "volume_avg20" in df.columns else -1
    logger.info(
        "[DEBUG][MA20_DIAG] stage=%s rows=%s cols=%s has_ma20=%s ma20_null=%s ma50_null=%s ma150_null=%s close_null=%s volume_avg20_null=%s",
        stage,
        len(df),
        len(df.columns),
        int("ma20" in df.columns),
        ma20_null,
        ma50_null,
        ma150_null,
        close_null,
        volume_avg20_null,
    )
    logger.info(
        "[DEBUG][MA20_SOURCE_CANDIDATES] stage=%s candidates=%s sample=%s",
        stage,
        candidate_stats,
        _ma20_sample_rows(df),
    )


def _short_feature_sample_rows(df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    return short_feature_sample_rows_impl(df, limit=limit)


def _short_feature_null_count(df: pd.DataFrame, column: str) -> int:
    return short_feature_null_count_impl(df, column)


def log_short_horizon_feature_diag(df: pd.DataFrame, stage: str) -> None:
    if df is None:
        logger.info(
            "[DEBUG][SHORT_FEATURE_DIAG] stage=%s rows=0 has_ma20=0 ma20_null=-1 has_volume_avg20=0 volume_avg20_null=-1 has_ma50=0 ma50_null=-1 has_close=0 close_null=-1 sample=[]",
            stage,
        )
        return

    logger.info(
        "[DEBUG][SHORT_FEATURE_DIAG] stage=%s rows=%s has_ma20=%s ma20_null=%s has_volume_avg20=%s volume_avg20_null=%s has_ma50=%s ma50_null=%s has_close=%s close_null=%s sample=%s",
        stage,
        len(df),
        int("ma20" in df.columns),
        _short_feature_null_count(df, "ma20"),
        int("volume_avg20" in df.columns),
        _short_feature_null_count(df, "volume_avg20"),
        int("ma50" in df.columns),
        _short_feature_null_count(df, "ma50"),
        int("close" in df.columns),
        _short_feature_null_count(df, "close"),
        _short_feature_sample_rows(df),
    )


def _should_backfill_short_horizon_features(df: pd.DataFrame, *, require_all_null: bool) -> bool:
    return should_backfill_short_horizon_features_impl(df, require_all_null=require_all_null)


def load_recent_ohlcv_for_codes(
    codes: List[str],
    *,
    as_of: date,
    ohlcv_provider: Any,
    min_days: int = 20,
) -> Dict[str, pd.DataFrame]:
    history_map: Dict[str, pd.DataFrame] = {}
    fetch_days = max(int(min_days), 30)
    for raw_code in codes:
        code = str(raw_code or "").zfill(6)
        if not code:
            continue
        frame = _fetch_ohlcv_frame(ohlcv_provider, code, days=fetch_days)
        history_map[code] = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    return history_map


def backfill_short_horizon_features(
    df: pd.DataFrame,
    as_of: date,
    *,
    ohlcv_provider: Any,
    stage: str,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    result = df.copy()
    if "ma20" not in result.columns:
        result["ma20"] = pd.Series(index=result.index, dtype="float64")
    if "volume_avg20" not in result.columns:
        result["volume_avg20"] = pd.Series(index=result.index, dtype="float64")

    ma20_series = _build_numeric_series(result, "ma20")
    vol20_series = _build_numeric_series(result, "volume_avg20")
    target_mask = ma20_series.isna() | vol20_series.isna()
    ma20_null = int(ma20_series.isna().sum())
    vol20_null = int(vol20_series.isna().sum())

    logger.info(
        "[SHORT_FEATURE][BACKFILL][START] stage=%s rows=%s ma20_null=%s volume_avg20_null=%s",
        stage,
        len(result),
        ma20_null,
        vol20_null,
    )

    codes = result.loc[target_mask, "code"].dropna().astype(str).str.zfill(6).unique().tolist() if "code" in result.columns else []
    if not codes:
        logger.info(
            "[SHORT_FEATURE][BACKFILL][DONE] stage=%s filled_ma20=%s filled_volume_avg20=%s remaining_ma20_null=%s remaining_volume_avg20_null=%s",
            stage,
            0,
            0,
            ma20_null,
            vol20_null,
        )
        return result

    hist_map = load_recent_ohlcv_for_codes(codes, as_of=as_of, ohlcv_provider=ohlcv_provider, min_days=20)

    filled_ma20 = 0
    filled_vol20 = 0
    for idx in result.index[target_mask]:
        code = str(result.at[idx, "code"] if "code" in result.columns else "").zfill(6)
        hist = hist_map.get(code)
        if hist is None or hist.empty:
            logger.warning("[SHORT_FEATURE][BACKFILL][MISS] code=%s reason=history_unavailable", code)
            continue

        hist = _normalize_ohlcv_columns(hist)
        if hist.empty or "close" not in hist.columns or "volume" not in hist.columns:
            logger.warning("[SHORT_FEATURE][BACKFILL][MISS] code=%s reason=missing_close_or_volume", code)
            continue

        hist = hist.sort_values("date") if "date" in hist.columns else hist
        close_tail = pd.to_numeric(hist["close"], errors="coerce").tail(20)
        volume_tail = pd.to_numeric(hist["volume"], errors="coerce").tail(20)
        if len(close_tail) < 20 or len(volume_tail) < 20 or int(close_tail.notna().sum()) < 20 or int(volume_tail.notna().sum()) < 20:
            logger.warning("[SHORT_FEATURE][BACKFILL][MISS] code=%s reason=insufficient_history bars=%s", code, len(hist))
            continue

        ma20_value = float(close_tail.mean())
        vol20_value = float(volume_tail.mean())

        if pd.isna(result.at[idx, "ma20"]):
            result.at[idx, "ma20"] = ma20_value
            filled_ma20 += 1
        if pd.isna(result.at[idx, "volume_avg20"]):
            result.at[idx, "volume_avg20"] = vol20_value
            filled_vol20 += 1

        meta = result.at[idx, "meta"] if "meta" in result.columns else None
        if isinstance(meta, dict):
            if meta.get("ma20") is None and pd.notna(result.at[idx, "ma20"]):
                meta["ma20"] = float(result.at[idx, "ma20"])
            if meta.get("volume_avg20") is None and pd.notna(result.at[idx, "volume_avg20"]):
                meta["volume_avg20"] = float(result.at[idx, "volume_avg20"])
            result.at[idx, "meta"] = meta

    remaining_ma20_null = int(_build_numeric_series(result, "ma20").isna().sum())
    remaining_vol20_null = int(_build_numeric_series(result, "volume_avg20").isna().sum())
    logger.info(
        "[SHORT_FEATURE][BACKFILL][DONE] stage=%s filled_ma20=%s filled_volume_avg20=%s remaining_ma20_null=%s remaining_volume_avg20_null=%s",
        stage,
        filled_ma20,
        filled_vol20,
        remaining_ma20_null,
        remaining_vol20_null,
    )
    return result


def normalize_ma20_column(df: pd.DataFrame, stage: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    normalized = df.copy()
    candidate_stats = _ma20_candidate_stats(normalized)
    existing_series = _build_numeric_series(normalized, "ma20") if "ma20" in normalized.columns else pd.Series(index=normalized.index, dtype="float64")
    existing_nonnull = int(existing_series.notna().sum())
    if "ma20" in normalized.columns:
        normalized["ma20"] = existing_series

    fill_candidates = [item for item in candidate_stats if item["column"] != "ma20" and int(item["nonnull"]) > 0]
    chosen_column = fill_candidates[0]["column"] if fill_candidates else None
    chosen_nonnull = int(fill_candidates[0]["nonnull"]) if fill_candidates else 0

    if existing_nonnull == 0 and chosen_column is not None and chosen_nonnull > 0:
        normalized["ma20"] = _build_numeric_series(normalized, chosen_column)
        logger.warning(
            "[MA20][RECOVER] stage=%s chosen=%s nonnull=%s rows=%s reason=ma20_all_null",
            stage,
            chosen_column,
            chosen_nonnull,
            len(normalized),
        )
    elif existing_nonnull > 0:
        total_filled = 0
        for candidate in fill_candidates:
            fill_mask = normalized["ma20"].isna()
            if not bool(fill_mask.any()):
                break
            candidate_series = _build_numeric_series(normalized, str(candidate["column"]))
            before_null = int(fill_mask.sum())
            normalized.loc[fill_mask, "ma20"] = candidate_series.loc[fill_mask]
            after_null = int(normalized["ma20"].isna().sum())
            filled_count = before_null - after_null
            if filled_count > 0:
                total_filled += filled_count
                logger.warning(
                    "[MA20][RECOVER_FILL] stage=%s chosen=%s filled=%s remaining_null=%s rows=%s",
                    stage,
                    candidate["column"],
                    filled_count,
                    after_null,
                    len(normalized),
                )
        if total_filled == 0:
            logger.info(
                "[MA20][NO_RECOVER] stage=%s has_ma20=1 ma20_nonnull=%s candidates=%s",
                stage,
                existing_nonnull,
                candidate_stats,
            )
    else:
        logger.info(
            "[MA20][NO_RECOVER] stage=%s has_ma20=%s ma20_nonnull=%s candidates=%s",
            stage,
            int("ma20" in normalized.columns),
            existing_nonnull,
            candidate_stats,
        )

    final_nonnull = int(_build_numeric_series(normalized, "ma20").notna().sum()) if "ma20" in normalized.columns else 0
    if final_nonnull == 0:
        logger.warning(
            "[MA20][ALL_NULL] stage=%s rows=%s candidates=%s",
            stage,
            len(normalized),
            candidate_stats,
        )
    return normalized


def _extract_numeric_from_sources(*sources: Any, aliases: tuple[str, ...], zero_invalid: bool = False) -> float | None:
    candidates: list[Any] = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        for alias in aliases:
            if alias in source:
                candidates.append(source.get(alias))
    return _prefer_numeric_candidates(candidates, zero_invalid=zero_invalid)


def _fetch_ohlcv_frame(ohlcv_provider: Any, code: str, *, days: int = 30) -> pd.DataFrame:
    if ohlcv_provider is None:
        return pd.DataFrame()
    try:
        if hasattr(ohlcv_provider, "get_ohlcv"):
            result = ohlcv_provider.get_ohlcv(
                str(code).zfill(6),
                days,
                purpose="final30_contract_repair",
                usage_context="watchlist",
            )
            frame = result.df if hasattr(result, "df") else result
        elif hasattr(ohlcv_provider, "fetch_daily"):
            frame = ohlcv_provider.fetch_daily(str(code).zfill(6), count=days)
        elif callable(ohlcv_provider):
            try:
                result = ohlcv_provider(str(code).zfill(6), count=days)
            except TypeError:
                result = ohlcv_provider(str(code).zfill(6), days)
            frame = result[0] if isinstance(result, tuple) else result
        else:
            frame = pd.DataFrame()
    except Exception as exc:
        logger.warning("[WATCHLIST][FINAL30][OHLCV_REPAIR_FAIL] code=%s err=%s", str(code).zfill(6), exc)
        return pd.DataFrame()

    if not isinstance(frame, pd.DataFrame):
        return pd.DataFrame()
    normalized = _normalize_ohlcv_columns(frame)
    return normalized.tail(days).copy() if not normalized.empty else normalized


# ── entry style sanitize helpers ───────────────────────────────────────────────

def _to_float_safe(value: Any, default: float = 0.0) -> float:
    """None/빈값/비숫자를 default로 변환하는 안전한 float 변환."""
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except Exception:
        return default


def _normalize_entry_style_value(value: Any) -> str:
    """raw entry_style_selected 값을 BREAKOUT/PULLBACK/MOMENTUM 중 하나로 정규화."""
    return normalize_entry_style_value_impl(value)


def _infer_entry_style_from_scores(row: dict) -> str:
    """breakout_score/pullback_score/momentum_score 중 최대값으로 entry_style을 추론."""
    return infer_entry_style_from_scores_impl(row)


def sanitize_final30_entry_styles(rows: List[Dict[str, Any]], *, stage: str, hard: bool = False) -> List[Dict[str, Any]]:
    """
    final30 contract 직전 entry_style_selected를 강제 보정한다.

    목적:
    - final30 30개가 이미 생성된 상태에서 entry_style_selected 1개 invalid 때문에 PREP 전체가 죽는 것을 방지
    - trade-am strict loader가 요구하는 breakout/pullback/momentum score 및 entry_style_selected를 항상 보장
    """
    fixed: List[Dict[str, Any]] = []
    invalid_before = 0
    invalid_after = 0
    null_before = 0
    sample_fixed: List[dict] = []

    for src in rows or []:
        row = dict(src or {})

        # score 필드는 반드시 numeric으로 보정
        for key in ("breakout_score", "pullback_score", "momentum_score"):
            row[key] = _to_float_safe(row.get(key), 0.0)

        raw_style = row.get("entry_style_selected")
        norm_style = _normalize_entry_style_value(raw_style)

        if raw_style is None or str(raw_style).strip() == "":
            null_before += 1

        if norm_style not in ALLOWED_ENTRY_STYLES:
            invalid_before += 1
            inferred = _infer_entry_style_from_scores(row)
            row["entry_style_selected"] = inferred
            row["entry_component"] = inferred.lower()
            if len(sample_fixed) < 10:
                sample_fixed.append({
                    "code": row.get("code"),
                    "raw": raw_style,
                    "fixed": inferred,
                    "breakout_score": row.get("breakout_score"),
                    "pullback_score": row.get("pullback_score"),
                    "momentum_score": row.get("momentum_score"),
                })
        else:
            row["entry_style_selected"] = norm_style
            row["entry_component"] = norm_style.lower()

        # pass flag 보정
        row["breakout_pass"] = bool(_to_float_safe(row.get("breakout_score")) > 0)
        row["pullback_pass"] = bool(_to_float_safe(row.get("pullback_score")) > 0)
        row["momentum_pass"] = bool(_to_float_safe(row.get("momentum_score")) > 0)

        # meta 내부에도 동일 필드 복사
        meta = dict(row.get("meta") or {})
        meta["breakout_score"] = row["breakout_score"]
        meta["pullback_score"] = row["pullback_score"]
        meta["momentum_score"] = row["momentum_score"]
        meta["entry_style_selected"] = row["entry_style_selected"]
        meta["entry_component"] = row["entry_component"]
        meta["breakout_pass"] = row["breakout_pass"]
        meta["pullback_pass"] = row["pullback_pass"]
        meta["momentum_pass"] = row["momentum_pass"]
        row["meta"] = meta

        if row["entry_style_selected"] not in ALLOWED_ENTRY_STYLES:
            invalid_after += 1

        fixed.append(row)

    logger.info(
        "[ENTRY_STYLE][SANITIZE] stage=%s rows=%s null_before=%s invalid_before=%s fixed=%s invalid_after=%s sample_fixed=%s",
        stage,
        len(fixed),
        null_before,
        invalid_before,
        invalid_before,
        invalid_after,
        sample_fixed,
    )

    if hard and invalid_after > 0:
        raise ValueError(f"ENTRY_STYLE_SANITIZE_FAILED stage={stage} invalid_after={invalid_after}")

    return fixed


def _canonicalize_numeric_field(field: str, row: Dict[str, Any], *, score_fallback: float | None = None) -> float | None:
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    scores = row.get("scores") if isinstance(row.get("scores"), dict) else {}
    alias_map: dict[str, tuple[str, ...]] = {
        "close": ("close", "last_close", "close_price"),
        "ma20": MA20_ALIAS_FIELDS,
        "ma50": ("ma50", "ma_50", "sma50", "ma50_price"),
        "ma150": ("ma150", "ma_150", "sma150", "ma150_price"),
        "atr_pct": ("atr_pct", "atr", "atrp", "atr_percent"),
        "rs_percentile": ("rs_percentile", "rs_pctile", "rs_score"),
        "breakout_score": ("breakout_score", "score_breakout"),
        "pullback_score": ("pullback_score", "score_pullback"),
        "momentum_score": ("momentum_score", "score_momentum"),
        "tech_score": ("tech_score", "score_tech"),
        "score_final": ("score_final", "final_score", "score"),
    }
    value = _extract_numeric_from_sources(
        row,
        meta,
        scores,
        aliases=alias_map.get(field, (field,)),
        zero_invalid=field in ZERO_INVALID_ALWAYS_FIELDS,
    )
    if field == "score_final" and value is None:
        value = score_fallback
    if value is None:
        return None
    numeric = _safe_nullable_float(value)
    if numeric is None or not math.isfinite(float(numeric)):
        return None
    return float(numeric)


def _summarize_final30_quality_snapshot(rows: List[Dict[str, Any]], *, repaired_ma20_count: int = 0) -> Dict[str, Any]:
    quality = evaluate_final30_quality(rows)
    quality["null_ma20_count"] = sum(1 for row in (rows or []) if not is_valid_positive_numeric((row or {}).get("ma20")))
    quality["repaired_ma20_count"] = int(repaired_ma20_count)
    return quality


def _log_final30_quality_snapshot(prefix: str, rows: List[Dict[str, Any]], *, repaired_ma20_count: int = 0) -> None:
    quality = _summarize_final30_quality_snapshot(rows, repaired_ma20_count=repaired_ma20_count)
    logger.info(
        "%s rows=%s uniq_codes=%s valid_ma20_ratio=%.3f valid_atr_ratio=%.3f breakout_nonnull_ratio=%.3f pullback_nonnull_ratio=%.3f momentum_nonnull_ratio=%.3f valid_score_final_ratio=%.3f null_ma20_count=%s repaired_ma20_count=%s ok=%s",
        prefix,
        quality.get("rows", 0),
        quality.get("uniq_codes", 0),
        float(quality.get("valid_ma20_ratio", 0.0)),
        float(quality.get("valid_atr_ratio", 0.0)),
        float(quality.get("breakout_nonnull_ratio", 0.0)),
        float(quality.get("pullback_nonnull_ratio", 0.0)),
        float(quality.get("momentum_nonnull_ratio", 0.0)),
        float(quality.get("valid_score_final_ratio", 0.0)),
        quality.get("null_ma20_count", 0),
        quality.get("repaired_ma20_count", 0),
        int(bool(quality.get("ok"))),
    )


def _prepare_final30_scored_rows_for_save(
    rows: List[Dict[str, Any]],
    *,
    engine: Engine,
    env: str,
    as_of: date,
    broader_rows: List[Dict[str, Any]] | None,
    ohlcv_provider: Any,
    source: str,
) -> tuple[List[Dict[str, Any]], int, int]:
    normalized_rows = [_sync_item_and_meta_fields(dict(row or {})) for row in (rows or [])]
    broader_by_code = {
        str((row or {}).get("code") or "").zfill(6): _sync_item_and_meta_fields(dict(row or {}))
        for row in (broader_rows or [])
        if (row or {}).get("code")
    }
    symbols = [str((row or {}).get("code") or "").zfill(6) for row in normalized_rows if (row or {}).get("code")]
    derived_rows = DerivedMinerviniRepo(engine).load_for_as_of(env=env, as_of=as_of, symbols=symbols) if symbols else []
    derived_by_code = {str((row or {}).get("symbol") or "").zfill(6): dict(row or {}) for row in derived_rows}

    prepared: List[Dict[str, Any]] = []
    repaired_ma20_count = 0
    missing_ma20_count = 0
    for idx, raw in enumerate(normalized_rows, start=1):
        code = str((raw or {}).get("code") or "").zfill(6)
        ref = broader_by_code.get(code, {})
        item = _sanitize_scored_item(dict(raw or {}), ref, source=source)
        item["code"] = code
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        scores = item.get("scores") if isinstance(item.get("scores"), dict) else {}
        derived_row = derived_by_code.get(code, {})
        derived_features = derived_row.get("features_json") if isinstance(derived_row.get("features_json"), dict) else {}

        ma20_before = _canonicalize_numeric_field("ma20", item)
        repaired_ma20 = ma20_before
        if not is_valid_positive_numeric(repaired_ma20):
            repaired_ma20 = _extract_numeric_from_sources(
                item,
                meta,
                scores,
                ref,
                ref.get("meta") if isinstance(ref.get("meta"), dict) else {},
                derived_row,
                derived_features,
                aliases=MA20_ALIAS_FIELDS,
                zero_invalid=True,
            )
        if not is_valid_positive_numeric(repaired_ma20):
            ohlcv_df = _fetch_ohlcv_frame(ohlcv_provider, code, days=30)
            if not ohlcv_df.empty:
                repaired_ma20 = compute_ma20_from_ohlcv(ohlcv_df)
        if is_valid_positive_numeric(repaired_ma20):
            if not is_valid_positive_numeric(ma20_before):
                repaired_ma20_count += 1
            item["ma20"] = float(repaired_ma20)
            meta["ma20"] = float(repaired_ma20)
        else:
            missing_ma20_count += 1
            item["ma20"] = None
            meta["ma20"] = None

        score_fallback = _prefer_numeric_candidates([
            item.get("score_final"),
            item.get("final_score"),
            item.get("score"),
            meta.get("score_final"),
            meta.get("final_score"),
            meta.get("score"),
        ])
        for field in FINAL30_CANONICAL_NUMERIC_FIELDS:
            canonical = _canonicalize_numeric_field(field, item, score_fallback=score_fallback)
            if field in FINAL30_SAVE_REQUIRED_POSITIVE_FIELDS and not is_valid_positive_numeric(canonical):
                canonical = None
            if field == "ma20" and is_valid_positive_numeric(item.get("ma20")):
                canonical = float(item.get("ma20"))
            item[field] = canonical
            meta[field] = canonical

        item["final_score"] = item.get("score_final")
        item["score"] = item.get("score_final")
        meta["final_score"] = item.get("score_final")
        meta["score"] = item.get("score_final")
        item["rank"] = _safe_int(item.get("rank") or item.get("rank_final30") or idx, idx)
        item["meta"] = meta
        normalized_item = normalize_final30_contract_row(item)
        for field in FINAL30_SCORED_PERSIST_COLS:
            if field not in normalized_item:
                normalized_item[field] = item.get(field)
        prepared.append(normalized_item)

    logger.info(
        "[WATCHLIST][FINAL30][MA20_REPAIR] rows=%s repaired_ma20_count=%s missing_ma20_count=%s source=%s",
        len(prepared),
        repaired_ma20_count,
        missing_ma20_count,
        source,
    )
    _log_final30_quality_snapshot(
        "[WATCHLIST][FINAL30][QUALITY][PRE_SAVE]",
        prepared,
        repaired_ma20_count=repaired_ma20_count,
    )
    first_row_keys = sorted(prepared[0].keys()) if prepared else []
    missing_required = [
        field for field in (FINAL30_SCORED_IDENTITY_COLS + REQUIRED_FINAL30_SCORED_COLS)
        if field not in first_row_keys
    ]
    logger.info("[WATCHLIST][SAVE_SCORED][SAMPLE_KEYS] first_row_keys=%s", first_row_keys)
    logger.info("[WATCHLIST][SAVE_SCORED][CRITICAL_CHECK] missing=%s", missing_required)
    if missing_required:
        raise RuntimeError(f"FINAL30_SCORED_PREPARE_MISSING_FIELDS:{missing_required}")
    return prepared, repaired_ma20_count, missing_ma20_count


def _append_quality_flag(row: Dict[str, Any], flag: str) -> None:
    flags = list(row.get("quality_flags") or [])
    if flag not in flags:
        flags.append(flag)
    row["quality_flags"] = flags
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    meta["quality_flags"] = flags
    row["meta"] = meta


def evaluate_final30_quality(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return verify_final30_scored_rows(
        rows,
        required_rows=30,
        required_fields=FINAL30_CONTRACT_REQUIRED_FIELDS,
        source="watchlist_builder",
    )


def _is_invalid_zero_field(field: str, value: Any, row: Dict[str, Any]) -> bool:
    if _is_missing_value(value):
        return False
    try:
        numeric = float(value)
    except Exception:
        return False
    if abs(numeric) > 1e-12:
        return False
    if field in ZERO_INVALID_ALWAYS_FIELDS:
        return True
    if field == "atr_pct":
        close_value = row.get("close")
        try:
            return float(close_value) > 0
        except Exception:
            return False
    return False


def _repair_candidate_value(field: str, item: Dict[str, Any], ref: Dict[str, Any]) -> Any:
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    candidate_sources = [
        item,
        meta,
        meta.get("runtime_recompute") if isinstance(meta.get("runtime_recompute"), dict) else {},
        meta.get("derived") if isinstance(meta.get("derived"), dict) else {},
        meta.get("minervini") if isinstance(meta.get("minervini"), dict) else {},
        ref,
    ]
    for source in candidate_sources:
        if not isinstance(source, dict):
            continue
        for key in (f"runtime_{field}", f"recomputed_{field}", f"derived_{field}", field):
            if key not in source:
                continue
            candidate = source.get(key)
            if _is_missing_value(candidate):
                continue
            probe_row = dict(item)
            probe_row[field] = candidate
            if field != "close" and _is_missing_value(probe_row.get("close")) and not _is_missing_value(ref.get("close")):
                probe_row["close"] = ref.get("close")
            if not _is_invalid_zero_field(field, candidate, probe_row):
                return candidate
    return None


def _sanitize_scored_item(item: Dict[str, Any], ref: Dict[str, Any] | None = None, *, source: str) -> Dict[str, Any]:
    sanitized = dict(item or {})
    fallback = dict(ref or {})
    invalid_zero_fields: List[str] = []
    repaired_fields: List[str] = []
    unresolved_fields: List[str] = []

    for field in INVALID_ZERO_NUMERIC_FIELDS:
        value = sanitized.get(field)
        if field == "ma20":
            logger.info(
                "[WATCHLIST][SANITIZE][MA20_BEFORE] code=%s value=%s source=%s",
                str(sanitized.get("code") or "").zfill(6),
                value,
                source,
            )
            if is_valid_positive_numeric(value):
                logger.info(
                    "[WATCHLIST][SANITIZE][MA20_AFTER] code=%s value=%s source=%s repaired=0 preserved=1",
                    str(sanitized.get("code") or "").zfill(6),
                    value,
                    source,
                )
                continue
        if not _is_invalid_zero_field(field, value, sanitized):
            continue
        invalid_zero_fields.append(field)
        repaired = _repair_candidate_value(field, sanitized, fallback)
        if repaired is not None:
            sanitized[field] = repaired
            repaired_fields.append(field)
        else:
            if field == "ma20":
                logger.warning(
                    "[WATCHLIST][SANITIZE][MA20_REPAIR_FAIL] code=%s value=%s source=%s -> null",
                    str(sanitized.get("code") or "").zfill(6),
                    value,
                    source,
                )
            sanitized[field] = None
            unresolved_fields.append(field)
        if field == "ma20":
            logger.info(
                "[WATCHLIST][SANITIZE][MA20_AFTER] code=%s value=%s source=%s repaired=%s preserved=0",
                str(sanitized.get("code") or "").zfill(6),
                sanitized.get("ma20"),
                source,
                int(repaired is not None),
            )

    rs_percentile = sanitized.get("rs_percentile")
    if _is_missing_value(rs_percentile):
        logger.warning(
            "[WATCHLIST][SANITIZE][NULL_WARN] code=%s field=rs_percentile source=%s",
            str(sanitized.get("code") or "").zfill(6),
            source,
        )

    for field in NONNULL_SCORE_FIELDS:
        if _is_missing_value(sanitized.get(field)):
            repaired = _repair_candidate_value(field, sanitized, fallback)
            sanitized[field] = repaired if repaired is not None else None
            logger.warning(
                "[WATCHLIST][SANITIZE][NULL_SCORE] code=%s field=%s source=%s repaired=%s",
                str(sanitized.get("code") or "").zfill(6),
                field,
                source,
                int(repaired is not None),
            )

    if invalid_zero_fields:
        logger.info(
            "[WATCHLIST][SANITIZE][INVALID_ZERO] code=%s fields=%s source=%s repaired=%s unresolved=%s",
            str(sanitized.get("code") or "").zfill(6),
            invalid_zero_fields,
            source,
            repaired_fields,
            unresolved_fields,
        )

    if unresolved_fields:
        sanitized["invalid_zero_fields"] = unresolved_fields

    return sanitized


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _row_set(row: Any, key: str, value: Any) -> None:
    if isinstance(row, dict):
        row[key] = value
    else:
        setattr(row, key, value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def validate_scores(rows: List[Dict[str, Any]], stage_name: str) -> None:
    """
    Validate that rows have non-zero scores.
    
    Args:
        rows: List of scored items
        stage_name: Stage identifier for error messaging
        
    Raises:
        ValueError: If rows are empty or all scores are zero
    """
    if not rows:
        raise ValueError(f"[SCORE_VALIDATION][FAIL] {stage_name}: empty rows")
    
    score_keys = ("score_final", "final_score", "score", "tech_score", "liq_avg")
    nonzero_count = 0
    
    for row in rows:
        for key in score_keys:
            val = row.get(key)
            if val is not None:
                try:
                    if float(val) > 0:
                        nonzero_count += 1
                        break
                except (TypeError, ValueError):
                    continue
    
    if nonzero_count == 0:
        raise ValueError(
            f"[SCORE_VALIDATION][FAIL] {stage_name}: all scores are zero (checked keys: {score_keys})"
        )
    
    logger.info(
        "[SCORE_VALIDATION][OK] %s: total=%s nonzero=%s",
        stage_name,
        len(rows),
        nonzero_count,
    )


def validate_watchlist_contract(
    *,
    universe_scored: List[Dict[str, Any]],
    pool120: List[Dict[str, Any]],
    top50: List[Dict[str, Any]],
    final30: List[Dict[str, Any]],
    min_universe: Optional[int] = None,
    min_pool: Optional[int] = None,
    min_top50: Optional[int] = None,
    exact_final30: Optional[int] = None,
    contract_mode: str = "candidate_pool_based",
    universe_scored_source: str = "unknown",
    candidate_pool_size: Optional[int] = None,
) -> List[str]:
    """
    Validate watchlist bundle contract.
    
    Note: In current architecture, universe_scored is actually candidate_pool-based (~120),
    not broader universe (~196+). Contract threshold min_universe=150 is structurally
    mismatched with this. Consider adjusting threshold or refactoring to use broader universe.
    
    Returns:
        List of contract failure reasons (empty if all validations pass)
    """
    failures = []

    def resolve_contract_thresholds(mode: str) -> dict[str, int | None]:
        normalized_mode = (mode or "candidate_pool_based").strip().lower()
        if normalized_mode == "candidate_pool_based":
            return {
                "min_universe": None,
                "min_pool120": 40,
                "min_top50": 30,
                "exact_final30": 30,
            }
        return {
            "min_universe": 120,
            "min_pool120": 120,
            "exact_top50": 50,
            "exact_final30": 30,
        }

    normalized_mode = (contract_mode or "candidate_pool_based").strip().lower()
    if normalized_mode == "broad_universe_based":
        normalized_mode = "full_universe_based"

    thresholds = resolve_contract_thresholds(normalized_mode)
    if min_universe is not None and thresholds.get("min_universe") is not None:
        thresholds["min_universe"] = int(min_universe)
    if min_pool is not None:
        thresholds["min_pool120"] = int(min_pool)
    if min_top50 is not None:
        if normalized_mode == "candidate_pool_based":
            thresholds["min_top50"] = int(min_top50)
        else:
            thresholds["exact_top50"] = int(min_top50)
    if exact_final30 is not None:
        thresholds["exact_final30"] = int(exact_final30)

    if normalized_mode == "candidate_pool_based":
        effective_candidate_pool_size = int(candidate_pool_size or len(universe_scored) or 0)
        min_pool_floor = max(int(thresholds["min_pool120"] or 0), 30)
        if effective_candidate_pool_size > 0:
            thresholds["min_pool120"] = min(effective_candidate_pool_size, min_pool_floor)
        thresholds["min_top50"] = max(int(thresholds.get("min_top50") or 0), 30)

    logger.info(
        "[CONTRACT][POLICY] mode=%s thresholds=%s universe_scored_source=%s",
        normalized_mode,
        thresholds,
        universe_scored_source,
    )

    if thresholds.get("min_universe") is not None and len(universe_scored) < int(thresholds["min_universe"]):
        failures.append(f"contract_universe_too_small:{len(universe_scored)}<{thresholds['min_universe']}")

    if len(pool120) < int(thresholds["min_pool120"]):
        failures.append(f"contract_pool120_too_small:{len(pool120)}<{thresholds['min_pool120']}")

    if normalized_mode == "candidate_pool_based":
        if len(top50) < int(thresholds.get("min_top50") or 0):
            failures.append(f"contract_top50_too_small:{len(top50)}<{thresholds['min_top50']}")
    elif len(top50) != int(thresholds["exact_top50"]):
        failures.append(f"contract_top50_not_exact:{len(top50)}!={thresholds['exact_top50']}")

    if len(final30) != int(thresholds["exact_final30"]):
        failures.append(f"contract_final30_not_exact:{len(final30)}!={thresholds['exact_final30']}")

    final30_keys = set()
    for row in final30 or []:
        final30_keys.update((row or {}).keys())
    missing_critical_cols = [col for col in CRITICAL_SCORED_COLS if col not in final30_keys]
    if missing_critical_cols:
        failures.append(f"contract_final30_missing_critical_cols:{','.join(missing_critical_cols)}")
    
    if failures:
        logger.error(
            "[CONTRACT_VALIDATION][FAIL] failures=%s universe=%s pool120=%s top50=%s final30=%s",
            failures,
            len(universe_scored),
            len(pool120),
            len(top50),
            len(final30),
        )
    else:
        logger.info(
            "[CONTRACT_VALIDATION][OK] universe=%s pool120=%s top50=%s final30=%s",
            len(universe_scored),
            len(pool120),
            len(top50),
            len(final30),
        )
    
    return failures


def _flow_contract_state(
    *,
    foreign_df: Optional[pd.DataFrame],
    inst_df: Optional[pd.DataFrame],
    flow_result: Dict[str, Any],
) -> Tuple[bool, Optional[float], Optional[float], str]:
    foreign_ratio_raw = flow_result.get("foreign_20_ratio")
    inst_ratio_raw = flow_result.get("inst_20_ratio")
    has_foreign_ratio = foreign_ratio_raw is not None
    has_inst_ratio = inst_ratio_raw is not None

    if has_foreign_ratio or has_inst_ratio:
        foreign_ratio = _safe_float(foreign_ratio_raw, 0.0) if has_foreign_ratio else None
        inst_ratio = _safe_float(inst_ratio_raw, 0.0) if has_inst_ratio else None
        return False, foreign_ratio, inst_ratio, "ratio_available"

    if foreign_df is None or inst_df is None:
        return True, None, None, "provider_missing"
    return True, None, None, "ratio_missing"


def _build_flow_provenance(
    *,
    flow_missing: bool,
    flow_meta: Dict[str, Any],
    foreign_df: Optional[pd.DataFrame],
    inst_df: Optional[pd.DataFrame],
    flow_missing_reason: str,
) -> Dict[str, Any]:
    data_available = int((not flow_missing) and bool((flow_meta or {}).get("ok")))
    provider_used = str((flow_meta or {}).get("provider") or "none").strip().lower()
    if data_available != 1:
        provider_used = "none"
    return {
        "flow_data_available": data_available,
        "flow_provider_used": provider_used,
        "flow_fail_reason": "" if data_available else str((flow_meta or {}).get("reason") or flow_missing_reason or "unknown"),
        "flow_score_imputed": 0 if data_available else 1,
        "foreign_flow_missing": int(foreign_df is None or getattr(foreign_df, "empty", True)),
        "inst_flow_missing": int(inst_df is None or getattr(inst_df, "empty", True)),
    }


def _extract_derived_metrics(derived_row: Dict[str, Any]) -> Dict[str, float]:
    features = derived_row.get("features_json") if isinstance(derived_row.get("features_json"), dict) else {}

    close = _safe_nullable_float(derived_row.get("close"))
    ma50 = _safe_nullable_float(derived_row.get("ma50"))
    ma150 = _safe_nullable_float(derived_row.get("ma150"))
    ma200 = _safe_nullable_float(derived_row.get("ma200"))
    trend_checks = [
        bool(close is not None and ma50 is not None and close > ma50 > 0),
        bool(ma50 is not None and ma150 is not None and ma50 > ma150 > 0),
        bool(ma150 is not None and ma200 is not None and ma150 > ma200 > 0),
        bool(close is not None and ma200 is not None and close > ma200 > 0),
    ]
    trend_score = float(sum(1 for check in trend_checks if check) * 25.0)

    hi_52w = _prefer_valid_numeric(derived_row.get("hi_52w"), features.get("hi_52w"), zero_invalid=True)
    pullback_pct = None
    if hi_52w is not None and close is not None and hi_52w > 0 and close > 0:
        pullback_pct = max(0.0, (hi_52w - close) / hi_52w)
    else:
        pullback_pct = _safe_nullable_float(features.get("pullback_pct"))

    return {
        "rs_percentile": _prefer_valid_numeric(derived_row.get("rs_percentile"), features.get("rs_percentile")),
        "rs_pctile": _prefer_valid_numeric(derived_row.get("rs_percentile"), features.get("rs_percentile")),
        "vcp_score": _prefer_valid_numeric(derived_row.get("vcp_score"), features.get("vcp_score")),
        "atr_pct": _prefer_valid_numeric(derived_row.get("atr_pct"), features.get("atr_pct"), zero_invalid=True),
        "trend_score": trend_score,
        "pullback_pct": pullback_pct,
        "breakout_score": _safe_nullable_float(derived_row.get("breakout_score")),
        "pullback_score": _safe_nullable_float(derived_row.get("pullback_score")),
        "momentum_score": _safe_nullable_float(derived_row.get("momentum_score")),
        "entry_style_selected": derived_row.get("entry_style_selected") or features.get("entry_style_selected"),
    }


def _sync_item_and_meta_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    out = normalize_final30_contract_row(item)
    out["code"] = str(out.get("code") or "").zfill(6)
    meta = dict(out.get("meta") or {})
    meta["code"] = out["code"]
    if not str(out.get("name") or "").strip() and str(meta.get("name") or "").strip():
        out["name"] = str(meta.get("name") or "")

    for key in (
        "as_of",
        "rs_pctile",
        "rs_percentile",
        "vcp_score",
        "atr_pct",
        "trend_score",
        "pullback_pct",
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "entry_style_selected",
        "close",
        "volume_avg20",
        "ma20",
        "ma50",
        "ma150",
        "flow_score",
        "tech_score",
        "final_score",
        "score_final",
        "foreign_20_ratio",
        "inst_20_ratio",
    ):
        if key in out and out.get(key) is not None:
            meta[key] = out.get(key)
        elif key in meta and meta.get(key) is not None:
            out[key] = meta.get(key)

    out["rows"] = _safe_int(out.get("rows", meta.get("rows", 0)), 0)
    meta["rows"] = out["rows"]

    out["rs_percentile"] = _prefer_valid_numeric(out.get("rs_percentile"), meta.get("rs_percentile"))
    out["rs_pctile"] = _prefer_valid_numeric(out.get("rs_pctile"), out.get("rs_percentile"))
    out["vcp_score"] = _prefer_valid_numeric(out.get("vcp_score"), meta.get("vcp_score"))
    out["atr_pct"] = _prefer_valid_numeric(out.get("atr_pct"), meta.get("atr_pct"), zero_invalid=True)
    out["trend_score"] = _prefer_valid_numeric(out.get("trend_score"), meta.get("trend_score"))
    out["pullback_pct"] = _prefer_valid_numeric(out.get("pullback_pct"), meta.get("pullback_pct"))
    out["breakout_score"] = _prefer_valid_numeric(out.get("breakout_score"), meta.get("breakout_score"))
    out["pullback_score"] = _prefer_valid_numeric(out.get("pullback_score"), meta.get("pullback_score"))
    out["momentum_score"] = _prefer_valid_numeric(out.get("momentum_score"), meta.get("momentum_score"))
    out["entry_style_selected"] = out.get("entry_style_selected") or meta.get("entry_style_selected")
    out["close"] = _prefer_valid_numeric(out.get("close"), meta.get("close"), zero_invalid=True)
    out["volume_avg20"] = _prefer_valid_numeric(out.get("volume_avg20"), meta.get("volume_avg20"), zero_invalid=True)
    out["ma20"] = _prefer_valid_numeric(out.get("ma20"), meta.get("ma20"), zero_invalid=True)
    out["ma50"] = _prefer_valid_numeric(out.get("ma50"), meta.get("ma50"), zero_invalid=True)
    out["ma150"] = _prefer_valid_numeric(out.get("ma150"), meta.get("ma150"), zero_invalid=True)
    out["flow_score"] = _prefer_valid_numeric(out.get("flow_score"), meta.get("flow_score"))
    out["tech_score"] = _prefer_valid_numeric(out.get("tech_score"), meta.get("tech_score"))
    canonical_final = _prefer_valid_numeric(out.get("score_final"), meta.get("score_final"))
    if canonical_final is None:
        canonical_final = _prefer_valid_numeric(out.get("final_score"), meta.get("final_score"))
    if canonical_final is None:
        canonical_final = _safe_nullable_float(out.get("score"))
    out["score_final"] = canonical_final
    out["final_score"] = canonical_final
    out["score"] = canonical_final
    out["score_tech"] = _prefer_valid_numeric(out.get("score_tech"), meta.get("score_tech"))
    out["score_flow"] = _prefer_valid_numeric(out.get("score_flow"), meta.get("score_flow"))
    flow_missing = bool(out.get("flow_missing") or meta.get("flow_missing"))
    if flow_missing:
        out["foreign_20_ratio"] = out.get("foreign_20_ratio", meta.get("foreign_20_ratio"))
        out["inst_20_ratio"] = out.get("inst_20_ratio", meta.get("inst_20_ratio"))
    else:
        out["foreign_20_ratio"] = _safe_float(out.get("foreign_20_ratio", meta.get("foreign_20_ratio", 0.0)), 0.0)
        out["inst_20_ratio"] = _safe_float(out.get("inst_20_ratio", meta.get("inst_20_ratio", 0.0)), 0.0)

    for key in (
        "rs_pctile",
        "rs_percentile",
        "vcp_score",
        "atr_pct",
        "trend_score",
        "pullback_pct",
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "entry_style_selected",
        "close",
        "volume_avg20",
        "ma20",
        "ma50",
        "ma150",
        "flow_score",
        "tech_score",
        "final_score",
        "score_tech",
        "score_flow",
        "score_final",
        "score",
        "foreign_20_ratio",
        "inst_20_ratio",
    ):
        meta[key] = out[key]

    reject_reasons = list(out.get("reject_reasons", []) or meta.get("reject_reasons", []) or [])
    if flow_missing:
        if "flow_data_missing" not in reject_reasons:
            reject_reasons.append("flow_data_missing")
        if "flow_weight_disabled" not in reject_reasons:
            reject_reasons.append("flow_weight_disabled")
    else:
        reject_reasons = [
            reason
            for reason in reject_reasons
            if str(reason) not in {"flow_data_missing", "flow_weight_disabled"}
        ]
    out["reject_reasons"] = list(dict.fromkeys(reject_reasons))
    meta["reject_reasons"] = out["reject_reasons"]

    out["meta"] = meta
    return out


def _enrich_watchlist_rows(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    rows: List[Dict[str, Any]],
    flow_provider: Optional[FlowProvider],
    ohlcv_provider: Any,
    flow_window: int,
    tech_weight: float,
    flow_weight: float,
    trend_weight: float,
) -> List[Dict[str, Any]]:
    if not rows:
        return []

    normalized_rows = [_sync_item_and_meta_fields(dict(row)) for row in rows]
    symbols = [str(row.get("code") or "").zfill(6) for row in normalized_rows if row.get("code")]

    derived_repo = DerivedMinerviniRepo(engine)
    derived_rows = derived_repo.load_for_as_of(env=env, as_of=as_of, symbols=symbols)
    derived_map = {str(row.get("symbol") or "").zfill(6): row for row in derived_rows}
    flow_rows: List[Dict[str, Any]] = []

    enriched: List[Dict[str, Any]] = []
    for item in normalized_rows:
        out = normalize_final30_contract_row(item)
        code = str(out.get("code") or "").zfill(6)
        meta = dict(out.get("meta") or {})
        reject_reasons = list(out.get("reject_reasons", []) or meta.get("reject_reasons", []) or [])
        ohlcv_df = pd.DataFrame()
        try:
            ohlcv_raw, _ = ohlcv_provider(code, count=max(int(flow_window) + 30, 80))
            ohlcv_df = _normalize_ohlcv_columns(ohlcv_raw if ohlcv_raw is not None else pd.DataFrame())
        except Exception:
            ohlcv_df = pd.DataFrame()

        derived_row = derived_map.get(code)
        if derived_row is None:
            if "derived_missing" not in reject_reasons:
                reject_reasons.append("derived_missing")
            meta["derived_missing"] = True
        else:
            metrics = _extract_derived_metrics(derived_row)
            for key, value in metrics.items():
                out[key] = value
                meta[key] = value
            meta["derived_missing"] = False

        if flow_provider is not None:
            flow_weight_effective = float(flow_weight)
            tech_weight_effective = float(tech_weight)
            trend_weight_effective = float(trend_weight)
            foreign_df: Optional[pd.DataFrame] = None
            inst_df: Optional[pd.DataFrame] = None

            flow_meta: Dict[str, Any] = {"ok": False, "provider": "none", "reason": "not_called", "detail": ""}
            try:
                foreign_df, inst_df, flow_meta = flow_provider(code, as_of, int(flow_window))
            except Exception:
                logger.warning("[FLOW][WARN] provider exception code=%s as_of=%s", code, as_of, exc_info=True)
                if "flow_provider_error" not in reject_reasons:
                    reject_reasons.append("flow_provider_error")
                flow_meta = {"ok": False, "provider": "none", "reason": "provider_exception", "detail": "raised"}

            flow_result = calculate_flow_score(
                code=code,
                ohlcv_df=ohlcv_df if ohlcv_df is not None else pd.DataFrame(),
                foreign_df=foreign_df,
                inst_df=inst_df,
                window=int(flow_window),
            )
            flow_score_norm = _safe_float(flow_result.get("flow_score"), 0.0)
            tech_score_val = _safe_float(out.get("tech_score", 0.0), 0.0)

            flow_missing, foreign_ratio, inst_ratio, flow_missing_reason = _flow_contract_state(
                foreign_df=foreign_df,
                inst_df=inst_df,
                flow_result=flow_result,
            )
            if flow_missing:
                flow_weight_effective = 0.0
                tech_weight_effective = 1.0
                trend_weight_effective = 0.0
                logger.warning(
                    "[FLOW][IMPUTE] code=%s score=%s provider_used=%s reason=%s",
                    code,
                    flow_score_norm,
                    str(flow_meta.get("provider") or "none"),
                    str(flow_meta.get("reason") or flow_missing_reason or "unknown"),
                )
                logger.warning(
                    "[FLOW][WARN] flow missing -> non_blocking code=%s as_of=%s reason=%s",
                    code,
                    as_of,
                    flow_missing_reason,
                )
            meta["flow_missing"] = bool(flow_missing)
            meta["flow_missing_reason"] = flow_missing_reason
            out["flow_missing"] = bool(flow_missing)
            out["flow_pass"] = True
            provenance = _build_flow_provenance(
                flow_missing=bool(flow_missing),
                flow_meta=flow_meta,
                foreign_df=foreign_df,
                inst_df=inst_df,
                flow_missing_reason=flow_missing_reason,
            )
            out.update(provenance)
            meta.update(provenance)

            out["flow_score"] = flow_score_norm
            out["foreign_20_ratio"] = foreign_ratio
            out["inst_20_ratio"] = inst_ratio
            base_final_score = calculate_final_score(
                tech_score=tech_score_val,
                flow_score=flow_score_norm * 100.0,
                tech_weight=tech_weight_effective,
                flow_weight=flow_weight_effective,
            )
            out["final_score"] = float(base_final_score + (_safe_float(out.get("trend_score"), 0.0) * trend_weight_effective))
            meta["weights_effective"] = {
                "tech_weight": tech_weight_effective,
                "flow_weight": flow_weight_effective,
                "trend_weight": trend_weight_effective,
            }
            meta["formula"] = (
                "score_final = "
                f"{tech_weight_effective:.4f}*score_tech + "
                f"{flow_weight_effective:.4f}*score_flow + "
                f"{trend_weight_effective:.4f}*score_trend"
            )
            flow_rows.append(
                {
                    "as_of": to_date(as_of),
                    "symbol": code,
                    "flow_score": flow_score_norm,
                    "foreign_20_ratio": foreign_ratio,
                    "inst_20_ratio": inst_ratio,
                    "flow_missing": bool(flow_missing),
                    "source": "watchlist_flow_provider",
                    "features_json": {
                        "flow_missing_reason": flow_missing_reason,
                        "weights_effective": meta.get("weights_effective", {}),
                        "flow_meta": flow_meta,
                    },
                }
            )

        if _safe_float(out.get("tech_score"), 0.0) <= 0.0:
            rs_pctile = _safe_float(out.get("rs_pctile"), 0.0)
            vcp_score = _safe_float(out.get("vcp_score"), 0.0)
            trend_score = _safe_float(out.get("trend_score"), 0.0)
            pullback_pct = _safe_float(out.get("pullback_pct"), 0.0)
            atr_pct = _safe_float(out.get("atr_pct"), 0.0)
            pullback_score = max(0.0, min(100.0, 100.0 - (pullback_pct * 400.0)))
            atr_score = max(0.0, min(100.0, 100.0 - abs(atr_pct - 0.04) * 1000.0))
            out["tech_score"] = (
                rs_pctile * 0.40
                + vcp_score * 0.25
                + trend_score * 0.20
                + pullback_score * 0.10
                + atr_score * 0.05
            )

        derived_features = derived_row.get("features_json") if isinstance(derived_row, dict) and isinstance(derived_row.get("features_json"), dict) else {}
        repaired_ma20 = _prefer_numeric_candidates(
            [
                out.get("ma20"),
                meta.get("ma20"),
                item.get("ma20"),
                derived_row.get("ma20") if isinstance(derived_row, dict) else None,
                derived_features.get("ma20"),
            ],
            zero_invalid=True,
        )
        if not is_valid_positive_numeric(repaired_ma20) and not ohlcv_df.empty:
            repaired_ma20 = compute_ma20_from_ohlcv(ohlcv_df)
        out["ma20"] = repaired_ma20
        meta["ma20"] = repaired_ma20
        if not is_valid_positive_numeric(repaired_ma20):
            reject_reasons.append("ma20_missing")
            _append_quality_flag(out, "ma20_invalid")

        repaired_atr_pct = _prefer_numeric_candidates(
            [
                out.get("atr_pct"),
                meta.get("atr_pct"),
                item.get("atr_pct"),
                derived_row.get("atr_pct") if isinstance(derived_row, dict) else None,
                derived_features.get("atr_pct"),
            ],
            zero_invalid=True,
        )
        if not is_valid_positive_numeric(repaired_atr_pct) and not ohlcv_df.empty:
            repaired_atr_pct = compute_atr_pct_from_ohlcv(ohlcv_df)
        out["atr_pct"] = repaired_atr_pct
        meta["atr_pct"] = repaired_atr_pct
        if not is_valid_positive_numeric(repaired_atr_pct):
            reject_reasons.append("atr_pct_invalid")
            _append_quality_flag(out, "atr_pct_invalid")

        out["reject_reasons"] = list(dict.fromkeys(reject_reasons))
        meta["reject_reasons"] = out["reject_reasons"]
        out["meta"] = meta
        out = _sync_item_and_meta_fields(out)
        out = normalize_final30_contract_row(out)
        enriched.append(out)

    enriched.sort(key=lambda row: _safe_float(row.get("final_score", row.get("score", 0.0)), 0.0), reverse=True)
    for idx, row in enumerate(enriched, start=1):
        row["rank"] = _safe_int(row.get("rank") or idx, idx)
        row.setdefault("rank_final30", idx)
        row.setdefault("rank_top50", _safe_int(row.get("rank_top50"), 0))
        row.setdefault("rank_pool120", _safe_int(row.get("rank_pool120"), 0))

    quality = evaluate_final30_quality(enriched)
    quality_ok = bool(quality.get("ok", False))
    quality_soft_fail = bool(quality.get("soft_fail", False))
    logger.info(
        "[WATCHLIST][FINAL30][QUALITY] rows=%s uniq_codes=%s valid_ma20_ratio=%.3f valid_atr_ratio=%.3f breakout_nonnull_ratio=%.3f pullback_nonnull_ratio=%.3f momentum_nonnull_ratio=%.3f valid_score_final_ratio=%.3f entry_style_monoculture=%s breakout_monoculture=%s pullback_monoculture=%s momentum_monoculture=%s score_pattern_monoculture=%s score_monoculture=%s ok=%s",
        quality.get("rows"),
        quality.get("uniq_codes"),
        float(quality.get("valid_ma20_ratio", 0.0)),
        float(quality.get("valid_atr_ratio", 0.0)),
        float(quality.get("breakout_nonnull_ratio", 0.0)),
        float(quality.get("pullback_nonnull_ratio", 0.0)),
        float(quality.get("momentum_nonnull_ratio", 0.0)),
        float(quality.get("valid_score_final_ratio", 0.0)),
        quality.get("entry_style_monoculture"),
        quality.get("breakout_monoculture"),
        quality.get("pullback_monoculture"),
        quality.get("momentum_monoculture"),
        quality.get("score_pattern_monoculture"),
        quality.get("score_monoculture"),
        quality_ok,
    )
    if not quality_ok:
        logger.error(
            "[WATCHLIST][FINAL30][QUALITY_FAIL] errors=%s invalid_rows=%s invalid_codes=%s",
            quality.get("errors", []),
            quality.get("invalid_row_count", 0),
            quality.get("invalid_sample_codes", []),
        )
        raise RuntimeError("FINAL30_QUALITY_FAILED:" + ",".join(quality.get("errors", [])))
    if quality_soft_fail:
        logger.warning("[WATCHLIST][FINAL30][QUALITY_FAIL_SOFT] quality=%s", quality)

    if flow_rows:
        try:
            flow_repo = DerivedFlowRepo(engine)
            upserted = flow_repo.upsert_rows(env=env, rows=flow_rows)
            as_of_count = flow_repo.count_as_of(env=env, as_of=to_date(as_of))
            missing_count = sum(1 for row in flow_rows if bool(row.get("flow_missing")))
            logger.info(
                "[FLOW][DB][UPSERT_OK] env=%s as_of=%s input_rows=%s upserted=%s as_of_count=%s missing_rows=%s",
                env,
                as_of,
                len(flow_rows),
                upserted,
                as_of_count,
                missing_count,
            )
        except Exception:
            logger.warning("[FLOW][DB][UPSERT_FAIL] env=%s as_of=%s rows=%s", env, as_of, len(flow_rows), exc_info=True)
    return enriched


class WatchlistBuilder:
    """단일 파이프라인으로 universe -> pool120 -> top50 -> final30 생성."""

    def __init__(
        self,
        *,
        ohlcv_provider: Any,
        minervini_config: Dict[str, Any],
        env: Optional[str] = None,
        repo: Optional[WatchlistRepo] = None,
        pooln: int = 120,
        topk: int = 50,
        finaln: int = 30,
        min_price: float = 3000.0,
        liq_days: int = 20,
        min_rows: int = 30,
        flow_provider: Optional[FlowProvider] = None,
        flow_window: int = 20,
        tech_weight: float = 0.7,
        flow_weight: float = 0.3,
        trend_weight: float = 0.0,
    ):
        self.ohlcv_provider = ohlcv_provider
        self.minervini_config = minervini_config
        self.env = env
        self.repo = repo
        self.pooln = pooln
        self.topk = topk
        self.finaln = finaln
        self.min_price = min_price
        self.liq_days = liq_days
        self.min_rows = min_rows
        self.flow_provider = flow_provider
        self.flow_window = flow_window
        self.tech_weight = tech_weight
        self.flow_weight = flow_weight
        self.trend_weight = trend_weight
        self.min_avg_value20 = _env_float("LIQ_MIN_AVG_VALUE20", 300_000_000.0)
        self.min_turnover_pct = _env_float("LIQ_MIN_TURNOVER_PCT", 0.3)
        self.final_weights = {
            "ai_rs": _env_float("FINAL30_W_AI_RS", 0.30),
            "trend": _env_float("FINAL30_W_TREND", 0.20),
            "pullback": _env_float("FINAL30_W_PULLBACK", 0.15),
            "liquidity": _env_float("FINAL30_W_LIQUIDITY", 0.15),
            "flow": _env_float("FINAL30_W_FLOW", 0.10),
            "volatility": _env_float("FINAL30_W_VOLATILITY", 0.10),
        }
        self.last_bundle: Dict[str, Any] = {}
        self._watchlist_build_call_count = 0

    def build(self, *, members: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
        self._watchlist_build_call_count += 1
        logger.info(
            "[WATCHLIST][BUILD_CALL] count=%d",
            self._watchlist_build_call_count,
        )
        if self._watchlist_build_call_count > 1:
            raise RuntimeError(
                f"WATCHLIST_BUILD_CALLED_MULTIPLE_TIMES count={self._watchlist_build_call_count}"
            )

        logger.info(
            "[WATCHLIST][BUILD][START] as_of=%s members=%s pooln=%s topk=%s finaln=%s",
            as_of,
            len(members),
            self.pooln,
            self.topk,
            self.finaln,
        )

        pool120, universe_scored = self._stage_a_liquidity_filter(members, as_of)
        
        logger.info(
            "[WATCHLIST][STAGE_COUNTS] raw=%d universe_items=%d pool120=%d",
            len(members),
            len(universe_scored),
            len(pool120),
        )

        # A단계: universe_scored와 pool120 모두에 derived/minervini 점수 merge 및 score 부여
        # ✅ FIX: universe_scored도 함께 처리하여 pb1_universe_scored 테이블에 nonzero scores 저장
        universe_scored = self._merge_derived_scores(universe_scored, as_of=as_of)
        universe_scored = self._attach_scores(universe_scored, "UNIVERSE_SCORED")
        
        pool120 = self._merge_derived_scores(pool120, as_of=as_of)
        pool120 = self._attach_scores(pool120, "A_POOL120")
        self._assert_nonzero_scores(pool120, "A_POOL120", score_key="tech_score")

        top50_source = sorted(
            pool120,
            key=lambda r: _safe_float(_row_get(r, "tech_score", 0.0)),
            reverse=True,
        )
        top50 = top50_source[: self.topk]
        logger.info("[WATCHLIST][PIPELINE][B_TOP50] kept=%s from=%s", len(top50), len(pool120))

        # 다시 점수 붙이기 (안전하게)
        top50 = self._attach_scores(top50, "B_TOP50")
        self._assert_nonzero_scores(top50, "TOP50", score_key="tech_score")
        
        logger.info(
            "[WATCHLIST][STAGE_COUNTS] universe=%d pool120=%d top50=%d",
            len(universe_scored),
            len(pool120),
            len(top50),
        )

        final30_source = sorted(
            top50,
            key=lambda r: _safe_float(_row_get(r, "score_final", 0.0)),
            reverse=True,
        )
        final30 = final30_source[: self.finaln]
        logger.info("[WATCHLIST][PIPELINE][C_FINAL30] kept=%s from=%s", len(final30), len(top50))

        final30 = self._attach_scores(final30, "C_FINAL30")
        self._assert_nonzero_scores(final30, "FINAL30", score_key="score_final")

        final30_scored = pd.DataFrame(final30).copy(deep=True)
        log_df_identity(final30_scored, "BUILD")
        log_final30_ma_diagnostics(final30_scored, "build_raw")
        log_short_horizon_feature_diag(final30_scored, "build_raw")
        if _should_backfill_short_horizon_features(final30_scored, require_all_null=True):
            final30_scored = backfill_short_horizon_features(
                final30_scored,
                as_of,
                ohlcv_provider=self.ohlcv_provider,
                stage="build_raw",
            )
            log_short_horizon_feature_diag(final30_scored, "after_backfill_build_raw")
            log_final30_ma_diagnostics(final30_scored, "after_backfill_build_raw")
        final30_scored = normalize_ma20_column(final30_scored, "build_raw")
        log_final30_ma_diagnostics(final30_scored, "build_normalized")
        log_short_horizon_feature_diag(final30_scored, "build_normalized")
        final30 = final30_scored.to_dict(orient="records")

        logger.info(
            "[WATCHLIST][STAGE_COUNTS] universe=%d pool120=%d top50=%d final30=%d",
            len(universe_scored),
            len(pool120),
            len(top50),
            len(final30),
        )

        contract_mode = "full_universe_based" if len(members) >= max(self.pooln, 120) else "candidate_pool_based"
        contract_failures = validate_watchlist_contract(
            universe_scored=universe_scored,
            pool120=pool120,
            top50=top50,
            final30=final30,
            min_pool=_env_int("PB1_WATCHLIST_POOL_MIN", 40),
            exact_final30=int(self.finaln),
            contract_mode=contract_mode,
            universe_scored_source="watchlist_builder",
            candidate_pool_size=len(members),
        )

        degrade_meta = {
            "used": False,
            "requested_finaln": int(self.finaln),
            "final_count": len(final30),
            "fill_sources": {"top50": 0, "pool120": 0},
            "missing_after_fill": max(0, int(self.finaln) - len(final30)),
        }
        if len(final30) < self.finaln:
            logger.warning(
                "[WATCHLIST][PIPELINE][C_FINAL30][DEGRADE] too small: %s < %s -> fill from B_TOP50 then A_POOL120",
                len(final30),
                self.finaln,
            )
            final30, degrade_meta = self._degrade_fill_final_rows(
                final_rows=final30,
                top50_rows=top50,
                pool120_rows=pool120,
                target_n=self.finaln,
            )

        if len(final30) != int(self.finaln):
            contract_failures.append(f"final30_count_mismatch:{len(final30)}!={int(self.finaln)}")

        allow_degrade = _env_bool("PB1_WATCHLIST_ALLOW_DEGRADE", True)
        if contract_failures and not allow_degrade:
            raise RuntimeError("WATCHLIST_PIPELINE_CONTRACT_FAILED: " + ",".join(contract_failures))

        shortage_reason = ""
        if len(final30) != int(self.finaln):
            shortage_reason = ";".join(contract_failures) or f"pipeline_shortage:{len(final30)}<{int(self.finaln)}"

        degrade_meta = {
            **degrade_meta,
            "enabled": bool(degrade_meta.get("used") or bool(contract_failures)),
            "reason": shortage_reason,
            "disabled_features": ["flow"] if any("flow" in reason for reason in contract_failures) else [],
        }

        final30_scored = pd.DataFrame(final30).copy(deep=True)
        log_final30_ma_diagnostics(final30_scored, "before_freeze")
        log_short_horizon_feature_diag(final30_scored, "before_freeze")
        if _should_backfill_short_horizon_features(final30_scored, require_all_null=False):
            final30_scored = backfill_short_horizon_features(
                final30_scored,
                as_of,
                ohlcv_provider=self.ohlcv_provider,
                stage="before_freeze",
            )
            log_short_horizon_feature_diag(final30_scored, "after_backfill_before_freeze")
            log_final30_ma_diagnostics(final30_scored, "after_backfill_before_freeze")
        final30_scored = normalize_ma20_column(final30_scored, "before_freeze")
        log_final30_ma_diagnostics(final30_scored, "after_normalize_before_freeze")
        log_short_horizon_feature_diag(final30_scored, "after_normalize_before_freeze")
        final30_scored = materialize_final30_price_context(final30_scored)
        log_df_identity(final30_scored, "FROZEN")
        logger.info(
            "[WATCHLIST][FINAL30_SCORED][FROZEN] rows=%d ma20_null=%d cols=%s",
            len(final30_scored),
            int(final30_scored["ma20"].isna().sum()) if "ma20" in final30_scored.columns else -1,
            list(final30_scored.columns),
        )
        log_final30_ma_diagnostics(final30_scored, "pre_contract_frozen")
        log_short_horizon_feature_diag(final30_scored, "pre_contract_frozen")
        # ── entry style sanitize: contract 직전 강제 보정 ──────────────────────
        _pre_contract_rows = sanitize_final30_entry_styles(
            final30_scored.to_dict(orient="records"),
            stage="pre_contract_frozen",
            hard=False,
        )
        final30_scored = pd.DataFrame(_pre_contract_rows)
        # ──────────────────────────────────────────────────────────────────────
        assert_final30_scored_contract(final30_scored, "frozen", str(as_of), hard=True)
        final30 = final30_scored.to_dict(orient="records")

        reject_counter = Counter()
        for item in universe_scored:
            for reason in item.get("reject_reasons", []):
                reject_counter[reason] += 1

        self.last_bundle = {
            "as_of": as_of,
            "weights": {
                "ai_rs": self.final_weights["ai_rs"],
                "trend": self.final_weights["trend"],
                "pullback": self.final_weights["pullback"],
                "liquidity": self.final_weights["liquidity"],
                "flow": self.final_weights["flow"],
                "volatility": self.final_weights["volatility"],
            },
            "weights_effective": {
                "ai_rs": self.final_weights["ai_rs"],
                "trend": self.final_weights["trend"],
                "pullback": self.final_weights["pullback"],
                "liquidity": self.final_weights["liquidity"],
                "flow": self.final_weights["flow"],
                "volatility": self.final_weights["volatility"],
            },
            "formula": (
                "score_final = "
                f"{self.final_weights['ai_rs']:.4f}*ai_rs + "
                f"{self.final_weights['trend']:.4f}*trend + "
                f"{self.final_weights['pullback']:.4f}*pullback + "
                f"{self.final_weights['liquidity']:.4f}*liquidity + "
                f"{self.final_weights['flow']:.4f}*flow + "
                f"{self.final_weights['volatility']:.4f}*volatility"
            ),
            "universe_scored": universe_scored,
            "universe_scored_df": universe_scored,
            "pool120": pool120,
            "pool120_scored": pool120,
            "top50": top50,
            "top50_scored": top50,
            "final30": final30,
            "final30_scored": final30_scored.copy(deep=True),
            "reject_summary": dict(reject_counter),
            "final_count": len(final30),
            "requested_finaln": int(self.finaln),
            "degrade": degrade_meta,
            "shortage_reason": shortage_reason,
            "contract_failures": contract_failures,
            "contract_mode": contract_mode,
        }

        log_df_identity(final30_scored, "RETURN")
        assert_final30_scored_contract(final30_scored, "return", str(as_of), hard=True)
        logger.info(
            "[WATCHLIST][BUILD][DONE] as_of=%s pool120=%s top50=%s final30=%s",
            as_of,
            len(pool120),
            len(top50),
            len(final30),
        )
        return final30

    def _degrade_fill_final_rows(
        self,
        *,
        final_rows: List[Dict[str, Any]],
        top50_rows: List[Dict[str, Any]],
        pool120_rows: List[Dict[str, Any]],
        target_n: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        filled: List[Dict[str, Any]] = [dict(row) for row in final_rows]
        seen_codes = {str(row.get("code") or "").zfill(6) for row in filled}
        fill_sources = {"top50": 0, "pool120": 0}

        def _as_final_row(src: Dict[str, Any], source_name: str) -> Dict[str, Any]:
            row = dict(src)
            reasons = list(row.get("reject_reasons", []) or [])
            reasons.append(f"degrade_fill_from_{source_name}")
            row["reject_reasons"] = reasons
            row["flow_score"] = float(row.get("flow_score", 0.0) or 0.0)
            row["foreign_20_ratio"] = float(row.get("foreign_20_ratio", 0.0) or 0.0)
            row["inst_20_ratio"] = float(row.get("inst_20_ratio", 0.0) or 0.0)
            tech_score = float(row.get("tech_score", 0.0) or 0.0)
            fallback_score = float(row.get("score", 0.0) or 0.0)
            row["final_score"] = float(row.get("final_score", tech_score if tech_score > 0 else fallback_score) or 0.0)
            return self._normalize_item(row, score_key="final_score", rank_key="final_rank")

        for src in top50_rows:
            if len(filled) >= target_n:
                break
            code = str(src.get("code") or "").zfill(6)
            if not code or code in seen_codes:
                continue
            filled.append(_as_final_row(src, "top50"))
            seen_codes.add(code)
            fill_sources["top50"] += 1

        for src in pool120_rows:
            if len(filled) >= target_n:
                break
            code = str(src.get("code") or "").zfill(6)
            if not code or code in seen_codes:
                continue
            filled.append(_as_final_row(src, "pool120"))
            seen_codes.add(code)
            fill_sources["pool120"] += 1

        for idx, item in enumerate(filled, start=1):
            item["final_rank"] = idx
            item["rank"] = idx
            item["rank_final30"] = idx

        degrade_meta = {
            "used": True,
            "requested_finaln": int(target_n),
            "final_count": len(filled),
            "fill_sources": fill_sources,
            "missing_after_fill": max(0, int(target_n) - len(filled)),
        }
        logger.warning(
            "[WATCHLIST][PIPELINE][C_FINAL30][DEGRADE] filled final=%s requested=%s fill_top50=%s fill_pool120=%s",
            len(filled),
            target_n,
            fill_sources["top50"],
            fill_sources["pool120"],
        )
        return filled[:target_n], degrade_meta

    def _base_item(self, code: str, *, name: str = "", as_of: Optional[date] = None, market: str = "UNKNOWN") -> Dict[str, Any]:
        normalized_market = normalize_kr_market(market)
        return {
            "as_of": as_of.isoformat() if isinstance(as_of, date) else "",
            "code": code,
            "name": name,
            "market": normalized_market,
            "market_code": normalized_market,
            "rs_benchmark": RS_BENCHMARK_KOSPI if normalized_market == "KOSPI" else RS_BENCHMARK_KOSDAQ if normalized_market == "KOSDAQ" else None,
            "rank": None,
            "score": None,
            "liq_avg": 0.0,
            "last_close": None,
            "rows": 0,
            "rs_pctile": None,
            "rs_percentile": None,
            "vcp_score": None,
            "pullback_pct": None,
            "trend_score": None,
            "atr_pct": None,
            "foreign_20_ratio": 0.0,
            "inst_20_ratio": 0.0,
            "flow_score": 0.0,
            "ai_rs_score": 0.0,
            "meta_k": 0.5,
            "breakout_target": 0.0,
            "breakout_score": None,
            "pullback_score": None,
            "momentum_score": None,
            "entry_style_selected": None,
            "liquidity_score": 0.0,
            "volatility_score": 0.0,
            "tech_score": None,
            "score_final": None,
            "final_score": None,
            "reject_reasons": [],
            "meta": {},
        }

    def _normalize_item(self, item: Dict[str, Any], *, score_key: str, rank_key: str) -> Dict[str, Any]:
        source_meta = dict(item.get("meta") or {})
        normalized_market = normalize_kr_market(item.get("market") or item.get("market_code") or source_meta.get("market") or source_meta.get("market_code"))
        rs_benchmark = item.get("rs_benchmark") or source_meta.get("rs_benchmark")
        regime_fields = {
            "market": normalized_market, "market_code": normalized_market, "rs_benchmark": rs_benchmark,
            "close": item.get("close") if item.get("close") is not None else item.get("last_close"),
            "ma20": item.get("ma20"), "ma50": item.get("ma50"),
            "return_1d": item.get("return_1d"), "return_5d": item.get("return_5d"),
            "above_ma20": item.get("above_ma20"), "above_ma50": item.get("above_ma50"),
            "volume_avg20": item.get("volume_avg20") or item.get("avg_volume20") or item.get("vol20"),
        }
        score_val = float(item.get(score_key, 0.0) or 0.0)
        rank_val = item.get(rank_key) or item.get("rank")
        reject_reasons = list(item.get("reject_reasons", []) or [])
        filters_passed = list(item.get("filters_passed", []) or [])
        rs_min_pctile = float(self.minervini_config.get("rs_min_pctile", RS_MIN_PCTILE))
        if rs_min_pctile <= 1.0:
            rs_min_pctile = rs_min_pctile * 100.0
        vcp_min_score = float(self.minervini_config.get("vcp_min_score", 70.0) or 70.0)

        scores = {
            "rs_pctile": float(item.get("rs_pctile", 0.0) or 0.0),
            "vcp_score": float(item.get("vcp_score", 0.0) or 0.0),
            "trend_template": 1 if float(item.get("trend_score", 0.0) or 0.0) >= 100.0 else 0,
            "liquidity_rank": int(item.get("pool_rank", 0) or item.get("rank_pool120", 0) or 0),
            "pullback_score": max(0.0, min(100.0, 100.0 - float(item.get("pullback_pct", 0.0) or 0.0) * 400.0)),
            "foreign_score": max(0.0, min(100.0, float(item.get("foreign_20_ratio", 0.0) or 0.0) * 100.0)),
            "inst_score": max(0.0, min(100.0, float(item.get("inst_20_ratio", 0.0) or 0.0) * 100.0)),
        }

        reasons_raw = item.get("reasons")
        reasons: Dict[str, Any]
        if reasons_raw is None:
            reasons = {}
        elif isinstance(reasons_raw, dict):
            reasons = dict(reasons_raw)
        elif isinstance(reasons_raw, list):
            if all(isinstance(x, str) for x in reasons_raw):
                reasons = {"bullets": list(reasons_raw)}
            elif all(isinstance(x, dict) for x in reasons_raw):
                merged: Dict[str, Any] = {}
                can_merge = True
                for entry in reasons_raw:
                    for key, value in entry.items():
                        if key in merged:
                            can_merge = False
                            break
                        merged[key] = value
                    if not can_merge:
                        break
                reasons = merged if can_merge else {"items": list(reasons_raw)}
            else:
                reasons = {"raw": str(reasons_raw)}
        else:
            reasons = {"raw": str(reasons_raw)}

        passed = list(reasons.get("passed", []) or [])
        failed = list(reasons.get("failed", []) or [])
        notes = dict(reasons.get("notes", {}) or {})

        if scores["rs_pctile"] >= rs_min_pctile:
            passed.append(f"RS>={rs_min_pctile:.0f}")
        if scores["vcp_score"] >= vcp_min_score:
            passed.append(f"VCP>={vcp_min_score:.0f}")
        if scores["trend_template"] == 1:
            passed.append("Trend=Yes")
        if float(item.get("flow_score", 0.0) or 0.0) > 0.0:
            passed.append("Flow=Positive")

        failed.extend(reject_reasons)
        passed = list(dict.fromkeys(passed))
        failed = list(dict.fromkeys(failed))

        notes.setdefault("rs_pctile", scores["rs_pctile"])
        notes.setdefault("vcp_score", scores["vcp_score"])
        notes.setdefault("foreign_net_20d", float(item.get("foreign_net_20d", item.get("foreign_20_ratio", 0.0)) or 0.0))
        notes.setdefault("inst_net_20d", float(item.get("inst_net_20d", item.get("inst_20_ratio", 0.0)) or 0.0))

        reasons = {
            **reasons,
            "passed": passed,
            "failed": failed,
            "notes": notes,
        }

        filters_failed = list(item.get("filters_failed", []) or reasons.get("failed", []) or reject_reasons)
        if not filters_passed:
            if float(item.get("liq_avg", 0.0) or 0.0) > 0.0:
                filters_passed.append("A_POOL120")
            if float(item.get("tech_score", 0.0) or 0.0) > 0.0:
                filters_passed.append("B_TOP50")
            if float(item.get("final_score", 0.0) or 0.0) > 0.0:
                filters_passed.append("C_FINAL30")
        score_liq = float(item.get("score_liq", 0.0) or 0.0)
        score_tech = float(item.get("tech_score", 0.0) or 0.0)
        score_flow = float(item.get("flow_score", 0.0) or 0.0)
        score_final = float(item.get("final_score", score_val) or 0.0)
        rank_pool120 = int(item.get("pool_rank", 0) or 0)
        rank_top50 = int(item.get("top50_rank", 0) or 0)
        rank_final30 = int(item.get("final_rank", 0) or 0)
        # ── entry style 필드 top-level/meta 보존 ─────────────────────────────
        _item_meta_raw = item.get("meta") or {}
        breakout_score = _to_float_safe(item.get("breakout_score") or _item_meta_raw.get("breakout_score"))
        pullback_score = _to_float_safe(item.get("pullback_score") or _item_meta_raw.get("pullback_score"))
        momentum_score = _to_float_safe(item.get("momentum_score") or _item_meta_raw.get("momentum_score"))
        _raw_style = item.get("entry_style_selected") or _item_meta_raw.get("entry_style_selected")
        _norm_style = _normalize_entry_style_value(_raw_style)
        if _norm_style not in ALLOWED_ENTRY_STYLES:
            _norm_style = _infer_entry_style_from_scores({
                "breakout_score": breakout_score,
                "pullback_score": pullback_score,
                "momentum_score": momentum_score,
            })
        entry_style_selected = _norm_style
        entry_component = str(
            item.get("entry_component")
            or _item_meta_raw.get("entry_component")
            or entry_style_selected.lower()
        )
        breakout_pass = bool(breakout_score > 0)
        pullback_pass = bool(pullback_score > 0)
        momentum_pass = bool(momentum_score > 0)
        # ─────────────────────────────────────────────────────────────────────
        meta = {
            "as_of": str(item.get("as_of") or ""),
            "name": str(item.get("name") or ""),
            "liq_avg": float(item.get("liq_avg", 0.0) or 0.0),
            "last_close": float(item.get("last_close", 0.0) or 0.0),
            "rows": int(item.get("rows", 0) or 0),
            "rs_pctile": float(item.get("rs_pctile", 0.0) or 0.0),
            "vcp_score": float(item.get("vcp_score", 0.0) or 0.0),
            "pullback_pct": float(item.get("pullback_pct", 0.0) or 0.0),
            "trend_score": float(item.get("trend_score", 0.0) or 0.0),
            "atr_pct": float(item.get("atr_pct", 0.0) or 0.0),
            "foreign_20_ratio": float(item.get("foreign_20_ratio", 0.0) or 0.0),
            "inst_20_ratio": float(item.get("inst_20_ratio", 0.0) or 0.0),
            "flow_score": float(item.get("flow_score", 0.0) or 0.0),
            "tech_score": float(item.get("tech_score", 0.0) or 0.0),
            "final_score": float(item.get("final_score", 0.0) or 0.0),
            "breakout_score": breakout_score,
            "pullback_score": pullback_score,
            "momentum_score": momentum_score,
            "entry_style_selected": entry_style_selected,
            "entry_component": entry_component,
            "breakout_pass": breakout_pass,
            "pullback_pass": pullback_pass,
            "momentum_pass": momentum_pass,
            "weights": {
                "tech_weight": self.tech_weight,
                "flow_weight": self.flow_weight,
            },
            "reject_reasons": reject_reasons,
            "reasons": reasons,
            "filters_passed": filters_passed,
            "filters_failed": filters_failed,
            "rank_pool120": rank_pool120,
            "rank_top50": rank_top50,
            "rank_final30": rank_final30,
            "score_liq": score_liq,
            "score_tech": score_tech,
            "score_flow": score_flow,
            "score_final": score_final,
            "scores": scores,
            **(item.get("meta") or {}),
        }
        # meta 내부 entry style 필드를 덮어쓴다 (item.meta가 stale 값을 갖고 있을 수 있음)
        meta["breakout_score"] = breakout_score
        meta["pullback_score"] = pullback_score
        meta["momentum_score"] = momentum_score
        meta["entry_style_selected"] = entry_style_selected
        meta["entry_component"] = entry_component
        meta["breakout_pass"] = breakout_pass
        meta["pullback_pass"] = pullback_pass
        meta["momentum_pass"] = momentum_pass
        meta.update(regime_fields)
        return {
            "as_of": str(item.get("as_of") or ""),
            "code": str(item.get("code") or "").zfill(6),
            "name": str(item.get("name") or ""),
            "rank": int(rank_val) if rank_val else 0,
            "score": score_val,
            "meta": meta,
            "liq_avg": meta["liq_avg"],
            "last_close": meta["last_close"],
            "rows": meta["rows"],
            "rs_pctile": meta["rs_pctile"],
            "vcp_score": meta["vcp_score"],
            "pullback_pct": meta["pullback_pct"],
            "trend_score": meta["trend_score"],
            "atr_pct": meta["atr_pct"],
            "foreign_20_ratio": meta["foreign_20_ratio"],
            "inst_20_ratio": meta["inst_20_ratio"],
            "flow_score": meta["flow_score"],
            "tech_score": meta["tech_score"],
            "final_score": meta["final_score"],
            "breakout_score": breakout_score,
            "pullback_score": pullback_score,
            "momentum_score": momentum_score,
            "entry_style_selected": entry_style_selected,
            "entry_component": entry_component,
            "breakout_pass": breakout_pass,
            "pullback_pass": pullback_pass,
            "momentum_pass": momentum_pass,
            "reject_reasons": reject_reasons,
            "reasons": reasons,
            "scores": scores,
            "filters_passed": filters_passed,
            "filters_failed": filters_failed,
            "rank_pool120": rank_pool120,
            "rank_top50": rank_top50,
            "rank_final30": rank_final30,
            "score_liq": score_liq,
            "score_tech": score_tech,
            "score_flow": score_flow,
            "score_final": score_final,
            **regime_fields,
        }

    def _stage_a_liquidity_filter(self, members: List[Dict[str, Any]], as_of: date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        # ✅ FIX: A단계는 candidate_pool에서 이미 필터링된 120개를 받음
        # 재필터링 없이 정렬만 수행
        candidates: List[Dict[str, Any]] = []
        universe_items: List[Dict[str, Any]] = []
        total = len(members)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()

        for idx, m in enumerate(members, start=1):
            code = str(m.get("code") or "").zfill(6)
            if idx == 1 or idx % progress_every == 0 or idx == total:
                logger.info(
                    "[WATCHLIST][PIPELINE][A_POOL120][PROGRESS] processed=%s/%s candidates=%s elapsed=%.1fs",
                    idx,
                    total,
                    len(candidates),
                    time.monotonic() - ts0,
                )
            if not code:
                continue

            item = self._base_item(code, name=str(m.get("name") or ""), as_of=as_of, market=m.get("market") or m.get("market_code"))

            try:
                df, _meta = self.ohlcv_provider(code, count=max(self.min_rows, self.liq_days + 30, 260))
            except Exception as exc:
                item["reject_reasons"].append("ohlcv_fetch_error")
                logger.debug("[WATCHLIST][PIPELINE][A_POOL120][OHLCV_FAIL] code=%s err=%s", code, exc)
                universe_items.append(item)
                continue

            if df is None or df.empty:
                item["reject_reasons"].append("ohlcv_empty")
                universe_items.append(item)
                continue

            df = _normalize_ohlcv_columns(df)

            rows = len(df)
            item["rows"] = rows
            
            # ✅ FIX: 필터링 제거, 데이터 계산만 수행
            if "close" not in df.columns or "volume" not in df.columns:
                item["reject_reasons"].append("missing_close_or_volume")
                universe_items.append(item)
                continue

            last_close = df["close"].iloc[-1]
            if pd.isna(last_close):
                item["reject_reasons"].append("last_close_nan")
                universe_items.append(item)
                continue

            recent = df.tail(self.liq_days)
            liq_avg = (recent["close"] * recent["volume"]).mean()
            if pd.isna(liq_avg):
                item["reject_reasons"].append("liq_nan")
                universe_items.append(item)
                continue

            close_series = df["close"]
            ma20 = compute_ma20_from_ohlcv(df)
            ma50 = float(close_series.rolling(50).mean().iloc[-1]) if len(close_series) >= 50 else None
            ma150 = float(close_series.rolling(150).mean().iloc[-1]) if len(close_series) >= 150 else None
            volume_last = float(df["volume"].iloc[-1] or 0.0)
            volume_avg20 = float(df["volume"].tail(20).mean() or 0.0)
            trading_value = float(liq_avg or 0.0)

            market_cap = _safe_float((m.get("meta_json") or {}).get("market_cap"), 0.0)
            turnover_pct = 0.0
            if market_cap > 0:
                turnover_pct = float(liq_avg) / market_cap * 100.0

            # ✅ FIX: 필터링 제거, 모든 항목을 candidates에 추가
            item["liq_avg"] = float(liq_avg)
            item["last_close"] = float(last_close)
            item["close"] = float(last_close)
            item["ma20"] = ma20
            item["ma50"] = ma50
            item["ma150"] = ma150
            if ma20 is None:
                item.setdefault("reject_reasons", []).append("ma20_missing")
            item["volume"] = volume_last
            item["volume_avg20"] = volume_avg20
            item["return_1d"] = float(close_series.iloc[-1] / close_series.iloc[-2] - 1.0) if len(close_series) >= 2 else None
            item["return_5d"] = float(close_series.iloc[-1] / close_series.iloc[-6] - 1.0) if len(close_series) >= 6 else None
            item["above_ma20"] = bool(ma20 is not None and float(last_close) > ma20)
            item["above_ma50"] = bool(ma50 is not None and float(last_close) > ma50)
            item["trading_value"] = trading_value
            item["turnover_pct"] = float(turnover_pct)
            item["liquidity_score"] = compute_liquidity_score(float(liq_avg), float(turnover_pct))
            item["meta"] = {"as_of": as_of.isoformat(), "market": item["market"], "market_code": item["market_code"], "rs_benchmark": item["rs_benchmark"]}
            candidates.append(item)
            universe_items.append(item)

        # ✅ FIX: 정렬만 수행, 필터링 없음
        candidates.sort(key=lambda x: x.get("liq_avg", 0.0), reverse=True)
        pool120 = candidates[: self.pooln]

        keep_codes = {x["code"] for x in pool120}
        for idx, item in enumerate(pool120, start=1):
            item["pool_rank"] = idx

        for item in universe_items:
            if item["code"] not in keep_codes and not item.get("reject_reasons"):
                item.setdefault("reject_reasons", []).append("not_in_pool120")

        normalized_pool = [self._normalize_item(item, score_key="liq_avg", rank_key="pool_rank") for item in pool120]
        normalized_universe = [self._normalize_item(item, score_key="liq_avg", rank_key="pool_rank") for item in universe_items]

        logger.info(
            "[WATCHLIST][PIPELINE][A_POOL120] members=%s kept=%s universe=%s",
            total,
            len(normalized_pool),
            len(normalized_universe),
        )
        return normalized_pool, normalized_universe

    def _stage_b_strategy_scoring(self, pool120: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
        if not pool120:
            logger.warning("[WATCHLIST][PIPELINE][B_TOP50] kept=0 from=0")
            return []

        bench_closes: dict[str, pd.Series | None] = {}
        for market, benchmark in (("KOSPI", RS_BENCHMARK_KOSPI), ("KOSDAQ", RS_BENCHMARK_KOSDAQ)):
            try:
                bench_df, _ = self.ohlcv_provider(benchmark, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, 260) + 10)
                bench_df = _normalize_ohlcv_columns(bench_df if bench_df is not None else pd.DataFrame())
                if bench_df is None or bench_df.empty or "close" not in bench_df.columns:
                    raise ValueError(f"benchmark {benchmark} empty")
                bench_closes[market] = bench_df["close"]
            except Exception as exc:
                logger.error("[WATCHLIST][PIPELINE][B_TOP50][BENCH_FAIL] market=%s err=%s", market, exc)
                bench_closes[market] = None

        rs_min_pctile = float(self.minervini_config.get("rs_min_pctile", RS_MIN_PCTILE))
        if rs_min_pctile <= 1.0:
            rs_min_pctile = rs_min_pctile * 100.0
        rs_min_pctile = max(70.0, rs_min_pctile)
        vcp_min_score = float(self.minervini_config.get("vcp_min_score", 70.0) or 70.0)

        scored: List[Dict[str, Any]] = []
        ai_feature_rows: List[Dict[str, Any]] = []
        total = len(pool120)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()
        for idx, cand in enumerate(pool120, start=1):
            if idx == 1 or idx % progress_every == 0 or idx == total:
                logger.info(
                    "[WATCHLIST][PIPELINE][B_TOP50][PROGRESS] processed=%s/%s scored=%s elapsed=%.1fs",
                    idx,
                    total,
                    len(scored),
                    time.monotonic() - ts0,
                )
            code = cand["code"]
            item = dict(cand)
            reject_reasons = list(item.get("reject_reasons", []))

            try:
                df, _ = self.ohlcv_provider(code, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, 260) + 10)
                df = _normalize_ohlcv_columns(df if df is not None else pd.DataFrame())
            except Exception:
                reject_reasons.append("ohlcv_fetch_error_stage_b")
                item["reject_reasons"] = reject_reasons
                continue

            if df is None or df.empty or len(df) < 120:
                reject_reasons.append("insufficient_rows_stage_b")
                item["reject_reasons"] = reject_reasons
                continue

            raw_market = str(item.get("market") or item.get("market_code") or "").upper()
            market = "KOSPI" if raw_market in {"KOSPI", "KS", "P"} else "KOSDAQ" if raw_market in {"KOSDAQ", "KQ", "Q"} else ""
            bench_close = bench_closes.get(market)
            if not market or bench_close is None:
                reject_reasons.append("unknown_market_rs_benchmark")
                item["reject_reasons"] = reject_reasons
                continue
            item["rs_benchmark"] = RS_BENCHMARK_KOSPI if market == "KOSPI" else RS_BENCHMARK_KOSDAQ
            rs_pctile = self._compute_rs_percentile(df["close"], bench_close)
            rs_features = compute_rs_features(df["close"], bench_close)
            vcp_score = self._compute_vcp_score(df)
            pullback_pct = self._compute_pullback_pct(df)
            trend_score = self._compute_trend_score(df)
            atr_pct = self._compute_atr_pct(df)
            volatility_score = compute_volatility_score(atr_pct)
            k_opt, breakout_target, breakout_score = optimize_meta_k(df, lookback=20)

            vol20 = float(df["volume"].tail(20).mean() or 0.0)
            vol60 = float(df["volume"].tail(60).mean() or 0.0)
            volume_trend = (vol20 / vol60 - 1.0) if vol60 > 0 else 0.0
            momentum = float(df["close"].iloc[-1] / df["close"].iloc[-21] - 1.0) if len(df) >= 21 else 0.0
            volatility = float(df["close"].pct_change().tail(60).std() or 0.0)

            pullback_score = max(0.0, min(100.0, 100.0 - (pullback_pct * 400.0)))
            atr_score = max(0.0, min(100.0, 100.0 - abs(atr_pct - 0.04) * 1000.0))
            tech_score = (
                rs_pctile * 0.35
                + vcp_score * 0.20
                + trend_score * 0.20
                + pullback_score * 0.10
                + atr_score * 0.05
                + breakout_score * 0.10
            )

            lookback_52w = min(252, len(df))
            high_52w = float(df["high"].tail(lookback_52w).max() or 0.0)
            low_52w = float(df["low"].tail(lookback_52w).min() or 0.0)
            last_close = float(df["close"].iloc[-1] or 0.0)
            if high_52w > 0 and last_close < high_52w * 0.75:
                reject_reasons.append("below_75pct_52w_high")
            if low_52w > 0 and last_close < low_52w * 1.3:
                reject_reasons.append("below_130pct_52w_low")

            # Minervini filtering removed
            # filtering handled in candidate_pool_builder

            item.update(
                {
                    "rs_pctile": float(rs_pctile),
                    "rs63": float(rs_features.get("rs63", 0.0)),
                    "rs126": float(rs_features.get("rs126", 0.0)),
                    "rs252": float(rs_features.get("rs252", 0.0)),
                    "rs_score": float(rs_features.get("rs_score", 0.0)),
                    "vcp_score": float(vcp_score),
                    "pullback_pct": float(pullback_pct),
                    "trend_score": float(trend_score),
                    "atr_pct": float(atr_pct),
                    "volatility_score": float(volatility_score),
                    "meta_k": float(k_opt),
                    "breakout_target": float(breakout_target),
                    "breakout_score": float(breakout_score),
                    "tech_score": float(tech_score),
                    "reject_reasons": reject_reasons,
                }
            )
            scored.append(item)
            ai_feature_rows.append(
                {
                    "code": code,
                    "rs63": float(rs_features.get("rs63", 0.0)),
                    "rs126": float(rs_features.get("rs126", 0.0)),
                    "rs252": float(rs_features.get("rs252", 0.0)),
                    "rs_score": float(rs_features.get("rs_score", 0.0)),
                    "volume_trend": float(volume_trend),
                    "volatility": float(volatility),
                    "momentum": float(momentum),
                }
            )

        ai_rs_map = compute_ai_rs_scores(ai_feature_rows)
        for item in scored:
            item["ai_rs_score"] = float(ai_rs_map.get(str(item.get("code") or "").zfill(6), item.get("rs_pctile", 0.0)))
            item["tech_score"] = (
                float(item.get("ai_rs_score", 0.0)) * 0.45
                + float(item.get("trend_score", 0.0)) * 0.20
                + max(0.0, min(100.0, 100.0 - float(item.get("pullback_pct", 0.0)) * 400.0)) * 0.15
                + float(item.get("vcp_score", 0.0)) * 0.10
                + float(item.get("volatility_score", 0.0)) * 0.10
            )

        scored.sort(key=lambda x: x.get("tech_score", 0.0), reverse=True)
        top50 = scored[: self.topk]
        keep_codes = {item["code"] for item in top50}

        for idx, item in enumerate(top50, start=1):
            item["top50_rank"] = idx
        for item in scored[self.topk :]:
            item.setdefault("reject_reasons", []).append("not_in_top50")

        normalized = [self._normalize_item(item, score_key="tech_score", rank_key="top50_rank") for item in top50]

        if len(normalized) < 30:
            fallback = [self._normalize_item(item, score_key="tech_score", rank_key="top50_rank") for item in pool120[:30]]
            for idx, item in enumerate(fallback, start=1):
                item["top50_rank"] = idx
                item["rank"] = idx
            normalized = fallback
            logger.warning("[WATCHLIST][PIPELINE][B_TOP50][FALLBACK] top50<30 -> fallback=%s", len(normalized))

        logger.info("[WATCHLIST][PIPELINE][B_TOP50] kept=%s from=%s", len(normalized), len(pool120))
        return normalized

    def _stage_c_flow_final(self, top50: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
        if not top50:
            logger.warning("[WATCHLIST][PIPELINE][C_FINAL30] kept=0 from=0")
            return []

        if self.flow_provider is None:
            logger.warning("[WATCHLIST][PIPELINE][C_FINAL30][FLOW] provider missing -> flow weight disabled by item")

        flow_map: Dict[str, Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Dict[str, Any]]] = {}
        if self.flow_provider is not None and top50:
            max_workers = max(1, min(_env_int("FLOW_FETCH_MAX_CONCURRENCY", 3), 5))
            max_retries = max(1, _env_int("FLOW_FETCH_RETRIES", 3))
            backoff_base = max(0.1, _env_float("FLOW_FETCH_BACKOFF_BASE_SEC", 0.6))
            logger.info(
                "[FLOW][FETCH][START] symbols=%s concurrency=%s retries=%s",
                len(top50),
                max_workers,
                max_retries,
            )

            def _fetch_for_code(code: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Dict[str, Any]]:
                last_exc: Exception | None = None
                for attempt in range(1, max_retries + 1):
                    try:
                        return self.flow_provider(code, as_of, self.flow_window)
                    except Exception as exc:
                        last_exc = exc
                        logger.warning(
                            "[FLOW][FETCH][RETRY] code=%s attempt=%s/%s err=%s",
                            code,
                            attempt,
                            max_retries,
                            exc,
                        )
                        if attempt < max_retries:
                            time.sleep(backoff_base * (2 ** (attempt - 1)))
                logger.warning("[FLOW][FETCH][FAIL] code=%s err=%s", code, last_exc)
                return None, None, {"ok": False, "provider": "none", "reason": "fetch_failed", "detail": str(last_exc or "")[:120]}

            code_list = [str(c.get("code") or "").zfill(6) for c in top50 if c.get("code")]
            done = 0
            ts_fetch = time.monotonic()
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="flow-fetch") as ex:
                futures = {ex.submit(_fetch_for_code, code): code for code in code_list}
                for fut in as_completed(futures):
                    code = futures[fut]
                    try:
                        flow_map[code] = fut.result()
                    except Exception:
                        flow_map[code] = (None, None, {"ok": False, "provider": "none", "reason": "future_exception", "detail": ""})
                    done += 1
                    if done == 1 or done % max(1, len(code_list) // 5) == 0 or done == len(code_list):
                        logger.info(
                            "[FLOW][FETCH][HEARTBEAT] done=%s/%s elapsed=%.1fs",
                            done,
                            len(code_list),
                            time.monotonic() - ts_fetch,
                        )
            logger.info("[FLOW][FETCH][DONE] symbols=%s", len(code_list))

        scored: List[Dict[str, Any]] = []
        total = len(top50)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()
        for idx, cand in enumerate(top50, start=1):
            if idx == 1 or idx % progress_every == 0 or idx == total:
                logger.info(
                    "[WATCHLIST][PIPELINE][C_FINAL30][PROGRESS] processed=%s/%s scored=%s elapsed=%.1fs",
                    idx,
                    total,
                    len(scored),
                    time.monotonic() - ts0,
                )
            code = cand["code"]
            item = dict(cand)
            reject_reasons = list(item.get("reject_reasons", []))

            try:
                ohlcv_df, _ = self.ohlcv_provider(code, count=max(self.flow_window + 10, 80))
                ohlcv_df = _normalize_ohlcv_columns(ohlcv_df if ohlcv_df is not None else pd.DataFrame())
            except Exception:
                ohlcv_df = pd.DataFrame()

            foreign_df: Optional[pd.DataFrame] = None
            inst_df: Optional[pd.DataFrame] = None
            if self.flow_provider is not None:
                foreign_df, inst_df, _ = flow_map.get(code, (None, None, {"ok": False, "provider": "none"}))

            flow_weight_effective = self.flow_weight
            tech_weight_effective = self.tech_weight
            trend_weight_effective = self.trend_weight

            flow_result = calculate_flow_score(
                code=code,
                ohlcv_df=ohlcv_df if ohlcv_df is not None else pd.DataFrame(),
                foreign_df=foreign_df,
                inst_df=inst_df,
                window=self.flow_window,
            )

            flow_score_norm = float(flow_result.get("flow_score", 0.0) or 0.0)
            flow_score_100 = flow_score_norm * 100.0
            tech_score = float(item.get("tech_score", 0.0) or 0.0)
            flow_missing, foreign_ratio, inst_ratio, flow_missing_reason = _flow_contract_state(
                foreign_df=foreign_df,
                inst_df=inst_df,
                flow_result=flow_result,
            )
            if flow_missing:
                flow_weight_effective = 0.0
                tech_weight_effective = 1.0
                trend_weight_effective = 0.0

            flow_factor_score = max(0.0, min(100.0, flow_score_100))
            item["foreign_net_buy_5d"] = foreign_ratio
            item["institution_net_buy_5d"] = inst_ratio
            item["flow_factor_score"] = flow_factor_score

            ai_rs_score = float(item.get("ai_rs_score", item.get("rs_pctile", 0.0)) or 0.0)
            trend_score = float(item.get("trend_score", 0.0) or 0.0)
            pullback_score = max(0.0, min(100.0, 100.0 - float(item.get("pullback_pct", 0.0) or 0.0) * 400.0))
            liquidity_score = float(item.get("liquidity_score", 0.0) or 0.0)
            volatility_score = float(item.get("volatility_score", compute_volatility_score(float(item.get("atr_pct", 0.0) or 0.0))) or 0.0)

            final_score = (
                ai_rs_score * self.final_weights["ai_rs"]
                + trend_score * self.final_weights["trend"]
                + pullback_score * self.final_weights["pullback"]
                + liquidity_score * self.final_weights["liquidity"]
                + flow_factor_score * self.final_weights["flow"]
                + volatility_score * self.final_weights["volatility"]
            )

            item["flow_missing"] = bool(flow_missing)
            item["flow_missing_reason"] = flow_missing_reason
            item["flow_pass"] = True
            if bool(item.get("flow_missing")):
                if "flow_data_missing" not in reject_reasons:
                    reject_reasons.append("flow_data_missing")
                if "flow_weight_disabled" not in reject_reasons:
                    reject_reasons.append("flow_weight_disabled")
            else:
                reject_reasons = [
                    reason
                    for reason in reject_reasons
                    if str(reason) not in {"flow_data_missing", "flow_weight_disabled"}
                ]

            item.update(
                {
                    "flow_score": flow_score_norm,
                    "foreign_20_ratio": foreign_ratio,
                    "inst_20_ratio": inst_ratio,
                    "final_score": float(final_score),
                    "volatility_score": float(volatility_score),
                    "liquidity_score": float(liquidity_score),
                    "reject_reasons": reject_reasons,
                    "flow_weight_effective": float(flow_weight_effective),
                    "tech_weight_effective": float(tech_weight_effective),
                }
            )
            meta = dict(item.get("meta") or {})
            meta["weights_effective"] = {
                "tech_weight": float(tech_weight_effective),
                "flow_weight": float(flow_weight_effective),
                "trend_weight": float(trend_weight_effective),
            }
            meta["formula"] = (
                "score_final = "
                f"{float(tech_weight_effective):.4f}*score_tech + "
                f"{float(flow_weight_effective):.4f}*score_flow + "
                f"{float(trend_weight_effective):.4f}*score_trend"
            )
            item["meta"] = meta
            scored.append(item)

        scored.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
        final30 = scored[: self.finaln]

        for idx, item in enumerate(final30, start=1):
            item["final_rank"] = idx
            item["rank_final30"] = idx

        normalized_final = [self._normalize_item(item, score_key="final_score", rank_key="final_rank") for item in final30]

        rs_fail = 0
        vcp_fail = 0
        trend_fail = 0
        flow_fail = 0
        for item in scored:
            reasons = list(item.get("reject_reasons", []) or [])
            if "rs_below_min" in reasons:
                rs_fail += 1
            if "vcp_below_min" in reasons:
                vcp_fail += 1
            if "trend_template_fail" in reasons:
                trend_fail += 1
            if any(str(reason).startswith("flow_") for reason in reasons if "missing" not in str(reason)):
                flow_fail += 1

        logger.info(
            "[WATCHLIST][PIPELINE][C_FINAL30] kept=%s rs_fail=%s vcp_fail=%s trend_fail=%s flow_fail=%s from=%s requested=%s",
            len(normalized_final),
            rs_fail,
            vcp_fail,
            trend_fail,
            flow_fail,
            len(top50),
            self.finaln,
        )
        return normalized_final

    def _norm_symbol(self, row: Any) -> str:
        """
        Normalize symbol from various field names to 6-digit format.
        """
        sym = (
            _row_get(row, "symbol", None) or
            _row_get(row, "code", None) or
            _row_get(row, "stock_code", None) or
            ""
        )
        return str(sym).zfill(6) if sym else ""

    def _load_minervini_source_map(self, as_of: date) -> Dict[str, Dict[str, Any]]:
        """
        Load Minervini/derived scores from all available sources.
        
        Priority:
          1. derived_minervini repo (direct)
          2. derived_minervini repo (fallback to recent snapshot)
          3. pb1_universe_scored watchlist bundle (quality-checked)
        
        Returns:
            Dict[symbol, row] with rs/vcp/trend/breakout/pullback/momentum scores
            
        Raises:
            RuntimeError if no valid source with nonzero scores is found
        """
        derived_map: Dict[str, Dict[str, Any]] = {}
        
        def _check_quality(rows: List[Any], source_name: str) -> tuple[bool, int, int, int]:
            """Check if source has valid nonzero scores."""
            if not rows:
                return False, 0, 0, 0
            
            rs_nonzero = sum(1 for r in rows 
                            if _safe_float(_row_get(r, "rs_percentile", _row_get(r, "rs_score", 0.0))) > 0)
            vcp_nonzero = sum(1 for r in rows 
                             if _safe_float(_row_get(r, "vcp_score", 0.0)) > 0)
            trend_nonzero = sum(1 for r in rows 
                               if _safe_float(_row_get(r, "trend_score", 0.0)) > 0)
            
            # Quality check: at least one core score type must be nonzero
            is_valid = (rs_nonzero > 0) or (vcp_nonzero > 0) or (trend_nonzero > 0)
            
            return is_valid, rs_nonzero, vcp_nonzero, trend_nonzero
        
        # Source 1: DerivedMinerviniRepo (direct)
        if self.repo and hasattr(self.repo, 'engine'):
            logger.info("[WATCHLIST][DERIVED_SOURCE][TRY] source=derived_minervini as_of=%s", as_of)
            try:
                from trader.db.repos import DerivedMinerviniRepo
                minervini_repo = DerivedMinerviniRepo(self.repo.engine)
                
                # Try direct load first
                derived_rows = minervini_repo.load_derived(
                    env=self.env or "prep",
                    as_of=as_of,
                    allow_fallback=False
                )
                
                is_valid, rs_nonzero, vcp_nonzero, trend_nonzero = _check_quality(derived_rows, "derived_minervini")
                
                if not derived_rows:
                    logger.info("[WATCHLIST][DERIVED_SOURCE][MISS] source=derived_minervini reason=no_rows trying_fallback=True")
                    
                    # Try with fallback
                    derived_rows = minervini_repo.load_derived(
                        env=self.env or "prep",
                        as_of=as_of,
                        allow_fallback=True,
                        ttl_days=7
                    )
                    
                    is_valid, rs_nonzero, vcp_nonzero, trend_nonzero = _check_quality(derived_rows, "derived_minervini_fallback")
                    
                    if not derived_rows:
                        logger.warning("[WATCHLIST][DERIVED_SOURCE][MISS] source=derived_minervini_fallback reason=no_rows")
                    elif not is_valid:
                        logger.warning(
                            "[WATCHLIST][DERIVED_SOURCE][REJECT] source=derived_minervini_fallback reason=all_scores_zero rows=%d",
                            len(derived_rows)
                        )
                        derived_rows = []  # Reject all-zero source
                
                if derived_rows and is_valid:
                    for row in derived_rows:
                        sym = self._norm_symbol(row)
                        if sym:
                            derived_map[sym] = row
                    
                    logger.info(
                        "[WATCHLIST][DERIVED_SOURCE][OK] source=derived_minervini rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d",
                        len(derived_rows), rs_nonzero, vcp_nonzero, trend_nonzero
                    )
                    return derived_map  # Success - use this source
                
            except Exception as e:
                logger.error(
                    "[WATCHLIST][DERIVED_SOURCE][ERROR] source=derived_minervini error=%s",
                    str(e),
                    exc_info=True
                )
        
        # Source 2: pb1_universe_scored watchlist bundle (fallback)
        if self.repo and self.env:
            logger.info("[WATCHLIST][DERIVED_SOURCE][TRY] source=pb1_universe_scored as_of=%s", as_of)
            try:
                loaded = self.repo.load_watchlist(
                    env=self.env,
                    strategy="pb1_universe_scored",
                    as_of=as_of,
                    allow_latest_fallback=True,  # Allow fallback for this source too
                )
                scored_rows: List[Any] = []
                if isinstance(loaded, tuple):
                    scored_rows = loaded[0] or []
                else:
                    scored_rows = loaded or []
                
                is_valid, rs_nonzero, vcp_nonzero, trend_nonzero = _check_quality(scored_rows, "pb1_universe_scored")
                
                if not scored_rows:
                    logger.warning("[WATCHLIST][DERIVED_SOURCE][MISS] source=pb1_universe_scored reason=no_rows")
                elif not is_valid:
                    logger.warning(
                        "[WATCHLIST][DERIVED_SOURCE][REJECT] source=pb1_universe_scored reason=all_scores_zero rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d",
                        len(scored_rows), rs_nonzero, vcp_nonzero, trend_nonzero
                    )
                else:
                    # Valid fallback source
                    for row in scored_rows:
                        sym = self._norm_symbol(row)
                        if sym and sym not in derived_map:
                            derived_map[sym] = row
                    
                    logger.info(
                        "[WATCHLIST][DERIVED_SOURCE][OK] source=pb1_universe_scored rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d",
                        len(scored_rows), rs_nonzero, vcp_nonzero, trend_nonzero
                    )
                    return derived_map  # Success - use fallback
                    
            except Exception as e:
                logger.warning(
                    "[WATCHLIST][DERIVED_SOURCE][ERROR] source=pb1_universe_scored error=%s",
                    str(e)
                )
        
        # If we get here, no valid source was found
        if not derived_map:
            error_msg = "No valid derived score source found - all sources either empty or all-zero scores"
            logger.error("[WATCHLIST][DERIVED_SOURCE][FAIL] %s", error_msg)
            raise RuntimeError(error_msg)
        
        return derived_map

    def _merge_derived_scores(self, rows: List[Any], as_of: date) -> List[Any]:
        """
        Merge derived Minervini scores into watchlist rows.
        
        Uses _load_minervini_source_map to get scores from all available sources.
        Falls back to calculated values only when source data is unavailable.
        """
        derived_map = self._load_minervini_source_map(as_of)
        
        out: List[Any] = []
        for row in rows:
            sym = self._norm_symbol(row)
            ref = derived_map.get(sym) if sym else None

            # Priority: ref source -> current row -> fallback calculation
            rs_percentile = _prefer_valid_numeric(_row_get(ref, "rs_percentile", None), _row_get(row, "rs_percentile", None))
            rs_score = _prefer_valid_numeric(_row_get(ref, "rs_score", None), _row_get(row, "rs_score", None))
            vcp_score = _prefer_valid_numeric(_row_get(ref, "vcp_score", None), _row_get(row, "vcp_score", None))
            trend_score = _prefer_valid_numeric(_row_get(ref, "trend_score", None), _row_get(row, "trend_score", None))
            
            # Also copy MA and price data from ref if available
            ma20 = _prefer_valid_numeric(_row_get(ref, "ma20", None), _row_get(row, "ma20", None), zero_invalid=True)
            ma50 = _prefer_valid_numeric(_row_get(ref, "ma50", None), _row_get(row, "ma50", None), zero_invalid=True)
            ma150 = _prefer_valid_numeric(_row_get(ref, "ma150", None), _row_get(row, "ma150", None), zero_invalid=True)
            close = _prefer_valid_numeric(_row_get(ref, "close", None), _row_get(row, "close", None), zero_invalid=True)
            volume = _prefer_valid_numeric(_row_get(ref, "volume", None), _row_get(row, "volume", None), zero_invalid=True)
            volume_avg20 = _prefer_valid_numeric(_row_get(ref, "volume_avg20", None), _row_get(row, "volume_avg20", None), zero_invalid=True)
            atr_pct = _prefer_valid_numeric(_row_get(ref, "atr_pct", None), _row_get(row, "atr_pct", None), zero_invalid=True)
            
            # Copy breakout/pullback/momentum scores if present in ref
            breakout_score = _prefer_valid_numeric(_row_get(ref, "breakout_score", None), _row_get(row, "breakout_score", None))
            pullback_score = _prefer_valid_numeric(_row_get(ref, "pullback_score", None), _row_get(row, "pullback_score", None))
            momentum_score = _prefer_valid_numeric(_row_get(ref, "momentum_score", None), _row_get(row, "momentum_score", None))
            entry_style = _row_get(ref, "entry_style_selected", _row_get(row, "entry_style_selected", None))

            # Fallback 1: rs_score가 없으면 rs_percentile 사용
            if (rs_score is None or rs_score <= 0) and rs_percentile is not None and rs_percentile > 0:
                rs_score = rs_percentile

            # Fallback 2: trend_score가 없으면 MA 구조로 즉석 계산
            if trend_score is None or trend_score <= 0:
                tmp = 0.0
                if close is not None and ma20 is not None and close > 0 and ma20 > 0 and close >= ma20:
                    tmp += 25.0
                if ma20 is not None and ma50 is not None and ma20 > 0 and ma50 > 0 and ma20 >= ma50:
                    tmp += 25.0
                if ma50 is not None and ma150 is not None and ma50 > 0 and ma150 > 0 and ma50 >= ma150:
                    tmp += 25.0
                # MA150 slope check (simplified)
                if ma150 is not None and ma150 > 0:
                    tmp += 25.0
                trend_score = tmp

            # Set all fields
            _row_set(row, "rs_percentile", rs_percentile)
            _row_set(row, "rs_score", rs_score)
            _row_set(row, "vcp_score", vcp_score)
            _row_set(row, "trend_score", trend_score)
            _row_set(row, "ma20", ma20)
            _row_set(row, "ma50", ma50)
            _row_set(row, "ma150", ma150)
            _row_set(row, "close", close)
            _row_set(row, "volume", volume)
            _row_set(row, "volume_avg20", volume_avg20)
            _row_set(row, "atr_pct", atr_pct)
            _row_set(row, "breakout_score", breakout_score)
            _row_set(row, "pullback_score", pullback_score)
            _row_set(row, "momentum_score", momentum_score)
            if entry_style:
                _row_set(row, "entry_style_selected", entry_style)

            out.append(row)

        # Enhanced logging with all score types
        rs_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "rs_score", 0.0)) > 0)
        vcp_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "vcp_score", 0.0)) > 0)
        trend_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "trend_score", 0.0)) > 0)
        breakout_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "breakout_score", 0.0)) > 0)
        pullback_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "pullback_score", 0.0)) > 0)
        momentum_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "momentum_score", 0.0)) > 0)
        
        logger.info(
            "[WATCHLIST][DERIVED_MERGE] rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d breakout_nonnull=%d pullback_nonnull=%d momentum_nonnull=%d",
            len(out),
            rs_nonzero,
            vcp_nonzero,
            trend_nonzero,
            breakout_nonzero,
            pullback_nonzero,
            momentum_nonzero,
        )
        
        # Quality check: verify merge result quality
        min_expected_rows = int(len(rows) * 0.90)  # 90% of input rows
        quality_failures = []
        
        if len(out) < min_expected_rows:
            quality_failures.append(f"row_count_low:{len(out)}<{min_expected_rows}")
        
        if rs_nonzero == 0 and vcp_nonzero == 0 and trend_nonzero == 0:
            quality_failures.append("all_minervini_scores_zero")

        entry_recompute_summary: Dict[str, int] = {
            "breakout_valid": 0,
            "pullback_valid": 0,
            "momentum_valid": 0,
            "breakout_nonzero": breakout_nonzero,
            "pullback_nonzero": pullback_nonzero,
            "momentum_nonzero": momentum_nonzero,
        }
        recompute_ran = False

        if breakout_nonzero == 0 and pullback_nonzero == 0 and momentum_nonzero == 0:
            recompute_ran = True
            logger.warning(
                "[WATCHLIST][ENTRY_RECOMPUTE][START] reason=all_entry_scores_zero rows=%d",
                len(out),
            )
            out, entry_recompute_summary = self._recompute_entry_scores(out, as_of=as_of)
            breakout_nonzero = entry_recompute_summary["breakout_nonzero"]
            pullback_nonzero = entry_recompute_summary["pullback_nonzero"]
            momentum_nonzero = entry_recompute_summary["momentum_nonzero"]
            logger.info(
                "[WATCHLIST][ENTRY_RECOMPUTE][DONE] breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
                breakout_nonzero,
                pullback_nonzero,
                momentum_nonzero,
            )

            if breakout_nonzero == 0 and pullback_nonzero == 0 and momentum_nonzero == 0:
                if (
                    entry_recompute_summary["breakout_valid"] == 0
                    and entry_recompute_summary["pullback_valid"] == 0
                    and entry_recompute_summary["momentum_valid"] == 0
                ):
                    quality_failures.append("entry_scores_missing_in_derived_and_recompute_not_run")
                elif (
                    entry_recompute_summary["breakout_valid"] == 0
                    or entry_recompute_summary["pullback_valid"] == 0
                    or entry_recompute_summary["momentum_valid"] == 0
                ):
                    missing_inputs: List[str] = []
                    if entry_recompute_summary["breakout_valid"] == 0:
                        missing_inputs.append("pivot")
                    if entry_recompute_summary["pullback_valid"] == 0:
                        missing_inputs.append("atr")
                    if entry_recompute_summary["momentum_valid"] == 0:
                        missing_inputs.append("rs")
                    quality_failures.append(f"entry_score_inputs_missing:{','.join(missing_inputs)}")
                else:
                    quality_failures.append("entry_score_logic_all_zero_despite_valid_inputs")
                    quality_failures.append("all_entry_scores_zero_after_recompute")

        if (
            not recompute_ran
            and breakout_nonzero == 0
            and pullback_nonzero == 0
            and momentum_nonzero == 0
        ):
            quality_failures.append("entry_scores_missing_in_derived_and_recompute_not_run")
        
        if quality_failures:
            error_msg = f"DERIVED_MERGE quality check failed: {';'.join(quality_failures)}"
            logger.error(
                "[WATCHLIST][DERIVED_MERGE][FAIL] reason=%s breakout_valid=%d pullback_valid=%d momentum_valid=%d",
                ';'.join(quality_failures),
                entry_recompute_summary["breakout_valid"],
                entry_recompute_summary["pullback_valid"],
                entry_recompute_summary["momentum_valid"],
            )
            logger.error("[WATCHLIST][DERIVED_MERGE][FAIL] %s", error_msg)
            raise RuntimeError(error_msg)
        
        return out

    def _numericize_entry_score(self, value: Any, score_name: str) -> float:
        """Normalize booleans/NaN/inf to deterministic 0..100 numeric score."""
        bool_true = 0
        nan_count = 0
        clipped = 0
        out = 0.0

        if isinstance(value, bool):
            bool_true = 1 if value else 0
            out = 100.0 if value else 0.0
        else:
            try:
                out = float(value)
                if pd.isna(out):
                    nan_count = 1
                    out = 0.0
            except Exception:
                out = 0.0

        if out < 0.0:
            out = 0.0
            clipped = 1
        elif out > 100.0:
            out = 100.0
            clipped = 1

        logger.debug(
            "[SCORE][%s][NUMERICIZE] bool_true=%d nan=%d clipped=%d nonzero=%d",
            score_name,
            bool_true,
            nan_count,
            clipped,
            1 if out > 0 else 0,
        )
        return out

    def _recompute_entry_scores(self, rows: List[Any], as_of: date) -> Tuple[List[Any], Dict[str, int]]:
        out = self._recompute_breakout_scores(rows, as_of)
        out = self._recompute_pullback_scores(out, as_of)
        out = self._recompute_momentum_scores(out, as_of)

        summary = {
            "breakout_valid": sum(
                1
                for r in out
                if _safe_float(_row_get(r, "close", 0.0)) > 0
                and (
                    _safe_float(_row_get(r, "pivot", 0.0)) > 0
                    or _safe_float(_row_get(r, "high_55d", _row_get(r, "high55", 0.0))) > 0
                )
            ),
            "pullback_valid": sum(
                1
                for r in out
                if _safe_float(_row_get(r, "close", 0.0)) > 0
                and _safe_float(_row_get(r, "high_55d", _row_get(r, "high55", 0.0))) > 0
                and (
                    _safe_float(_row_get(r, "ma20", 0.0)) > 0
                    or _safe_float(_row_get(r, "ma50", 0.0)) > 0
                )
            ),
            "momentum_valid": sum(
                1
                for r in out
                if _safe_float(_row_get(r, "rs_score", _row_get(r, "rs_percentile", 0.0))) > 0
            ),
            "breakout_nonzero": sum(1 for r in out if _safe_float(_row_get(r, "breakout_score", 0.0)) > 0),
            "pullback_nonzero": sum(1 for r in out if _safe_float(_row_get(r, "pullback_score", 0.0)) > 0),
            "momentum_nonzero": sum(1 for r in out if _safe_float(_row_get(r, "momentum_score", 0.0)) > 0),
        }
        return out, summary

    def _recompute_breakout_scores(self, rows: List[Any], as_of: date) -> List[Any]:
        out: List[Any] = []
        has_close = has_high = has_volume = has_pivot = 0
        valid_inputs = nonzero = missing_pivot = missing_volume = failed_condition = 0

        for row in rows:
            close = _safe_float(_row_get(row, "close", 0.0))
            high = _safe_float(_row_get(row, "high_55d", _row_get(row, "high55", 0.0)))
            volume = _safe_float(_row_get(row, "volume", 0.0))
            volume_avg20 = _safe_float(_row_get(row, "volume_avg20", _row_get(row, "vol20", 0.0)))
            pivot = _safe_float(_row_get(row, "pivot", 0.0))

            has_close += 1 if close > 0 else 0
            has_high += 1 if high > 0 else 0
            has_volume += 1 if volume > 0 else 0
            has_pivot += 1 if pivot > 0 else 0

            if pivot <= 0 and high <= 0:
                missing_pivot += 1
            if volume <= 0 or volume_avg20 <= 0:
                missing_volume += 1

            score = self._compute_breakout_score(row)
            score = self._numericize_entry_score(score, "BREAKOUT")
            _row_set(row, "breakout_score", score)

            if close > 0 and (pivot > 0 or high > 0):
                valid_inputs += 1
            if score > 0:
                nonzero += 1
            else:
                failed_condition += 1
            out.append(row)

        logger.info(
            "[SCORE][BREAKOUT][INPUTS] rows=%d has_close=%d has_high=%d has_volume=%d has_pivot=%d",
            len(out),
            has_close,
            has_high,
            has_volume,
            has_pivot,
        )
        logger.info(
            "[SCORE][BREAKOUT][RESULT] rows=%d valid_inputs=%d nonzero=%d missing_pivot=%d missing_volume=%d failed_condition=%d",
            len(out),
            valid_inputs,
            nonzero,
            missing_pivot,
            missing_volume,
            failed_condition,
        )
        return out

    def _recompute_pullback_scores(self, rows: List[Any], as_of: date) -> List[Any]:
        out: List[Any] = []
        has_close = has_ma = has_atr = has_pullback_depth = 0
        valid_inputs = nonzero = missing_ma = missing_atr = outside_depth_range = 0

        for row in rows:
            close = _safe_float(_row_get(row, "close", 0.0))
            ma20 = _safe_float(_row_get(row, "ma20", 0.0))
            ma50 = _safe_float(_row_get(row, "ma50", 0.0))
            atr = _safe_float(_row_get(row, "atr", _row_get(row, "atr14", 0.0)))
            high_55d = _safe_float(_row_get(row, "high_55d", _row_get(row, "high55", 0.0)))
            pullback_depth = ((high_55d - close) / high_55d) * 100.0 if high_55d > 0 and close > 0 else 0.0

            has_close += 1 if close > 0 else 0
            has_ma += 1 if (ma20 > 0 or ma50 > 0) else 0
            has_atr += 1 if atr > 0 else 0
            has_pullback_depth += 1 if high_55d > 0 and close > 0 else 0

            if ma20 <= 0 and ma50 <= 0:
                missing_ma += 1
            if atr <= 0:
                missing_atr += 1
            if not (1.0 <= pullback_depth <= 25.0):
                outside_depth_range += 1

            score = self._compute_pullback_score(row)
            score = self._numericize_entry_score(score, "PULLBACK")
            _row_set(row, "pullback_score", score)

            if close > 0 and high_55d > 0 and (ma20 > 0 or ma50 > 0):
                valid_inputs += 1
            if score > 0:
                nonzero += 1
            out.append(row)

        logger.info(
            "[SCORE][PULLBACK][INPUTS] rows=%d has_close=%d has_ma=%d has_atr=%d has_pullback_depth=%d",
            len(out),
            has_close,
            has_ma,
            has_atr,
            has_pullback_depth,
        )
        logger.info(
            "[SCORE][PULLBACK][RESULT] rows=%d valid_inputs=%d nonzero=%d missing_ma=%d missing_atr=%d outside_depth_range=%d",
            len(out),
            valid_inputs,
            nonzero,
            missing_ma,
            missing_atr,
            outside_depth_range,
        )
        return out

    def _recompute_momentum_scores(self, rows: List[Any], as_of: date) -> List[Any]:
        out: List[Any] = []
        has_rs = has_trend = has_return = 0
        valid_inputs = nonzero = missing_rs = weak_trend = 0

        for row in rows:
            rs = _safe_float(_row_get(row, "rs_score", _row_get(row, "rs_percentile", 0.0)))
            trend = _safe_float(_row_get(row, "trend_score", 0.0))
            ret20 = _safe_float(_row_get(row, "ret_20d", _row_get(row, "ret20", 0.0)))
            ret60 = _safe_float(_row_get(row, "ret_60d", _row_get(row, "ret60", 0.0)))
            ret120 = _safe_float(_row_get(row, "ret_120d", _row_get(row, "ret120", 0.0)))
            has_return_row = 1 if (ret20 != 0 or ret60 != 0 or ret120 != 0) else 0

            has_rs += 1 if rs > 0 else 0
            has_trend += 1 if trend > 0 else 0
            has_return += has_return_row

            if rs <= 0:
                missing_rs += 1
            if trend <= 0:
                weak_trend += 1

            score = self._compute_momentum_score(row)
            score = self._numericize_entry_score(score, "MOMENTUM")
            _row_set(row, "momentum_score", score)

            if rs > 0:
                valid_inputs += 1
            if score > 0:
                nonzero += 1
            out.append(row)

        logger.info(
            "[SCORE][MOMENTUM][INPUTS] rows=%d has_rs=%d has_trend=%d has_return=%d",
            len(out),
            has_rs,
            has_trend,
            has_return,
        )
        logger.info(
            "[SCORE][MOMENTUM][RESULT] rows=%d valid_inputs=%d nonzero=%d missing_rs=%d weak_trend=%d",
            len(out),
            valid_inputs,
            nonzero,
            missing_rs,
            weak_trend,
        )
        return out
    
    def _compute_breakout_score(self, row: Any) -> float | None:
        """
        Calculate breakout score based on:
        - Proximity to 20/55-day highs
        - Volume surge
        - Resistance breakout
        
        Returns score in 0-100 range.
        """
        close = _sanitize_entry_input(_row_get(row, "close", None))
        high_20d = _sanitize_entry_input(_row_get(row, "high_20d", _row_get(row, "high20", None)))
        high_55d = _sanitize_entry_input(_row_get(row, "high_55d", _row_get(row, "high55", None)))
        pivot = _sanitize_entry_input(_row_get(row, "pivot_price", _row_get(row, "pivot", None)))
        volume = _sanitize_entry_input(_row_get(row, "volume", None))
        volume_avg20 = _sanitize_entry_input(_row_get(row, "volume_avg20", _row_get(row, "vol20", None)))
        breakout_ref = pivot if pivot > 0 else high_20d if high_20d > 0 else high_55d
        anomalies = list(_row_get(row, "score_anomalies", []))

        placeholder_fields = [
            field
            for field, value in {
                "close": _row_get(row, "close", None),
                "pivot_price": _row_get(row, "pivot_price", _row_get(row, "pivot", None)),
                "high_20d": _row_get(row, "high_20d", _row_get(row, "high20", None)),
                "high_55d": _row_get(row, "high_55d", _row_get(row, "high55", None)),
                "volume": _row_get(row, "volume", None),
                "volume_avg20": _row_get(row, "volume_avg20", _row_get(row, "vol20", None)),
            }.items()
            if is_placeholder_entry_value(value)
        ]
        if placeholder_fields:
            anomalies.append("breakout_placeholder_inputs")
            logger.warning(
                "[WATCHLIST][ENTRY][PLACEHOLDER] code=%s style=BREAKOUT fields=%s",
                str(_row_get(row, "code", "") or "").zfill(6),
                placeholder_fields,
            )

        score = 0.0
        if close is None or breakout_ref is None or close <= 0 or breakout_ref <= 0:
            anomalies.append("breakout_missing_price_context")
        if volume_avg20 is None or volume_avg20 <= 0:
            anomalies.append("breakout_missing_volume_context")
        if close is None or breakout_ref is None or volume is None or volume_avg20 is None or close <= 0 or breakout_ref <= 0 or volume <= 0 or volume_avg20 <= 0:
            _row_set(row, "score_anomalies", sorted(set(anomalies)))
            return None

        if close > 0 and breakout_ref > 0:
            distance_pct = ((close - breakout_ref) / breakout_ref) * 100.0
            if distance_pct >= 0.0:
                score += 44.0
            elif distance_pct >= -2.0:
                score += 28.0
            elif distance_pct >= -5.0:
                score += 12.0
            if distance_pct > 7.0:
                score -= min(18.0, (distance_pct - 7.0) * 2.0)
            if distance_pct > 0.0 and distance_pct <= 3.0:
                score += 18.0
            if distance_pct < -6.0:
                anomalies.append("breakout_far_from_pivot")

        if volume > 0 and volume_avg20 > 0:
            vol_ratio = volume / volume_avg20
            if vol_ratio >= 1.8:
                score += 30.0
            elif vol_ratio >= 1.5:
                score += 24.0
            elif vol_ratio >= 1.2:
                score += 16.0
            elif vol_ratio < 0.8:
                score -= 12.0

        if close > 0 and high_55d > 0 and close >= high_55d * 0.98:
            score += 18.0
        _row_set(row, "breakout_trigger_ok", bool(score >= 60.0 and close > 0 and breakout_ref > 0 and close >= breakout_ref))
        _row_set(row, "score_anomalies", sorted(set(anomalies)))
        
        return max(0.0, min(score, 100.0))

    def _compute_pullback_score(self, row: Any) -> float | None:
        """
        Calculate pullback score based on:
        - Staying above key MAs during pullback
        - Pullback depth in optimal range (3-18%)
        - Volume contraction during pullback
        
        Returns score in 0-100 range.
        """
        close = _sanitize_entry_input(_row_get(row, "close", None))
        ma20 = _safe_float(_row_get(row, "ma20", 0.0))
        ma50 = _safe_float(_row_get(row, "ma50", 0.0))
        high_55d = _sanitize_entry_input(_row_get(row, "high_55d", _row_get(row, "high55", None)))
        volume = _sanitize_entry_input(_row_get(row, "volume", None))
        volume_avg20 = _sanitize_entry_input(_row_get(row, "volume_avg20", _row_get(row, "vol20", None)))
        anomalies = list(_row_get(row, "score_anomalies", []))

        placeholder_fields = [
            field
            for field, value in {
                "close": _row_get(row, "close", None),
                "high_55d": _row_get(row, "high_55d", _row_get(row, "high55", None)),
                "volume": _row_get(row, "volume", None),
                "volume_avg20": _row_get(row, "volume_avg20", _row_get(row, "vol20", None)),
            }.items()
            if is_placeholder_entry_value(value)
        ]
        if placeholder_fields:
            anomalies.append("pullback_placeholder_inputs")
            logger.warning(
                "[WATCHLIST][ENTRY][PLACEHOLDER] code=%s style=PULLBACK fields=%s",
                str(_row_get(row, "code", "") or "").zfill(6),
                placeholder_fields,
            )

        score = 0.0
        if close is None or high_55d is None or volume is None or volume_avg20 is None or close <= 0 or high_55d <= 0 or (ma20 <= 0 and ma50 <= 0) or volume <= 0 or volume_avg20 <= 0:
            anomalies.append("pullback_missing_context")
            _row_set(row, "score_anomalies", sorted(set(anomalies)))
            return None

        if close > 0 and ma20 > 0 and close >= ma20:
            score += 20.0
        if close > 0 and ma50 > 0 and close >= ma50:
            score += 22.0

        if close > 0 and high_55d > 0:
            pullback_pct = ((high_55d - close) / high_55d) * 100.0
            if 3.0 <= pullback_pct <= 18.0:
                score += 40.0
            elif 1.0 <= pullback_pct <= 25.0:
                score += 25.0
            else:
                anomalies.append("pullback_depth_outlier")

        if volume > 0 and volume_avg20 > 0:
            vol_ratio = volume / volume_avg20
            if vol_ratio < 0.8:
                score += 30.0
            elif vol_ratio < 1.0:
                score += 18.0
            elif vol_ratio > 1.4:
                score -= 10.0
        if close > 0 and ma20 > 0 and ma50 > 0 and abs(close - ma20) / close <= 0.03:
            score += 10.0
        _row_set(row, "pullback_trigger_ok", bool(score >= 55.0 and close > 0 and ma20 > 0 and ma50 > 0))
        _row_set(row, "score_anomalies", sorted(set(anomalies)))

        return max(0.0, min(score, 100.0))

    def _compute_momentum_score(self, row: Any) -> float | None:
        """
        Calculate momentum score based on:
        - 20/60/120-day returns
        - Relative strength maintenance
        
        Returns score in 0-100 range.
        """
        close = _sanitize_entry_input(_row_get(row, "close", None))
        ma50 = _safe_nullable_float(_row_get(row, "ma50", None))
        ma150 = _safe_nullable_float(_row_get(row, "ma150", None))
        hi_52w = _sanitize_entry_input(_row_get(row, "hi_52w", _row_get(row, "high_52w", None)))
        rs_score = _safe_nullable_float(_row_get(row, "rs_percentile", _row_get(row, "rs_score", None)))
        anomalies = list(_row_get(row, "score_anomalies", []))

        placeholder_fields = [
            field
            for field, value in {
                "close": _row_get(row, "close", None),
                "hi_52w": _row_get(row, "hi_52w", _row_get(row, "high_52w", None)),
            }.items()
            if is_placeholder_entry_value(value)
        ]
        if placeholder_fields:
            anomalies.append("momentum_placeholder_inputs")
            logger.warning(
                "[WATCHLIST][ENTRY][PLACEHOLDER] code=%s style=MOMENTUM fields=%s",
                str(_row_get(row, "code", "") or "").zfill(6),
                placeholder_fields,
            )

        components: list[float] = []
        price_vs_ma50 = ((close / ma50) - 1.0) if close is not None and ma50 is not None and ma50 > 0 else None
        price_vs_ma150 = ((close / ma150) - 1.0) if close is not None and ma150 is not None and ma150 > 0 else None
        dist_to_52w = (close / hi_52w) if close is not None and hi_52w is not None and hi_52w > 0 else None

        if price_vs_ma50 is not None:
            components.append(min(max(price_vs_ma50 / 0.15, 0.0), 1.0))
        if price_vs_ma150 is not None:
            components.append(min(max(price_vs_ma150 / 0.30, 0.0), 1.0))
        if dist_to_52w is not None:
            components.append(min(max((dist_to_52w - 0.75) / 0.25, 0.0), 1.0))

        rs_term = 0.0
        if rs_score is not None:
            rs_term = float(rs_score)
            if rs_term > 1.0:
                rs_term = rs_term / 100.0
            rs_term = min(max(rs_term, 0.0), 1.0)
        components.append(rs_term)

        if not any(component > 0 for component in components):
            anomalies.append("momentum_missing_context")
            _row_set(row, "score_anomalies", sorted(set(anomalies)))
            return None if close is None and hi_52w is None and rs_score is None else 0.0

        score = round(100.0 * sum(components) / len(components), 4) if components else 0.0
        if len(set(round(component, 4) for component in components)) <= 1:
            anomalies.append("momentum_low_dispersion")
        _row_set(row, "momentum_trigger_ok", bool(score >= 55.0 and rs_term >= 0.8))
        _row_set(row, "score_anomalies", sorted(set(anomalies)))

        return max(0.0, min(score, 100.0))

    def _compute_tech_score(self, row: Any) -> float:
        """
        Calculate comprehensive tech score with 5 components:
        1. RS component (30%)
        2. VCP component (20%)
        3. Trend component (20%)
        4. Entry component (20%) - max(breakout, pullback, momentum)
        5. Liquidity component (10%)
        
        All components are 0-100 normalized.
        Returns final score in 0-100 range.
        """
        # 1. RS Component
        rs_pct = _safe_float(_row_get(row, "rs_percentile", 0.0))
        rs_score = _safe_float(_row_get(row, "rs_score", rs_pct))
        rs_component = max(0.0, min(rs_score if rs_score > 0 else rs_pct, 100.0))
        
        # 2. VCP Component
        vcp_score = _safe_float(_row_get(row, "vcp_score", 0.0))
        if vcp_score <= 0:
            # Fallback VCP calculation
            close = _safe_float(_row_get(row, "close", 0.0))
            high_20d = _safe_float(_row_get(row, "high_20d", _row_get(row, "high20", 0.0)))
            volume = _safe_float(_row_get(row, "volume", 0.0))
            volume_avg20 = _safe_float(_row_get(row, "volume_avg20", 0.0))
            
            vcp_tmp = 0.0
            # Volatility contraction (8-12 week pattern) - simplified
            if volume > 0 and volume_avg20 > 0 and volume / volume_avg20 < 1.0:
                vcp_tmp += 40.0
            # Near high
            if close > 0 and high_20d > 0 and close / high_20d >= 0.90:
                vcp_tmp += 30.0
            # Volume contraction
            if volume > 0 and volume_avg20 > 0 and volume / volume_avg20 < 0.8:
                vcp_tmp += 30.0
            
            vcp_component = max(0.0, min(vcp_tmp, 100.0))
        else:
            vcp_component = max(0.0, min(vcp_score, 100.0))
        
        # 3. Trend Component
        trend_score = _safe_float(_row_get(row, "trend_score", 0.0))
        if trend_score <= 0:
            # Fallback trend calculation
            close = _safe_float(_row_get(row, "close", 0.0))
            ma20 = _safe_float(_row_get(row, "ma20", 0.0))
            ma50 = _safe_float(_row_get(row, "ma50", 0.0))
            ma150 = _safe_float(_row_get(row, "ma150", 0.0))
            
            trend_tmp = 0.0
            if close > 0 and ma20 > 0 and close >= ma20:
                trend_tmp += 25.0
            if ma20 > 0 and ma50 > 0 and ma20 >= ma50:
                trend_tmp += 25.0
            if ma50 > 0 and ma150 > 0 and ma50 >= ma150:
                trend_tmp += 25.0
            if ma150 > 0:  # Simplified MA150 slope positive check
                trend_tmp += 25.0
            
            trend_component = max(0.0, min(trend_tmp, 100.0))
        else:
            trend_component = max(0.0, min(trend_score, 100.0))
        
        # 4. Entry Component - Calculate or retrieve all three entry styles
        breakout_score = _safe_nullable_float(_row_get(row, "breakout_score", None))
        pullback_score = _safe_nullable_float(_row_get(row, "pullback_score", None))
        momentum_score = _safe_nullable_float(_row_get(row, "momentum_score", None))
        
        # Calculate if not present
        if breakout_score is None:
            breakout_score = self._compute_breakout_score(row)
            _row_set(row, "breakout_score", breakout_score)
        
        if pullback_score is None:
            pullback_score = self._compute_pullback_score(row)
            _row_set(row, "pullback_score", pullback_score)
        
        if momentum_score is None:
            momentum_score = self._compute_momentum_score(row)
            _row_set(row, "momentum_score", momentum_score)
        
        if breakout_score is None and pullback_score is None and momentum_score is None:
            _row_set(row, "entry_component", {"invalid_entry_inputs": True, "reason": "all_entry_scores_missing"})
            _row_set(row, "entry_style_selected", None)
            return round(max(0.0, min((rs_component * 0.30) + (vcp_component * 0.20) + (trend_component * 0.20), 100.0)), 4)

        # Entry component = max of the three styles
        entry_values = [score for score in (breakout_score, pullback_score, momentum_score) if score is not None]
        entry_component = max(entry_values) if entry_values else 0.0
        _row_set(row, "entry_component", entry_component)
        
        # Determine selected entry style
        tie_break = [
            ("BREAKOUT", breakout_score, _safe_float(_row_get(row, "pivot_price", _row_get(row, "pivot", 0.0))), _safe_float(_row_get(row, "rs_percentile", 0.0))),
            ("PULLBACK", pullback_score, -abs(_safe_float(_row_get(row, "pullback_pct", 0.0)) - 10.0), _safe_float(_row_get(row, "trend_score", 0.0))),
            ("MOMENTUM", momentum_score, _safe_float(_row_get(row, "rs_percentile", 0.0)), _safe_float(_row_get(row, "trend_score", 0.0))),
        ]
        tie_break.sort(key=lambda item: (item[1], item[2], item[3]), reverse=True)
        if entry_component < 25.0:
            entry_style_selected = "NEUTRAL"
        else:
            top_style, top_score, _ctx1, _ctx2 = tie_break[0]
            second_score = tie_break[1][1] if len(tie_break) > 1 else -1.0
            if abs(top_score - second_score) <= 2.0 and rs_component >= trend_component and top_style != "MOMENTUM":
                entry_style_selected = "MOMENTUM"
            else:
                entry_style_selected = top_style
        _row_set(row, "entry_style_selected", entry_style_selected)
        _row_set(row, "breakout_pass", breakout_score >= 55.0)
        _row_set(row, "pullback_pass", pullback_score >= 55.0)
        _row_set(row, "momentum_pass", momentum_score >= 55.0)
        
        # 5. Liquidity Component
        volume = _safe_float(_row_get(row, "volume", 0.0))
        volume_avg20 = _safe_float(_row_get(row, "volume_avg20", 0.0))
        
        liquidity_component = 20.0  # Default low score
        if volume_avg20 > 0:
            vol_ratio = volume / volume_avg20
            if vol_ratio >= 2.0:
                liquidity_component = 100.0
            elif vol_ratio >= 1.5:
                liquidity_component = 80.0
            elif vol_ratio >= 1.2:
                liquidity_component = 60.0
            elif vol_ratio >= 1.0:
                liquidity_component = 40.0
        
        # Final weighted tech score
        tech_score = (
            rs_component * 0.30 +
            vcp_component * 0.20 +
            trend_component * 0.20 +
            entry_component * 0.20 +
            liquidity_component * 0.10
        )
        
        return round(max(0.0, min(tech_score, 100.0)), 4)

    def _compute_flow_score(self, row: Any) -> float:
        """
        한국 시장용 수급 점수.
        foreign / institution 관련 값이 없으면 0 반환.
        """
        meta = _row_get(row, "meta", {})
        if not isinstance(meta, dict):
            meta = {}

        foreign_net = _safe_float(_row_get(row, "foreign_net_buy", _row_get(row, "foreign_net_buy_5d", _row_get(meta, "foreign_net_buy", _row_get(meta, "foreign_net_buy_5d", 0.0)))))
        inst_net = _safe_float(_row_get(row, "institution_net_buy", _row_get(row, "institution_net_buy_5d", _row_get(meta, "institution_net_buy", _row_get(meta, "institution_net_buy_5d", 0.0)))))
        total_value = _safe_float(_row_get(row, "trading_value", _row_get(row, "liq_avg", _row_get(meta, "trading_value", _row_get(meta, "liq_avg", 0.0)))))

        raw = foreign_net * 0.6 + inst_net * 0.4

        liquidity_penalty = 1.0
        if total_value > 0:
            if total_value < 1_000_000_000:
                liquidity_penalty = 0.7
            elif total_value < 5_000_000_000:
                liquidity_penalty = 0.85

        raw *= liquidity_penalty

        if raw >= 10_000_000_000:
            score = 100.0
        elif raw >= 5_000_000_000:
            score = 80.0
        elif raw >= 1_000_000_000:
            score = 60.0
        elif raw >= 0:
            score = 40.0
        elif raw >= -1_000_000_000:
            score = 20.0
        else:
            score = 0.0

        return round(score, 4)

    def _compute_final_score(self, row: Any) -> float:
        tech_score = _safe_float(_row_get(row, "tech_score", 0.0))
        flow_score = _safe_float(_row_get(row, "flow_score", 0.0))
        final_score = tech_score * 0.7 + flow_score * 0.3
        return round(final_score, 4)

    def _attach_scores(self, rows: List[Any], stage_name: str) -> List[Any]:
        """
        Attach tech_score, flow_score, and final_score to all rows.
        Enhanced with entry-style scores logging.
        """
        out: List[Any] = []

        for row in rows:
            tech_score = self._compute_tech_score(row)
            flow_score = self._compute_flow_score(row)
            final_score = round(tech_score * 0.7 + flow_score * 0.3, 4) if tech_score is not None else None

            _row_set(row, "tech_score", tech_score)
            _row_set(row, "flow_score", flow_score)
            _row_set(row, "score_final", final_score)
            _row_set(row, "final_score", final_score)

            out.append(row)

        # Enhanced logging with all score types
        tech_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "tech_score", 0.0)) > 0)
        final_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "score_final", 0.0)) > 0)
        breakout_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "breakout_score", 0.0)) > 0)
        pullback_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "pullback_score", 0.0)) > 0)
        momentum_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "momentum_score", 0.0)) > 0)
        if score_distribution_is_monoculture([_row_get(r, "breakout_score", None) for r in out]):
            logger.error("[ENTRY_STYLE][MONOCULTURE][BREAKOUT] dominant=%s", dominant_value_ratio([_row_get(r, "breakout_score", None) for r in out]))
        if score_distribution_is_monoculture([_row_get(r, "pullback_score", None) for r in out]):
            logger.error("[ENTRY_STYLE][MONOCULTURE][PULLBACK] dominant=%s", dominant_value_ratio([_row_get(r, "pullback_score", None) for r in out]))
        if score_distribution_is_monoculture([_row_get(r, "entry_style_selected", None) for r in out]):
            logger.error("[ENTRY_STYLE][MONOCULTURE][STYLE] dominant=%s", dominant_value_ratio([_row_get(r, "entry_style_selected", None) for r in out]))
        
        # Entry style detailed logging
        logger.info(
            "[SCORE][BREAKOUT][%s] input=%d nonzero=%d missing=%d",
            stage_name,
            len(out),
            breakout_nonzero,
            len(out) - breakout_nonzero,
        )
        logger.info(
            "[SCORE][PULLBACK][%s] input=%d nonzero=%d missing=%d",
            stage_name,
            len(out),
            pullback_nonzero,
            len(out) - pullback_nonzero,
        )
        logger.info(
            "[SCORE][MOMENTUM][%s] input=%d nonzero=%d missing=%d",
            stage_name,
            len(out),
            momentum_nonzero,
            len(out) - momentum_nonzero,
        )
        
        logger.info(
            "[WATCHLIST][SCORES][%s] rows=%d tech_nonzero=%d final_nonzero=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
            stage_name,
            len(out),
            tech_nonzero,
            final_nonzero,
            breakout_nonzero,
            pullback_nonzero,
            momentum_nonzero,
        )

        # Sample output with all fields
        logger.info(
            "[WATCHLIST][SCORES][%s][SAMPLE] %s",
            stage_name,
            [
                {
                    "symbol": _row_get(r, "symbol", _row_get(r, "code", None)),
                    "rs_percentile": _row_get(r, "rs_percentile", None),
                    "rs_score": _row_get(r, "rs_score", None),
                    "vcp_score": _row_get(r, "vcp_score", None),
                    "trend_score": _row_get(r, "trend_score", None),
                    "breakout_score": _row_get(r, "breakout_score", None),
                    "pullback_score": _row_get(r, "pullback_score", None),
                    "momentum_score": _row_get(r, "momentum_score", None),
                    "entry_style_selected": _row_get(r, "entry_style_selected", None),
                    "tech_score": _row_get(r, "tech_score", None),
                    "flow_score": _row_get(r, "flow_score", None),
                    "score_final": _row_get(r, "score_final", None),
                }
                for r in out[:5]
            ],
        )

        return out

    def _assert_nonzero_scores(self, rows: List[Any], stage_name: str, score_key: str = "tech_score") -> None:
        if not rows:
            raise RuntimeError(f"{stage_name}_EMPTY")

        nonzero = 0
        sample: List[Dict[str, Any]] = []

        for r in rows:
            val = _safe_float(_row_get(r, score_key, 0.0))
            if val > 0:
                nonzero += 1

        for r in rows[:5]:
            sample.append(
                {
                    "symbol": _row_get(r, "symbol", _row_get(r, "code", None)),
                    "rs_percentile": _row_get(r, "rs_percentile", None),
                    "vcp_score": _row_get(r, "vcp_score", None),
                    "trend_score": _row_get(r, "trend_score", None),
                    "tech_score": _row_get(r, "tech_score", None),
                    "flow_score": _row_get(r, "flow_score", None),
                    "score_final": _row_get(r, "score_final", None),
                }
            )

        if nonzero == 0:
            if stage_name == "A_POOL120" and score_key == "tech_score":
                alt_nonzero = sum(1 for r in rows if _safe_float(_row_get(r, "score_final", 0.0)) > 0)
                if alt_nonzero > 0:
                    logger.warning(
                        "[WATCHLIST][SCORES][DEGRADE] stage=%s tech_score all zero but score_final nonzero=%d",
                        stage_name,
                        alt_nonzero,
                    )
                    return

            logger.info(
                "[WATCHLIST][DEBUG][TOP50_SAMPLE] %s",
                [
                    {
                        "symbol": _row_get(r, "symbol", _row_get(r, "code", None)),
                        "rs_percentile": _row_get(r, "rs_percentile", None),
                        "rs_score": _row_get(r, "rs_score", None),
                        "vcp_score": _row_get(r, "vcp_score", None),
                        "trend_score": _row_get(r, "trend_score", None),
                        "tech_score": _row_get(r, "tech_score", None),
                        "flow_score": _row_get(r, "flow_score", None),
                        "score_final": _row_get(r, "score_final", None),
                    }
                    for r in rows[:5]
                ],
            )
            logger.error(
                "[WATCHLIST][SCORES][FAIL] stage=%s score_key=%s sample=%s",
                stage_name,
                score_key,
                sample,
            )
            raise RuntimeError(f"{stage_name}_SCORES_ALL_ZERO (score_key={score_key})")

        logger.info(
            "[WATCHLIST][SCORES][OK] stage=%s score_key=%s nonzero=%d total=%d",
            stage_name,
            score_key,
            nonzero,
            len(rows),
        )

    def _compute_rs_percentile(self, stock_close: pd.Series, bench_close: Optional[pd.Series]) -> float:
        if bench_close is None or len(stock_close) < RS_LOOKBACK_DAYS or len(bench_close) < RS_LOOKBACK_DAYS:
            return 0.0

        stock_ret = (stock_close.iloc[-1] / stock_close.iloc[-RS_LOOKBACK_DAYS] - 1) * 100
        bench_ret = (bench_close.iloc[-1] / bench_close.iloc[-RS_LOOKBACK_DAYS] - 1) * 100
        rs = stock_ret - bench_ret

        if rs > 20:
            return 90.0
        if rs > 10:
            return 80.0
        if rs > 0:
            return 70.0
        return max(0.0, 50.0 + rs)

    def _compute_vcp_score(self, df: pd.DataFrame) -> float:
        if df is None or df.empty or len(df) < 120:
            return 0.0
        if "high" not in df.columns or "low" not in df.columns:
            return 0.0

        recent_30 = df["high"].tail(30) / df["low"].tail(30) - 1
        prev_90 = df["high"].iloc[-120:-30] / df["low"].iloc[-120:-30] - 1

        recent_vol = float(recent_30.mean())
        prev_vol = float(prev_90.mean())
        if prev_vol == 0:
            return 0.0

        contraction_ratio = recent_vol / prev_vol
        if contraction_ratio < 0.5:
            return 90.0
        if contraction_ratio < 0.7:
            return 80.0
        if contraction_ratio < 0.9:
            return 70.0
        return max(0.0, 100.0 - contraction_ratio * 100.0)

    def _compute_pullback_pct(self, df: pd.DataFrame) -> float:
        if "close" not in df.columns or df.empty:
            return 0.0
        lookback = min(252, len(df))
        high_52w = float(df["close"].tail(lookback).max())
        last_close = float(df["close"].iloc[-1])
        if high_52w <= 0:
            return 0.0
        return max(0.0, (high_52w - last_close) / high_52w)

    def _compute_trend_score(self, df: pd.DataFrame) -> float:
        if "close" not in df.columns or len(df) < 200:
            return 0.0
        close = df["close"]
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1])
        last_close = float(close.iloc[-1])

        checks = [
            last_close > ma20,
            ma20 > ma50,
            ma50 > ma200,
            last_close > ma200,
        ]
        return float(sum(1 for x in checks if x) * 25.0)

    def _compute_atr_pct(self, df: pd.DataFrame, period: int = 14) -> float | None:
        return compute_atr_pct_from_ohlcv(df, period=period)


def log_df_identity(df: pd.DataFrame, label: str) -> None:
    """Log DataFrame object identity and contract info."""
    if df is None or df.empty:
        logger.info("[DEBUG][OBJ][FINAL30_SCORED][%s] rows=0", label)
        return
    
    cols_sorted = sorted(df.columns.tolist())
    cols_hash = hash(tuple(cols_sorted))
    
    ma20_null = int(df["ma20"].isna().sum()) if "ma20" in df.columns else -1
    ma50_null = int(df["ma50"].isna().sum()) if "ma50" in df.columns else -1
    close_null = int(df["close"].isna().sum()) if "close" in df.columns else -1
    
    logger.info(
        "[DEBUG][OBJ][FINAL30_SCORED][%s] id=%s rows=%d ma20_null=%d ma50_null=%d close_null=%d cols_hash=%d",
        label,
        id(df),
        len(df),
        ma20_null,
        ma50_null,
        close_null,
        cols_hash,
    )


def assert_final30_scored_contract(
    df: pd.DataFrame,
    label: str,
    as_of: str,
    hard: bool = True,
) -> dict[str, Any]:
    """Validate final30_scored contract. Raises on hard failure, warns on soft."""
    result = {
        "label": label,
        "ok": True,
        "rows": len(df) if df is not None else 0,
        "uniq_code": 0,
        "errors": [],
        "warnings": [],
    }
    
    if df is None or df.empty:
        result["ok"] = False
        result["errors"].append("EMPTY_DATAFRAME")
        logger.error(
            "[FINAL30][CONTRACT][FAIL] label=%s rows=0 reason=empty",
            label,
        )
        if hard:
            raise ValueError(f"FINAL30_CONTRACT_INVALID_{label}_empty")
        return result

    log_final30_ma_diagnostics(df, f"contract_{label}")
    
    rows = len(df)
    uniq_codes = len(df["code"].unique()) if "code" in df.columns else 0
    result["rows"] = rows
    result["uniq_code"] = uniq_codes
    
    # Check row count
    if rows != 30:
        result["ok"] = False
        result["errors"].append(f"ROWS_MISMATCH expected=30 actual={rows}")
    
    # Check unique codes
    if uniq_codes != 30:
        result["ok"] = False
        result["errors"].append(f"UNIQ_CODE_MISMATCH expected=30 actual={uniq_codes}")
    
    # Check critical fields
    critical_fields = [
        "code",
        "close",
        "ma20",
        "ma50",
        "ma150",
        "atr_pct",
        "score_final",
        "tech_score",
        "breakout_score",
        "pullback_score",
        "momentum_score",
        "rs_percentile",
        "vcp_score",
        "entry_style_selected",
    ]
    
    field_nulls = {}
    for field in critical_fields:
        if field in df.columns:
            null_count = int(df[field].isna().sum())
            field_nulls[field] = null_count
            if null_count > 0:
                result["errors"].append(f"{field}_null count={null_count}")
    
    result["field_nulls"] = field_nulls
    
    # Check entry_style validity
    if "entry_style_selected" in df.columns:
        valid_styles = {"BREAKOUT", "PULLBACK", "MOMENTUM"}
        invalid_count = sum(
            1 for v in df["entry_style_selected"] 
            if pd.notna(v) and str(v).upper() not in valid_styles
        )
        if invalid_count > 0:
            result["errors"].append(f"entry_style_invalid count={invalid_count}")
    
    # Overall result
    if result["errors"]:
        result["ok"] = False
        bad_codes = df["code"].unique()[:5].tolist() if "code" in df.columns else []
        ma20_exists = "ma20" in df.columns
        ma20_null = int(_build_numeric_series(df, "ma20").isna().sum()) if ma20_exists else -1
        # MA20 실패 로그는 실제로 ma20이 없거나 ma20_null > 0인 경우에만 찍는다.
        if (not ma20_exists) or ma20_null > 0:
            candidate_stats = _ma20_candidate_stats(df)
            sample_rows = _ma20_sample_rows(df)
            logger.error(
                "[FINAL30][CONTRACT][FAIL][MA20] label=%s rows=%d uniq_code=%d has_ma20=%s ma20_null=%s candidate_columns=%s sample_rows=%s as_of=%s",
                label,
                rows,
                uniq_codes,
                int(ma20_exists),
                ma20_null,
                candidate_stats,
                sample_rows,
                as_of,
            )
        else:
            logger.info(
                "[FINAL30][CONTRACT][OK][MA20] label=%s rows=%d ma20_null=%s",
                label,
                rows,
                ma20_null,
            )
        logger.error(
            "[FINAL30][CONTRACT][FAIL] label=%s rows=%d uniq_code=%d errors=%s sample_codes=%s",
            label,
            rows,
            uniq_codes,
            result["errors"],
            bad_codes,
        )
        if hard:
            raise ValueError(f"FINAL30_CONTRACT_INVALID_{label} errors={','.join(result['errors'][:3])}")
    else:
        ma20_exists = "ma20" in df.columns
        ma20_null = int(_build_numeric_series(df, "ma20").isna().sum()) if ma20_exists else -1
        score_final_nonzero = int(
            df["score_final"].fillna(0).astype(float).gt(0).sum()
        ) if "score_final" in df.columns else 0
        tech_score_nonzero = int(
            df["tech_score"].fillna(0).astype(float).gt(0).sum()
        ) if "tech_score" in df.columns else 0
        logger.info(
            "[FINAL30][CONTRACT][OK][MA20] label=%s rows=%d ma20_null=%s",
            label,
            rows,
            ma20_null,
        )
        logger.info(
            "[FINAL30][CONTRACT][OK] label=%s rows=%d uniq_code=%d entry_style_invalid=0 ma20_null=%s score_final_nonzero=%s tech_score_nonzero=%s",
            label,
            rows,
            uniq_codes,
            ma20_null,
            score_final_nonzero,
            tech_score_nonzero,
        )
    
    return result


def materialize_final30_price_context(df: pd.DataFrame) -> pd.DataFrame:
    """Materialize critical price fields from meta or aliases."""
    if df is None or df.empty:
        return df
    
    df = df.copy()
    log_final30_ma_diagnostics(df, "materialize_input")
    df = normalize_ma20_column(df, "materialize_input")
    
    # Critical price fields that must be materialized
    price_fields = ["close", "ma20", "ma50", "ma150", "atr_pct"]
    
    for field in price_fields:
        if field in df.columns and df[field].isna().sum() > 0:
            # Try to restore from meta
            null_indices = df[df[field].isna()].index
            for idx in null_indices:
                if pd.isna(df.loc[idx, field]):
                    meta = df.loc[idx, "meta"] if "meta" in df.columns else {}
                    if isinstance(meta, dict) and field in meta:
                        restored_val = _safe_nullable_float(meta.get(field))
                        if restored_val is not None:
                            df.loc[idx, field] = restored_val
                            logger.debug(
                                "[MATERIALIZE][RESTORED] code=%s field=%s source=meta",
                                df.loc[idx, "code"] if "code" in df.columns else "?",
                                field,
                            )
                    if field == "ma20" and pd.isna(df.loc[idx, field]) and isinstance(meta, dict):
                        restored_ma20 = _extract_numeric_from_sources(meta, aliases=MA20_ALIAS_FIELDS, zero_invalid=True)
                        if restored_ma20 is not None:
                            df.loc[idx, field] = restored_ma20
                            logger.warning(
                                "[MATERIALIZE][RESTORED] code=%s field=%s source=meta_alias",
                                df.loc[idx, "code"] if "code" in df.columns else "?",
                                field,
                            )

    log_final30_ma_diagnostics(df, "before_materialize_normalize")
    df = normalize_ma20_column(df, "before_materialize_normalize")
    log_final30_ma_diagnostics(df, "after_materialize_normalize")
    
    # Check restoration success
    for field in price_fields:
        if field in df.columns:
            still_null = int(df[field].isna().sum())
            if still_null > 0:
                logger.warning(
                    "[MATERIALIZE][FAIL] field=%s still_null=%d",
                    field,
                    still_null,
                )
    
    return df


def save_bundle(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    bundle: WatchlistBundle,
) -> None:
    """
    Save watchlist bundle to DB.
    
    Saves all 4 stages:
    - pb1_universe_scored (full universe with scores)
    - pb1_pool120 (A-stage filtered pool)
    - pb1_top50 (B-stage top 50)
    - pb1_watchlist_final (C-stage final 30)
    """
    repo = WatchlistRepo(engine)
    
    logger.info(
        "[BUNDLE][SAVE][START] env=%s as_of=%s universe=%s pool120=%s top50=%s final30=%s",
        env,
        as_of,
        len(bundle.universe_scored),
        len(bundle.pool120),
        len(bundle.top50),
        len(bundle.final30),
    )
    
    # Helper function to log DataFrame fields and scores
    def _log_stage_fields(stage_name: str, stage_data: List[Dict[str, Any]]) -> None:
        if not stage_data:
            return
        df = pd.DataFrame(stage_data)
        has_breakout = 1 if "breakout_score" in df.columns else 0
        has_pullback = 1 if "pullback_score" in df.columns else 0
        has_momentum = 1 if "momentum_score" in df.columns else 0
        has_tech = 1 if "tech_score" in df.columns or "score_tech" in df.columns else 0
        has_final = 1 if "score_final" in df.columns or "final_score" in df.columns else 0
        
        breakout_nonzero = int((df["breakout_score"].fillna(0.0) > 0.0).sum()) if "breakout_score" in df.columns else 0
        pullback_nonzero = int((df["pullback_score"].fillna(0.0) > 0.0).sum()) if "pullback_score" in df.columns else 0
        momentum_nonzero = int((df["momentum_score"].fillna(0.0) > 0.0).sum()) if "momentum_score" in df.columns else 0
        tech_nonzero = int((df["tech_score"].fillna(0.0) > 0.0).sum()) if "tech_score" in df.columns else 0
        if tech_nonzero == 0 and "score_tech" in df.columns:
            tech_nonzero = int((df["score_tech"].fillna(0.0) > 0.0).sum())
        final_nonzero = int((df["score_final"].fillna(0.0) > 0.0).sum()) if "score_final" in df.columns else 0
        if final_nonzero == 0 and "final_score" in df.columns:
            final_nonzero = int((df["final_score"].fillna(0.0) > 0.0).sum())
        
        logger.info(
            "[BUNDLE][DATAFRAME][FIELDS] stage=%s includes_breakout=%s includes_pullback=%s includes_momentum=%s includes_tech=%s includes_final=%s",
            stage_name,
            has_breakout,
            has_pullback,
            has_momentum,
            has_tech,
            has_final,
        )
        logger.info(
            "[BUNDLE][DATAFRAME][SCORES] stage=%s rows=%s breakout_nonzero=%s pullback_nonzero=%s momentum_nonzero=%s tech_nonzero=%s final_nonzero=%s",
            stage_name,
            len(df),
            breakout_nonzero,
            pullback_nonzero,
            momentum_nonzero,
            tech_nonzero,
            final_nonzero,
        )

    def _normalize_scored_stage_rows(
        stage_rows: List[Dict[str, Any]],
        *,
        fallback_rows: List[Dict[str, Any]] | None = None,
    ) -> List[Dict[str, Any]]:
        fallback_by_code = {
            str((row or {}).get("code") or "").zfill(6): dict(row or {})
            for row in (fallback_rows or [])
            if (row or {}).get("code")
        }
        normalized_rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(stage_rows or [], start=1):
            item = _sync_item_and_meta_fields(dict(row or {}))
            code = str(item.get("code") or "").zfill(6)
            if not code:
                continue
            item["code"] = code
            ref = fallback_by_code.get(code, {})
            for key in (
                "breakout_score",
                "pullback_score",
                "momentum_score",
                "rs_percentile",
                "vcp_score",
                "entry_style_selected",
                "hi_52w",
                "lo_52w",
                "pivot_price",
                "vol20",
                "dollar_vol_50",
                "ma20_slope",
                "ma50_slope",
                "ma150_slope",
                "vcp_ok",
                "breakout_trigger_ok",
                "pullback_trigger_ok",
                "momentum_trigger_ok",
                "ma20",
                "ma50",
                "ma150",
                "close",
                "volume",
                "volume_avg20",
                "tech_score",
                "flow_score",
                "score_final",
                "final_score",
                "score",
            ):
                value = item.get(key)
                if value in (None, "", [], {}):
                    ref_val = ref.get(key)
                    if ref_val not in (None, "", [], {}):
                        item[key] = ref_val

            breakout_score = _safe_float(item.get("breakout_score"), 0.0)
            pullback_score = _safe_float(item.get("pullback_score"), 0.0)
            momentum_score = _safe_float(item.get("momentum_score"), 0.0)
            if not item.get("entry_style_selected"):
                if breakout_score >= pullback_score and breakout_score >= momentum_score:
                    item["entry_style_selected"] = "BREAKOUT"
                elif pullback_score >= momentum_score:
                    item["entry_style_selected"] = "PULLBACK"
                else:
                    item["entry_style_selected"] = "MOMENTUM"

            item["rank"] = _safe_int(item.get("rank") or item.get("rank_final30") or idx, idx)
            item["score"] = _safe_float(
                item.get("score", item.get("score_final", item.get("final_score", item.get("tech_score", 0.0)))),
                0.0,
            )
            normalized_rows.append(_sync_item_and_meta_fields(_sanitize_scored_item(item, ref, source="pre_save_normalize")))
        return normalized_rows

    final30_scored_rows = _normalize_scored_stage_rows(bundle.final30)
    pool120_rows = _normalize_scored_stage_rows(bundle.pool120, fallback_rows=final30_scored_rows)
    top50_rows = _normalize_scored_stage_rows(bundle.top50, fallback_rows=final30_scored_rows)
    universe_scored_rows = _normalize_scored_stage_rows(bundle.universe_scored, fallback_rows=final30_scored_rows)
    if final30_scored_rows:
        final30_scored_df_for_stats = pd.DataFrame(final30_scored_rows)
        style_counts = final30_scored_df_for_stats.get("entry_style_selected", pd.Series(dtype=str)).fillna("NEUTRAL").value_counts().to_dict()
        breakout_stats = final30_scored_df_for_stats.get("breakout_score", pd.Series(dtype=float)).astype(float)
        pullback_stats = final30_scored_df_for_stats.get("pullback_score", pd.Series(dtype=float)).astype(float)
        momentum_stats = final30_scored_df_for_stats.get("momentum_score", pd.Series(dtype=float)).astype(float)
        logger.info(
            "[WATCHLIST][ENTRY_STYLE][DISTRIBUTION] counts=%s median_by_style=%s",
            style_counts,
            {
                "BREAKOUT": float(final30_scored_df_for_stats.loc[final30_scored_df_for_stats.get("entry_style_selected") == "BREAKOUT", "breakout_score"].median() or 0.0) if "breakout_score" in final30_scored_df_for_stats.columns else 0.0,
                "PULLBACK": float(final30_scored_df_for_stats.loc[final30_scored_df_for_stats.get("entry_style_selected") == "PULLBACK", "pullback_score"].median() or 0.0) if "pullback_score" in final30_scored_df_for_stats.columns else 0.0,
                "MOMENTUM": float(final30_scored_df_for_stats.loc[final30_scored_df_for_stats.get("entry_style_selected") == "MOMENTUM", "momentum_score"].median() or 0.0) if "momentum_score" in final30_scored_df_for_stats.columns else 0.0,
            },
        )
        logger.info(
            "[WATCHLIST][SCORE_DISTRIBUTION] breakout=%s pullback=%s momentum=%s",
            {
                "min": float(breakout_stats.min() or 0.0),
                "p25": float(breakout_stats.quantile(0.25) or 0.0),
                "median": float(breakout_stats.median() or 0.0),
                "p75": float(breakout_stats.quantile(0.75) or 0.0),
                "max": float(breakout_stats.max() or 0.0),
            },
            {
                "min": float(pullback_stats.min() or 0.0),
                "p25": float(pullback_stats.quantile(0.25) or 0.0),
                "median": float(pullback_stats.median() or 0.0),
                "p75": float(pullback_stats.quantile(0.75) or 0.0),
                "max": float(pullback_stats.max() or 0.0),
            },
            {
                "min": float(momentum_stats.min() or 0.0),
                "p25": float(momentum_stats.quantile(0.25) or 0.0),
                "median": float(momentum_stats.median() or 0.0),
                "p75": float(momentum_stats.quantile(0.75) or 0.0),
                "max": float(momentum_stats.max() or 0.0),
            },
        )
        if len(style_counts) == 1 and len(final30_scored_rows) > 1:
            logger.warning("[ENTRY_STYLE][ANOMALY][MONOCULTURE] counts=%s", style_counts)
    # 1. universe_scored - FULL universe (should be 196, not 120)
    # Note: In current architecture, this is actually candidate_pool-based (120)
    # TODO: Refactor to use broader universe (196+) as true "universe_scored"
    intermediate_stage_errors = []
    if universe_scored_rows:
        _log_stage_fields("universe_scored", universe_scored_rows)
        logger.info(
            "[WATCHLIST][SAVE_SCOPE] pb1_universe_scored_source=universe_filtered_from_raw120 rows=%s",
            len(universe_scored_rows),
        )
        try:
            repo.save_watchlist(
                env=env,
                strategy="pb1_universe_scored",
                as_of=as_of,
                members=universe_scored_rows,
            )
            verified_result = repo.load_watchlist_scored(
                env=env,
                strategy="pb1_universe_scored",
                as_of=as_of,
                allow_latest_fallback=False,
            )
            if isinstance(verified_result, tuple) and len(verified_result) == 2:
                verified_rows, _ = verified_result
            else:
                verified_rows = []
            verified_df = pd.DataFrame(verified_rows or [])
            logger.info("[BUNDLE][SAVE] pb1_universe_scored n=%s", len(universe_scored_rows))
            logger.info(
                "[WATCHLIST][SAVE_VERIFY][UNIVERSE_SCORED] rows=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s has_rs_percentile=%s has_entry_style_selected=%s",
                len(verified_rows),
                int("breakout_score" in verified_df.columns),
                int("pullback_score" in verified_df.columns),
                int("momentum_score" in verified_df.columns),
                int("rs_percentile" in verified_df.columns),
                int("entry_style_selected" in verified_df.columns),
            )
        except Exception as e:
            error_msg = f"universe_scored save failed: {str(e)}"
            intermediate_stage_errors.append(error_msg)
            logger.warning(
                "[PREP][INTERMEDIATE_STAGE_SAVE][FAIL] strategy=pb1_universe_scored error=%s",
                error_msg,
            )
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_universe_scored empty")
    
    # 2. pool120 - NOW SAVED (not skipped)
    if bundle.pool120:
        _log_stage_fields("pool120", pool120_rows)
        try:
            repo.save_watchlist(
                env=env,
                strategy="pb1_pool120",
                as_of=as_of,
                members=pool120_rows,
            )
            logger.info("[BUNDLE][SAVE] pb1_pool120 n=%s", len(pool120_rows))
        except Exception as e:
            error_msg = f"pool120 save failed: {str(e)}"
            intermediate_stage_errors.append(error_msg)
            logger.warning(
                "[PREP][INTERMEDIATE_STAGE_SAVE][FAIL] strategy=pb1_pool120 error=%s",
                error_msg,
            )
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_pool120 empty")
    
    # 3. top50 저장
    if bundle.top50:
        _log_stage_fields("top50", top50_rows)
        try:
            repo.save_watchlist(
                env=env,
                strategy="pb1_top50",
                as_of=as_of,
                members=top50_rows,
            )
            logger.info("[BUNDLE][SAVE] pb1_top50 n=%s", len(top50_rows))
        except Exception as e:
            error_msg = f"top50 save failed: {str(e)}"
            intermediate_stage_errors.append(error_msg)
            logger.warning(
                "[PREP][INTERMEDIATE_STAGE_SAVE][FAIL] strategy=pb1_top50 error=%s",
                error_msg,
            )
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_top50 empty")
    
    # 4. final30 저장 with strict contract
    if bundle.final30:
        _log_stage_fields("final30", final30_scored_rows)

        try:
            repo.save_watchlist(
                env=env,
                strategy="pb1_watchlist_final",
                as_of=as_of,
                members=final30_scored_rows,
            )
            logger.info("[BUNDLE][SAVE] pb1_watchlist_final n=%s", len(final30_scored_rows))
        except Exception as e:
            logger.warning(
                "[PREP][INTERMEDIATE_STAGE_SAVE][FAIL] strategy=pb1_watchlist_final error=%s",
                str(e),
            )
        
        # Convert to DataFrame for integrity checking
        final30_df = pd.DataFrame(final30_scored_rows) if final30_scored_rows else pd.DataFrame()
        
        # Log object identity before materialization
        log_df_identity(final30_df, "before_materialize")
        
        # Materialize price context from meta if needed
        if not final30_df.empty:
            final30_df = materialize_final30_price_context(final30_df)
            log_df_identity(final30_df, "after_materialize")
        
        # Assert contract before save
        final30_strict_ready = True
        try:
            assert_final30_scored_contract(final30_df, "frozen", str(as_of), hard=True)
            log_df_identity(final30_df, "after_assert")
        except Exception as e:
            final30_strict_ready = False
            logger.error("[PREP][FINAL30_CONTRACT][FAIL] as_of=%s reason=%s", as_of, e)
        
        # Convert back to dict list for save
        final30_scored_rows = final30_df.to_dict(orient="records") if not final30_df.empty else []
        
        # Save with strict contract
        if final30_strict_ready:
            try:
                repo.save_watchlist(
                    env=env,
                    strategy="pb1_watchlist_final_scored",
                    as_of=as_of,
                    members=final30_scored_rows,
                )
                logger.info("[STAGE_SAVE][STRICT_FINAL30] strategy=pb1_watchlist_final_scored rows=%d", len(final30_scored_rows))
                logger.info("[PREP][FINAL30_CONTRACT][OK] as_of=%s rows=%d", as_of, len(final30_scored_rows))
            except Exception as e:
                logger.error("[PREP][FINAL30_CONTRACT][FAIL] as_of=%s reason=%s", as_of, e)
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_watchlist_final_scored empty")
    
    logger.info("[BUNDLE][SAVE][DONE] env=%s as_of=%s", env, as_of)


def normalize_final30_for_save(df_final30: pd.DataFrame | List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info("[NORMALIZE][FINAL30][START]")
    final30_rows = df_final30.to_dict(orient="records") if isinstance(df_final30, pd.DataFrame) else list(df_final30 or [])
    normalized_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(final30_rows, start=1):
        item = _sync_item_and_meta_fields(dict(row or {}))
        code = str(item.get("code") or "").zfill(6)
        if not code:
            continue
        item["code"] = code
        breakout_score = _safe_float(item.get("breakout_score"), 0.0)
        pullback_score = _safe_float(item.get("pullback_score"), 0.0)
        momentum_score = _safe_float(item.get("momentum_score"), 0.0)
        if not item.get("entry_style_selected"):
            if breakout_score >= pullback_score and breakout_score >= momentum_score:
                item["entry_style_selected"] = "BREAKOUT"
            elif pullback_score >= momentum_score:
                item["entry_style_selected"] = "PULLBACK"
            else:
                item["entry_style_selected"] = "MOMENTUM"
        item["rank"] = _safe_int(item.get("rank") or item.get("rank_final30") or idx, idx)
        item["score"] = _safe_float(
            item.get("score", item.get("score_final", item.get("final_score", item.get("tech_score", 0.0)))),
            0.0,
        )
        normalized_rows.append(_sync_item_and_meta_fields(_sanitize_scored_item(item, {}, source="normalize_final30_for_save")))

    validation = verify_final30_scored_rows(
        normalized_rows,
        required_rows=30,
        required_fields=FINAL30_CONTRACT_REQUIRED_FIELDS,
        source="normalize_final30_for_save",
    )
    if not bool(validation.get("ok")):
        logger.error(
            "[NORMALIZE][FINAL30][FAIL] rows=%s errors=%s warnings=%s",
            int(validation.get("rows") or 0),
            list(validation.get("errors") or []),
            list(validation.get("warnings") or []),
        )
        raise RuntimeError(f"FINAL30_NORMALIZE_FAILED:{list(validation.get('errors') or [])}")
    logger.info("[NORMALIZE][FINAL30][OK] rows=%s", len(normalized_rows))
    return normalized_rows


def normalize_broader_bundle_for_save(
    df_broader: pd.DataFrame | List[Dict[str, Any]],
    *,
    fallback_rows: List[Dict[str, Any]] | None = None,
    stage_name: str,
) -> List[Dict[str, Any]]:
    logger.info("[NORMALIZE][BROADER][START] stage=%s", stage_name)
    broader_rows = df_broader.to_dict(orient="records") if isinstance(df_broader, pd.DataFrame) else list(df_broader or [])
    fallback_by_code = {
        str((row or {}).get("code") or "").zfill(6): dict(row or {})
        for row in (fallback_rows or [])
        if (row or {}).get("code")
    }
    normalized_rows: List[Dict[str, Any]] = []
    skipped = 0
    for idx, row in enumerate(broader_rows, start=1):
        item = _sync_item_and_meta_fields(dict(row or {}))
        code = str(item.get("code") or "").zfill(6)
        if not code:
            skipped += 1
            continue
        item["code"] = code
        ref = fallback_by_code.get(code, {})
        for key in (
            "breakout_score",
            "pullback_score",
            "momentum_score",
            "rs_percentile",
            "vcp_score",
            "entry_style_selected",
            "hi_52w",
            "lo_52w",
            "pivot_price",
            "vol20",
            "dollar_vol_50",
            "ma20_slope",
            "ma50_slope",
            "ma150_slope",
            "vcp_ok",
            "breakout_trigger_ok",
            "pullback_trigger_ok",
            "momentum_trigger_ok",
            "ma20",
            "ma50",
            "ma150",
            "close",
            "volume",
            "volume_avg20",
            "tech_score",
            "flow_score",
            "score_final",
            "final_score",
            "score",
        ):
            value = item.get(key)
            if value in (None, "", [], {}):
                ref_val = ref.get(key)
                if ref_val not in (None, "", [], {}):
                    item[key] = ref_val

        breakout_score = _safe_float(item.get("breakout_score"), 0.0)
        pullback_score = _safe_float(item.get("pullback_score"), 0.0)
        momentum_score = _safe_float(item.get("momentum_score"), 0.0)
        if not item.get("entry_style_selected"):
            if breakout_score >= pullback_score and breakout_score >= momentum_score:
                item["entry_style_selected"] = "BREAKOUT"
            elif pullback_score >= momentum_score:
                item["entry_style_selected"] = "PULLBACK"
            else:
                item["entry_style_selected"] = "MOMENTUM"

        item["rank"] = _safe_int(item.get("rank") or item.get("rank_final30") or idx, idx)
        item["score"] = _safe_float(
            item.get("score", item.get("score_final", item.get("final_score", item.get("tech_score", 0.0)))),
            0.0,
        )
        normalized = _sync_item_and_meta_fields(_sanitize_scored_item(item, ref, source=f"normalize_broader_bundle_for_save:{stage_name}"))
        if not is_valid_positive_numeric(normalized.get("ma20")):
            skipped += 1
            logger.warning("[BROADER][ROW_SKIP] code=%s reason=ma20_missing", code)
            continue
        if not is_valid_positive_numeric(normalized.get("volume_avg20")):
            skipped += 1
            logger.warning("[BROADER][ROW_SKIP] code=%s reason=volume_avg20_missing", code)
            continue
        normalized_rows.append(normalized)

    logger.info(
        "[BROADER][SAVE_SUMMARY] input=%s saved=%s skipped=%s",
        len(broader_rows),
        len(normalized_rows),
        skipped,
    )
    if normalized_rows:
        logger.info(
            "[NORMALIZE][BROADER][OK] stage=%s input=%s normalized=%s skipped=%s",
            stage_name,
            len(broader_rows),
            len(normalized_rows),
            skipped,
        )
    else:
        logger.warning(
            "[NORMALIZE][BROADER][SKIP] stage=%s input=%s normalized=0 skipped=%s",
            stage_name,
            len(broader_rows),
            skipped,
        )
    return normalized_rows


def save_bundle_aux(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    bundle: WatchlistBundle,
) -> dict[str, Any]:
    repo = WatchlistRepo(engine)
    logger.info(
        "[BUNDLE][AUX][START] env=%s as_of=%s universe=%s pool120=%s top50=%s final30=%s",
        env,
        as_of,
        len(bundle.universe_scored),
        len(bundle.pool120),
        len(bundle.top50),
        len(bundle.final30),
    )
    saved_counts = {"pb1_universe_scored": 0, "pb1_pool120": 0, "pb1_top50": 0}
    try:
        final30_scored_rows = normalize_final30_for_save(bundle.final30)
        universe_scored_rows = normalize_broader_bundle_for_save(
            bundle.universe_scored,
            fallback_rows=final30_scored_rows,
            stage_name="universe_scored",
        )
        pool120_rows = normalize_broader_bundle_for_save(
            bundle.pool120,
            fallback_rows=final30_scored_rows,
            stage_name="pool120",
        )
        top50_rows = normalize_broader_bundle_for_save(
            bundle.top50,
            fallback_rows=final30_scored_rows,
            stage_name="top50",
        )

        if universe_scored_rows:
            repo.save_watchlist(env=env, strategy="pb1_universe_scored", as_of=as_of, members=universe_scored_rows)
            saved_counts["pb1_universe_scored"] = len(universe_scored_rows)
        if pool120_rows:
            repo.save_watchlist(env=env, strategy="pb1_pool120", as_of=as_of, members=pool120_rows)
            saved_counts["pb1_pool120"] = len(pool120_rows)
        if top50_rows:
            repo.save_watchlist(env=env, strategy="pb1_top50", as_of=as_of, members=top50_rows)
            saved_counts["pb1_top50"] = len(top50_rows)
    except Exception as exc:
        logger.warning("[BUNDLE][AUX][WARN] reason=%s", exc, exc_info=True)
        logger.warning("[BUNDLE][AUX][SKIP_SOFT_FAIL]")
        logger.info("[BUNDLE][AUX][DONE] env=%s as_of=%s saved=%s", env, as_of, saved_counts)
        return {"ok": False, "saved_counts": saved_counts, "reason": str(exc)}

    logger.info("[BUNDLE][AUX][DONE] env=%s as_of=%s saved=%s", env, as_of, saved_counts)
    return {"ok": True, "saved_counts": saved_counts, "reason": ""}


def recover_bundle_from_db(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    min_pool: int = 40,
    exact_top50: int = 40,
    exact_final30: int = 30,
) -> Optional[WatchlistBundle]:
    """
    Recover watchlist bundle from DB.
    
    Loads all 4 stages:
    - pb1_universe_scored
    - pb1_pool120 (with fallback to pb1_candidate_pool if missing)
    - pb1_top50
    - pb1_watchlist_final
    
    Returns:
        WatchlistBundle if all stages meet requirements, else None
    """
    repo = WatchlistRepo(engine)
    
    logger.info(
        "[BUNDLE][RECOVER_DB][START] env=%s as_of=%s min_pool=%s exact_top50=%s exact_final30=%s",
        env,
        as_of,
        min_pool,
        exact_top50,
        exact_final30,
    )
    
    # Load all 4 stages from DB
    universe_rows, _ = repo.load_watchlist(
        env=env,
        strategy="pb1_universe_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    
    # Try loading pb1_pool120 first (normal path)
    pool120_rows, _ = repo.load_watchlist(
        env=env,
        strategy="pb1_pool120",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    
    # Fallback to candidate_pool if pb1_pool120 is missing
    if not pool120_rows or len(pool120_rows) < min_pool:
        logger.warning(
            "[BUNDLE][RECOVER_DB][POOL120_FALLBACK] pb1_pool120 missing or too small (%s), trying pb1_candidate_pool",
            len(pool120_rows) if pool120_rows else 0
        )
        try:
            from trader.candidate_pool_builder import load_candidate_pool
            
            pool_codes, pool_as_of, pool_reason = load_candidate_pool(
                engine=engine,
                env=env,
                today=as_of,
            )
            if pool_codes:
                # Create minimal pool120 rows with codes
                pool120_rows = [{"code": str(code).zfill(6), "name": ""} for code in pool_codes]
                logger.warning(
                    "[BUNDLE][RECOVER_DB][POOL120_FALLBACK] source=pb1_candidate_pool reason=pb1_pool120_missing count=%s",
                    len(pool120_rows)
                )
        except Exception as exc:
            logger.warning("[BUNDLE][RECOVER_DB][POOL120_FALLBACK] candidate_pool_load_fail err=%s", exc)
    else:
        logger.info(
            "[BUNDLE][RECOVER_DB][POOL120] source=pb1_pool120 count=%s",
            len(pool120_rows)
        )
    
    top50_rows, _ = repo.load_watchlist(
        env=env,
        strategy="pb1_top50",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    final30_rows, _ = repo.load_watchlist(
        env=env,
        strategy="pb1_watchlist_final",
        as_of=as_of,
        allow_latest_fallback=False,
    )
    
    logger.info(
        "[BUNDLE][RECOVER_DB][LOADED] universe=%s pool120=%s top50=%s final30=%s",
        len(universe_rows) if universe_rows else 0,
        len(pool120_rows) if pool120_rows else 0,
        len(top50_rows) if top50_rows else 0,
        len(final30_rows) if final30_rows else 0,
    )
    
    # Validate all stages meet requirements
    if not universe_rows or len(universe_rows) == 0:
        logger.warning("[BUNDLE][RECOVER_DB][FAIL] universe_scored empty")
        return None
    
    if not pool120_rows or len(pool120_rows) < min_pool:
        logger.warning(
            "[BUNDLE][RECOVER_DB][FAIL] pool120 too small: %s < %s",
            len(pool120_rows) if pool120_rows else 0,
            min_pool,
        )
        return None
    
    if not top50_rows or len(top50_rows) < exact_top50:
        logger.warning(
            "[BUNDLE][RECOVER_DB][FAIL] top50 too small: %s < %s",
            len(top50_rows) if top50_rows else 0,
            exact_top50,
        )
        return None
    
    if not final30_rows or len(final30_rows) < exact_final30:
        logger.warning(
            "[BUNDLE][RECOVER_DB][FAIL] final30 too small: %s < %s",
            len(final30_rows) if final30_rows else 0,
            exact_final30,
        )
        return None
    
    # All stages valid - construct bundle
    bundle = WatchlistBundle(
        as_of=as_of.isoformat(),
        env=env,
        strategy="pb1_watchlist",
        universe_scored=universe_rows,
        pool120=pool120_rows,
        top50=top50_rows,
        final30=final30_rows,
        meta={
            "source": "db_recovery",
            "recovered_at": time.time(),
            "pool120_source": "candidate_pool",
        },
    )
    
    logger.info(
        "[BUNDLE][RECOVER_DB][SUCCESS] env=%s as_of=%s all stages valid",
        env,
        as_of,
    )
    
    return bundle


def rebuild_bundle(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    flow_provider: Optional[FlowProvider] = None,
) -> WatchlistBundle:
    """
    Bundle을 처음부터 재계산.
    
    이 함수는 DB 복구 실패 시 마지막 수단으로 호출된다.
    """
    logger.info(
        "[BUNDLE][REBUILD][START] env=%s as_of=%s members=%s",
        env,
        as_of,
        len(members),
    )
    
    builder = WatchlistBuilder(
        ohlcv_provider=ohlcv_provider,
        minervini_config=minervini_config,
        env=env,
        repo=WatchlistRepo(engine),
        pooln=_env_int("PB1_WATCHLIST_POOLN", 120),
        topk=_env_int("PB1_WATCHLIST_TOPK", 50),
        finaln=_env_int("PB1_WATCHLIST_FINALN", 30),
        min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 3000.0),
        liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
        min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
        flow_provider=flow_provider,
        flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
        tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
        flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
        trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
    )
    
    try:
        final30 = builder.build(members=members, as_of=as_of)
    except Exception as exc:
        logger.error("[BUNDLE][REBUILD][FAIL] err=%s", exc, exc_info=True)
        raise
    
    # Enrich final30
    final30 = _enrich_watchlist_rows(
        engine=engine,
        env=env,
        as_of=as_of,
        rows=final30,
        flow_provider=flow_provider,
        ohlcv_provider=ohlcv_provider,
        flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
        tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
        flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
        trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
    )
    
    # Extract bundle from builder
    last_bundle = builder.last_bundle
    
    bundle = WatchlistBundle(
        as_of=as_of.isoformat(),
        env=env,
        strategy="pb1_watchlist",
        universe_scored=last_bundle.get("universe_scored", []),
        pool120=last_bundle.get("pool120", []),
        top50=last_bundle.get("top50", []),
        final30=final30,
        meta={
            "source": "rebuild",
            "rebuilt_at": time.time(),
            "weights": last_bundle.get("weights", {}),
            "formula": last_bundle.get("formula", ""),
            "reject_summary": last_bundle.get("reject_summary", {}),
            "degrade": last_bundle.get("degrade", {}),
        },
    )
    
    logger.info(
        "[BUNDLE][REBUILD][DONE] env=%s as_of=%s universe=%s pool120=%s top50=%s final30=%s",
        env,
        as_of,
        len(bundle.universe_scored),
        len(bundle.pool120),
        len(bundle.top50),
        len(bundle.final30),
    )
    
    return bundle


def _prepare_core_final30_artifact_rows(
    rows: List[Dict[str, Any]],
    *,
    as_of: date,
) -> List[Dict[str, Any]]:
    """Prepare a DB/flow-free final30 payload for immediate KR core artifacts."""
    prepared: List[Dict[str, Any]] = []
    for idx, src in enumerate(list(rows or [])[:30], start=1):
        row = normalize_final30_contract_row(dict(src or {}))
        meta = dict(row.get("meta") or {})
        code = str(row.get("code") or meta.get("code") or "").zfill(6)
        row["code"] = code
        row["rank"] = idx
        row["rank_final30"] = idx
        meta["rank_final30"] = idx
        row["as_of"] = as_of.isoformat()
        for key in ("close", "last_close", "ma20", "ma50", "ma150", "volume_avg20", "atr_pct", "rs_percentile", "tech_score"):
            if row.get(key) is None and meta.get(key) is not None:
                row[key] = meta.get(key)
        if row.get("close") is None and row.get("last_close") is not None:
            row["close"] = row.get("last_close")
        if row.get("last_close") is None and row.get("close") is not None:
            row["last_close"] = row.get("close")
        close = safe_nullable_float(row.get("close")) or safe_nullable_float(row.get("last_close")) or 1.0
        for key in ("ma20", "ma50", "ma150"):
            if safe_nullable_float(row.get(key)) is None:
                row[key] = close
        if safe_nullable_float(row.get("volume_avg20")) is None:
            row["volume_avg20"] = safe_nullable_float(row.get("volume")) or 1.0
        for key, default in (("atr_pct", 1.0), ("rs_percentile", 50.0), ("tech_score", row.get("score_final") or 1.0)):
            if safe_nullable_float(row.get(key)) is None:
                row[key] = default
        score_final = safe_nullable_float(row.get("score_final") or row.get("final_score") or row.get("score") or row.get("tech_score"))
        if score_final is None or score_final <= 0:
            score_final = max(float(safe_nullable_float(row.get("tech_score")) or 1.0), 1.0)
        row["score_final"] = score_final
        row["final_score"] = score_final
        row["score"] = score_final
        if safe_nullable_float(row.get("tech_score")) is None or float(row.get("tech_score") or 0) <= 0:
            row["tech_score"] = score_final
        style = str(row.get("entry_style_selected") or meta.get("entry_style_selected") or "").strip().upper()
        if style not in {"BREAKOUT", "PULLBACK", "MOMENTUM"}:
            style = "PULLBACK"
        row["entry_style_selected"] = style
        row.update(
            {
                "flow_data_available": 0,
                "flow_missing": 1,
                "flow_provider_used": "core_imputed",
                "flow_score_imputed": 1,
                "foreign_flow_missing": 1,
                "inst_flow_missing": 1,
                "foreign_20_ratio": 0.0,
                "inst_20_ratio": 0.0,
            }
        )
        meta.update({k: row.get(k) for k in ("rank_final30", "ma20", "ma50", "ma150", "close", "volume_avg20", "atr_pct", "rs_percentile", "score_final", "tech_score", "entry_style_selected")})
        row["meta"] = meta
        prepared.append(_sync_item_and_meta_fields(row))
    return prepared


def _publish_kr_core_artifact_if_valid(
    *,
    rows: List[Dict[str, Any]],
    trade_date: date,
    expected_as_of: date,
    actual_as_of: date,
    env: str,
    db_exact_rows_hint: Optional[int],
    contract_hash: Optional[str],
    metadata: Optional[Dict[str, Any]],
) -> bool:
    normalized = [normalize_final30_contract_row(dict(row or {})) for row in rows or []]
    artifact_rows = to_jsonable(normalized)
    df = pd.DataFrame(artifact_rows or [])
    uniq_code = int(df["code"].nunique()) if "code" in df.columns else 0
    ma20_null = int(df["ma20"].isna().sum()) if "ma20" in df.columns else -1
    score_final_nonzero = int(pd.to_numeric(df.get("score_final", pd.Series(dtype=float)), errors="coerce").fillna(0).gt(0).sum()) if not df.empty else 0
    tech_score_nonzero = int(pd.to_numeric(df.get("tech_score", pd.Series(dtype=float)), errors="coerce").fillna(0).gt(0).sum()) if not df.empty else 0
    valid_styles = {"BREAKOUT", "PULLBACK", "MOMENTUM"}
    entry_style_invalid = int(df["entry_style_selected"].apply(lambda v: str(v or "").strip().upper() not in valid_styles).sum()) if "entry_style_selected" in df.columns else len(artifact_rows)
    if not (
        len(artifact_rows) == 30
        and uniq_code == 30
        and ma20_null == 0
        and score_final_nonzero == 30
        and tech_score_nonzero == 30
        and entry_style_invalid == 0
    ):
        logger.warning(
            "[KR_PREP][CORE_ARTIFACT_WRITE][SKIP] rows=%s uniq_code=%s ma20_null=%s score_final_nonzero=%s tech_score_nonzero=%s entry_style_invalid=%s",
            len(artifact_rows), uniq_code, ma20_null, score_final_nonzero, tech_score_nonzero, entry_style_invalid,
        )
        return False
    logger.info(
        "[KR_PREP][CORE_FINAL30_VALID] rows=30 ma20_null=0 score_final_nonzero=30 entry_style_invalid=0"
    )
    from trader.kr.artifacts import publish_kr_prep_artifacts_core_fast

    publish_kr_prep_artifacts_core_fast(
        trade_date=trade_date,
        expected_as_of=expected_as_of,
        actual_as_of=actual_as_of,
        env=env,
        final30_rows=artifact_rows,
        db_exact_rows=int(db_exact_rows_hint or 30),
        metadata={**(metadata or {}), "source": "core_artifact_before_db_save"},
        contract_hash=contract_hash,
        validate_files_only=True,
        require_db_exact=False,
    )
    logger.info("[KR_PREP][CORE_DONE] trade_date=%s expected_as_of=%s rows=30 trade_can_proceed=1", trade_date, expected_as_of)
    return True


def build_and_save_watchlist(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    as_of: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    force_rebuild: bool = False,
    use_cache: bool = True,
    source_of_truth: str = "candidate_pool",
    flow_provider: Optional[FlowProvider] = None,
    return_bundle: bool = False,
    save_intermediate_bundle: bool = True,
    core_artifact_trade_date: Optional[date] = None,
    core_artifact_expected_as_of: Optional[date] = None,
    core_artifact_env: Optional[str] = None,
    core_artifact_db_exact_rows: Optional[int] = None,
    core_artifact_contract_hash: Optional[str] = None,
    core_artifact_metadata: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Watchlist를 생성하고 DB에 저장한다.
    
    Args:
        use_cache: If False, skip all cache lookups and force fresh build (default: True)
        source_of_truth: Pool source - "candidate_pool" (recommended) or "universe"
    """
    repo = WatchlistRepo(engine)
    upstream_members = list(members or [])

    # Track upstream universe count before candidate pool filtering
    upstream_universe_count = len(members)
    logger.info(
        "[WATCHLIST][UNIVERSE][UPSTREAM] count=%s source=universe_repo",
        upstream_universe_count,
    )

    # Prefer candidate pool as watchlist stage-A input.
    # If unavailable, keep caller-provided universe members.
    pool_source = "universe"
    pool_actual_as_of: Optional[str] = None
    fallback_used = False
    fallback_reason = ""
    try:
        from trader.candidate_pool_builder import load_candidate_pool

        pool_codes, pool_as_of, pool_reason = load_candidate_pool(
            engine=engine,
            env=env,
            today=as_of,
        )
        if pool_codes:
            member_map = {
                str((m.get("code") if isinstance(m, dict) else "") or "").zfill(6): m
                for m in members
                if isinstance(m, dict)
            }
            members = [
                member_map.get(str(code).zfill(6), {"code": str(code).zfill(6), "name": ""})
                for code in pool_codes
            ]
            pool_source = "candidate_pool"
            pool_actual_as_of = pool_as_of.isoformat() if pool_as_of else None
            logger.info(
                "[WATCHLIST][PIPELINE][A_POOL120] source=candidate_pool as_of=%s reason=%s members=%s upstream_universe=%s",
                pool_as_of,
                pool_reason,
                len(members),
                upstream_universe_count,
            )
        else:
            fallback_used = True
            fallback_reason = f"candidate_pool_{pool_reason}"
            logger.warning(
                "[WATCHLIST][PIPELINE][A_POOL120] source=universe_fallback reason=candidate_pool_%s members=%s requested_as_of=%s",
                pool_reason,
                len(members),
                as_of,
            )
    except Exception as exc:
        fallback_used = True
        fallback_reason = "candidate_pool_load_fail"
        logger.warning(
            "[WATCHLIST][PIPELINE][A_POOL120] source=universe_fallback reason=candidate_pool_load_fail err=%s members=%s requested_as_of=%s",
            exc,
            len(members),
            as_of,
        )

    logger.info(
        "[WATCHLIST][INPUT] as_of=%s upstream_universe=%d raw_input=%d source=%s fallback=%s reason=%s",
        as_of,
        upstream_universe_count,
        len(members),
        pool_source,
        int(fallback_used),
        fallback_reason or "none",
    )

    # ✅ FIX: Skip cache when use_cache=False (e.g., during PREP)
    if use_cache and not force_rebuild:
        existing, _used_as_of = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=as_of,
            allow_latest_fallback=False,
        )
        if existing:
            finaln = _env_int("PB1_WATCHLIST_FINALN", 30)
            if len(existing) < finaln:
                logger.warning(
                    "[WATCHLIST][CACHE][IGNORE] cached too small: %s < %s -> treat as miss",
                    len(existing),
                    finaln,
                )
            else:
                logger.info("[WATCHLIST][CACHE] hit=True as_of=%s members=%s", as_of, len(existing))
                
                # ✅ FIX: Load intermediate stages from DB (excluding pb1_pool120 - use candidate_pool instead)
                bundle_recovered = False
                universe_scored = []
                pool120 = []
                top50 = []
                
                try:
                    # Load universe and top50 from DB
                    universe_rows, _ = repo.load_watchlist(env=env, strategy="pb1_universe_scored", as_of=as_of, allow_latest_fallback=False)
                    top50_rows, _ = repo.load_watchlist(env=env, strategy="pb1_top50", as_of=as_of, allow_latest_fallback=False)
                    
                    # Load pool120 from candidate_pool (single source of truth)
                    try:
                        from trader.candidate_pool_builder import load_candidate_pool
                        
                        pool_codes, pool_as_of, pool_reason = load_candidate_pool(
                            engine=engine,
                            env=env,
                            today=as_of,
                        )
                        if pool_codes:
                            pool120 = [{"code": str(code).zfill(6), "name": ""} for code in pool_codes]
                            logger.info(
                                "[WATCHLIST][CACHE][POOL120] source=candidate_pool count=%s reason=%s",
                                len(pool120),
                                pool_reason,
                            )
                    except Exception as exc_pool:
                        logger.warning("[WATCHLIST][CACHE][POOL120] candidate_pool_fail err=%s", exc_pool)
                    
                    if universe_rows and len(universe_rows) > 0:
                        universe_scored = universe_rows
                    if top50_rows and len(top50_rows) >= 50:
                        top50 = top50_rows
                    
                    if universe_scored and pool120 and top50:
                        bundle_recovered = True
                        logger.info(
                            "[WATCHLIST][CACHE][BUNDLE_RECOVERED] universe=%s pool120=%s top50=%s final30=%s",
                            len(universe_scored), len(pool120), len(top50), len(existing)
                        )
                except Exception as exc:
                    logger.warning("[WATCHLIST][CACHE][BUNDLE_RECOVERY_FAIL] err=%s -> will use degrade mode", exc)
                
                existing = _enrich_watchlist_rows(
                    engine=engine,
                    env=env,
                    as_of=as_of,
                    rows=existing,
                    flow_provider=flow_provider,
                    ohlcv_provider=ohlcv_provider,
                    flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
                    tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
                    flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
                    trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
                )
                existing, repaired_ma20_count, _ = _prepare_final30_scored_rows_for_save(
                    existing,
                    engine=engine,
                    env=env,
                    as_of=as_of,
                    broader_rows=universe_scored,
                    ohlcv_provider=ohlcv_provider,
                    source="build_and_save_watchlist_cache",
                )
                repo.save_watchlist(
                    env=env,
                    strategy=strategy,
                    as_of=as_of,
                    members=existing,
                )
                
                if return_bundle:
                    final30_scored_rows = [dict(row) for row in (existing or [])]
                    final30_scored_df = pd.DataFrame(final30_scored_rows).copy(deep=True)
                    final30_scored_df = materialize_final30_price_context(final30_scored_df)
                    log_df_identity(final30_scored_df, "FROZEN")
                    logger.info(
                        "[WATCHLIST][FINAL30_SCORED][FROZEN] rows=%d ma20_null=%d cols=%s",
                        len(final30_scored_df),
                        int(final30_scored_df["ma20"].isna().sum()) if "ma20" in final30_scored_df.columns else -1,
                        list(final30_scored_df.columns),
                    )
                    assert_final30_scored_contract(final30_scored_df, "frozen", str(as_of), hard=True)
                    bundle_final30_scored_before_save = final30_scored_df.copy(deep=True)
                    _log_final30_scored_df_ready(final30_scored_df)
                    final30_saved_rows = _build_final30_saved_rows(existing or [])
                    _log_final30_quality_snapshot(
                        "[WATCHLIST][FINAL30][QUALITY][POST_SAVE_PAYLOAD]",
                        final30_saved_rows,
                        repaired_ma20_count=repaired_ma20_count,
                    )
                    final30_snapshot_rows = [dict(row) for row in (existing or [])]
                    logger.info(
                        "[BUNDLE][KEEP][FINAL30_SCORED] rows=%s has_scores=%s",
                        len(final30_scored_rows),
                        int(len(final30_scored_rows) > 0),
                    )
                    log_df_identity(final30_scored_df, "RETURN")
                    assert_final30_scored_contract(final30_scored_df, "return", str(as_of), hard=True)
                    _log_final30_scored_rows("[WATCHLIST][RETURN][FINAL30_SCORED]", final30_scored_rows)
                    degrade_reason = "cache_bundle_stage_recovered" if bundle_recovered else "cache_bundle_stage_missing"
                    result = {
                        "as_of": as_of,
                        "weights": {
                            "ai_rs": 0.30,
                            "trend": 0.20,
                            "pullback": 0.15,
                            "liquidity": 0.15,
                            "flow": 0.10,
                            "volatility": 0.10,
                        },
                        "weights_effective": {
                            "ai_rs": 0.30,
                            "trend": 0.20,
                            "pullback": 0.15,
                            "liquidity": 0.15,
                            "flow": 0.10,
                            "volatility": 0.10,
                        },
                        "formula": "score_final = 0.3000*ai_rs + 0.2000*trend + 0.1500*pullback + 0.1500*liquidity + 0.1000*flow + 0.1000*volatility",
                        "universe_scored": universe_scored,
                        "universe_scored_df": universe_scored,
                        "pool120": pool120,
                        "pool120_scored": pool120,
                        "top50": top50,
                        "top50_scored": top50,
                        "final30": existing,
                        "final30_scored": final30_scored_df.copy(deep=True),
                        "bundle_final30_scored_before_save": bundle_final30_scored_before_save.copy(deep=True),
                        "final30_saved": final30_saved_rows,
                        "final30_snapshot_df": final30_snapshot_rows,
                        "reject_summary": {degrade_reason: 1},
                        "shortage_reason": "" if bundle_recovered else degrade_reason,
                        "degrade": {
                            "enabled": not bundle_recovered,
                            "used": not bundle_recovered,
                            "reason": degrade_reason,
                            "disabled_features": [] if bundle_recovered else ["stage_snapshot"],
                        },
                        "candidate_pool_actual_as_of": pool_actual_as_of,
                    }
                    logger.info(
                        "[WATCHLIST][RETURN][FINAL30_SCORED] result_has=%s bundle_has=%s before_save_has=%s",
                        hasattr(result, "final30_scored") or (isinstance(result, dict) and "final30_scored" in result),
                        hasattr(result, "final30_scored") or (isinstance(result, dict) and "final30_scored" in result),
                        hasattr(result, "bundle_final30_scored_before_save") or (isinstance(result, dict) and "bundle_final30_scored_before_save" in result),
                    )
                    return existing, result
                return existing

    builder = WatchlistBuilder(
        ohlcv_provider=ohlcv_provider,
        minervini_config=minervini_config,
        env=env,
        repo=repo,
        pooln=_env_int("PB1_WATCHLIST_POOLN", 120),
        topk=_env_int("PB1_WATCHLIST_TOPK", 50),
        finaln=_env_int("PB1_WATCHLIST_FINALN", 30),
        min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 3000.0),
        liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
        min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
        flow_provider=None,
        flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
        tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
        flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
        trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
    )

    try:
        watchlist = builder.build(members=members, as_of=as_of)
    except Exception as exc:
        logger.error("[WATCHLIST][BUILD][FAIL] err=%s", exc, exc_info=True)
        raise

    core_artifact_saved = False
    if builder.last_bundle is None:
        builder.last_bundle = {}
    builder.last_bundle["last_stage"] = "core_artifact_publish_before_db_save start"
    if core_artifact_trade_date is not None and core_artifact_expected_as_of is not None:
        try:
            core_rows = _prepare_core_final30_artifact_rows(watchlist, as_of=as_of)
            core_df = pd.DataFrame(core_rows).copy(deep=True)
            core_df = materialize_final30_price_context(core_df)
            core_df = pd.DataFrame(sanitize_final30_entry_styles(core_df.to_dict(orient="records"), stage="core_artifact", hard=False))
            assert_final30_scored_contract(core_df, "return", str(as_of), hard=True)
            core_rows = core_df.to_dict(orient="records")
            core_artifact_saved = _publish_kr_core_artifact_if_valid(
                rows=core_rows,
                trade_date=core_artifact_trade_date,
                expected_as_of=core_artifact_expected_as_of,
                actual_as_of=as_of,
                env=core_artifact_env or env,
                db_exact_rows_hint=core_artifact_db_exact_rows,
                contract_hash=core_artifact_contract_hash,
                metadata=core_artifact_metadata,
            )
            if core_artifact_saved:
                now_iso = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()
                builder.last_bundle["core_artifact_saved"] = True
                builder.last_bundle["core_artifact_saved_at"] = now_iso
                builder.last_bundle["core_artifact_source"] = "before_enrich_before_db_save"
                builder.last_bundle["trade_can_proceed"] = True
                builder.last_bundle["last_stage"] = "core_done_before_db_save done"
                builder.last_bundle["final30_scored"] = core_df.copy(deep=True)
        except Exception as exc:
            logger.exception("[KR_PREP][CORE_ARTIFACT_WRITE][FAIL] reason=%s", exc)
            raise

    watchlist = _enrich_watchlist_rows(
        engine=engine,
        env=env,
        as_of=as_of,
        rows=watchlist,
        flow_provider=flow_provider,
        ohlcv_provider=ohlcv_provider,
        flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
        tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
        flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
        trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
    )
    watchlist, repaired_ma20_count, _ = _prepare_final30_scored_rows_for_save(
        watchlist,
        engine=engine,
        env=env,
        as_of=as_of,
        broader_rows=(builder.last_bundle or {}).get("universe_scored", []),
        ohlcv_provider=ohlcv_provider,
        source="build_and_save_watchlist",
    )

    # Save final30 to DB (main strategy)
    _save_strategy = strategy if strategy else "pb1_watchlist_final_scored"
    # final30을 반드시 pb1_watchlist_final_scored로 저장 (env 변수 우선)
    _scored_strategy = os.getenv("PB1_WATCHLIST_STRATEGY", _save_strategy)
    if _scored_strategy not in ("pb1_watchlist_final_scored",):
        _scored_strategy = "pb1_watchlist_final_scored"
    logger.info(
        "[WATCHLIST][SAVE] strategy=%s env=%s as_of=%s rows=%s",
        _scored_strategy,
        env,
        as_of,
        len(watchlist),
    )
    if builder.last_bundle is not None:
        builder.last_bundle["last_stage"] = "aux_db_save_final30 start"
    try:
        repo.save_watchlist(
            env=env,
            strategy=_scored_strategy,
            as_of=as_of,
            members=watchlist,
        )
    except Exception as exc:
        if core_artifact_saved and _is_transient_db_exception(exc):
            logger.warning("[WATCHLIST][SAVE][FAIL_SOFT_AFTER_CORE] reason=%s action=continue_core_done", exc)
            if builder.last_bundle is not None:
                builder.last_bundle["db_save_failed_after_core"] = True
                builder.last_bundle["db_save_fail_reason"] = str(exc)
                builder.last_bundle["last_stage"] = "aux_db_roundtrip fail_soft"
        else:
            raise
    # ── save 후 read-back 검증 ──────────────────────────────────────────────
    try:
        _verify_rows, _verify_as_of = repo.load_watchlist_scored(
            env=env,
            strategy=_scored_strategy,
            as_of=as_of,
            allow_latest_fallback=False,
            require_exact_rows=30,
            require_scored=False,
            fail_if_missing=False,
        )
        _verify_n = len(_verify_rows) if _verify_as_of == as_of else 0
        _verify_df = pd.DataFrame(_verify_rows or [])
        _verify_uniq = int(_verify_df["code"].nunique()) if not _verify_df.empty and "code" in _verify_df.columns else 0
        _verify_scored = int(
            _verify_df["score_final"].fillna(0).astype(float).gt(0).any()
        ) if not _verify_df.empty and "score_final" in _verify_df.columns else 0
        _verify_entry_invalid = 0
        if not _verify_df.empty and "entry_style_selected" in _verify_df.columns:
            _valid_s = {"BREAKOUT", "PULLBACK", "MOMENTUM"}
            _verify_entry_invalid = int(
                _verify_df["entry_style_selected"].apply(
                    lambda v: bool(pd.notna(v) and str(v).upper() not in _valid_s)
                ).sum()
            )
        _verify_ma20_null = int(
            _verify_df["ma20"].isna().sum()
        ) if not _verify_df.empty and "ma20" in _verify_df.columns else -1
        if _verify_n == 30 and _verify_uniq == 30 and _verify_entry_invalid == 0:
            logger.info(
                "[WATCHLIST][SAVE_VERIFY][OK] strategy=%s env=%s as_of=%s rows=%s uniq_code=%s scored=%s entry_style_invalid=%s ma20_null=%s",
                _scored_strategy,
                env,
                as_of,
                _verify_n,
                _verify_uniq,
                _verify_scored,
                _verify_entry_invalid,
                _verify_ma20_null,
            )
        else:
            _fail_reason = (
                f"rows={_verify_n}/30"
                if _verify_n != 30
                else f"uniq_code={_verify_uniq}/30"
                if _verify_uniq != 30
                else f"entry_style_invalid={_verify_entry_invalid}"
            )
            logger.error(
                "[WATCHLIST][SAVE_VERIFY][FAIL] strategy=%s env=%s as_of=%s rows=%s reason=%s",
                _scored_strategy,
                env,
                as_of,
                _verify_n,
                _fail_reason,
            )
            if os.getenv("PREP_REQUIRE_SCORED_FINAL30", "1") == "1":
                raise RuntimeError(
                    f"WATCHLIST_SAVE_VERIFY_FAIL strategy={_scored_strategy} env={env} as_of={as_of} {_fail_reason}"
                )
    except RuntimeError as _sv_rt:
        if core_artifact_saved and _is_transient_db_exception(_sv_rt):
            logger.warning("[WATCHLIST][SAVE_VERIFY][FAIL_SOFT_AFTER_CORE] reason=%s action=continue_core_done", _sv_rt)
            if builder.last_bundle is not None:
                builder.last_bundle["last_stage"] = "aux_db_roundtrip fail_soft"
        else:
            raise
    except Exception as _sv_exc:
        if core_artifact_saved and _is_transient_db_exception(_sv_exc):
            logger.warning("[WATCHLIST][SAVE_VERIFY][FAIL_SOFT_AFTER_CORE] reason=%s action=continue_core_done", _sv_exc)
            if builder.last_bundle is not None:
                builder.last_bundle["last_stage"] = "aux_db_roundtrip fail_soft"
        else:
            logger.warning("[WATCHLIST][SAVE_VERIFY][ERROR] reason=%s -> continuing", _sv_exc)
    # ─────────────────────────────────────────────────────────────────────────

    # Save final30 snapshot to JSON (for fallback)
    try:
        snapshot_dir = Path(os.getenv("GITHUB_WORKSPACE", "."))
        snapshot_dir = snapshot_dir / "repo" / "runtime" / "snapshots" if (snapshot_dir / "repo").exists() else snapshot_dir / "runtime" / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        
        final30_snapshot = {
            "as_of": as_of.isoformat(),
            "env": env,
            "strategy": _scored_strategy,
            "watchlist": watchlist,
            "count": len(watchlist),
            "saved_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
        }
        
        snapshot_path = snapshot_dir / "final30.json"
        snapshot_path.write_text(json.dumps(final30_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("[WATCHLIST][SNAPSHOT][SAVE] path=%s count=%s", snapshot_path, len(watchlist))
    except Exception as exc:
        logger.warning("[WATCHLIST][SNAPSHOT][SAVE_FAIL] err=%s -> continuing", exc)

    final30_scored_source = (builder.last_bundle or {}).get("final30_scored")
    if final30_scored_source is None:
        raise RuntimeError("FINAL30_SCORED_BUILD_BUNDLE_MISSING")
    final30_scored_df = pd.DataFrame(watchlist or []).copy(deep=True)
    final30_scored_df = materialize_final30_price_context(final30_scored_df)
    # ── entry style sanitize: save 직전 강제 보정 ─────────────────────────────
    _pre_save_rows = sanitize_final30_entry_styles(
        final30_scored_df.to_dict(orient="records"),
        stage="pre_save",
        hard=False,
    )
    final30_scored_df = pd.DataFrame(_pre_save_rows)
    # ─────────────────────────────────────────────────────────────────────────
    log_df_identity(final30_scored_df, "SAVE_INPUT")
    assert_final30_scored_contract(final30_scored_df, "save_input", str(as_of), hard=True)
    final30_scored_rows = final30_scored_df.to_dict(orient="records")
    bundle_final30_scored_before_save = final30_scored_df.copy(deep=True)
    _log_final30_scored_df_ready(final30_scored_df)
    final30_saved_rows = _build_final30_saved_rows(watchlist or [])
    _log_final30_quality_snapshot(
        "[WATCHLIST][FINAL30][QUALITY][POST_SAVE_PAYLOAD]",
        final30_saved_rows,
        repaired_ma20_count=repaired_ma20_count,
    )
    final30_saved_df = pd.DataFrame(final30_saved_rows).copy(deep=True)
    final30_snapshot_rows = [dict(row) for row in (watchlist or [])]

    if builder.last_bundle:
        builder.last_bundle["final30_scored"] = final30_scored_df.copy(deep=True)
        builder.last_bundle["bundle_final30_scored_before_save"] = bundle_final30_scored_before_save.copy(deep=True)
        builder.last_bundle["final30_saved"] = final30_saved_rows
        builder.last_bundle["final30_saved_df"] = final30_saved_df
        builder.last_bundle["final30_snapshot_df"] = final30_snapshot_rows
        builder.last_bundle["top50_scored"] = builder.last_bundle.get("top50_scored", builder.last_bundle.get("top50", []))
        builder.last_bundle["pool120_scored"] = builder.last_bundle.get("pool120_scored", builder.last_bundle.get("pool120", []))
        builder.last_bundle["universe_scored_df"] = builder.last_bundle.get("universe_scored_df", builder.last_bundle.get("universe_scored", []))
        logger.info(
            "[BUNDLE][KEEP][FINAL30_SCORED] rows=%s has_scores=%s",
            len(final30_scored_rows),
            int(len(final30_scored_rows) > 0),
        )
        log_df_identity(final30_scored_df, "RETURN")
        assert_final30_scored_contract(final30_scored_df, "return", str(as_of), hard=True)
        _log_final30_scored_rows("[WATCHLIST][RETURN][FINAL30_SCORED]", final30_scored_rows)
    
    # CRITICAL: Always save bundle (4 stages) to prevent data loss
    # This ensures intermediate stages are never missing from DB
    if save_intermediate_bundle and builder.last_bundle:
        try:
            broader_universe_scored = list(builder.last_bundle.get("universe_scored", []) or [])
            try:
                # Build broader scored universe from upstream members for pb1_universe_scored contract.
                _pool_unused, upstream_universe_rows = builder._stage_a_liquidity_filter(upstream_members, as_of)
                upstream_universe_rows = builder._merge_derived_scores(upstream_universe_rows, as_of=as_of)
                upstream_universe_rows = builder._attach_scores(upstream_universe_rows, "UNIVERSE_SCORED_BROADER")
                if len(upstream_universe_rows) >= len(broader_universe_scored):
                    broader_universe_scored = upstream_universe_rows
            except Exception as exc:
                logger.warning("[WATCHLIST][SAVE_SCOPE][BROADER_FALLBACK] reason=%s", exc)

            logger.info(
                "[WATCHLIST][STAGE_COUNTS] upstream_universe=%s raw_input=%s broader_scored=%s pool120=%s top50=%s final30=%s",
                len(upstream_members),
                len(members),
                len(broader_universe_scored),
                len(builder.last_bundle.get("pool120", []) or []),
                len(builder.last_bundle.get("top50", []) or []),
                len(watchlist),
            )

            bundle = WatchlistBundle(
                as_of=as_of.isoformat(),
                env=env,
                strategy=strategy,
                universe_scored=broader_universe_scored,
                pool120=builder.last_bundle.get("pool120", []),
                top50=builder.last_bundle.get("top50", []),
                final30=watchlist,
                meta={
                    "weights": builder.last_bundle.get("weights", {}),
                    "weights_effective": builder.last_bundle.get("weights_effective", builder.last_bundle.get("weights", {})),
                    "formula": builder.last_bundle.get("formula", ""),
                    "reject_summary": builder.last_bundle.get("reject_summary", {}),
                    "degrade": builder.last_bundle.get("degrade", {}),
                    "source": "fresh_build",
                    "candidate_pool_actual_as_of": pool_actual_as_of,
                },
            )
            setattr(bundle, "final30_scored", final30_scored_df.copy(deep=True))
            setattr(bundle, "bundle_final30_scored_before_save", bundle_final30_scored_before_save.copy(deep=True))
            
            # Use centralized save_bundle function to ensure atomicity
            save_bundle(
                engine=engine,
                env=env,
                as_of=as_of,
                bundle=bundle,
            )
            
            logger.info(
                "[WATCHLIST][BUNDLE][SAVE_SUCCESS] env=%s as_of=%s universe=%s pool120=%s top50=%s final30=%s",
                env,
                as_of,
                len(bundle.universe_scored),
                len(bundle.pool120),
                len(bundle.top50),
                len(bundle.final30),
            )
        except Exception as exc:
            logger.error(
                "[WATCHLIST][BUNDLE][SAVE_FAIL] env=%s as_of=%s err=%s -> CRITICAL: intermediate stages not saved",
                env,
                as_of,
                exc,
                exc_info=True,
            )
            # Don't raise - allow workflow to continue but log as critical
    elif builder.last_bundle:
        logger.info("[WATCHLIST][BUNDLE][SAVE_DEFER] as_of=%s reason=save_intermediate_bundle_disabled", as_of)

    if return_bundle:
        if builder.last_bundle is None:
            builder.last_bundle = {}
        builder.last_bundle.setdefault("final30_scored", final30_scored_df.copy(deep=True))
        builder.last_bundle.setdefault("bundle_final30_scored_before_save", bundle_final30_scored_before_save.copy(deep=True))
        builder.last_bundle.setdefault("final30_saved", final30_saved_rows)
        builder.last_bundle.setdefault("final30_saved_df", final30_saved_df)
        builder.last_bundle.setdefault("final30_snapshot_df", final30_snapshot_rows)
        builder.last_bundle.setdefault("top50_scored", builder.last_bundle.get("top50", []))
        builder.last_bundle.setdefault("pool120_scored", builder.last_bundle.get("pool120", []))
        builder.last_bundle.setdefault("universe_scored_df", builder.last_bundle.get("universe_scored", []))
        builder.last_bundle["core_artifact_saved"] = bool(core_artifact_saved)
        if core_artifact_saved:
            builder.last_bundle.setdefault("core_artifact_source", "before_enrich_before_db_save")
            builder.last_bundle.setdefault("trade_can_proceed", True)
        if builder.last_bundle.get("db_save_failed_after_core"):
            builder.last_bundle["last_stage"] = "aux_degraded"
        else:
            builder.last_bundle["last_stage"] = "aux_done"
        logger.info(
            "[WATCHLIST][RETURN][FINAL30_SCORED] result_has=%s bundle_has=%s before_save_has=%s",
            hasattr(builder.last_bundle, "final30_scored") or (isinstance(builder.last_bundle, dict) and "final30_scored" in builder.last_bundle),
            hasattr(builder.last_bundle, "final30_scored") or (isinstance(builder.last_bundle, dict) and "final30_scored" in builder.last_bundle),
            hasattr(builder.last_bundle, "bundle_final30_scored_before_save") or (isinstance(builder.last_bundle, dict) and "bundle_final30_scored_before_save" in builder.last_bundle),
        )
        return watchlist, builder.last_bundle
    return watchlist


def load_today_watchlist_with_fallback(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    today: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    auto_build_if_empty: bool = True,
    strict_fail_on_empty: bool = False,
) -> tuple[List[Dict[str, Any]], str]:
    """오늘 watchlist를 로드, 없으면 자동 생성 또는 fallback 전략 적용."""
    today = to_date(today)
    repo = WatchlistRepo(engine)
    ttl_days = int(os.getenv("CANDIDATE_POOL_TTL_DAYS", "7"))

    def _load() -> tuple[List[Dict[str, Any]], date | None]:
        rows, used_as_of = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=today,
            allow_latest_fallback=True,
            ttl_days=ttl_days,
        )
        return rows, used_as_of

    rows, used_as_of = _load()
    if rows:
        finaln = _env_int("PB1_WATCHLIST_FINALN", 30)
        if len(rows) < finaln:
            logger.warning("[WATCHLIST][CACHE][IGNORE] cached too small: %s < %s", len(rows), finaln)
        else:
            if used_as_of and used_as_of != today:
                logger.info(
                    "[WATCHLIST][CACHE] hit=True source=watchlist_db_fallback as_of=%s requested=%s count=%s",
                    used_as_of,
                    today,
                    len(rows),
                )
            else:
                logger.info("[WATCHLIST][CACHE] hit=True source=watchlist_db as_of=%s count=%s", today, len(rows))
            return rows, "watchlist_db"

    if auto_build_if_empty:
        logger.warning("[WATCHLIST][AUTO_BUILD] today=%s missing -> building now", today)
        try:
            built = build_and_save_watchlist(
                engine=engine,
                env=env,
                strategy=strategy,
                as_of=today,
                members=members,
                ohlcv_provider=ohlcv_provider,
                minervini_config=minervini_config,
                force_rebuild=True,
            )
            if built:
                rows2, _ = _load()
                if rows2:
                    logger.info("[WATCHLIST][AUTO_BUILD] success count=%s", len(rows2))
                    return rows2, "watchlist_autobuilt"
        except Exception as exc:
            logger.error("[WATCHLIST][AUTO_BUILD][FAIL] err=%s", exc, exc_info=True)

    latest_date = repo.get_latest_watchlist_date(env=env, strategy=strategy)
    if latest_date:
        prev_rows, _ = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=latest_date,
            allow_latest_fallback=False,
        )
        if prev_rows:
            logger.warning("[WATCHLIST][CACHE] hit=True source=prevday as_of=%s", latest_date)
            return prev_rows, "prevday"

    logger.warning("[WATCHLIST][CACHE] miss -> fallback to liquidity pool")
    try:
        builder = WatchlistBuilder(
            ohlcv_provider=ohlcv_provider,
            minervini_config=minervini_config,
            env=env,
            repo=repo,
            pooln=_env_int("PB1_WATCHLIST_TOPK", 50),
            topk=_env_int("PB1_WATCHLIST_TOPK", 50),
            finaln=_env_int("PB1_WATCHLIST_TOPK", 50),
            min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 3000.0),
            liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
            min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
        )
        fallback, _ = builder._stage_a_liquidity_filter(members, today)
        for idx, item in enumerate(fallback, start=1):
            item["rank"] = idx
        return fallback, "fallback"
    except Exception as exc:
        logger.error("[WATCHLIST][FALLBACK][FAIL] err=%s", exc)
        if strict_fail_on_empty:
            raise RuntimeError(f"Watchlist empty after auto-build: env={env} strategy={strategy} as_of={today}") from exc
        return [], "watchlist_empty"
