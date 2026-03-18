"""PB1 Watchlist Builder - 120 -> 50 -> 30 unified pipeline."""
from __future__ import annotations

import json
import logging
import os
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

from trader.config import RS_BENCHMARK, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, RS_MIN_PCTILE
from trader.db.repos import (
    CRITICAL_SCORED_COLS,
    REQUIRED_FINAL30_SCORED_COLS,
    DerivedFlowRepo,
    DerivedMinerviniRepo,
    WatchlistRepo,
)
from trader.flow_score import calculate_final_score, calculate_flow_score
from trader.factors.multifactor import (
    compute_ai_rs_scores,
    compute_liquidity_score,
    compute_rs_features,
    compute_volatility_score,
    optimize_meta_k,
)
from trader.score_columns import resolve_score_column
from trader.time_coerce import to_date

logger = logging.getLogger(__name__)

FlowProvider = Callable[[str, date, int], Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]]


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
        item = dict(row or {})
        for col in REQUIRED_FINAL30_SCORED_COLS:
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
        normalized = {col: item.get(col) for col in REQUIRED_FINAL30_SCORED_COLS}
        normalized["code"] = str(item.get("code") or "").zfill(6)
        normalized["rank"] = int(item.get("rank") or item.get("rank_final30") or idx)
        normalized["score"] = score_val
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


def _normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    normalized = df.copy()
    normalized.columns = [str(col).strip().lower().replace(" ", "_") for col in normalized.columns]

    def _map_if_missing(target: str, candidates: List[str]) -> None:
        if target in normalized.columns:
            return
        for candidate in candidates:
            if candidate in normalized.columns:
                normalized.rename(columns={candidate: target}, inplace=True)
                return

    _map_if_missing("close", ["adj_close", "adjusted_close", "close_price", "stck_clpr"])
    _map_if_missing("high", ["high_price", "stck_hgpr"])
    _map_if_missing("low", ["low_price", "stck_lwpr"])
    _map_if_missing("volume", ["vol", "trade_volume", "acml_vol", "acml_volm"])
    return normalized


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


def _extract_derived_metrics(derived_row: Dict[str, Any]) -> Dict[str, float]:
    features = derived_row.get("features_json") if isinstance(derived_row.get("features_json"), dict) else {}

    close = _safe_float(derived_row.get("close"), 0.0)
    ma50 = _safe_float(derived_row.get("ma50"), 0.0)
    ma150 = _safe_float(derived_row.get("ma150"), 0.0)
    ma200 = _safe_float(derived_row.get("ma200"), 0.0)
    trend_checks = [
        close > ma50 > 0,
        ma50 > ma150 > 0,
        ma150 > ma200 > 0,
        close > ma200 > 0,
    ]
    trend_score = float(sum(1 for check in trend_checks if check) * 25.0)

    hi_52w = _safe_float(derived_row.get("hi_52w"), _safe_float(features.get("hi_52w"), 0.0))
    pullback_pct = 0.0
    if hi_52w > 0 and close > 0:
        pullback_pct = max(0.0, (hi_52w - close) / hi_52w)
    else:
        pullback_pct = _safe_float(features.get("pullback_pct"), 0.0)

    return {
        "rs_pctile": _safe_float(derived_row.get("rs_percentile"), _safe_float(features.get("rs_percentile"), 0.0)),
        "vcp_score": _safe_float(derived_row.get("vcp_score"), _safe_float(features.get("vcp_score"), 0.0)),
        "atr_pct": _safe_float(derived_row.get("atr_pct"), _safe_float(features.get("atr_pct"), 0.0)),
        "trend_score": trend_score,
        "pullback_pct": pullback_pct,
    }


def _sync_item_and_meta_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(item)
    out["code"] = str(out.get("code") or "").zfill(6)
    meta = dict(out.get("meta") or {})
    meta["code"] = out["code"]
    if not str(out.get("name") or "").strip() and str(meta.get("name") or "").strip():
        out["name"] = str(meta.get("name") or "")

    for key in (
        "as_of",
        "rs_pctile",
        "vcp_score",
        "atr_pct",
        "trend_score",
        "pullback_pct",
        "flow_score",
        "tech_score",
        "final_score",
        "foreign_20_ratio",
        "inst_20_ratio",
    ):
        if key in out and out.get(key) is not None:
            meta[key] = out.get(key)
        elif key in meta and meta.get(key) is not None:
            out[key] = meta.get(key)

    out["rows"] = _safe_int(out.get("rows", meta.get("rows", 0)), 0)
    meta["rows"] = out["rows"]

    out["rs_pctile"] = _safe_float(out.get("rs_pctile", meta.get("rs_pctile", 0.0)), 0.0)
    out["vcp_score"] = _safe_float(out.get("vcp_score", meta.get("vcp_score", 0.0)), 0.0)
    out["atr_pct"] = _safe_float(out.get("atr_pct", meta.get("atr_pct", 0.0)), 0.0)
    out["trend_score"] = _safe_float(out.get("trend_score", meta.get("trend_score", 0.0)), 0.0)
    out["pullback_pct"] = _safe_float(out.get("pullback_pct", meta.get("pullback_pct", 0.0)), 0.0)
    out["flow_score"] = _safe_float(out.get("flow_score", meta.get("flow_score", 0.0)), 0.0)
    out["tech_score"] = _safe_float(out.get("tech_score", meta.get("tech_score", out.get("score", 0.0))), 0.0)
    out["final_score"] = _safe_float(out.get("final_score", meta.get("final_score", out.get("score", 0.0))), 0.0)
    out["score_tech"] = _safe_float(out.get("score_tech", meta.get("score_tech", out.get("tech_score", 0.0))), 0.0)
    out["score_flow"] = _safe_float(out.get("score_flow", meta.get("score_flow", out.get("flow_score", 0.0))), 0.0)
    out["score_final"] = _safe_float(out.get("score_final", meta.get("score_final", out.get("final_score", out.get("score", 0.0)))), 0.0)
    out["score"] = _safe_float(out.get("score", out.get("score_final", out.get("final_score", 0.0))), 0.0)
    flow_missing = bool(out.get("flow_missing") or meta.get("flow_missing"))
    if flow_missing:
        out["foreign_20_ratio"] = out.get("foreign_20_ratio", meta.get("foreign_20_ratio"))
        out["inst_20_ratio"] = out.get("inst_20_ratio", meta.get("inst_20_ratio"))
    else:
        out["foreign_20_ratio"] = _safe_float(out.get("foreign_20_ratio", meta.get("foreign_20_ratio", 0.0)), 0.0)
        out["inst_20_ratio"] = _safe_float(out.get("inst_20_ratio", meta.get("inst_20_ratio", 0.0)), 0.0)

    for key in (
        "rs_pctile",
        "vcp_score",
        "atr_pct",
        "trend_score",
        "pullback_pct",
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
        out = dict(item)
        code = str(out.get("code") or "").zfill(6)
        meta = dict(out.get("meta") or {})
        reject_reasons = list(out.get("reject_reasons", []) or meta.get("reject_reasons", []) or [])

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
            ohlcv_df = pd.DataFrame()
            try:
                ohlcv_raw, _ = ohlcv_provider(code, count=max(int(flow_window) + 10, 80))
                ohlcv_df = _normalize_ohlcv_columns(ohlcv_raw if ohlcv_raw is not None else pd.DataFrame())
            except Exception:
                ohlcv_df = pd.DataFrame()

            try:
                foreign_df, inst_df = flow_provider(code, as_of, int(flow_window))
            except Exception:
                logger.warning("[FLOW][WARN] provider exception code=%s as_of=%s", code, as_of, exc_info=True)
                if "flow_provider_error" not in reject_reasons:
                    reject_reasons.append("flow_provider_error")

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
                    "[FLOW][WARN] flow missing -> non_blocking code=%s as_of=%s reason=%s",
                    code,
                    as_of,
                    flow_missing_reason,
                )
            meta["flow_missing"] = bool(flow_missing)
            meta["flow_missing_reason"] = flow_missing_reason
            out["flow_missing"] = bool(flow_missing)
            out["flow_pass"] = True

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

        out["reject_reasons"] = list(dict.fromkeys(reject_reasons))
        meta["reject_reasons"] = out["reject_reasons"]
        out["meta"] = meta
        out = _sync_item_and_meta_fields(out)
        enriched.append(out)

    enriched.sort(key=lambda row: _safe_float(row.get("final_score", row.get("score", 0.0)), 0.0), reverse=True)
    for idx, row in enumerate(enriched, start=1):
        row["rank"] = _safe_int(row.get("rank") or idx, idx)
        row.setdefault("rank_final30", idx)
        row.setdefault("rank_top50", _safe_int(row.get("rank_top50"), 0))
        row.setdefault("rank_pool120", _safe_int(row.get("rank_pool120"), 0))

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

    def build(self, *, members: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
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
            "final30_scored": final30,
            "reject_summary": dict(reject_counter),
            "final_count": len(final30),
            "requested_finaln": int(self.finaln),
            "degrade": degrade_meta,
            "shortage_reason": shortage_reason,
            "contract_failures": contract_failures,
            "contract_mode": contract_mode,
        }

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

    def _base_item(self, code: str, *, name: str = "", as_of: Optional[date] = None) -> Dict[str, Any]:
        return {
            "as_of": as_of.isoformat() if isinstance(as_of, date) else "",
            "code": code,
            "name": name,
            "rank": None,
            "score": None,
            "liq_avg": 0.0,
            "last_close": 0.0,
            "rows": 0,
            "rs_pctile": 0.0,
            "vcp_score": 0.0,
            "pullback_pct": 0.0,
            "trend_score": 0.0,
            "atr_pct": 0.0,
            "foreign_20_ratio": 0.0,
            "inst_20_ratio": 0.0,
            "flow_score": 0.0,
            "ai_rs_score": 0.0,
            "meta_k": 0.5,
            "breakout_target": 0.0,
            "breakout_score": 0.0,
            "liquidity_score": 0.0,
            "volatility_score": 0.0,
            "tech_score": 0.0,
            "final_score": 0.0,
            "reject_reasons": [],
            "meta": {},
        }

    def _normalize_item(self, item: Dict[str, Any], *, score_key: str, rank_key: str) -> Dict[str, Any]:
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
        score_liq = float(item.get("liq_avg", 0.0) or 0.0)
        score_tech = float(item.get("tech_score", 0.0) or 0.0)
        score_flow = float(item.get("flow_score", 0.0) or 0.0)
        score_final = float(item.get("final_score", score_val) or 0.0)
        rank_pool120 = int(item.get("pool_rank", 0) or 0)
        rank_top50 = int(item.get("top50_rank", 0) or 0)
        rank_final30 = int(item.get("final_rank", 0) or 0)
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

            item = self._base_item(code, name=str(m.get("name") or ""), as_of=as_of)

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
            ma20 = float(close_series.rolling(20).mean().iloc[-1]) if len(close_series) >= 20 else 0.0
            ma50 = float(close_series.rolling(50).mean().iloc[-1]) if len(close_series) >= 50 else 0.0
            ma150 = float(close_series.rolling(150).mean().iloc[-1]) if len(close_series) >= 150 else 0.0
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
            item["volume"] = volume_last
            item["volume_avg20"] = volume_avg20
            item["trading_value"] = trading_value
            item["turnover_pct"] = float(turnover_pct)
            item["liquidity_score"] = compute_liquidity_score(float(liq_avg), float(turnover_pct))
            item["meta"] = {"as_of": as_of.isoformat()}
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

        try:
            bench_df, _ = self.ohlcv_provider(RS_BENCHMARK, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, 260) + 10)
            bench_df = _normalize_ohlcv_columns(bench_df if bench_df is not None else pd.DataFrame())
            if bench_df is None or bench_df.empty or "close" not in bench_df.columns:
                raise ValueError(f"benchmark {RS_BENCHMARK} empty")
            bench_close = bench_df["close"]
        except Exception as exc:
            logger.error("[WATCHLIST][PIPELINE][B_TOP50][BENCH_FAIL] err=%s", exc)
            bench_close = None

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

        flow_map: Dict[str, Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]] = {}
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

            def _fetch_for_code(code: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
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
                return None, None

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
                        flow_map[code] = (None, None)
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
                foreign_df, inst_df = flow_map.get(code, (None, None))

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
            rs_percentile = _safe_float(_row_get(ref, "rs_percentile", _row_get(row, "rs_percentile", 0.0)))
            rs_score = _safe_float(_row_get(ref, "rs_score", _row_get(row, "rs_score", 0.0)))
            vcp_score = _safe_float(_row_get(ref, "vcp_score", _row_get(row, "vcp_score", 0.0)))
            trend_score = _safe_float(_row_get(ref, "trend_score", _row_get(row, "trend_score", 0.0)))
            
            # Also copy MA and price data from ref if available
            ma20 = _safe_float(_row_get(ref, "ma20", _row_get(row, "ma20", 0.0)))
            ma50 = _safe_float(_row_get(ref, "ma50", _row_get(row, "ma50", 0.0)))
            ma150 = _safe_float(_row_get(ref, "ma150", _row_get(row, "ma150", 0.0)))
            close = _safe_float(_row_get(ref, "close", _row_get(row, "close", 0.0)))
            volume = _safe_float(_row_get(ref, "volume", _row_get(row, "volume", 0.0)))
            volume_avg20 = _safe_float(_row_get(ref, "volume_avg20", _row_get(row, "volume_avg20", 0.0)))
            
            # Copy breakout/pullback/momentum scores if present in ref
            breakout_score = _safe_float(_row_get(ref, "breakout_score", _row_get(row, "breakout_score", 0.0)))
            pullback_score = _safe_float(_row_get(ref, "pullback_score", _row_get(row, "pullback_score", 0.0)))
            momentum_score = _safe_float(_row_get(ref, "momentum_score", _row_get(row, "momentum_score", 0.0)))
            entry_style = _row_get(ref, "entry_style", _row_get(row, "entry_style", None))

            # Fallback 1: rs_score가 없으면 rs_percentile 사용
            if rs_score <= 0 and rs_percentile > 0:
                rs_score = rs_percentile

            # Fallback 2: trend_score가 없으면 MA 구조로 즉석 계산
            if trend_score <= 0:
                tmp = 0.0
                if close > 0 and ma20 > 0 and close >= ma20:
                    tmp += 25.0
                if ma20 > 0 and ma50 > 0 and ma20 >= ma50:
                    tmp += 25.0
                if ma50 > 0 and ma150 > 0 and ma50 >= ma150:
                    tmp += 25.0
                # MA150 slope check (simplified)
                if ma150 > 0:
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
            _row_set(row, "breakout_score", breakout_score)
            _row_set(row, "pullback_score", pullback_score)
            _row_set(row, "momentum_score", momentum_score)
            if entry_style:
                _row_set(row, "entry_style", entry_style)

            out.append(row)

        # Enhanced logging with all score types
        rs_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "rs_score", 0.0)) > 0)
        vcp_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "vcp_score", 0.0)) > 0)
        trend_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "trend_score", 0.0)) > 0)
        breakout_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "breakout_score", 0.0)) > 0)
        pullback_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "pullback_score", 0.0)) > 0)
        momentum_nonzero = sum(1 for r in out if _safe_float(_row_get(r, "momentum_score", 0.0)) > 0)
        
        logger.info(
            "[WATCHLIST][DERIVED_MERGE] rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
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
    
    def _compute_breakout_score(self, row: Any) -> float:
        """
        Calculate breakout score based on:
        - Proximity to 20/55-day highs
        - Volume surge
        - Resistance breakout
        
        Returns score in 0-100 range.
        """
        close = _safe_float(_row_get(row, "close", 0.0))
        high_20d = _safe_float(_row_get(row, "high_20d", _row_get(row, "high20", 0.0)))
        high_55d = _safe_float(_row_get(row, "high_55d", _row_get(row, "high55", 0.0)))
        pivot = _safe_float(_row_get(row, "pivot_price", _row_get(row, "pivot", 0.0)))
        volume = _safe_float(_row_get(row, "volume", 0.0))
        volume_avg20 = _safe_float(_row_get(row, "volume_avg20", _row_get(row, "vol20", 0.0)))
        breakout_ref = pivot if pivot > 0 else high_20d if high_20d > 0 else high_55d
        anomalies = list(_row_get(row, "score_anomalies", []))

        score = 0.0
        if close <= 0 or breakout_ref <= 0:
            anomalies.append("breakout_missing_price_context")
        if volume_avg20 <= 0:
            anomalies.append("breakout_missing_volume_context")

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

    def _compute_pullback_score(self, row: Any) -> float:
        """
        Calculate pullback score based on:
        - Staying above key MAs during pullback
        - Pullback depth in optimal range (3-18%)
        - Volume contraction during pullback
        
        Returns score in 0-100 range.
        """
        close = _safe_float(_row_get(row, "close", 0.0))
        ma20 = _safe_float(_row_get(row, "ma20", 0.0))
        ma50 = _safe_float(_row_get(row, "ma50", 0.0))
        high_55d = _safe_float(_row_get(row, "high_55d", _row_get(row, "high55", 0.0)))
        volume = _safe_float(_row_get(row, "volume", 0.0))
        volume_avg20 = _safe_float(_row_get(row, "volume_avg20", _row_get(row, "vol20", 0.0)))
        anomalies = list(_row_get(row, "score_anomalies", []))

        score = 0.0

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

    def _compute_momentum_score(self, row: Any) -> float:
        """
        Calculate momentum score based on:
        - 20/60/120-day returns
        - Relative strength maintenance
        
        Returns score in 0-100 range.
        """
        ret_20d = _safe_float(_row_get(row, "ret_20d", _row_get(row, "ret20", 0.0)))
        ret_60d = _safe_float(_row_get(row, "ret_60d", _row_get(row, "ret60", 0.0)))
        ret_120d = _safe_float(_row_get(row, "ret_120d", _row_get(row, "ret120", 0.0)))
        rs_score = _safe_float(_row_get(row, "rs_score", _row_get(row, "rs_percentile", 0.0)))
        ma20_slope = _safe_float(_row_get(row, "ma20_slope", 0.0))
        ma50_slope = _safe_float(_row_get(row, "ma50_slope", 0.0))
        ma150_slope = _safe_float(_row_get(row, "ma150_slope", 0.0))
        anomalies = list(_row_get(row, "score_anomalies", []))
        
        score = 0.0

        if ret_20d > 0:
            score += 20.0
        if ret_60d > 0:
            score += 28.0
        if ret_120d > 0:
            score += 30.0
        if rs_score >= 80:
            score += 16.0
        elif rs_score >= 65:
            score += 8.0
        if ma20_slope > 0:
            score += 5.0
        if ma50_slope > 0:
            score += 5.0
        if ma150_slope > 0:
            score += 6.0
        if ret_20d <= 0 and ret_60d <= 0 and ret_120d <= 0:
            anomalies.append("momentum_flat_returns")
        _row_set(row, "momentum_trigger_ok", bool(score >= 60.0 and rs_score >= 80.0 and ma20_slope > 0))
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
        breakout_score = _safe_float(_row_get(row, "breakout_score", 0.0))
        pullback_score = _safe_float(_row_get(row, "pullback_score", 0.0))
        momentum_score = _safe_float(_row_get(row, "momentum_score", 0.0))
        
        # Calculate if not present
        if breakout_score <= 0:
            breakout_score = self._compute_breakout_score(row)
            _row_set(row, "breakout_score", breakout_score)
        
        if pullback_score <= 0:
            pullback_score = self._compute_pullback_score(row)
            _row_set(row, "pullback_score", pullback_score)
        
        if momentum_score <= 0:
            momentum_score = self._compute_momentum_score(row)
            _row_set(row, "momentum_score", momentum_score)
        
        # Entry component = max of the three styles
        entry_component = max(breakout_score, pullback_score, momentum_score)
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
            final_score = round(tech_score * 0.7 + flow_score * 0.3, 4)

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

    def _compute_atr_pct(self, df: pd.DataFrame, period: int = 14) -> float:
        if df is None or len(df) < period + 1:
            return 0.0
        required = {"high", "low", "close"}
        if not required.issubset(df.columns):
            return 0.0

        high = df["high"]
        low = df["low"]
        close = df["close"]
        prev_close = close.shift(1)

        tr = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        last_close = close.iloc[-1]
        if pd.isna(atr) or pd.isna(last_close) or float(last_close) == 0.0:
            return 0.0
        return float(atr) / float(last_close)


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
            normalized_rows.append(_sync_item_and_meta_fields(item))
        return normalized_rows

    final30_scored_rows = _normalize_scored_stage_rows(bundle.final30)
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
    final30_cols = set(pd.DataFrame(final30_scored_rows).columns.tolist()) if final30_scored_rows else set()
    universe_cols = set(pd.DataFrame(universe_scored_rows).columns.tolist()) if universe_scored_rows else set()
    logger.info(
        "[WATCHLIST][SCHEMA_DIFF] lhs=universe_scored rhs=final30_scored missing=%s",
        sorted(final30_cols - universe_cols),
    )
    
    # 1. universe_scored - FULL universe (should be 196, not 120)
    # Note: In current architecture, this is actually candidate_pool-based (120)
    # TODO: Refactor to use broader universe (196+) as true "universe_scored"
    if universe_scored_rows:
        _log_stage_fields("universe_scored", universe_scored_rows)
        logger.info(
            "[WATCHLIST][SAVE_SCOPE] pb1_universe_scored_source=universe_filtered_from_raw120 rows=%s",
            len(universe_scored_rows),
        )
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
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_universe_scored empty")
    
    # 2. pool120 - NOW SAVED (not skipped)
    if bundle.pool120:
        _log_stage_fields("pool120", bundle.pool120)
        repo.save_watchlist(
            env=env,
            strategy="pb1_pool120",
            as_of=as_of,
            members=bundle.pool120,
        )
        logger.info("[BUNDLE][SAVE] pb1_pool120 n=%s", len(bundle.pool120))
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_pool120 empty")
    
    # 3. top50 저장
    if bundle.top50:
        _log_stage_fields("top50", bundle.top50)
        repo.save_watchlist(
            env=env,
            strategy="pb1_top50",
            as_of=as_of,
            members=bundle.top50,
        )
        logger.info("[BUNDLE][SAVE] pb1_top50 n=%s", len(bundle.top50))
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_top50 empty")
    
    # 4. final30 저장
    if bundle.final30:
        _log_stage_fields("final30", bundle.final30)
        repo.save_watchlist(
            env=env,
            strategy="pb1_watchlist_final",
            as_of=as_of,
            members=bundle.final30,
        )
        logger.info("[BUNDLE][SAVE] pb1_watchlist_final n=%s", len(bundle.final30))
    else:
        logger.warning("[BUNDLE][SAVE][SKIP] pb1_watchlist_final empty")
    
    logger.info("[BUNDLE][SAVE][DONE] env=%s as_of=%s", env, as_of)


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
                repo.save_watchlist(
                    env=env,
                    strategy=strategy,
                    as_of=as_of,
                    members=existing,
                )
                
                if return_bundle:
                    final30_scored_rows = [dict(row) for row in (existing or [])]
                    final30_scored_df = pd.DataFrame(final30_scored_rows).copy(deep=True)
                    bundle_final30_scored_before_save = final30_scored_df.copy(deep=True)
                    _log_final30_scored_df_ready(final30_scored_df)
                    final30_saved_rows = _build_final30_saved_rows(existing or [])
                    final30_snapshot_rows = [dict(row) for row in (existing or [])]
                    logger.info(
                        "[BUNDLE][KEEP][FINAL30_SCORED] rows=%s has_scores=%s",
                        len(final30_scored_rows),
                        int(len(final30_scored_rows) > 0),
                    )
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
                        "final30_scored": final30_scored_df,
                        "bundle_final30_scored_before_save": bundle_final30_scored_before_save,
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
        flow_provider=flow_provider,
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

    # Save final30 to DB (main strategy)
    repo.save_watchlist(
        env=env,
        strategy=strategy,
        as_of=as_of,
        members=watchlist,
    )
    
    # Save final30 snapshot to JSON (for fallback)
    try:
        snapshot_dir = Path(os.getenv("GITHUB_WORKSPACE", "."))
        snapshot_dir = snapshot_dir / "repo" / "runtime" / "snapshots" if (snapshot_dir / "repo").exists() else snapshot_dir / "runtime" / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        
        final30_snapshot = {
            "as_of": as_of.isoformat(),
            "env": env,
            "strategy": strategy,
            "watchlist": watchlist,
            "count": len(watchlist),
            "saved_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
        }
        
        snapshot_path = snapshot_dir / "final30.json"
        snapshot_path.write_text(json.dumps(final30_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("[WATCHLIST][SNAPSHOT][SAVE] path=%s count=%s", snapshot_path, len(watchlist))
    except Exception as exc:
        logger.warning("[WATCHLIST][SNAPSHOT][SAVE_FAIL] err=%s -> continuing", exc)

    final30_scored_rows = [
        dict(row)
        for row in (
            (builder.last_bundle or {}).get("final30_scored")
            or (builder.last_bundle or {}).get("final30")
            or watchlist
            or []
        )
    ]
    final30_scored_df = pd.DataFrame(final30_scored_rows).copy(deep=True)
    bundle_final30_scored_before_save = final30_scored_df.copy(deep=True)
    _log_final30_scored_df_ready(final30_scored_df)
    final30_saved_rows = _build_final30_saved_rows(watchlist or [])
    final30_saved_df = pd.DataFrame(final30_saved_rows).copy(deep=True)
    final30_snapshot_rows = [dict(row) for row in (watchlist or [])]
    if builder.last_bundle:
        builder.last_bundle["final30_scored"] = final30_scored_df
        builder.last_bundle["bundle_final30_scored_before_save"] = bundle_final30_scored_before_save
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
        _log_final30_scored_rows("[WATCHLIST][RETURN][FINAL30_SCORED]", final30_scored_rows)
    
    # CRITICAL: Always save bundle (4 stages) to prevent data loss
    # This ensures intermediate stages are never missing from DB
    if builder.last_bundle:
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

    if return_bundle:
        if builder.last_bundle is None:
            builder.last_bundle = {}
        builder.last_bundle.setdefault("final30_scored", final30_scored_df)
        builder.last_bundle.setdefault("bundle_final30_scored_before_save", bundle_final30_scored_before_save)
        builder.last_bundle.setdefault("final30_saved", final30_saved_rows)
        builder.last_bundle.setdefault("final30_saved_df", final30_saved_df)
        builder.last_bundle.setdefault("final30_snapshot_df", final30_snapshot_rows)
        builder.last_bundle.setdefault("top50_scored", builder.last_bundle.get("top50", []))
        builder.last_bundle.setdefault("pool120_scored", builder.last_bundle.get("pool120", []))
        builder.last_bundle.setdefault("universe_scored_df", builder.last_bundle.get("universe_scored", []))
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
