from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Hashable, Mapping

import pandas as pd

from trader.score_columns import collect_nonzero_score_stats

logger = logging.getLogger(__name__)
def _collect_score_nonzero_stats(df: pd.DataFrame) -> tuple[Dict[str, int], Dict[str, str | None]]:
    stats, alias_cols = collect_nonzero_score_stats(df, ("tech", "final", "breakout", "pullback", "momentum"))
    for key in ("tech_nonzero", "final_nonzero", "score_final_nonzero", "breakout_nonzero", "pullback_nonzero", "momentum_nonzero"):
        stats.setdefault(key, 0)
    for key in ("tech", "final", "breakout", "pullback", "momentum"):
        alias_cols.setdefault(key, None)
    return stats, alias_cols

_REQUIRED_EXPORT_KEYS = [
    "as_of",
    "code",
    "name",
    "rank",
    "score",
    "tech_score",
    "flow_score",
    "final_score",
    "scores",
    "reasons",
    "filters_passed",
    "filters_failed",
    "rank_pool120",
    "rank_top50",
    "rank_final30",
    "score_liq",
    "score_tech",
    "score_flow",
    "score_final",
]


def _safe_float(val: Any) -> float | None:
    try:
        if val is None:
            return None
        if isinstance(val, str) and not val.strip():
            return None
        out = float(val)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _normalize_reasons(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        if all(isinstance(x, str) for x in value):
            return {"bullets": list(value)}
        if all(isinstance(x, dict) for x in value):
            merged: Dict[str, Any] = {}
            can_merge = True
            for entry in value:
                for key, item_value in entry.items():
                    if key in merged:
                        can_merge = False
                        break
                    merged[key] = item_value
                if not can_merge:
                    break
            return merged if can_merge else {"items": list(value)}
        return {"raw": str(value)}
    return {"raw": str(value)}


def _normalize_record(record: Mapping[Hashable, Any]) -> Dict[str, Any]:
    # Pandas records can carry non-str key types; normalize keys to strings for stable schema handling.
    normalized: Dict[str, Any] = {str(k): v for k, v in dict(record or {}).items()}
    meta = dict(normalized.get("meta") or {})

    def _pick(*keys: str, default: Any = 0.0) -> Any:
        for key in keys:
            if key in normalized:
                val = normalized.get(key)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    return val
        for key in keys:
            if key in meta:
                val = meta.get(key)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    return val
        return default

    def _pick_score(*keys: str) -> float:
        # Keep original score values if present; do not overwrite with default 0.
        for key in keys:
            val = _safe_float(normalized.get(key, None))
            if val is not None:
                return float(val)
        for key in keys:
            val = _safe_float(meta.get(key, None))
            if val is not None:
                return float(val)
        return 0.0

    reject_reasons = _as_list(normalized.get("reject_reasons"))
    reasons = _normalize_reasons(normalized.get("reasons"))

    failed = _as_list(reasons.get("failed"))
    if not failed and reject_reasons:
        failed = list(reject_reasons)
    reasons["failed"] = failed
    reasons.setdefault("passed", _as_list(reasons.get("passed")))
    reasons.setdefault("notes", dict(reasons.get("notes") or {}))
    normalized["reasons"] = reasons

    scores = normalized.get("scores")
    if not isinstance(scores, dict):
        scores = {}
    scores.setdefault("rs_pctile", float(_pick("rs_pctile") or 0.0))
    scores.setdefault("vcp_score", float(_pick("vcp_score") or 0.0))
    scores.setdefault("trend_template", 1 if float(_pick("trend_score") or 0.0) >= 100.0 else 0)
    scores.setdefault("liquidity_rank", int(_pick("rank_pool120", "pool_rank", default=0) or 0))
    scores.setdefault("pullback_score", max(0.0, min(100.0, 100.0 - float(_pick("pullback_pct") or 0.0) * 400.0)))
    scores.setdefault("foreign_score", max(0.0, min(100.0, float(_pick("foreign_20_ratio") or 0.0) * 100.0)))
    scores.setdefault("inst_score", max(0.0, min(100.0, float(_pick("inst_20_ratio") or 0.0) * 100.0)))
    normalized["scores"] = scores

    normalized["as_of"] = str(_pick("as_of", default="") or "")
    normalized["name"] = str(_pick("name", default="") or "")
    
    # Enhanced score normalization with proper priority
    tech_score = _pick_score("tech_score", "score_tech")
    flow_score = _pick_score("flow_score", "score_flow")
    score_final = _pick_score("score_final", "final_score", "score")
    
    normalized["tech_score"] = tech_score
    normalized["flow_score"] = flow_score
    normalized["final_score"] = score_final
    
    # Add entry-style scores
    normalized["breakout_score"] = _pick_score("breakout_score", "score_breakout")
    normalized["pullback_score"] = _pick_score("pullback_score", "score_pullback")
    normalized["momentum_score"] = _pick_score("momentum_score", "score_momentum")
    normalized["entry_style_selected"] = str(_pick("entry_style_selected", "entry_style", default="") or "")
    normalized["entry_component"] = float(_pick("entry_component", default=0.0) or 0.0)

    normalized["filters_passed"] = _as_list(normalized.get("filters_passed"))
    normalized["filters_failed"] = _as_list(normalized.get("filters_failed") or failed)

    normalized["rank_pool120"] = int(_pick("rank_pool120", default=0) or 0)
    normalized["rank_top50"] = int(_pick("rank_top50", default=0) or 0)
    normalized["rank_final30"] = int(_pick("rank_final30", default=0) or 0)

    normalized["score_liq"] = _pick_score("score_liq", "liq_avg")

    # Legacy score_tech/score_flow/score_final for backward compatibility
    score_tech = _pick_score("score_tech", "tech_score")
    if score_tech <= 0.0:
        fallback_tech = _pick_score("tech_score")
        if fallback_tech > 0.0:
            score_tech = fallback_tech
    normalized["score_tech"] = score_tech

    score_flow_legacy = _pick_score("score_flow", "flow_score")
    if score_flow_legacy <= 0.0:
        fallback_flow = _pick_score("flow_score")
        if fallback_flow > 0.0:
            score_flow_legacy = fallback_flow
    normalized["score_flow"] = score_flow_legacy

    score_final_legacy = _pick_score("score_final", "final_score", "score")
    if score_final_legacy <= 0.0:
        fallback_final = _pick_score("final_score", "score")
        if fallback_final > 0.0:
            score_final_legacy = fallback_final
    normalized["score_final"] = score_final_legacy

    for key in _REQUIRED_EXPORT_KEYS:
        if key in {"filters_passed", "filters_failed"}:
            normalized.setdefault(key, [])
        elif key in {"reasons", "scores"}:
            normalized.setdefault(key, {})
        elif key in {"as_of", "name"}:
            normalized.setdefault(key, "")
        else:
            normalized.setdefault(key, 0)

    return normalized


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=_REQUIRED_EXPORT_KEYS)
    records = [_normalize_record(rec) for rec in frame.to_dict(orient="records")]
    return pd.DataFrame(records)


def export_watchlist_bundle(
    *,
    out_dir: Path,
    frames_dict: Dict[str, pd.DataFrame],
    meta_dict: Dict[str, Any],
) -> Dict[str, Path]:
    """Watchlist 산출물을 CSV/JSON으로 저장한다."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: Dict[str, Path] = {}
    
    # Validate critical frames before export
    validation_failures = []
    if "universe_scored" in frames_dict:
        df = frames_dict["universe_scored"]
        if df is None or df.empty or len(df) == 0:
            validation_failures.append("universe_scored:empty")
    
    if "pool120" in frames_dict:
        df = frames_dict["pool120"]
        if df is None or df.empty or len(df) < 40:
            validation_failures.append(f"pool120:too_small:{len(df) if df is not None else 0}")
    
    if "top50" in frames_dict:
        df = frames_dict["top50"]
        if df is None or df.empty or len(df) < 40:
            validation_failures.append(f"top50:too_small:{len(df) if df is not None else 0}")
    
    if "final30" in frames_dict:
        df = frames_dict["final30"]
        if df is None or df.empty or len(df) < 30:
            validation_failures.append(f"final30:too_small:{len(df) if df is not None else 0}")
    
    if validation_failures:
        logger.error(
            "[EXPORT][VALIDATION_FAIL] failures=%s -> BLOCKING export to prevent corrupt data",
            validation_failures
        )
        # Add validation failures to meta for debugging
        meta_dict = dict(meta_dict) if meta_dict else {}
        meta_dict["export_validation_failures"] = validation_failures
        meta_dict["export_blocked"] = True
        meta_dict["export_blocked_reason"] = "validation_failed"
        
        # Write minimal meta.json to document the failure
        meta_path = out_dir / "meta.json"
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta_dict, f, ensure_ascii=False, indent=2, default=str)
        logger.info("[EXPORT][META_ONLY] wrote failure meta to %s", meta_path)
        
        # Raise exception to signal PREP that export failed
        raise ValueError(
            f"EXPORT_VALIDATION_FAILED: {validation_failures}. "
            f"Source data must be fixed before export. Check PREP pipeline for bundle recovery."
        )

    for name, frame in frames_dict.items():
        safe_name = name.strip().lower()
        pre_df = frame if frame is not None else pd.DataFrame()
        pre_stats, pre_alias_cols = _collect_score_nonzero_stats(pre_df)
        if safe_name == "final30":
            logger.info("[EXPORT][FINAL30][SOURCE_COLUMNS] cols=%s", sorted(pre_df.columns.tolist()))
            logger.info(
                "[EXPORT][FINAL30][ALIAS] breakout_col=%s pullback_col=%s momentum_col=%s tech_col=%s final_col=%s",
                pre_alias_cols.get("breakout") or "",
                pre_alias_cols.get("pullback") or "",
                pre_alias_cols.get("momentum") or "",
                pre_alias_cols.get("tech") or "",
                pre_alias_cols.get("final") or "",
            )
        logger.info(
            "[EXPORT][PRE_NORMALIZE][SCORES] name=%s rows=%s tech_nonzero=%s score_final_nonzero=%s breakout_nonzero=%s pullback_nonzero=%s momentum_nonzero=%s",
            safe_name,
            int(len(pre_df)),
            pre_stats["tech_nonzero"],
            pre_stats["score_final_nonzero"],
            pre_stats["breakout_nonzero"],
            pre_stats["pullback_nonzero"],
            pre_stats["momentum_nonzero"],
        )

        df = _normalize_frame(frame if frame is not None else pd.DataFrame())
        
        # Log warning for empty critical frames (should not happen if validation passed)
        if df.empty and name in {"universe_scored", "pool120", "top50", "final30"}:
            logger.warning(
                "[EXPORT][EMPTY_FRAME] name=%s rows=0 -> this should not happen after validation",
                safe_name
            )
        
        # Log field presence before export
        if not df.empty:
            has_breakout = 1 if "breakout_score" in df.columns else 0
            has_pullback = 1 if "pullback_score" in df.columns else 0
            has_momentum = 1 if "momentum_score" in df.columns else 0
            has_rs = 1 if "rs_score" in df.columns or "rs_pctile" in df.columns else 0
            has_vcp = 1 if "vcp_score" in df.columns else 0
            has_trend = 1 if "trend_score" in df.columns or "trend_template" in df.columns else 0
            has_tech = 1 if "tech_score" in df.columns or "score_tech" in df.columns else 0
            has_flow = 1 if "flow_score" in df.columns or "score_flow" in df.columns else 0
            has_final = 1 if "score_final" in df.columns or "final_score" in df.columns else 0
            
            logger.info(
                "[EXPORT][FIELDS] name=%s includes_breakout=%s includes_pullback=%s includes_momentum=%s includes_rs=%s includes_vcp=%s includes_trend=%s includes_tech=%s includes_flow=%s includes_final=%s",
                safe_name,
                has_breakout,
                has_pullback,
                has_momentum,
                has_rs,
                has_vcp,
                has_trend,
                has_tech,
                has_flow,
                has_final,
            )
        
        if not df.empty:
            post_stats, _post_alias_cols = _collect_score_nonzero_stats(df)
            tech_nonzero = post_stats["tech_nonzero"]
            score_final_nonzero = post_stats["score_final_nonzero"]
            final_score_nonzero = post_stats["final_nonzero"]
            breakout_nonzero = post_stats["breakout_nonzero"]
            pullback_nonzero = post_stats["pullback_nonzero"]
            momentum_nonzero = post_stats["momentum_nonzero"]
            
            logger.info(
                "[EXPORT][SCORES] name=%s rows=%s tech_nonzero=%s score_final_nonzero=%s final_score_nonzero=%s breakout_nonzero=%s pullback_nonzero=%s momentum_nonzero=%s",
                safe_name,
                int(len(df)),
                tech_nonzero,
                score_final_nonzero,
                final_score_nonzero,
                breakout_nonzero,
                pullback_nonzero,
                momentum_nonzero,
            )
            logger.info(
                "[EXPORT][POST_NORMALIZE][SCORES] name=%s rows=%s tech_nonzero=%s score_final_nonzero=%s breakout_nonzero=%s pullback_nonzero=%s momentum_nonzero=%s",
                safe_name,
                int(len(df)),
                tech_nonzero,
                score_final_nonzero,
                breakout_nonzero,
                pullback_nonzero,
                momentum_nonzero,
            )

            if pre_stats["tech_nonzero"] > tech_nonzero or pre_stats["score_final_nonzero"] > score_final_nonzero:
                logger.warning(
                    "[EXPORT][VALUE_RESET][WARN] name=%s tech_pre=%s tech_post=%s final_pre=%s final_post=%s",
                    safe_name,
                    pre_stats["tech_nonzero"],
                    tech_nonzero,
                    pre_stats["score_final_nonzero"],
                    score_final_nonzero,
                )
            if pre_stats["breakout_nonzero"] > breakout_nonzero or pre_stats["pullback_nonzero"] > pullback_nonzero or pre_stats["momentum_nonzero"] > momentum_nonzero:
                logger.warning(
                    "[EXPORT][FIELD_LOSS][WARN] name=%s breakout_pre=%s breakout_post=%s pullback_pre=%s pullback_post=%s momentum_pre=%s momentum_post=%s",
                    safe_name,
                    pre_stats["breakout_nonzero"],
                    breakout_nonzero,
                    pre_stats["pullback_nonzero"],
                    pullback_nonzero,
                    pre_stats["momentum_nonzero"],
                    momentum_nonzero,
                )

        csv_path = out_dir / f"{safe_name}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("[EXPORT] wrote %s rows=%s", csv_path, len(df))
        written[f"{safe_name}_csv"] = csv_path

        json_path = out_dir / f"{safe_name}.json"
        payload = [_normalize_record(rec) for rec in df.to_dict(orient="records")]
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        payload_nonzero = sum(1 for rec in payload if float(rec.get("score_final", 0.0) or 0.0) > 0.0)
        logger.info("[EXPORT] wrote %s rows=%s score_final_nonzero=%s", json_path, len(payload), payload_nonzero)
        written[f"{safe_name}_json"] = json_path

    meta_path = out_dir / "meta.json"
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta_dict or {}, f, ensure_ascii=False, indent=2, default=str)
    logger.info("[EXPORT] wrote %s", meta_path)
    written["meta_json"] = meta_path

    return written
