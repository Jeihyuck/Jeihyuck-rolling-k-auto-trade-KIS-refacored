from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)

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


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(record or {})
    meta = dict(normalized.get("meta") or {})

    def _pick(*keys: str, default: Any = 0.0) -> Any:
        for key in keys:
            if key in normalized and normalized.get(key) is not None:
                return normalized.get(key)
        for key in keys:
            if key in meta and meta.get(key) is not None:
                return meta.get(key)
        return default

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
    normalized["tech_score"] = float(_pick("tech_score", "score_tech") or 0.0)
    normalized["flow_score"] = float(_pick("flow_score", "score_flow") or 0.0)
    normalized["final_score"] = float(_pick("final_score", "score_final", "score") or 0.0)

    normalized["filters_passed"] = _as_list(normalized.get("filters_passed"))
    normalized["filters_failed"] = _as_list(normalized.get("filters_failed") or failed)

    normalized["rank_pool120"] = int(_pick("rank_pool120", default=0) or 0)
    normalized["rank_top50"] = int(_pick("rank_top50", default=0) or 0)
    normalized["rank_final30"] = int(_pick("rank_final30", default=0) or 0)

    normalized["score_liq"] = float(_pick("score_liq", "liq_avg") or 0.0)

    score_tech = float(_pick("score_tech", "tech_score") or 0.0)
    if score_tech <= 0.0:
        fallback_tech = float(_pick("tech_score", default=0.0) or 0.0)
        if fallback_tech > 0.0:
            score_tech = fallback_tech
    normalized["score_tech"] = score_tech

    score_flow = float(_pick("score_flow", "flow_score") or 0.0)
    if score_flow <= 0.0:
        fallback_flow = float(_pick("flow_score", default=0.0) or 0.0)
        if fallback_flow > 0.0:
            score_flow = fallback_flow
    normalized["score_flow"] = score_flow

    score_final = float(_pick("score_final", "final_score", "score") or 0.0)
    if score_final <= 0.0:
        fallback_final = float(_pick("final_score", "score", default=0.0) or 0.0)
        if fallback_final > 0.0:
            score_final = fallback_final
    normalized["score_final"] = score_final

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

    for name, frame in frames_dict.items():
        safe_name = name.strip().lower()
        df = _normalize_frame(frame if frame is not None else pd.DataFrame())
        if not df.empty:
            score_final_nonzero = int((df["score_final"].fillna(0.0) > 0.0).sum()) if "score_final" in df.columns else 0
            final_score_nonzero = int((df["final_score"].fillna(0.0) > 0.0).sum()) if "final_score" in df.columns else 0
            logger.info(
                "[EXPORT][SCORES] name=%s rows=%s score_final_nonzero=%s final_score_nonzero=%s",
                safe_name,
                int(len(df)),
                score_final_nonzero,
                final_score_nonzero,
            )

        csv_path = out_dir / f"{safe_name}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("[EXPORT] wrote %s", csv_path)
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
