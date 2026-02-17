from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)

_REQUIRED_EXPORT_KEYS = [
    "code",
    "rank",
    "score",
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


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(record or {})
    reject_reasons = _as_list(normalized.get("reject_reasons"))
    reasons = _as_list(normalized.get("reasons"))
    if not reasons and reject_reasons:
        reasons = list(reject_reasons)
    normalized["reasons"] = reasons
    normalized["filters_passed"] = _as_list(normalized.get("filters_passed"))
    normalized["filters_failed"] = _as_list(normalized.get("filters_failed") or reasons)

    normalized["rank_pool120"] = int(normalized.get("rank_pool120", 0) or 0)
    normalized["rank_top50"] = int(normalized.get("rank_top50", 0) or 0)
    normalized["rank_final30"] = int(normalized.get("rank_final30", 0) or 0)

    normalized["score_liq"] = float(normalized.get("score_liq", normalized.get("liq_avg", 0.0)) or 0.0)
    normalized["score_tech"] = float(normalized.get("score_tech", normalized.get("tech_score", 0.0)) or 0.0)
    normalized["score_flow"] = float(normalized.get("score_flow", normalized.get("flow_score", 0.0)) or 0.0)
    normalized["score_final"] = float(
        normalized.get("score_final", normalized.get("final_score", normalized.get("score", 0.0))) or 0.0
    )

    for key in _REQUIRED_EXPORT_KEYS:
        normalized.setdefault(key, [] if key in {"reasons", "filters_passed", "filters_failed"} else 0)

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

        csv_path = out_dir / f"{safe_name}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("[EXPORT] wrote %s", csv_path)
        written[f"{safe_name}_csv"] = csv_path

        json_path = out_dir / f"{safe_name}.json"
        payload = [_normalize_record(rec) for rec in df.to_dict(orient="records")]
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        logger.info("[EXPORT] wrote %s", json_path)
        written[f"{safe_name}_json"] = json_path

    meta_path = out_dir / "meta.json"
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta_dict or {}, f, ensure_ascii=False, indent=2, default=str)
    logger.info("[EXPORT] wrote %s", meta_path)
    written["meta_json"] = meta_path

    return written
