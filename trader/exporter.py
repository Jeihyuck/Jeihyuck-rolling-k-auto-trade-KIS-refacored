from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)


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
        df = frame if frame is not None else pd.DataFrame()

        csv_path = out_dir / f"{safe_name}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("[EXPORT] wrote %s", csv_path)
        written[f"{safe_name}_csv"] = csv_path

        json_path = out_dir / f"{safe_name}.json"
        payload = df.to_dict(orient="records")
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
