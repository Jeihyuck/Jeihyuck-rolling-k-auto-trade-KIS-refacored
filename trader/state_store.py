from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .config import KST

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1


def _default_state() -> Dict[str, Any]:
    return {"version": SCHEMA_VERSION, "lots": [], "updated_at": None}


def load_state(path_json: str) -> Dict[str, Any]:
    path = Path(path_json)
    if not path.exists():
        return _default_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            logger.warning("[STATE_STORE] invalid state format: %s", type(state))
            return _default_state()
        state.setdefault("version", SCHEMA_VERSION)
        state.setdefault("lots", [])
        state.setdefault("updated_at", None)
        return state
    except Exception:
        logger.exception("[STATE_STORE] failed to load %s", path_json)
        return _default_state()


def save_state(path_json: str, state: Dict[str, Any]) -> None:
    path = Path(path_json)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(state)
        payload.setdefault("version", SCHEMA_VERSION)
        payload.setdefault("lots", [])
        payload["updated_at"] = datetime.now(KST).isoformat()
        tmp_path = path.with_name(f"{path.name}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        logger.exception("[STATE_STORE] failed to save %s", path_json)
