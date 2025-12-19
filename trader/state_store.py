from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .config import KST

logger = logging.getLogger(__name__)


def _default_state() -> Dict[str, Any]:
    return {"version": 1, "lots": [], "updated_at": None}


def load_state(path_json: str) -> Dict[str, Any]:
    path = Path(path_json)
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                state = json.load(f)
            if not isinstance(state, dict):
                logger.warning("[STATE_STORE] invalid state format: %s", type(state))
                return _default_state()
            state.setdefault("version", 1)
            state.setdefault("lots", [])
            state.setdefault("updated_at", None)
            return state
        except Exception:
            logger.exception("[STATE_STORE] failed to load %s", path_json)
            return _default_state()
    return _default_state()


def save_state(path_json: str, state: Dict[str, Any]) -> None:
    path = Path(path_json)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(state)
        payload.setdefault("version", 1)
        payload.setdefault("lots", [])
        payload["updated_at"] = datetime.now(KST).isoformat()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("[STATE_STORE] failed to save %s", path_json)
