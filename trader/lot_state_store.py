from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

from .state_io import atomic_write_json

logger = logging.getLogger(__name__)


def load_lot_state(path_json: str) -> Dict[str, Any]:
    path = Path(path_json)
    if not path.exists():
        return {"lots": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            logger.warning("[LOT_STATE] invalid format: %s", type(state))
            return {"lots": []}
        state.setdefault("lots", [])
        return state
    except Exception:
        logger.exception("[LOT_STATE] failed to load %s", path_json)
        return {"lots": []}


def save_lot_state(path_json: str, state: Dict[str, Any]) -> None:
    path = Path(path_json)
    try:
        payload = dict(state)
        payload.setdefault("lots", [])
        atomic_write_json(path, payload)
    except Exception:
        logger.exception("[LOT_STATE] failed to save %s", path_json)
