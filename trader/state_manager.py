from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

from trader.config import LOG_DIR, STATE_FILE

logger = logging.getLogger(__name__)


_ENGINE_STATE_DIR = Path(__file__).parent / "engine_states"
_ENGINE_STATE_DIR.mkdir(exist_ok=True)


def _engine_state_file(engine_name: str) -> Path:
    name = (engine_name or "").strip().lower()
    if name in ("kosdaq", "kosdaq_alpha", "kosdaq_alpha_engine"):
        return STATE_FILE
    return _ENGINE_STATE_DIR / f"{name or 'engine'}.json"


def load_state(engine_name: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    state_path = _engine_state_file(engine_name)
    if state_path.exists():
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            return state.get("holding", {}) or {}, state.get("traded", {}) or {}
        except Exception:
            logger.exception("[STATE][%s] load failed", engine_name)
    return {}, {}


def save_state(engine_name: str, holding: Dict[str, Any], traded: Dict[str, Any]) -> None:
    state_path = _engine_state_file(engine_name)
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("[STATE][%s] save failed", engine_name)


def state_path(engine_name: str) -> Path:
    return _engine_state_file(engine_name)


def log_dir() -> Path:
    return LOG_DIR
