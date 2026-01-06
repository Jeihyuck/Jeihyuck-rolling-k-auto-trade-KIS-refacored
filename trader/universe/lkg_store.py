from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_LKG_ROOT = Path(
    os.getenv("UNIVERSE_LKG_ROOT")
    or Path(__file__).resolve().parent.parent / "state" / "universe_lkg"
)


def lkg_path(env: str, strategy: str) -> Path:
    env_norm = (env or "practice").lower()
    strategy_norm = strategy or "default"
    return _LKG_ROOT / env_norm / strategy_norm / "latest.json"


def _history_path(env: str, strategy: str, as_of: str | None) -> Path:
    env_norm = (env or "practice").lower()
    strategy_norm = strategy or "default"
    ts = as_of or datetime.utcnow().date().isoformat()
    return _LKG_ROOT / env_norm / strategy_norm / "history" / f"{ts}.json"


def load_lkg(env: str, strategy: str) -> Optional[dict]:
    path = lkg_path(env, strategy)
    try:
        if not path.exists():
            logger.info("[UNIVERSE][LKG][MISS] env=%s strategy=%s path=%s", env, strategy, path)
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        as_of = payload.get("params", {}).get("as_of") or payload.get("as_of")
        logger.info("[UNIVERSE][LKG][HIT] env=%s strategy=%s as_of=%s", env, strategy, as_of or "unknown")
        return payload
    except Exception:
        logger.exception("[UNIVERSE][LKG][LOAD_FAIL] env=%s strategy=%s path=%s", env, strategy, path)
        return None


def save_lkg(env: str, strategy: str, payload: dict) -> None:
    path = lkg_path(env, strategy)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        as_of = payload.get("params", {}).get("as_of") or payload.get("as_of")
        logger.info("[UNIVERSE][LKG][SAVE] path=%s members=%s", path, len(payload.get("members") or []))
        hist = _history_path(env, strategy, as_of)
        hist.parent.mkdir(parents=True, exist_ok=True)
        hist.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logger.exception("[UNIVERSE][LKG][SAVE_FAIL] env=%s strategy=%s path=%s", env, strategy, path)
