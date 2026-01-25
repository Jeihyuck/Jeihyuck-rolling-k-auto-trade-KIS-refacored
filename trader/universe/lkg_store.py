from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from typing import Optional

from trader.botstate_paths import runtime_root
from trader.config import UNIVERSE_NAMESPACE_MODE

logger = logging.getLogger(__name__)

_LKG_ROOT = Path(
    os.getenv("UNIVERSE_LKG_DIR")
    or os.getenv("UNIVERSE_LKG_ROOT")
    or (runtime_root() / "universe_lkg")
)


def lkg_path(env: str, strategy: str) -> Path:
    env_norm = (env or "practice").lower()
    namespace = _resolve_namespace(env_norm)
    strategy_norm = strategy or "default"
    return _LKG_ROOT / namespace / strategy_norm / "latest.json"


def _history_path(env: str, strategy: str, as_of: str | None) -> Path:
    env_norm = (env or "practice").lower()
    namespace = _resolve_namespace(env_norm)
    strategy_norm = strategy or "default"
    ts = as_of or datetime.utcnow().date().isoformat()
    return _LKG_ROOT / namespace / strategy_norm / "history" / f"{ts}.json"


def _resolve_namespace(env_norm: str) -> str:
    mode = (UNIVERSE_NAMESPACE_MODE or "ACCOUNT_ENV").upper()
    if mode == "ACCOUNT_ENV":
        cano = (os.getenv("CANO") or "").strip()
        acnt = (os.getenv("ACNT_PRDT_CD") or "").strip()
        raw = f"{env_norm}:{cano}:{acnt}"
        digest = sha1(raw.encode("utf-8")).hexdigest()[:10]
        return digest
    if mode == "ENV":
        return env_norm
    return env_norm


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
