from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from trader.time_utils import now_kst


logger = logging.getLogger(__name__)


def get_as_of_date() -> str:
    raw = (os.getenv("PB1_AS_OF_DATE") or "").strip()
    if raw:
        return raw
    return now_kst().date().isoformat()


def _final30_path(env: str, as_of: str) -> Path:
    return Path("bot_state") / "trader_ledger" / "reports" / as_of / f"final30.{env}.json"


def load_final30(env: str, as_of: str) -> list[str] | None:
    if (os.getenv("MODE") or "").strip().lower() == "trade":
        logger.warning(
            "[FINAL30][FILE_SNAPSHOT][DIAG_ONLY] env=%s as_of=%s mode=trade",
            env,
            as_of,
        )
    path = _final30_path(env=env, as_of=as_of)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    symbols = payload.get("symbols") or []
    result: list[str] = []
    for code in symbols:
        norm = str(code or "").zfill(6)
        if norm:
            result.append(norm)
    return result


def save_final30(
    env: str,
    as_of: str,
    symbols: list[str],
    meta: dict,
    overwrite: bool = False,
) -> Path:
    path = _final30_path(env=env, as_of=as_of)
    if path.exists() and not overwrite:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "env": env,
        "as_of": as_of,
        "name": (os.getenv("FINAL_LIST_NAME") or "final30").strip() or "final30",
        "symbols": [str(code or "").zfill(6) for code in symbols],
        "meta": meta or {},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
