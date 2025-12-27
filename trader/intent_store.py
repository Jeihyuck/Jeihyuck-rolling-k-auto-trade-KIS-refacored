from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

logger = logging.getLogger(__name__)


def append_intents(intents: Iterable[Dict[str, Any]], path: Path) -> None:
    """Append intents as JSON lines."""

    intents_list = list(intents)
    if not intents_list:
        return

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(intent, ensure_ascii=False) for intent in intents_list]
    try:
        with open(path, "a", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
    except Exception:
        logger.exception("[INTENT_STORE] failed to append %d intents to %s", len(lines), path)


def _load_cursor(cursor_state_path: Path) -> Dict[str, Any]:
    cursor_state_path = Path(cursor_state_path)
    if not cursor_state_path.exists():
        return {"offset": 0}
    try:
        with open(cursor_state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            return {"offset": 0}
        state.setdefault("offset", 0)
        return state
    except Exception:
        logger.exception("[INTENT_STORE] failed to load cursor from %s", cursor_state_path)
        return {"offset": 0}


def load_intents_since_cursor(
    path: Path, cursor_state_path: Path
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Read new intents from JSONL starting at cursor offset."""

    intents: list[Dict[str, Any]] = []
    path = Path(path)
    cursor = _load_cursor(cursor_state_path)
    offset = int(cursor.get("offset") or 0)
    start_offset = offset
    last_intent_id = cursor.get("last_intent_id")
    last_ts = cursor.get("last_ts")

    if not path.exists():
        return intents, {
            "offset": offset,
            "last_intent_id": last_intent_id,
            "last_ts": last_ts,
            "start_offset": start_offset,
        }

    try:
        with open(path, "r", encoding="utf-8") as f:
            try:
                f.seek(offset)
            except OSError:
                f.seek(0)
            while True:
                line = f.readline()
                if not line:
                    break
                try:
                    end_offset = f.tell()
                except OSError:
                    end_offset = offset
                offset = end_offset
                if not line.strip():
                    continue
                try:
                    intent = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("[INTENT_STORE] skip invalid intent line: %s", line.strip())
                    continue
                if isinstance(intent, dict):
                    intent["_end_offset"] = end_offset
                    intents.append(intent)
                    last_intent_id = intent.get("intent_id") or last_intent_id
                    last_ts = intent.get("ts") or last_ts
    except Exception:
        logger.exception("[INTENT_STORE] failed to load intents from %s", path)

    return intents, {
        "offset": offset,
        "last_intent_id": last_intent_id,
        "last_ts": last_ts,
        "start_offset": start_offset,
    }


def save_cursor(
    cursor_state_path: Path,
    *,
    offset: int,
    last_intent_id: str | None = None,
    last_ts: str | None = None,
) -> None:
    payload = {
        "offset": int(offset),
        "last_intent_id": last_intent_id,
        "last_ts": last_ts,
    }
    cursor_state_path = Path(cursor_state_path)
    cursor_state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cursor_state_path.with_name(f"{cursor_state_path.name}.tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, cursor_state_path)
    except Exception:
        logger.exception("[INTENT_STORE] failed to save cursor to %s", cursor_state_path)


def dedupe_intents(intents: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    deduped: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for intent in intents:
        if not isinstance(intent, dict):
            continue
        intent_id = intent.get("intent_id")
        if intent_id:
            if intent_id in seen:
                continue
            seen.add(intent_id)
        deduped.append(intent)
    return deduped
