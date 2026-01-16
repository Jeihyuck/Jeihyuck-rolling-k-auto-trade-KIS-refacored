from __future__ import annotations

from datetime import datetime
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def event_path(as_of: str) -> Path:
    return Path("bot_state/runtime/events") / f"pb1_{as_of}.jsonl"


def emit_event(*, as_of: str, event: str, **fields) -> None:
    """
    Append JSON line.
    Always safe: best-effort, never crash trading loop.
    """
    try:
        p = event_path(as_of)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": datetime.now().isoformat(),
            "as_of": as_of,
            "event": event,
            **fields,
        }
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        logger.exception("[EVENTLOG][FAIL] event=%s", event)
