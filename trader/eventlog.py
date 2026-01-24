from __future__ import annotations

from datetime import datetime
import json
import logging
from pathlib import Path

from trader.botstate_paths import runtime_root
from trader.utils.json_sanitize import to_jsonable

logger = logging.getLogger(__name__)


def event_path(as_of: str) -> Path:
    return runtime_root() / "runtime" / "events" / f"pb1_{as_of}.jsonl"


def emit_event(*, as_of: str, event: str, **fields) -> tuple[bool, Exception | None]:
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
        payload = to_jsonable(payload)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return True, None
    except Exception as exc:
        logger.exception("[EVENTLOG][FAIL] event=%s", event)
        return False, exc
