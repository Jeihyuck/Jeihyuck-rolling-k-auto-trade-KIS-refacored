from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def resolve_entry_cutoff(*, now_kst: datetime, entry_window_end: str, entry_cutoff_time: str | None) -> tuple[datetime, str]:
    raw = (entry_cutoff_time or entry_window_end or "").strip()
    if not raw:
        raw = "15:15"
    try:
        cutoff_time = datetime.strptime(raw, "%H:%M").time()
    except ValueError:
        logger.warning("[PB1][ENV] invalid ENTRY_CUTOFF_TIME=%s fallback=%s", raw, entry_window_end)
        cutoff_time = datetime.strptime(entry_window_end, "%H:%M").time()
        raw = entry_window_end
    cutoff = datetime.combine(now_kst.date(), cutoff_time, tzinfo=now_kst.tzinfo)
    return cutoff, raw
