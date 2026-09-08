from __future__ import annotations

from typing import Callable


def resolve_window_internal(
    *,
    internal: str,
    window_label: str | None,
    warn_on_mismatch: Callable[[str, str], None] | None = None,
) -> str:
    normalized = (window_label or "").strip().lower()
    label_map = {"preopen": "morning", "morning": "morning", "day": "day", "close": "close"}
    if normalized in label_map:
        forced = label_map[normalized]
        if internal != forced and warn_on_mismatch is not None:
            warn_on_mismatch(normalized, internal)
        return forced
    return internal
