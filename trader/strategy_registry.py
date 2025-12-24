from __future__ import annotations

from typing import Any


VALID_SIDS = {f"S{i}" for i in range(1, 6)}


def normalize_sid(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, str):
        text = value.strip().upper()
    else:
        text = str(value).strip().upper()
    if text in VALID_SIDS:
        return text
    if text.isdigit() and 1 <= int(text) <= 5:
        return f"S{int(text)}"
    if text == "MANUAL":
        return "MANUAL"
    return "UNKNOWN"
