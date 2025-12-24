from __future__ import annotations

from typing import Any


def normalize_code(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("A"):
        text = text[1:]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    digits = digits[-6:]
    return digits.zfill(6)
