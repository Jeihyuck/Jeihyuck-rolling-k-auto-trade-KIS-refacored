from __future__ import annotations

import re
from typing import Any


MA20_NORMALIZE_PRIORITY = (
    "ma20",
    "ma_20",
    "sma20",
    "close_ma20",
    "moving_avg20",
    "avg20",
    "ma20_price",
)


def normalize_column_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def ma20_candidate_priority(column: Any) -> int:
    normalized = normalize_column_token(column)
    for idx, candidate in enumerate(MA20_NORMALIZE_PRIORITY):
        candidate_token = normalize_column_token(candidate)
        if normalized == candidate_token:
            return idx
        if normalized in {f"{candidate_token}x", f"{candidate_token}y"}:
            return idx + len(MA20_NORMALIZE_PRIORITY)
    if normalized in {"ma20x", "ma20y"}:
        return len(MA20_NORMALIZE_PRIORITY)
    return 10_000


def is_ma20_candidate_column(column: Any) -> bool:
    normalized = normalize_column_token(column)
    candidate_tokens = {normalize_column_token(name) for name in MA20_NORMALIZE_PRIORITY}
    if normalized in candidate_tokens:
        return True
    if normalized in {f"{token}x" for token in candidate_tokens}:
        return True
    if normalized in {f"{token}y" for token in candidate_tokens}:
        return True
    if "ma20" in normalized:
        return True
    if "ma" in normalized and "20" in normalized:
        return True
    return False
