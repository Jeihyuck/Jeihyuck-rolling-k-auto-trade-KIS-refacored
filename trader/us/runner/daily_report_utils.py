# -*- coding: utf-8 -*-
"""Small pure helpers for the US daily report runner."""
from __future__ import annotations

import json


def fill_is_synthetic(fill: dict) -> bool:
    meta = fill.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return bool(
        meta.get("is_synthetic")
        or meta.get("synthetic")
        or meta.get("synthetic_fill")
        or meta.get("fill_evidence_type") in {"BALANCE_DELTA_SYNTHETIC", "LEGACY_SYNTHETIC"}
    )


def positive_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
