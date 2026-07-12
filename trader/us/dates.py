# -*- coding: utf-8 -*-
"""US date normalization helpers."""
from __future__ import annotations

from datetime import date, datetime


def canonical_us_bar_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            return None
    if len(raw) >= 10:
        try:
            return datetime.fromisoformat(raw[:10]).date()
        except ValueError:
            return None
    return None


def canonical_us_bar_date_str(value: object) -> str | None:
    d = canonical_us_bar_date(value)
    return d.isoformat() if d else None
