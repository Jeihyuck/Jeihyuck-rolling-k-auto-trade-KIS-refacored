# -*- coding: utf-8 -*-
"""Small KR sector classifier used by market-state overlay tests and PB1 glue."""
from __future__ import annotations

SECTOR_BY_SYMBOL_PREFIX = {"005930": "SEMICONDUCTOR", "000660": "SEMICONDUCTOR"}


def classify_kr_sector(symbol: str | None, row: dict | None = None) -> str:
    row = row or {}
    explicit = row.get("sector") or row.get("theme") or row.get("industry")
    if explicit:
        return str(explicit).strip().upper().replace(" ", "_")
    code = str(symbol or row.get("code") or row.get("symbol") or "").zfill(6)
    return SECTOR_BY_SYMBOL_PREFIX.get(code, "UNKNOWN")
