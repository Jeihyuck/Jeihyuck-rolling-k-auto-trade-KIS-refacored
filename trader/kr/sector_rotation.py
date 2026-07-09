# -*- coding: utf-8 -*-
"""KR sector rotation helpers for market-state overlay."""
from __future__ import annotations
from typing import Any


def summarize_sector_rotation(sector_returns: dict[str, dict[str, Any]] | None) -> dict[str, Any]:
    sector_returns = sector_returns or {}
    valid = {k: v for k, v in sector_returns.items() if (v or {}).get("source_quality") != "suspect" and (v or {}).get("return") is not None}
    leaders = sorted(valid.items(), key=lambda kv: float(kv[1].get("return") or 0.0), reverse=True)
    return {"leaders": [k for k, _ in leaders[:3]], "valid_sector_count": len(valid), "suspect_sector_count": len(sector_returns) - len(valid)}
