# -*- coding: utf-8 -*-
"""KR sector proxy basket return helpers for market-state overlay."""
from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)
ReturnProvider = Callable[[str, int], float | None]


def compute_sector_basket_return(sector: str, basket_symbols: list[str], lookback: int, return_provider: ReturnProvider) -> dict[str, Any]:
    symbols = [str(s).strip() for s in (basket_symbols or []) if str(s).strip()]
    returns: list[float] = []
    missing: list[str] = []
    for symbol in symbols:
        value = return_provider(symbol, int(lookback))
        if value is None:
            missing.append(symbol)
            continue
        returns.append(float(value))
    valid_count = len(returns)
    missing_count = len(missing)
    symbols_count = len(symbols)
    if symbols_count and valid_count == 0:
        logger.warning(
            "[KR_SECTOR_PROXY][BASKET_ALL_MISSING] sector=%s symbols=%s lookback=%s source=basket source_quality=suspect reason=all_basket_returns_missing valid_count=0 missing_count=%s symbols_count=%s",
            sector, symbols, lookback, missing_count, symbols_count,
        )
        source_quality = "suspect"
        avg_return = None
    else:
        source_quality = "medium" if symbols_count else "missing"
        avg_return = (sum(returns) / valid_count) if valid_count else None
    logger.info(
        "[KR_SECTOR_PROXY][RETURN] sector=%s lookback=%s source=basket source_quality=%s return=%s valid_count=%s missing_count=%s symbols_count=%s",
        sector, lookback, source_quality, avg_return, valid_count, missing_count, symbols_count,
    )
    return {
        "sector": sector,
        "source": "basket",
        f"return_{lookback}d": avg_return,
        "return": avg_return,
        "valid_count": valid_count,
        "missing_count": missing_count,
        "symbols_count": symbols_count,
        "source_quality": source_quality,
        "missing_symbols": missing,
    }


def load_sector_proxy_config(path: str) -> dict[str, Any]:
    import json
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))
