from __future__ import annotations

from typing import Any, Callable

from .scanner import scan_entry_candidates
from .sizing import calculate_position_size as calculate_entry_position_size


def scan_all_strategies(*, watchlist: list[dict[str, Any]], ohlcv_provider: Callable[[str, int], Any]) -> dict[str, list[Any]]:
    return scan_entry_candidates(watchlist=watchlist, ohlcv_provider=ohlcv_provider)


def calculate_position_size(*, capital: float, entry_price: float, atr: float, risk_per_trade: float = 0.01) -> dict[str, float]:
    return calculate_entry_position_size(
        capital=capital,
        entry_price=entry_price,
        atr=atr,
        risk_per_trade=risk_per_trade,
    )
