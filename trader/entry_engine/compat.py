from __future__ import annotations

from typing import Any, Callable

from .scanner import scan_entry_candidates
from .sizing import calculate_position_size as calculate_entry_position_size


def scan_all_strategies(
    *,
    watchlist: list[dict[str, Any]],
    ohlcv_provider: Callable[..., Any],
    precomputed_final30_df: Any | None = None,
    trade_precomputed_only: bool = False,
    data_metrics: dict[str, int] | None = None,
) -> dict[str, list[Any]]:
    return scan_entry_candidates(
        watchlist=watchlist,
        ohlcv_provider=ohlcv_provider,
        precomputed_final30_df=precomputed_final30_df,
        trade_precomputed_only=trade_precomputed_only,
        data_metrics=data_metrics,
    )


def calculate_position_size(*, capital: float, entry_price: float, atr: float, risk_per_trade: float = 0.01) -> dict[str, float]:
    return calculate_entry_position_size(
        capital=capital,
        entry_price=entry_price,
        atr=atr,
        risk_per_trade=risk_per_trade,
    )
