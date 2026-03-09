"""Entry engine package public API."""

from __future__ import annotations

import logging

from trader.entry_engine.entry_router import (
    generate_entry_signals,
    rank_signals,
    filter_by_regime,
)
from trader.entry_engine.entry_config import (
    ENTRY_WEIGHTS,
    STOP_RULES,
    STRATEGY_REGIME,
    MAX_POSITIONS_PER_STRATEGY,
)
from trader.entry_engine.compat import scan_all_strategies, calculate_position_size

logger = logging.getLogger(__name__)

__all__ = [
    "generate_entry_signals",
    "rank_signals",
    "filter_by_regime",
    "ENTRY_WEIGHTS",
    "STOP_RULES",
    "STRATEGY_REGIME",
    "MAX_POSITIONS_PER_STRATEGY",
    "scan_all_strategies",
    "calculate_position_size",
]

logger.info("[ENTRY_ENGINE][PUBLIC_API] exports=scan_all_strategies,calculate_position_size")
