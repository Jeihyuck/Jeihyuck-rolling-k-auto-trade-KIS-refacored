"""
Entry Engine - Multi-Strategy Signal Router
헤지펀드 수준의 멀티 전략 신호 라우팅 엔진
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, List

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

logger = logging.getLogger(__name__)

_LEGACY_MODULE: ModuleType | None = None


def _load_legacy_entry_engine_module() -> ModuleType:
    """Load legacy entry_engine.py module for backward compatibility."""
    global _LEGACY_MODULE
    if _LEGACY_MODULE is not None:
        return _LEGACY_MODULE

    legacy_module_path = Path(__file__).resolve().parent.parent / "entry_engine.py"
    spec = importlib.util.spec_from_file_location(
        "trader._legacy_entry_engine",
        legacy_module_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to load legacy entry engine from {legacy_module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _LEGACY_MODULE = module
    return module


def scan_all_strategies(
    *,
    watchlist: List[Dict[str, Any]],
    ohlcv_provider: Callable[[str, int], Any],
) -> Dict[str, List[Any]]:
    """Backward compatibility shim for legacy pb1_runner import path."""
    logger.info("[ENTRY_ENGINE][COMPAT] scan_all_strategies -> trader.entry_engine.py")
    legacy_module = _load_legacy_entry_engine_module()
    return legacy_module.scan_all_strategies(
        watchlist=watchlist,
        ohlcv_provider=ohlcv_provider,
    )


def calculate_position_size(
    *,
    capital: float,
    entry_price: float,
    atr: float,
    risk_per_trade: float = 0.01,
) -> Dict[str, float]:
    """Backward compatibility shim for legacy pb1_runner import path."""
    logger.info("[ENTRY_ENGINE][COMPAT] calculate_position_size -> trader.entry_engine.py")
    legacy_module = _load_legacy_entry_engine_module()
    return legacy_module.calculate_position_size(
        capital=capital,
        entry_price=entry_price,
        atr=atr,
        risk_per_trade=risk_per_trade,
    )

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
