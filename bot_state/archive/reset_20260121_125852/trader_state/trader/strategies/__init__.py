"""Strategy package exposing a common interface for multi-strategy trading."""
from .base import BaseStrategy
from .strategy_breakout import BreakoutStrategy
from .strategy_pullback import PullbackStrategy
from .strategy_momentum import MomentumStrategy
from .strategy_mean_reversion import MeanReversionStrategy
from .strategy_volatility import VolatilityStrategy

__all__ = [
    "BaseStrategy",
    "BreakoutStrategy",
    "PullbackStrategy",
    "MomentumStrategy",
    "MeanReversionStrategy",
    "VolatilityStrategy",
]
