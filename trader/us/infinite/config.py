from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool = False) -> bool:
    return str(os.getenv(name, "1" if default else "0")).strip().lower() in {"1", "true", "yes", "on"}


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class InfiniteConfig:
    policy_version: str = "ADAPTIVE_RUNWAY_V2"
    enabled: bool = True
    real_order: bool = True
    allow_buy: bool = True
    allow_sell: bool = True
    symbol: str = "TQQQ"
    total_capital_usd: float = 10_000.0
    core_capital_usd: float = 7_500.0
    reserve_capital_usd: float = 2_500.0
    unit_usd: float = 250.0
    max_daily_buy_usd: float = 250.0
    max_total_capital_usd: float = 10_000.0
    buy_premium_pct: float = 0.03
    take_profit_pct: float = 0.10
    drawdown_pause_pct: float = -0.30
    max_cycle_age_trading_days: int = 120
    routine_core_usd: float = 5_000.0
    deep_bear_unlock_drawdown: float = -0.15
    capital_preservation_drawdown: float = -0.30
    capital_preservation_core_used: float = 7_000.0
    chop_rv20_min: float = 0.35
    chop_efficiency_max: float = 0.25
    bear_step: float = 0.05
    chop_step: float = 0.075
    capital_preservation_step: float = 0.10
    bear_gap: int = 7
    bear_fallback_gap: int = 10
    chop_gap: int = 7
    capital_preservation_gap: int = 15
    rebound_cooldown: int = 3
    recovery_confirmation_days: int = 2

    @classmethod
    def from_env(cls) -> "InfiniteConfig":
        prefix = "US_TQQQ_INFINITE_"
        return cls(
            enabled=_bool(prefix + "ENABLED", True), real_order=_bool(prefix + "REAL_ORDER", True),
            allow_buy=_bool(prefix + "ALLOW_BUY", True), allow_sell=_bool(prefix + "ALLOW_SELL", True),
            symbol=os.getenv(prefix + "SYMBOL", "TQQQ").upper().strip() or "TQQQ",
            total_capital_usd=_float(prefix + "TOTAL_CAPITAL_USD", 10_000),
            core_capital_usd=_float(prefix + "CORE_CAPITAL_USD", 7_500),
            reserve_capital_usd=_float(prefix + "RESERVE_CAPITAL_USD", 2_500),
            unit_usd=_float(prefix + "UNIT_USD", 250),
            max_daily_buy_usd=_float(prefix + "MAX_DAILY_BUY_USD", 250),
            max_total_capital_usd=_float(prefix + "MAX_TOTAL_CAPITAL_USD", 10_000),
            buy_premium_pct=_float(prefix + "BUY_PREMIUM_PCT", .03),
            take_profit_pct=_float(prefix + "TAKE_PROFIT_PCT", .10),
            drawdown_pause_pct=_float(prefix + "DRAWDOWN_PAUSE_PCT", -.30),
            max_cycle_age_trading_days=_int(prefix + "MAX_CYCLE_AGE_TRADING_DAYS", 120),
            policy_version=os.getenv(prefix + "POLICY_VERSION", "ADAPTIVE_RUNWAY_V2"),
            routine_core_usd=_float(prefix + "ROUTINE_CORE_USD", 5_000),
            deep_bear_unlock_drawdown=_float(prefix + "DEEP_BEAR_UNLOCK_DRAWDOWN", -.15),
            capital_preservation_drawdown=_float(prefix + "CAPITAL_PRESERVATION_DRAWDOWN", -.30),
            capital_preservation_core_used=_float(prefix + "CAPITAL_PRESERVATION_CORE_USED", 7_000),
            chop_rv20_min=_float(prefix + "CHOP_RV20_MIN", .35),
            chop_efficiency_max=_float(prefix + "CHOP_EFFICIENCY_MAX", .25),
            bear_step=_float(prefix + "BEAR_STEP", .05), chop_step=_float(prefix + "CHOP_STEP", .075),
            capital_preservation_step=_float(prefix + "CAPITAL_PRESERVATION_STEP", .10),
            bear_gap=_int(prefix + "BEAR_GAP", 7), bear_fallback_gap=_int(prefix + "BEAR_FALLBACK_GAP", 10),
            chop_gap=_int(prefix + "CHOP_GAP", 7),
            capital_preservation_gap=_int(prefix + "CAPITAL_PRESERVATION_GAP", 15),
            rebound_cooldown=_int(prefix + "REBOUND_COOLDOWN", 3),
            recovery_confirmation_days=_int(prefix + "RECOVERY_CONFIRMATION_DAYS", 2),
        )

    def validate(self) -> None:
        values = (self.total_capital_usd, self.core_capital_usd, self.reserve_capital_usd,
                  self.unit_usd, self.max_daily_buy_usd, self.max_total_capital_usd)
        if any(v <= 0 for v in values):
            raise ValueError("capital parameters must be positive")
        if self.core_capital_usd + self.reserve_capital_usd > self.max_total_capital_usd:
            raise ValueError("core plus reserve exceeds hard cap")
        if self.total_capital_usd > self.max_total_capital_usd:
            raise ValueError("total capital exceeds hard cap")
