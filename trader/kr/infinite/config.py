from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, "1" if default else "0").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class InfiniteConfig:
    enabled: bool = False
    live: bool = False
    symbol: str = "122630"
    account_exposure_pct: float = .30
    safe_max_exposure_pct: float = .30
    total_units: int = 40
    core_units: int = 30
    reserve_units: int = 10
    take_profit_pct: float = .10
    risk_on_premium_pct: float = .02
    risk_on_gap_days: int = 1
    normal_gap_days: int = 2
    caution_discount_pct: float = .02
    caution_gap_days: int = 3
    risk_off_step_pct: float = .05
    risk_off_gap_days: int = 7
    recovery_probe_cooldown_days: int = 2
    long_cycle_days: int = 120
    long_cycle_step_pct: float = .07
    long_cycle_gap_days: int = 10
    same_day_restart: bool = False
    balance_reconcile_grace_attempts: int = 3

    @classmethod
    def from_env(cls) -> "InfiniteConfig":
        p = "KR_INFINITE_"
        f = lambda n, d: float(os.getenv(p + n, str(d)))
        i = lambda n, d: int(os.getenv(p + n, str(d)))
        return cls(enabled=_bool(p+"ENABLED"), live=_bool(p+"LIVE"), symbol=os.getenv(p+"SYMBOL", "122630").strip(),
                   account_exposure_pct=f("ACCOUNT_EXPOSURE_PCT", .30), safe_max_exposure_pct=f("SAFE_MAX_EXPOSURE_PCT", .30),
                   total_units=i("TOTAL_UNITS", 40), core_units=i("CORE_UNITS", 30), reserve_units=i("RESERVE_UNITS", 10),
                   take_profit_pct=f("TAKE_PROFIT_PCT", .10), risk_on_premium_pct=f("RISK_ON_PREMIUM_PCT", .02),
                   risk_on_gap_days=i("RISK_ON_GAP_DAYS", 1), normal_gap_days=i("NORMAL_GAP_DAYS", 2),
                   caution_discount_pct=f("CAUTION_DISCOUNT_PCT", .02), caution_gap_days=i("CAUTION_GAP_DAYS", 3),
                   risk_off_step_pct=f("RISK_OFF_STEP_PCT", .05), risk_off_gap_days=i("RISK_OFF_GAP_DAYS", 7),
                   recovery_probe_cooldown_days=i("RECOVERY_PROBE_COOLDOWN_DAYS", 2), long_cycle_days=i("LONG_CYCLE_DAYS", 120),
                   long_cycle_step_pct=f("LONG_CYCLE_STEP_PCT", .07), long_cycle_gap_days=i("LONG_CYCLE_GAP_DAYS", 10),
                   same_day_restart=_bool(p+"SAME_DAY_RESTART"),
                   balance_reconcile_grace_attempts=i("BALANCE_RECONCILE_GRACE_ATTEMPTS", 3))

    def validate(self) -> None:
        if self.symbol != "122630": raise ValueError("BLOCK_UNSUPPORTED_SYMBOL")
        if self.total_units != 40 or self.core_units != 30 or self.reserve_units != 10 or self.total_units != self.core_units + self.reserve_units:
            raise ValueError("INVALID_UNIT_CONFIGURATION")
        if not 0 < self.account_exposure_pct <= self.safe_max_exposure_pct <= 1: raise ValueError("INVALID_ACCOUNT_EXPOSURE")
        if self.take_profit_pct <= 0: raise ValueError("INVALID_TAKE_PROFIT")
        if self.balance_reconcile_grace_attempts < 1: raise ValueError("INVALID_BALANCE_RECONCILE_GRACE")

    def orders_allowed(self, kis_env: str) -> bool:
        """Practice orders need only ENABLED; real orders require the second gate."""
        env = kis_env.strip().lower()
        return self.enabled and (env not in {"real", "live"} or self.live)
