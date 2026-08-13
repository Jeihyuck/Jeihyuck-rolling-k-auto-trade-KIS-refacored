from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    return str(os.getenv(name, "1" if default else "0")).strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class InfiniteConfig:
    enabled: bool = True
    real_order: bool = True
    allow_buy: bool = True
    allow_sell: bool = True
    symbol: str = "122630"
    capital_krw: int = 15_000_000
    units: int = 40
    unit_krw: int = 375_000
    policy_version: str = "KR_INF_REGIME_GUARD_V1"
    take_profit_pct: float = .08
    buy_fee_rate: float = .00015
    sell_fee_rate: float = .00015
    sell_tax_rate: float = .0018
    quote_max_age_seconds: int = 60
    # Set in code only after the reproducible long-horizon validation passes.
    # Enabled remains true so reconcile/sell/telemetry continue fail-closed.
    deployable_buy_policy: bool = False

    @classmethod
    def from_env(cls) -> "InfiniteConfig":
        p = "KR_INFINITE_"
        return cls(
            enabled=_bool(p + "ENABLED", True), real_order=_bool(p + "REAL_ORDER", True),
            allow_buy=_bool(p + "ALLOW_BUY", True), allow_sell=_bool(p + "ALLOW_SELL", True),
            symbol=(os.getenv(p + "SYMBOL", "122630").strip() or "122630").zfill(6),
            capital_krw=_int(p + "CAPITAL_KRW", 15_000_000), units=_int(p + "UNITS", 40),
            unit_krw=_int(p + "UNIT_KRW", 375_000),
            policy_version=os.getenv(p + "POLICY_VERSION", "KR_INF_REGIME_GUARD_V1").strip(),
        )

    def validate(self) -> None:
        if self.symbol != "122630":
            raise ValueError("KR Infinite is restricted to 122630")
        if min(self.capital_krw, self.units, self.unit_krw) <= 0:
            raise ValueError("capital and units must be positive")
        if self.unit_krw * self.units != self.capital_krw:
            raise ValueError("unit allocation must exactly equal capital hard cap")
