from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import os


def _flag(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    if raw not in {"0", "1", "true", "false", "yes", "no", "on", "off"}:
        raise ValueError(f"{name} must be boolean")
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class InfiniteConfig:
    enabled: bool = True
    symbol: str = "122630"
    capital_krw: Decimal = Decimal("15000000")
    units: int = 40
    unit_krw: Decimal = Decimal("375000")
    allow_buy: bool = True
    allow_sell: bool = True
    order_mode: str = "INHERIT"
    target_net_return: Decimal = Decimal("0.10")
    buy_fee_rate: Decimal = Decimal("0.00015")
    sell_fee_rate: Decimal = Decimal("0.00015")
    sell_tax_rate: Decimal = Decimal("0.0018")
    slippage_rate: Decimal = Decimal("0.0005")
    policy_version: str = "KR_INF_V2_1"

    def __post_init__(self) -> None:
        if self.symbol != "122630":
            raise ValueError("KR infinite sleeve is fixed to 122630")
        if self.capital_krw <= 0 or self.units <= 0 or self.unit_krw <= 0:
            raise ValueError("capital and units must be positive")
        if self.capital_krw != self.unit_krw * self.units:
            raise ValueError("capital must equal units * unit_krw")
        if self.order_mode != "INHERIT":
            raise ValueError("only INHERIT order mode is supported")

    @classmethod
    def from_env(cls) -> "InfiniteConfig":
        return cls(
            enabled=_flag("KR_INFINITE_ENABLED", True),
            symbol=os.getenv("KR_INFINITE_SYMBOL", "122630").strip(),
            capital_krw=Decimal(os.getenv("KR_INFINITE_CAPITAL_KRW", "15000000")),
            units=int(os.getenv("KR_INFINITE_UNITS", "40")),
            unit_krw=Decimal(os.getenv("KR_INFINITE_UNIT_KRW", "375000")),
            allow_buy=_flag("KR_INFINITE_ALLOW_BUY", True),
            allow_sell=_flag("KR_INFINITE_ALLOW_SELL", True),
            order_mode=os.getenv("KR_INFINITE_ORDER_MODE", "INHERIT").strip().upper(),
        )

    def effective_env(self, kis_env: str) -> str:
        value = kis_env.strip().lower()
        if value == "practice": return "practice"
        if value in {"real", "prod"}: return "real"
        raise ValueError("unsupported KIS_ENV")
