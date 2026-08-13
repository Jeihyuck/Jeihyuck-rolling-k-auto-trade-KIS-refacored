from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"
    BLOCK = "BLOCK"


@dataclass(frozen=True)
class InfiniteState:
    strategy_id: str = "KR_KODEX_LEVERAGE_INFINITE_V1"
    symbol: str = "122630"
    book: str = "KR_INFINITE_BOOK"
    cycle_id: str | None = None
    cycle_status: str = "READY"
    policy_version: str = "KR_INF_REGIME_GUARD_V1"
    filled_quantity: int = 0
    authoritative_buy_notional: float = 0.0
    authoritative_sell_notional: float = 0.0
    authoritative_average_price: float = 0.0
    used_unit_fraction: float = 0.0
    last_buy_trade_date: date | None = None
    pending_order_key: str | None = None
    current_regime_state: str | None = None
    current_regime_score: float | None = None
    data_quality: str = "UNKNOWN"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerPosition:
    quantity: int
    average_price: float
    orderable_cash: float


@dataclass(frozen=True)
class Quote:
    price: float
    observed_at: datetime


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str
    quantity: int = 0
    estimated_cost: float = 0.0
    client_order_key: str | None = None
