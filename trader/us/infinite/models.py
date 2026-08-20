from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from enum import Enum
from typing import Any


class Status(str, Enum):
    READY = "READY"
    ACTIVE = "ACTIVE"
    RESERVE = "RESERVE"
    PAUSED_CRASH = "PAUSED_CRASH"
    PAUSED_DRAWDOWN = "PAUSED_DRAWDOWN"
    PAUSED_AGE = "PAUSED_AGE"
    EXIT_PENDING = "EXIT_PENDING"
    COMPLETE = "COMPLETE"


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"
    BLOCK = "BLOCK"


@dataclass(frozen=True)
class InfiniteState:
    strategy_id: str = "TQQQ_INFINITE_V3"
    symbol: str = "TQQQ"
    cycle_id: str | None = None
    cycle_start_date: date | None = None
    cycle_complete_date: date | None = None
    anchor_price: float | None = None
    core_filled_notional: float = 0.0
    reserve_filled_notional: float = 0.0
    last_buy_date: date | None = None
    last_exit_date: date | None = None
    market_crash_streak: int = 0
    material_market_crash: bool = False
    reserve_unlocked: bool = False
    cycle_age_trading_days: int = 0
    status: Status = Status.READY
    metadata: dict[str, Any] = field(default_factory=dict)
    version: int = 1

    @property
    def total_filled_notional(self) -> float:
        return self.core_filled_notional + self.reserve_filled_notional

    def validate(self, hard_cap: float = 10_000.0) -> "InfiniteState":
        if not isinstance(self.status, Status):
            raise ValueError("invalid status")
        if min(self.core_filled_notional, self.reserve_filled_notional, self.market_crash_streak,
               self.cycle_age_trading_days) < 0:
            raise ValueError("negative state value")
        if self.total_filled_notional > hard_cap + 1e-6:
            raise ValueError("filled notional exceeds hard cap")
        if self.status not in {Status.READY, Status.COMPLETE} and not self.cycle_id:
            raise ValueError("active status without cycle")
        if self.reserve_unlocked and not (self.material_market_crash or self.metadata.get("structural_bear_seen")):
            raise ValueError("reserve unlocked without market crash")
        return self

    def with_status(self, status: Status) -> "InfiniteState":
        return replace(self, status=status)


@dataclass(frozen=True)
class PositionSnapshot:
    qty: int = 0
    orderable_qty: int | None = None
    average_price: float = 0.0
    price: float = 0.0
    exchange: str = "NASDAQ"


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str
    qty: int = 0
    notional: float = 0.0
    next_status: Status | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
