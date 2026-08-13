from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class CycleStatus(str, Enum):
    READY="READY"; ENTRY_PENDING="ENTRY_PENDING"; ACTIVE="ACTIVE"; EXIT_PENDING="EXIT_PENDING"
    RECONCILE_PENDING="RECONCILE_PENDING"; COMPLETE="COMPLETE"; OWNERSHIP_CONFLICT="OWNERSHIP_CONFLICT"


@dataclass(frozen=True)
class SleeveState:
    cycle_id: str
    status: CycleStatus = CycleStatus.READY
    filled_quantity: int = 0
    buy_notional: Decimal = Decimal(0)
    sell_notional: Decimal = Decimal(0)
    average_price: Decimal = Decimal(0)
    last_buy_date: date | None = None
    last_sell_date: date | None = None
    pending_order_key: str | None = None
    reconciled: bool = True


@dataclass(frozen=True)
class MarketInput:
    trade_date: date
    quote_price: Decimal
    quote_at: datetime
    regime_state: str
    regime_as_of: date
    regime_at: datetime
    data_quality: str = "OK"
    regime_score: Decimal = Decimal(0)
    orderable_cash: Decimal = Decimal(0)
    kill_switch: bool = False
    policy_matches: bool = True
    recovery_confirmed: bool = False
    long_trend_broken: bool = False


@dataclass(frozen=True)
class Decision:
    action: str
    reason: str
    quantity: int = 0
    unit_intent: Decimal = Decimal(0)
    sell_kind: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
